package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// RoutingAgentRouter 是 RoutingAgent 编排的路由决策函数：根据原始 input 决定
// 路由到哪个分支。
//
// **在 classify node 内执行**（不在 ConditionalEdge）——决策属「干活」归 node，cond 只
// 纯读结果。因此 Router **可以做 HITL**：在里面调 compose_v2.Ask 提问，它在节点作用域
// 内，ID 由执行位置派生、跨 Resume 稳定。直接返回自己构造的 compose_v2.InterruptError
// 也行，但必须自填 InterruptID——空 ID 会被引擎当场拒成 compose_v2.ErrRunAborted。
//
// 入参：原始 input message
// 输出：目标分支 name（必须是 AddBranch 注册过的）+ error
//   - compose_v2.NodeID 返空字符串等价路由到 End；error nil
//   - error 非 nil 时 compose_v2.NodeID 应返空——error 由 classify node 直接 propagate
//   - error 为 compose_v2.InterruptError / ctx.Err() 是 control flow 必须 propagate（HITL）
//
// **R24 audit HIGH-2 fix**：原签名 `func(ctx, *Message) compose_v2.NodeID` 不返 error——
// classifier sub-agent.Run 抛 compose_v2.InterruptError 时无法向上 propagate，silent 走
// fallback 装作"路由失败"，HITL 链断。改返 error 让所有控制流入口对称 propagate。
type RoutingAgentRouter func(ctx context.Context, input *schema.Message) (compose_v2.NodeID, error)

// RoutingBranchFn 是 RoutingAgent 编排的单分支节点函数。
//
// 入参：原始 input（与 router 看到的同一份）
// 输出：本分支产出 final message
//
// **节点 idempotent 约定**（同 BasicAgentModelFn）：业务 NodeFn 必须可重入。
type RoutingBranchFn func(ctx context.Context, input *schema.Message) (*schema.Message, error)

type routingBranchDef struct {
	Name compose_v2.NodeID
	Fn   RoutingBranchFn
}

// RoutingAgentBuilder 是 RoutingAgent 编排：N 候选分支，Router 选一个跑。
//
// 拓扑：
//
//	compose_v2.StartNode → classify → ConditionalEdge → worker_<name> → compose_v2.EndNode
//
// **classify 节点跑路由决策**（调 Router → 校验 → 写 routingAgentTargetCh）；
// **ConditionalEdge 纯读 targetCh** 选目标——决策在 node、cond 只读结果。
// input 由 caller compose_v2.WithInitialWrites 写到 routingAgentInputCh。
// OnAgentStart 整个调用 1 次, 由 agent_v2.RoutingAgent.Run/Resume top-level fire
// （不在 classify 节点）；单 worker 执行后直接收尾。
//
// **为什么决策在 node**：cond 是纯路由判断、不能中断（cond 抛 InterruptError 会把
// completedFrontier 的成功成员拉回重跑，违反 PA-H1「成功成员不重跑」）。Router 可能是
// classifier sub-agent（跑 LLM + ask_user HITL）属「干活」，必须在 node —— nodeID +
// visit 协议稳定，走标准 node interrupt（稳定 ID + cp 存档 + Resume 续跑）。
//
// **vs Sequential / Parallel**：Sequential 串行链，Parallel 全跑合并，
// Routing 选一条路径——典型分流场景（按 query 类型派发到不同专家 agent）。
type RoutingAgentBuilder struct {
	name     string
	branches []routingBranchDef
	router   RoutingAgentRouter
	hooks    hook.Hooks
}

// 内部 channels (RoutingAgent 编排专用)
var (
	routingAgentInputCh  = compose_v2.LastValue[*schema.Message]("__routing_agent_input__")  // 原始 input（worker 读）
	routingAgentResultCh = compose_v2.LastValue[*schema.Message]("__routing_agent_result__") // 选中 worker 产出
	// routingAgentTargetCh 存 classify 节点算出的路由目标（worker node ID 或 EndNode），
	// 下方 ConditionalEdge 纯读它决定去向——路由**决策**在 node、cond 只**读结果**。
	//
	// 用 string 而非 compose_v2.NodeID：复用 basic_agent.go init 已 RegisterValueType 的
	// "string"，避免为本 channel 单独注册类型（worker 中断 Resume 时 cp 序列化本 channel
	// 需类型已注册）。
	routingAgentTargetCh = compose_v2.LastValue[string]("__routing_agent_target__")
)

// NewRoutingAgentBuilder 创建 RoutingAgentBuilder。
func NewRoutingAgentBuilder() *RoutingAgentBuilder {
	return &RoutingAgentBuilder{}
}

// WithName 设 RoutingAgent 名。
func (b *RoutingAgentBuilder) WithName(name string) *RoutingAgentBuilder {
	b.name = name
	return b
}

// AddBranch 追加一个路由分支。name 是 Router 返的目标名。
func (b *RoutingAgentBuilder) AddBranch(name string, fn RoutingBranchFn) *RoutingAgentBuilder {
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("RoutingAgentBuilder.AddBranch: name %q invalid for ns: %v", name, err))
	}
	if fn == nil {
		panic(fmt.Sprintf("RoutingAgentBuilder.AddBranch: name %q fn must not be nil", name))
	}
	// 路由目标 "__end__" 被当成显式结束（见 classify 节点），同名分支永远跑不到且不报错。
	// "__start__" 不受影响：分支节点带 worker_ 前缀，不会撞名。
	if name == string(compose_v2.EndNode) {
		panic(fmt.Sprintf("RoutingAgentBuilder.AddBranch: name %q 会被当成图的终点，请换名", name))
	}
	// 四编排 review F7: dup name 查重（镜像 ParallelAgentBuilder.AddBranch）——
	// dup → AddNode("worker_"+name) map 静默覆盖，前一个分支无声丢失。
	for _, existing := range b.branches {
		if existing.Name == compose_v2.NodeID(name) {
			panic(fmt.Sprintf("RoutingAgentBuilder.AddBranch: duplicate branch name %q (会让前一个分支被 worker_xxx map 静默覆盖)", name))
		}
	}
	b.branches = append(b.branches, routingBranchDef{Name: compose_v2.NodeID(name), Fn: fn})
	return b
}

// WithRouter 设 Router 决策函数。
func (b *RoutingAgentBuilder) WithRouter(fn RoutingAgentRouter) *RoutingAgentBuilder {
	b.router = fn
	return b
}

// WithAgentHooks 设业务 hook。
func (b *RoutingAgentBuilder) WithAgentHooks(h hook.Hooks) *RoutingAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
func (b *RoutingAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if len(b.branches) == 0 {
		return nil, fmt.Errorf("routing_agent: no branches; call AddBranch")
	}
	if b.router == nil {
		return nil, fmt.Errorf("routing_agent: Router required (use WithRouter)")
	}

	hooks := b.hooks
	// nil 防护: typed nil hook.Hooks 走 isFrameworkNil 退化 NoOpHooks——业务直接
	// NewRoutingAgentBuilder().AddBranch(...).Compile() 不调 WithAgentHooks 时, 选中 worker
	// 业务 error 路径调 `hooks.OnError` 直接 nil deref panic.
	// 与 7 编排统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "routing_agent"
	}

	bld := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(bld, routingAgentInputCh)
	compose_v2.WithChannel(bld, routingAgentResultCh)
	compose_v2.WithChannel(bld, routingAgentTargetCh)

	// 用 allowedIDs map 在 ConditionalEdge 校验 router 返回值
	allowed := make(map[compose_v2.NodeID]bool, len(b.branches))
	for _, br := range b.branches {
		allowed[compose_v2.NodeID("worker_"+string(br.Name))] = true
	}

	// classify 节点：**路由决策在这里跑**——调 Router（业务显式 Router / classifier
	// sub-agent 包装成的 Router）拿目标，校验后写 routingAgentTargetCh；下方
	// ConditionalEdge 只纯读该 channel 决定去向。
	//
	// **为什么决策在 node 而非 cond**（引擎深审 F-A 根治）：cond 是纯路由判断、**不能中断**
	// ——cond 抛 InterruptError 会把 completedFrontier 里的成功成员拉回 CurrentFrontier
	// 重跑，违反 PA-H1「成功成员不重跑」契约（非幂等副作用双跑）。而 classifier 跑 LLM +
	// 可能 ask_user HITL 属「干活」，必须在 node：nodeID + visit 协议稳定，走
	// runNodeWithHooks 标准 interrupt 路径（稳定 ID + cp 存档 + Resume 重跑本节点续跑）。
	//
	// 8-method 协议：OnAgentStart 整个调用 1 次, 由 agent_v2.RoutingAgent.Run/Resume
	// top-level fire——classify 节点不触发业务级 hook（只触发引擎层 OnNodeStart/End）。
	router := b.router
	classifyFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		input, _ := compose_v2.Get(chs, routingAgentInputCh)
		target, err := router(ctx, input)
		if err != nil {
			// 控制流（compose_v2.InterruptError / ctx.Err）与业务硬错都 propagate：
			//   - InterruptError → runNodeWithHooks 走标准 node interrupt
			//   - 其它 → node 失败，Run 返错
			// 注：升级链（decisionRouter）已把决策方和 classifier 的**业务错**就地升级到下一档或
			// 降级到兜底，故到这里的 err 只会是 compose_v2.MustPropagate 判为必传的（中断、喊停、
			// 软取消、陈旧写回、终局失败——含分类器里并发几路都定了路由的 SignalConflictError——、
			// 坏档）、走完升级链仍没有可用目标又无处升级（RouteUndecidedError，含 classifier 失败又
			// 没有兜底），或业务显式 Router 自己返的硬错。
			return nil, err
		}
		routeTarget := compose_v2.EndNode
		if target != "" && target != compose_v2.EndNode {
			workerID := compose_v2.NodeID("worker_" + string(target))
			if !allowed[workerID] {
				return nil, fmt.Errorf("routing_agent: router returned unregistered branch %q", target)
			}
			routeTarget = workerID
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(routingAgentTargetCh, string(routeTarget))}, nil
	}
	bld.AddNode("classify", classifyFn)
	bld.AddEdge(compose_v2.StartNode, "classify")

	// 注册 N 个 worker
	for _, br := range b.branches {
		br := br
		workerFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			input, _ := compose_v2.Get(chs, routingAgentInputCh)
			if input == nil {
				return nil, fmt.Errorf("routing_agent: branch %q: 没拿到 input", br.Name)
			}
			out, err := br.Fn(ctx, input)
			if err != nil {
				// **R25 audit MED-4 fix**：ctx.Err() 同 compose_v2.InterruptError 同属控制流
				if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
					info := &hook.AgentInfo{Name: name, Type: "routing", Round: 0}
					hooks.OnError(ctx, info, &schema.CommError{
						Code: "branch_error", Message: err.Error(),
					})
				}
				return nil, fmt.Errorf("branch %q: %w", br.Name, err)
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(routingAgentResultCh, out)}, nil
		}
		bld.AddNode(compose_v2.NodeID("worker_"+string(br.Name)), workerFn)
		bld.AddEdge(compose_v2.NodeID("worker_"+string(br.Name)), compose_v2.EndNode)
	}

	// classify → ConditionalEdge：**纯读** classify node 写的路由目标——不干活、不中断。
	//
	// 忽略 ctx：cond 是**框架生成**的纯判断（业务逻辑全在 classify node 的 Router 里），
	// 与 loop / handoff / reflect 的 exitChoice 同形态（都 `_ context.Context`）。
	// 决策已在 node 做完并校验，这里只把结果翻译成 NodeID。
	bld.AddConditionalEdge("classify", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		target, ok := compose_v2.Get(chs, routingAgentTargetCh)
		if !ok {
			// classify node 成功完成必写 targetCh——读不到是框架内部不变量违反
			return "", fmt.Errorf("routing_agent: classify 未写路由目标（内部不变量违反）")
		}
		return compose_v2.NodeID(target), nil
	})

	return bld.Compile()
}

// RoutingAgentInputChannel 暴露 routingAgentInputCh 让 caller compose_v2.WithInitialWrites 写入。
func RoutingAgentInputChannel() *compose_v2.Channel[*schema.Message] {
	return routingAgentInputCh
}

// RoutingAgentFinalMessage 从 cm 拿选中 worker 的产出。
func RoutingAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, routingAgentResultCh)
	return out
}
