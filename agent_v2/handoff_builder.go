package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// HandoffSignalKey 是**框架内部**路由用的 message annotation key——[HandoffAgent] 的
// stepFn 裁决出目标后写它，本包的条件边读它决定下一跳。
//
// ⚠️ **业务不要写它，写了也不作数**：stepFn 在裁决前会把入站产出上的这个字段清掉。
// 它曾经是一条业务可用的转交通道，而那条路绕过全部三道校验（有没有被授予 / 目标合不
// 合法 / 这一层有没有人接）——实测一个连 `WithHandoffTransfer()` 都没配的候选，一行
// 就把对话转走了，err 全程 nil。
//
// 业务要转交：LLM 驱动的成员用内置的 transfer_to_agent 工具（配 [WithHandoffTransfer]），
// 自己实现的 Agent 用 [HandoffTo]。
//
// 值是下一跳的候选名；空字符串 / 无此 key = 已完成、不再转交。
const HandoffSignalKey = "handoff_target"

// handoffHopsCtxKey 是 ctx-value key 让 agent_v2 stepFn 通过 GetHandoffHops(ctx) 读
// 真 hops 计数（持久化在 cp，跨 Resume 正确恢复）—— 避免业务 stepFn 用闭包局部
// 计数器跨 Resume 重置漂移（R30 视角 B HIGH-2 fix）。
type handoffHopsCtxKey struct{}

// GetHandoffHops 从 ctx 读 HandoffAgent stepFn 的当前 hop 计数（0-based）。
//
// **业务用法**：HandoffAgent 嵌套 stepFn 内调用本 helper 拿真 hops 值——可作
// childNS 序号防多 hop 同 active agent NS 互覆 + compose_v2.ResumeContext 跨 hop 串台
// （R30 视角 B HIGH-1 / HIGH-2 fix）。
//
// 拿不到（业务直接调 stepFn 不通过框架）返 0。
func GetHandoffHops(ctx context.Context) int {
	if v, ok := ctx.Value(handoffHopsCtxKey{}).(int); ok {
		return v
	}
	return 0
}

// HandoffStepFn 是 HandoffAgent 编排的单步节点函数。
//
// 入参：
//   - ctx：透传 framework ctx
//   - input：本步 input message（每跳恒为原始用户输入，前序处理经共享 session 历史看到）
//   - activeName：本步应执行的 agent 名（caller 内 lookup 实际 sub-agent）
//
// 输出：本步产出 message。message.Annotations[HandoffSignalKey] 决定下一步转交目标。
type HandoffStepFn func(ctx context.Context, input *schema.Message, activeName string) (*schema.Message, error)

// HandoffAgentBuilder 是 HandoffAgent 编排：N 个候选 agent，按 handoff signal 转交控制权。
//
// 拓扑：
//
//	compose_v2.StartNode → step → ConditionalEdge → [End (done/exhausted) | step]
//
// 退出条件：
//  1. 本步 output.Annotations[HandoffSignalKey] 为空 → done
//  2. hops >= MaxHops → exhausted（防 ping-pong 死循环）
//
// **vs RoutingAgent**：Routing 一次性根据 input 选 1 分支跑完，Handoff 是
// 多轮转交（每轮 active agent 可决定下轮谁接手）。
type HandoffAgentBuilder struct {
	name        string
	candidates  []string // agent name 集合（用于校验 handoff target 合法性）
	stepFn      HandoffStepFn
	initialName string
	maxHops     int
	hooks       hook.Hooks
}

// 内部 channels (HandoffAgent 编排专用)
//
// 注：与 handoff.go::HandoffBuilder 的 string 版 channel namespace 区分（前缀
// __handoff_agent_）—— 同 process 跑两套不会冲突。
var (
	handoffAgentActiveCh    = compose_v2.LastValue[string]("__handoff_agent_active__")          // 本轮应执行 agent name
	handoffAgentInputCh     = compose_v2.LastValue[*schema.Message]("__handoff_agent_input__")  // 本轮 input
	handoffAgentOutputCh    = compose_v2.LastValue[*schema.Message]("__handoff_agent_output__") // 本轮 active 产出（HandoffAgentFinalMessage 读；模型 B 已不喂下一轮 input）
	handoffAgentHopsCh      = compose_v2.Sum("__handoff_agent_hops__")                          // 累计 handoff 次数
	handoffAgentDoneCh      = compose_v2.OnceFlag("__handoff_agent_done__")                     // 正常结束信号
	handoffAgentExhaustedCh = compose_v2.OnceFlag("__handoff_agent_exhausted__")                // hops 耗尽
)

// GetHandoffAgentHops 从 compose_v2.ChannelManager 读累计 handoff hop 数 (Sub-5 audit Round 2 视角 G fix).
func GetHandoffAgentHops(cm *compose_v2.ChannelManager) int32 {
	if cm == nil {
		return 0
	}
	hops, _ := compose_v2.Get(cm.Channels(), handoffAgentHopsCh)
	return int32(hops)
}

// defaultHandoffMaxHops 是 handoff 单 Run 转交次数默认上限——NewHandoffAgentBuilder 缺省值，
// 也是 HandoffAgent wrapper 判 State().Exhausted 的有效上限（h.maxHops 未显式设时对齐此值，
// 否则 State() 与 builder 实际终止判定脱节）。
const defaultHandoffMaxHops = 10

// NewHandoffAgentBuilder 创建 HandoffAgentBuilder。默认 maxHops=defaultHandoffMaxHops。
func NewHandoffAgentBuilder() *HandoffAgentBuilder {
	return &HandoffAgentBuilder{maxHops: defaultHandoffMaxHops}
}

// WithName 设名。
func (b *HandoffAgentBuilder) WithName(name string) *HandoffAgentBuilder {
	b.name = name
	return b
}

// AddCandidate 注册候选 agent name（合法 handoff target 集合）。
func (b *HandoffAgentBuilder) AddCandidate(name string) *HandoffAgentBuilder {
	b.candidates = append(b.candidates, name)
	return b
}

// WithStepFn 设 step 函数（必填）。
func (b *HandoffAgentBuilder) WithStepFn(fn HandoffStepFn) *HandoffAgentBuilder {
	b.stepFn = fn
	return b
}

// WithInitial 设首轮执行 agent name（必填）。
func (b *HandoffAgentBuilder) WithInitial(name string) *HandoffAgentBuilder {
	b.initialName = name
	return b
}

// WithMaxHops 设最大转交次数（默认 10）。
func (b *HandoffAgentBuilder) WithMaxHops(n int) *HandoffAgentBuilder {
	if n > 0 {
		b.maxHops = n
	}
	return b
}

// WithAgentHooks 设业务 hook。
func (b *HandoffAgentBuilder) WithAgentHooks(h hook.Hooks) *HandoffAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
func (b *HandoffAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if b.stepFn == nil {
		return nil, fmt.Errorf("handoff_agent: step fn required (call WithStepFn)")
	}
	if len(b.candidates) == 0 {
		return nil, fmt.Errorf("handoff_agent: no candidates; call AddCandidate")
	}
	if b.initialName == "" {
		return nil, fmt.Errorf("handoff_agent: initial name required (call WithInitial)")
	}
	candSet := make(map[string]bool, len(b.candidates))
	for _, c := range b.candidates {
		candSet[c] = true
	}
	if !candSet[b.initialName] {
		return nil, fmt.Errorf("handoff_agent: initial name %q not in candidates", b.initialName)
	}

	hooks := b.hooks
	// nil 防护: typed nil hook.Hooks 走 isFrameworkNil 退化 NoOpHooks——业务直接
	// NewHandoffAgentBuilder().AddCandidate(...).Compile() 不调 WithAgentHooks 时, stepFn
	// 业务 error 路径调 `hooks.OnError` 直接 nil deref panic.
	// 与 7 编排统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "handoff_agent"
	}
	maxHops := b.maxHops

	bld := compose_v2.NewBuilder().WithName(name).WithMaxVisit(maxHops + 5)
	compose_v2.WithChannel(bld, handoffAgentActiveCh)
	compose_v2.WithChannel(bld, handoffAgentInputCh)
	compose_v2.WithChannel(bld, handoffAgentOutputCh)
	compose_v2.WithChannel(bld, handoffAgentHopsCh)
	compose_v2.WithChannel(bld, handoffAgentDoneCh)
	compose_v2.WithChannel(bld, handoffAgentExhaustedCh)

	stepFn := b.stepFn
	stepNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		active, _ := compose_v2.Get(chs, handoffAgentActiveCh)
		input, _ := compose_v2.Get(chs, handoffAgentInputCh)
		hops, _ := compose_v2.Get(chs, handoffAgentHopsCh)

		// 第 1 轮 active 没设——兜底初始化。**模型 B**：agent_v2 compose_v2.Run 入口通常已
		// compose_v2.WithInitialWrites 写 active=持久值/initial（doc/v2/handoff.md §7 入口路由）。
		if active == "" {
			active = b.initialName
		}
		// **模型 B**：每跳 input 恒为原始用户输入——转交后新 active 靠共享 session 历史
		// 看前序处理（doc/v2/handoff.md §8 input 语义）。模型 A 的"上一跳 output 接力"已删。
		if input == nil {
			return nil, fmt.Errorf("handoff_agent: 拿不到 input")
		}

		info := &hook.AgentInfo{Name: name, Type: "handoff", Round: int32(hops)}
		// 8-method 协议：OnAgentStart 整个调用 1 次, 由 agent_v2.HandoffAgent.Run/Resume
		// top-level fire——本节点不再每跳触发 hook。

		// **R30 视角B HIGH-1/2 fix**：push 真 hops 到 ctx 让业务 stepFn 通过
		// GetHandoffHops 读真值——避免业务闭包计数器跨 Resume 重置导致 NS 错位 +
		// State().Hops 失真。
		stepCtx := context.WithValue(ctx, handoffHopsCtxKey{}, hops)
		out, err := stepFn(stepCtx, input, active)
		if err != nil {
			// **R25 audit MED-4 fix**：ctx.Err() 同 compose_v2.InterruptError 同属控制流
			if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
				hooks.OnError(ctx, info, &schema.CommError{Code: "step_error", Message: err.Error()})
			}
			return nil, fmt.Errorf("handoff step (active=%q hops=%d): %w", active, hops, err)
		}
		if out == nil {
			out = schema.AssistantMessage("")
		}

		// 先解析 handoff signal（在写 handoffAgentOutputCh 前——下面写清除版防 final 泄漏）
		var nextTarget string
		if out.Annotations != nil {
			if t, ok := out.Annotations[HandoffSignalKey].(string); ok {
				nextTarget = t
			}
		}

		// 写 final 存储时清除 handoff_target annotation 防泄漏外层（audit fix，同 LoopAgent
		// break_loop 根因）：HandoffAgent final（handoffAgentOutputCh compose_v2.LastValue）被外层嵌套
		// 编排（Handoff/Loop/Routing）当 lastOut 时不该带本编排内部控制信号——尤其 maxHops
		// 耗尽 / 非法目标 done 时 out 仍带 agent_v2 写回的 handoff_target。nextTarget 已读出
		// 用于路由判断，清除不影响路由。
		outForFinal := out
		if nextTarget != "" {
			outForFinal = clearHandoffTargetAnnotation(out)
		}
		writes := []compose_v2.ChannelWrite{compose_v2.Write(handoffAgentOutputCh, outForFinal)}

		nextHops := hops + 1
		if nextTarget == "" {
			// 不转交 → done
			writes = append(writes, compose_v2.Write(handoffAgentDoneCh, true))
			return writes, nil
		}
		if !candSet[nextTarget] {
			// 转交目标非法 → 视为完成（不强制 fail，业务可能误填）
			writes = append(writes, compose_v2.Write(handoffAgentDoneCh, true))
			return writes, nil
		}
		if nextHops >= maxHops {
			// 跳数耗尽 → exhausted
			writes = append(writes, compose_v2.Write(handoffAgentExhaustedCh, true))
			return writes, nil
		}
		writes = append(writes,
			compose_v2.Write(handoffAgentActiveCh, nextTarget),
			compose_v2.Write(handoffAgentHopsCh, 1),
		)
		return writes, nil
	}
	bld.AddNode("step", stepNode)
	bld.AddEdge(compose_v2.StartNode, "step")

	// ConditionalEdge: done / exhausted → End；否则继续 step
	exitChoice := func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		done, _ := compose_v2.Get(chs, handoffAgentDoneCh)
		exhausted, _ := compose_v2.Get(chs, handoffAgentExhaustedCh)
		if done || exhausted {
			return compose_v2.EndNode, nil
		}
		return "step", nil
	}
	bld.AddConditionalEdge("step", exitChoice)

	return bld.Compile()
}

// HandoffAgentInputChannel 暴露原始 input channel 让 caller compose_v2.WithInitialWrites 写入。
func HandoffAgentInputChannel() *compose_v2.Channel[*schema.Message] {
	return handoffAgentInputCh
}

// HandoffAgentActiveChannel 暴露 active channel 让 caller（agent_v2 wrapper）通过
// compose_v2.WithInitialWrites 写入起始 active agent——**模型 B 入口路由**：agent_v2 compose_v2.Run 入口
// 读 store 持久 active（无则 initial）写入此 channel，让图从该 agent 起跑（而非恒从
// initial）。见 doc/v2/handoff.md §7。
func HandoffAgentActiveChannel() *compose_v2.Channel[string] {
	return handoffAgentActiveCh
}

// HandoffAgentFinalMessage 从 cm 拿最后一轮 active agent 的产出。
func HandoffAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, handoffAgentOutputCh)
	return out
}

// clearHandoffTargetAnnotation 返回删掉 HandoffSignalKey annotation 的 message 副本（shallow
// copy + Annotations 一层深拷贝，不污染原 message）——防 HandoffAgent final 的 handoff_target
// 泄漏到外层嵌套编排误读（audit fix，同 LoopAgent clearBreakLoopAnnotation 模式）。
func clearHandoffTargetAnnotation(msg *schema.Message) *schema.Message {
	cleared := *msg
	ann := make(map[string]any, len(msg.Annotations))
	for k, v := range msg.Annotations {
		if k != HandoffSignalKey {
			ann[k] = v
		}
	}
	cleared.Annotations = ann
	return &cleared
}
