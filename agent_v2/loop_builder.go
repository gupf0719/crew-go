package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// BreakLoopAnnotation 是 body output 主动 break 循环的 annotation key——LoopAgent
// exitCheck 读到 true 即退出（优先级高于 ExitCondition / maxIter）。用常量避免魔法
// 字符串（CLAUDE.md 规约）。命中后 exitCheck 会清除它防泄漏外层嵌套 LoopAgent。
const BreakLoopAnnotation = "break_loop"

// LoopBodyFn 是 LoopAgent 编排的 body 函数——每轮跑一次。
//
// 入参：
//   - input：本轮 input。第 1 轮是原始 input；后续轮是上一轮 output（或框架包装版）
//   - round：本轮序号（0-based）
//
// **idempotent 约定**：业务 NodeFn 必须可重入。
type LoopBodyFn func(ctx context.Context, input *schema.Message, round int) (*schema.Message, error)

// LoopAgentExitCondition 决定 LoopAgent 循环是否应退出。
//
// 返 (true, nil) → 循环结束；(false, nil) → 继续；(_, err) → 中止循环 propagate err。
//
// **HITL 支持**：业务可抛 `compose_v2.InterruptError` 做"循环退出前用户审批"等
// HITL gate（如 N 轮搜索后让用户决定是否继续）。框架按 graph 内 SaveNS 协议自动
// 存 cp + Resume 重跑 ExitCondition 取 ResumeContext 答案续跑。
//
// 入参：上一轮 output + 已完成轮数（1-based after first round）
// 输出：true=退出循环，false=继续下一轮
//
// 多个退出信号优先级（loop body 内部判断）：
//  1. output.Annotations["break_loop"] = true（业务主动 break）
//  2. ExitCondition 返 true
//  3. round >= maxIterations
type LoopAgentExitCondition func(ctx context.Context, lastOutput *schema.Message, round int) (bool, error)

// LoopAgentBuilder 是 LoopAgent 编排：单 body 函数循环跑。
//
// 拓扑：
//
//	compose_v2.StartNode → body → ConditionalEdge → [compose_v2.EndNode (退出) | body (继续)]
//
// 退出条件三层：BreakLoop annotation → ExitCondition → maxIterations
type LoopAgentBuilder struct {
	name          string
	body          LoopBodyFn
	exitCondition LoopAgentExitCondition
	maxIterations int
	hooks         hook.Hooks
}

// 内部 channels (LoopAgent 编排专用)
var (
	loopAgentInputCh   = compose_v2.LastValue[*schema.Message]("__loop_agent_input__")    // 当前轮 input（原始 / 上一轮 output）
	loopAgentLastOutCh = compose_v2.LastValue[*schema.Message]("__loop_agent_last_out__") // 上一轮 output
	loopAgentRoundCh   = compose_v2.Sum("__loop_agent_round__")                           // 已完成轮数
	loopAgentExitedCh  = compose_v2.OnceFlag("__loop_agent_exited__")                     // 退出信号（一旦 set 不可撤）
)

// GetLoopAgentRound 从 compose_v2.ChannelManager 读已完成轮数 (Sub-5 audit Round 2 视角 G fix).
func GetLoopAgentRound(cm *compose_v2.ChannelManager) int32 {
	if cm == nil {
		return 0
	}
	round, _ := compose_v2.Get(cm.Channels(), loopAgentRoundCh)
	return int32(round)
}

// NewLoopAgentBuilder 创建 LoopAgentBuilder。
func NewLoopAgentBuilder() *LoopAgentBuilder {
	return &LoopAgentBuilder{maxIterations: 10}
}

// WithName 设名。
func (b *LoopAgentBuilder) WithName(name string) *LoopAgentBuilder {
	b.name = name
	return b
}

// WithBody 设 body 函数（必填）。
func (b *LoopAgentBuilder) WithBody(fn LoopBodyFn) *LoopAgentBuilder {
	b.body = fn
	return b
}

// WithExitCondition 设退出条件回调。
func (b *LoopAgentBuilder) WithExitCondition(cond LoopAgentExitCondition) *LoopAgentBuilder {
	b.exitCondition = cond
	return b
}

// WithMaxIterations 设最大轮数（默认 10）。
func (b *LoopAgentBuilder) WithMaxIterations(n int) *LoopAgentBuilder {
	if n > 0 {
		b.maxIterations = n
	}
	return b
}

// WithAgentHooks 设业务 hook。
func (b *LoopAgentBuilder) WithAgentHooks(h hook.Hooks) *LoopAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
func (b *LoopAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if b.body == nil {
		return nil, fmt.Errorf("loop_agent: body required (call WithBody)")
	}
	hooks := b.hooks
	// nil 防护: typed nil hook.Hooks 走 isFrameworkNil 退化 NoOpHooks——业务直接
	// NewLoopAgentBuilder().WithBody(...).Compile() 不调 WithAgentHooks 时, body 错误路径
	// `hooks.OnError(loop_body_error)` 或 exitCondition 错误路径
	// `hooks.OnError(loop_exit_cond_error)` 调用直接 nil deref panic.
	// 与 7 编排统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "loop_agent"
	}
	maxIter := b.maxIterations
	exitCond := b.exitCondition

	bld := compose_v2.NewBuilder().WithName(name).WithMaxVisit(maxIter + 5)
	compose_v2.WithChannel(bld, loopAgentInputCh)
	compose_v2.WithChannel(bld, loopAgentLastOutCh)
	compose_v2.WithChannel(bld, loopAgentRoundCh)
	compose_v2.WithChannel(bld, loopAgentExitedCh)

	// **ISSUE-77 修**：把 exitCondition 从 body 节点抽到独立 exitCheck 节点——之前
	// body 节点跑 b.body 后**同步**调 exitCondition，exitCondition 抛 InterruptError 时
	// graph_loop SaveNS cp 含 body 节点 frontier → Resume 重跑 body 节点 → b.body 又
	// 跑一次（重 LLM 调用费 + tool call + OutputEvent 重写 sess.events）。
	//
	// 节点拆分后：
	//   - body 节点：跑 b.body + 写 lastOut + round + hooks
	//   - exitCheck 节点：读 lastOut → 调 exitCondition → 写 exited
	// exitCondition 抛 Interrupt 时 cp 含 exitCheck 节点 frontier，Resume 只重跑
	// exitCheck，body 已 commit 的 lastOut channel 在 cm.Restore 时恢复——b.body
	// 不重跑（关键 fix）。与 ISSUE-75 postamble 节点化同模式。
	bodyFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		input, _ := compose_v2.Get(chs, loopAgentInputCh)
		lastOut, _ := compose_v2.Get(chs, loopAgentLastOutCh)
		round, _ := compose_v2.Get(chs, loopAgentRoundCh)

		// 第 1 轮（round=0）用 input；后续用 lastOut
		actualInput := input
		if round > 0 && lastOut != nil {
			actualInput = lastOut
		}

		info := &hook.AgentInfo{Name: name, Type: "loop", Round: int32(round)}
		// 8-method 协议：OnAgentStart 整个调用 1 次, 由 agent_v2.LoopAgent.Run/Resume
		// top-level fire——body 节点不再每轮触发 hook。
		out, err := b.body(ctx, actualInput, round)
		if err != nil {
			// **R25 audit MED-4 fix**：ctx.Err() 同 InterruptError 同属控制流，对称 R24 finalize 守卫
			if _, isInt := IsInterrupt(err); !isInt && ctx.Err() == nil {
				hooks.OnError(ctx, info, &schema.CommError{Code: "loop_body_error", Message: err.Error()})
			}
			return nil, fmt.Errorf("loop body round %d: %w", round, err)
		}
		if out == nil {
			out = schema.AssistantMessage("")
		}

		// body 节点只写 lastOut + 增 round，不跑 exitCondition——由 exitCheck 节点跑
		return []compose_v2.ChannelWrite{
			compose_v2.Write(loopAgentLastOutCh, out),
			compose_v2.Write(loopAgentRoundCh, 1),
		}, nil
	}
	bld.AddNode("body", bodyFn)
	bld.AddEdge(compose_v2.StartNode, "body")

	// **ISSUE-77 修**：exitCheck 节点 — body 完成后跑退出条件检查（break_loop /
	// exitCondition / maxIter）。exitCondition 抛 InterruptError 时 graph_loop
	// SaveNS cp 含 exitCheck 节点 frontier；Resume cm.Restore 恢复 lastOut → 重跑
	// exitCheck → exitCondition 拿答案续跑。body 不重跑（lastOut channel 已有值）。
	exitCheckFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		out, _ := compose_v2.Get(chs, loopAgentLastOutCh)
		round, _ := compose_v2.Get(chs, loopAgentRoundCh)
		if out == nil {
			out = schema.AssistantMessage("")
		}

		// nextRound 是已完成的 round 数（body 写 +1 让 round 反映已完成轮数）；
		// exitCondition 入参用 nextRound 表示"已完成 N 轮"语义对齐原行为。
		nextRound := round

		shouldExit := false
		var clearedOut *schema.Message // break_loop 命中时的清除版，防泄漏外层嵌套 LoopAgent
		// 优先级 1: break_loop annotation
		if out.Annotations != nil {
			if v, ok := out.Annotations[BreakLoopAnnotation].(bool); ok && v {
				shouldExit = true
				// audit fix: 清除 break_loop 防泄漏——内层 final 带 break_loop 会被外层
				// LoopAgent 当 lastOut 误触发外层提前退（break_loop 信号消费即清除，
				// 标记外层忽略，annotation 载体下清除 key 是最自然的实现）。
				clearedOut = clearBreakLoopAnnotation(out)
			}
		}
		// 优先级 2: exitCondition 回调
		if !shouldExit && exitCond != nil {
			// **R30 视角A LOW-1 fix + ISSUE-77 R34 audit HIGH-1 fix**：
			// 与 actor / eval 路径对称 push round_<N> NS 让业务在 exitCondition 内 spawn
			// sub-agent + ask_user 跨多轮 InterruptID 不串台。**关键 suffix `_check`**:
			// LoopAgent body 节点 (agent_v2/loop_agent.go::bodyAgent.Run) 也 push
			// `round_<N>` NS——若 exitCheck 用同 `round_<N>` 与 body 同 NS 碰撞,业务在
			// exitCondition spawn 的 sub-agent 与 body 内 sub-agent 内 ask_user
			// InterruptID 串台。加 `_check` suffix 物理隔离两 NS。
			//
			// **ISSUE-77 修后**：exitCheck 是独立节点，抛 Interrupt 时 cp 含 exitCheck
			// frontier；Resume 重跑 exitCheck，body 已 commit 的 lastOut 不动 → b.body 不重跑。
			//
			// 退出判断是内部决策（同 Reflector / EvalFunc，见 decisionCtx）：它里面请教的 agent 不进会话历史、
			// 不取用户的插话。
			exitCtx := decisionCtx(compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), fmt.Sprintf("round_%d_check", round-1))))
			exited, condErr := exitCond(exitCtx, out, nextRound)
			if condErr != nil {
				// InterruptError / ctx.Err / 业务错统一 propagate——graph_loop defer
				// SaveNS 自动存 cp（含 exitCheck frontier），Resume 重跑本节点重 call
				// exitCond 拿答案续跑。控制流 err 不上报 OnError。
				info := &hook.AgentInfo{Name: name, Type: "loop", Round: int32(round - 1)}
				if _, isInt := IsInterrupt(condErr); !isInt && ctx.Err() == nil {
					hooks.OnError(ctx, info, &schema.CommError{Code: "loop_exit_cond_error", Message: condErr.Error()})
				}
				return nil, fmt.Errorf("loop exit condition round %d: %w", nextRound, condErr)
			}
			if exited {
				shouldExit = true
			}
		}
		// 优先级 3: maxIter
		if !shouldExit && nextRound >= maxIter {
			shouldExit = true
		}

		var writes []compose_v2.ChannelWrite
		if shouldExit {
			writes = append(writes, compose_v2.Write(loopAgentExitedCh, true))
			// 仅 break_loop 命中需清除（其他退出路径 out 不带 break_loop）：写回清除版
			// lastOut 让 LoopAgentFinalMessage 返回的 final 不带 break_loop，不泄漏外层。
			if clearedOut != nil {
				writes = append(writes, compose_v2.Write(loopAgentLastOutCh, clearedOut))
			}
		}
		return writes, nil
	}
	bld.AddNode("exitCheck", exitCheckFn)
	bld.AddEdge("body", "exitCheck")

	// ConditionalEdge: exited=true → End；否则继续 body
	exitChoice := func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		exited, _ := compose_v2.Get(chs, loopAgentExitedCh)
		if exited {
			return compose_v2.EndNode, nil
		}
		return "body", nil
	}
	bld.AddConditionalEdge("exitCheck", exitChoice)

	return bld.Compile()
}

// LoopAgentInputChannel 暴露原始 input channel 让 caller WithInitialWrites 写入。
func LoopAgentInputChannel() *compose_v2.Channel[*schema.Message] {
	return loopAgentInputCh
}

// LoopAgentFinalMessage 从 cm 拿最后一轮 output。
func LoopAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, loopAgentLastOutCh)
	return out
}

// clearBreakLoopAnnotation 返回删掉 BreakLoopAnnotation key 的 message 副本（shallow copy
// + Annotations 一层深拷贝，不污染原 message）——防内层 LoopAgent 的 break_loop 泄漏到外层
// 嵌套 LoopAgent 误触发外层提前退出（audit fix，break_loop 信号只作用于本层循环）。
func clearBreakLoopAnnotation(msg *schema.Message) *schema.Message {
	cleared := *msg
	ann := make(map[string]any, len(msg.Annotations))
	for k, v := range msg.Annotations {
		if k != BreakLoopAnnotation {
			ann[k] = v
		}
	}
	cleared.Annotations = ann
	return &cleared
}
