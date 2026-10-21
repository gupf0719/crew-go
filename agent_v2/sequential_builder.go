package agent_v2

import (
	"context"
	"crew-go/compose_v2"
	"fmt"

	"crew-go/hook"
	"crew-go/schema"
)

// SequentialStepFn 是 SequentialAgent 编排的单步节点函数。
//
// 入参：
//   - ctx：透传 framework ctx（含 cper / sid / graphNS 等）
//   - input：本步 input message——step 0 是 caller 传入的原始 input；
//     step N (N≥1) 是 framework 自动拼装的 "原始目标 + 上一步产出" user message
//
// 输出：本步产出 assistant message，喂给下一步
//
// **节点函数 idempotent 约定**（同 BasicAgentModelFn）：业务 NodeFn 必须可重入。
// Resume 路径下被中断的步骤会重跑，业务勿在闭包持"已 send 过"等状态。
//
// 业务实现典型：
//   - 调子 agent.Run 拿 final message
//   - 自定义 LLM 调用 / 数据加工
//   - 工具 chain 串联
type SequentialStepFn func(ctx context.Context, input *schema.Message) (*schema.Message, error)

// sequentialStepDef 是 SequentialAgentBuilder 注册的单步节点定义（内部用，
// 与 sequential.go::sequentialStep 区分——后者是简单 graph helper）。
type sequentialStepDef struct {
	Name string
	Fn   SequentialStepFn
}

// SequentialAgentBuilder 是 SequentialAgent 编排：线性步骤链。
//
// 拓扑：
//
//	compose_v2.StartNode → step[0] → step[1] → ... → step[N-1] → compose_v2.EndNode
//
// 每步 input 拼装（与老 agent.SequentialAgent ISSUE-44 行为对齐）：
//   - step 0：caller 传入的原始 input（通过 compose_v2.WithInitialWrites 写入）
//   - step N (N≥1)：framework 自动拼装：
//     "原始目标：<orig.Content>\n\n上一步（<prev step name>）的产出：\n<prev.Content>"
//     —— 让下一步 LLM 看到实际需要加工的素材
//
// **vs PoC NewSequential（简单 helper）**：
//   - NewSequential 是 graph builder 语法糖（业务自管 compose_v2.NodeFunc / channels）
//   - NewSequentialAgentBuilder 是**编排层**（含 hooks 触发 + 内置 messages/lastOut/step
//     channels + step input 自动拼装），与 NewBasicAgentBuilder 对称
//
// hooks 集成（audit³ D1 + ISSUE-82 后协议）：
//   - OnAgentStart / OnAgentEnd：本 builder **不再触发**——agent-level hook 由
//     agent_v2.SequentialAgent.Run/Resume top-level (OnAgentStart) + defer (OnAgentEnd)
//     fire (跟 BasicAgent 同款单一所有权; OnAgentStart 整个调用 1 次, 不再每步).
//   - OnError：节点失败时（compose_v2.InterruptError 跳过——HITL 控制流不算错）
//   - OnModelStart/End / OnToolStart/End：Sequential 不直接管，节点内自管 LLM/tool
//     时业务自行调（如 step 内调子 agent 时透传 hooks）
type SequentialAgentBuilder struct {
	name  string
	steps []sequentialStepDef
	hooks hook.Hooks
}

// 内部 channels (SequentialAgent 编排专用)
//
// 与 BasicAgent 隔离命名（前缀 __seq_agent_）—— 让同 process 同 cm 跑不同
// 编排时 channel namespace 不冲突。
var (
	seqAgentMessagesCh   = compose_v2.Append[*schema.Message]("__seq_agent_messages__")    // 历史消息累积（原始 user 输入 + 每步 output）
	seqAgentLastOutputCh = compose_v2.LastValue[*schema.Message]("__seq_agent_last_out__") // 上一步产出（节点间传递媒介）
	seqAgentOriginalCh   = compose_v2.LastValue[*schema.Message]("__seq_agent_original__") // 原始 input（step 1+ 拼装用）
	seqAgentStepCh       = compose_v2.Sum("__seq_agent_step__")                            // 跟踪当前 step index（hook AgentInfo.Round）
)

// GetSequentialAgentStep 从 compose_v2.ChannelManager 读最后一步 step index — agent_v2 layer
// compose_v2.Run/Resume 完成后 update 顶层 hook.AgentInfo.Round 让 OnAgentEnd 拿到真实步数
// (Sub-5 audit Round 2 视角 G HIGH fix: 跟 BasicAgent 同款 pattern,业务接 obs.AgentHooks()
// 后 MetricAgentRounds Histogram 跨编排都有真值).
func GetSequentialAgentStep(cm *compose_v2.ChannelManager) int32 {
	if cm == nil {
		return 0
	}
	step, _ := compose_v2.Get(cm.Channels(), seqAgentStepCh)
	return int32(step)
}

// NewSequentialAgentBuilder 创建 SequentialAgentBuilder。
//
// 与 NewSequential（简单 graph helper）的命名区分：本 builder 包含编排层
// 行为（hooks + 内置 channels + step input 拼装），适合"封装多步 agent 流程"
// 业务用法。简单 graph 拼接用 NewSequential。
func NewSequentialAgentBuilder() *SequentialAgentBuilder {
	return &SequentialAgentBuilder{}
}

// WithName 设 SequentialAgent 名字（用于 hook AgentInfo + 日志 + cp 寻址）。
func (b *SequentialAgentBuilder) WithName(name string) *SequentialAgentBuilder {
	b.name = name
	return b
}

// AddStep 追加一个步骤节点。name 用作图节点 ID + cp ns + hook AgentInfo.
//
// **fail-fast**：name 含 ns 命名规范禁止字符（"/" "#" "@"）时立即 panic——与
// 老 agent.SequentialAgent.AddAgent 一致。
func (b *SequentialAgentBuilder) AddStep(name string, fn SequentialStepFn) *SequentialAgentBuilder {
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("SequentialAgentBuilder.AddStep: name %q invalid for ns: %v", name, err))
	}
	if fn == nil {
		panic(fmt.Sprintf("SequentialAgentBuilder.AddStep: name %q fn must not be nil", name))
	}
	b.steps = append(b.steps, sequentialStepDef{Name: name, Fn: fn})
	return b
}

// WithAgentHooks 设业务 hook 实例。
func (b *SequentialAgentBuilder) WithAgentHooks(h hook.Hooks) *SequentialAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
//
// 调用方传入 input 通过 compose_v2.WithInitialWrites(compose_v2.Write(SeqAgentOriginalChannel(), input))
// 写入 seqAgentOriginalCh。framework 自动从中读 step 0 input + step 1+ 拼装。
func (b *SequentialAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if len(b.steps) == 0 {
		return nil, fmt.Errorf("sequential_agent: no steps; call AddStep")
	}

	hooks := b.hooks
	// nil 防护: typed nil hook.Hooks 走 isFrameworkNil 退化 NoOpHooks——业务直接
	// NewSequentialAgentBuilder().AddStep(...).Compile() 不调 WithAgentHooks 时, stepFn 业务
	// error / nil output 路径调 `hooks.OnError` 直接 nil deref panic.
	// 与 ParallelAgent / BasicAgent 等 7 编排统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "sequential_agent"
	}

	bld := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(bld, seqAgentMessagesCh)
	compose_v2.WithChannel(bld, seqAgentLastOutputCh)
	compose_v2.WithChannel(bld, seqAgentOriginalCh)
	compose_v2.WithChannel(bld, seqAgentStepCh)

	for idx, n := range b.steps {
		idx, n := idx, n // capture loop vars
		stepFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			step, _ := compose_v2.Get(chs, seqAgentStepCh)
			info := &hook.AgentInfo{Name: name, Type: "sequential", Round: int32(step)}

			origMsg, _ := compose_v2.Get(chs, seqAgentOriginalCh)
			lastOut, _ := compose_v2.Get(chs, seqAgentLastOutputCh)

			// 拼装本步 input
			var stepInput *schema.Message
			if idx == 0 {
				stepInput = origMsg
				if stepInput == nil {
					// caller 没传 original——fail-fast
					return nil, fmt.Errorf("sequential_agent: step 0 %q: 没拿到原始 input "+
						"(caller 应通过 compose_v2.WithInitialWrites(compose_v2.Write(SeqAgentOriginalChannel(), input)) 写入)", n.Name)
				}
			} else {
				// step 1+ 显式拼装"原始目标 + 上一步（prev name）产出"
				prevName := b.steps[idx-1].Name
				origContent := ""
				if origMsg != nil {
					origContent = origMsg.Content
				}
				prevContent := ""
				if lastOut != nil {
					prevContent = lastOut.Content
				}
				// 框架拼的，不是用户说的——标出来让会话历史跳过。step 0 不标（它的
				// stepInput 就是原始 user 输入本身，是真实发生的那一句）。
				stepInput = MarkDerivedInput(schema.UserMessage(fmt.Sprintf(
					"原始目标：%s\n\n上一步（%s）的产出：\n%s",
					origContent, prevName, prevContent,
				)))
			}

			// 8-method 协议：OnAgentStart 整个调用 1 次, 由
			// agent_v2.SequentialAgent.Run/Resume top-level fire——本 builder 不再每步触发。
			// 调业务步骤函数
			out, err := n.Fn(ctx, stepInput)
			if err != nil {
				// audit Round 7 Bug 3 修复：compose_v2.InterruptError 是控制流不是错误，不上报 OnError
				// **R25 audit MED-4 fix**：ctx.Err() 同属控制流（Go 标准 cancel/deadline 范式）
				if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
					hooks.OnError(ctx, info, &schema.CommError{
						Code: "step_error", Message: err.Error(),
					})
				}
				return nil, fmt.Errorf("step %d %q: %w", idx, n.Name, err)
			}
			if out == nil {
				err := fmt.Errorf("step %d %q: nil output", idx, n.Name)
				hooks.OnError(ctx, info, &schema.CommError{
					Code: "step_nil_output", Message: err.Error(),
				})
				return nil, err
			}

			// 写 channels：
			// - seqAgentMessagesCh: 累加本步产出（step 0 额外带上原始 user 输入，见下）
			// - seqAgentLastOutputCh: 覆盖（下一步 prev content 用）
			// - seqAgentStepCh: 累加 1
			//
			// **只记实际发生的事，不记派生的中间输入**：step 1+ 的 stepInput 是上面用
			// 模板拼的（"原始目标：…\n\n上一步（X）的产出：…"），把它一起写进去会让每条
			// 产出被复述两遍——一次作为本步的产出、一次嵌在下一步的 user 模板里，且以
			// user 角色再现一次别人说过的话。派生输入随时可从"原始目标 + 上一步产出"重算，
			// 不该占据消息流。
			//
			// step 0 是例外：它的 stepInput **就是** origMsg 本身（:165-171），是真实的
			// 用户输入不是派生物。丢了它，消息流就没有开头了。
			accum := []*schema.Message{out}
			if idx == 0 {
				accum = []*schema.Message{stepInput, out}
			}
			return []compose_v2.ChannelWrite{
				compose_v2.Write(seqAgentMessagesCh, accum),
				compose_v2.Write(seqAgentLastOutputCh, out),
				compose_v2.Write(seqAgentStepCh, 1),
			}, nil
		}
		bld.AddNode(compose_v2.NodeID(n.Name), stepFn)
	}

	// 加边：Start → step[0] → step[1] → ... → step[N-1] → End
	bld.AddEdge(compose_v2.StartNode, compose_v2.NodeID(b.steps[0].Name))
	for i := 0; i < len(b.steps)-1; i++ {
		bld.AddEdge(compose_v2.NodeID(b.steps[i].Name), compose_v2.NodeID(b.steps[i+1].Name))
	}
	bld.AddEdge(compose_v2.NodeID(b.steps[len(b.steps)-1].Name), compose_v2.EndNode)

	return bld.Compile()
}

// SeqAgentOriginalChannel 暴露内部 seqAgentOriginalCh 让 caller compose_v2.WithInitialWrites
// 写入原始 input。caller 用法：
//
//	g, _ := NewSequentialAgentBuilder().WithName("seq").
//	    AddStep("s1", fn1).AddStep("s2", fn2).Compile()
//	cm := compose_v2.NewChannelManager()
//	err := compose_v2.Run(ctx, g, cm,
//	    compose_v2.WithInitialWrites(
//	        compose_v2.Write(SeqAgentOriginalChannel(), schema.UserMessage("hi")),
//	    ),
//	)
//	final := SequentialAgentFinalMessage(cm.Channels())
func SeqAgentOriginalChannel() *compose_v2.Channel[*schema.Message] {
	return seqAgentOriginalCh
}

// SequentialAgentFinalMessage 从 cm 拿最终产出 message——caller compose_v2.Run 完后调用。
func SequentialAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, seqAgentLastOutputCh)
	return out
}

// SequentialAgentAllMessages 从 cm 拿全部累积 messages（含每步 input + output）。
func SequentialAgentAllMessages(chs compose_v2.Channels) []*schema.Message {
	msgs, _ := compose_v2.Get(chs, seqAgentMessagesCh)
	return msgs
}
