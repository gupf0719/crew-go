package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ReflectOpinion 是 Reflector 输出——评估 actor 输出是否达标 + 反馈。
//
//   - GoalMatched=true → 接受当前 actor 输出，退出循环
//   - GoalMatched=false → 继续循环；Opinion / Critique 喂下一轮（见各字段说明）
type ReflectOpinion struct {
	GoalMatched bool // true → 接受当前 actor 输出，退出循环

	// Opinion 给 actor 的改进方向——非空时拼入下一轮 actor 输入的"评审反馈"段。
	// Reflector fn 档业务直接填；Evaluator+ReflectorAgent 档由 feedback 节点的
	// ReflectFeedbackFn（reflectorAgent 包装）产出，evaluate 阶段留空。
	Opinion string

	// Critique 评估器批评全文（R33 拆图引入：critique 全文进 channel 喂下一轮）。
	// Evaluator 档框架填 evaluator 输出 Content——feedback 节点把它喂给
	// reflectorAgent（修 RF-M2"批评文本被丢只 parse yes/no"）；无 ReflectFeedbackFn
	// 时 fallback 直接作 Opinion（比留空多一行、信息量质变）。fn 档可不填。
	Critique string
}

// ReflectActorFn 是 ReflectAgent 编排的 actor 节点函数——每轮跑一次。
//
// 入参：
//   - ctx：透传 framework ctx
//   - input：本轮 input。第 1 轮为原始目标；后续轮为 framework 自动拼装的
//     "原始目标 + 上一轮 actor 输出 + 评审反馈"
//
// 输出：本轮 actor 产出（待 evaluate 节点评判）。
type ReflectActorFn func(ctx context.Context, input *schema.Message) (*schema.Message, error)

// Reflector 评估 actor 输出，返结构化反馈。
//
// R33 拆图签名变更：增 goal 参数（原始目标）——评估器必须看到原始目标才能判
// "goal matched"；旧签名 evaluator 判达标却不知道 goal 是什么（RF-M2）。goal
// 可能为 nil（caller 没写 original channel），业务自判。
type Reflector func(ctx context.Context, goal, output *schema.Message) (*ReflectOpinion, error)

// ReflectFeedbackFn 是 feedback 节点的可选反馈生成函数（R33 拆图引入）——
// 仅在"未达标且还有下一轮"时被调（budget 判定在前，耗尽轮不白跑，修 RF-M4）。
//
// 入参 critique 是 evaluate 阶段产出的评估器批评全文（ReflectOpinion.Critique）；
// 返回 opinion 拼入下一轮 actor 输入的"评审反馈"段。
//
// 典型实现：agent_v2 把 reflectorAgent 包成本函数（输入合成含 goal + actor
// 输出 + critique 的 user-role 消息）。不设时 feedback 节点直接用 evaluate
// 阶段的 Opinion（fn 档）或 Critique fallback（Evaluator 档无 reflectorAgent）。
type ReflectFeedbackFn func(ctx context.Context, goal, output *schema.Message, critique string) (string, error)

// EvalFunc 简化版评估——返 (true, nil)=GoalMatched 退出，(false, nil)=继续，
// (_, err)=中止 propagate err。
//
// **HITL 支持**：业务可抛 `compose_v2.InterruptError` 做"评估通过前用户确认"等
// HITL gate（如代码审查通过前请人工确认）。框架按 graph 内 SaveNS 协议自动存
// cp + Resume 重跑 EvalFunc 取 compose_v2.ResumeContext 答案续跑。
//
// **vs Reflector**：EvalFunc 没法给 actor 反馈意见——continues 时下一轮输入是
// 框架 synthesize 的模板（原始目标 + 上一轮你的回答 + 空评审反馈 + 请改进），
// 不携带任何具体改进方向，适合"只判断好坏不指导"场景。Reflector 适合
// "评估 + 给方向"场景（Opinion 拼入下一轮模板的评审反馈段）。
type EvalFunc func(ctx context.Context, output *schema.Message) (bool, error)

// ReflectAgentBuilder 是 ReflectAgent 编排：actor 跑 → evaluate 评 → 不达标
// feedback 产意见 → 重跑。
//
// 拓扑（R33 拆图——原 actor+eval 两节点，评估链整链跑在单 eval 节点内）：
//
//	compose_v2.StartNode → actor → evaluate → ConditionalEdge → [End | feedback] → actor(回边)
//
// 拆图动机（四编排 review RF-H1，双节点拆分，循同仓 LoopAgent body/exitCheck
// 拆分先例）：
//   - verdict/critique 在 evaluate 节点 superstep commit 持久化——feedback 内
//     reflectorAgent 抛 compose_v2.InterruptError 时 Resume 只重跑 feedback 节点，evaluator
//     不重评（LLM 非确定重评会让 verdict 翻面、用户给 reflectorAgent 的答案被
//     静默跳过）
//   - budget 判定在 evaluate 内、feedback 之前——maxRounds 耗尽轮 reflectorAgent
//     不白跑（RF-M4：退出判定置于反馈之前）
//
// 退出条件（evaluate 节点判定）：
//  1. 评估返 GoalMatched=true（业务认可输出）
//  2. round+1 >= MaxRounds（兜底防死循环）——final 消息 Annotations 标
//     "reflect_goal_matched"(bool) + "reflect_rounds"(int) 让业务区分
//     "达标退出"vs"预算耗尽兜底"（修 RF-M4 不可区分问题）。注意 rounds 经
//     cp/JSON round-trip 后为 JSON number（float64）——跨进程读用
//     `.(float64)` 或数值比较，同进程立即读仍是 int（bool 不漂）
//
// **vs LoopAgent**：LoopAgent 是通用循环（每轮 input=上轮 output），ReflectAgent
// 加了"评判 + 反馈拼装"语义——actor 看到的是"原目标 + 上轮自己输出 + 评审
// 反馈"，更适合迭代改进类任务（写作 / 代码生成 / 数学解题等）。
type ReflectAgentBuilder struct {
	name       string
	actor      ReflectActorFn
	reflector  Reflector
	feedbackFn ReflectFeedbackFn
	evalFn     EvalFunc
	maxRounds  int
	hooks      hook.Hooks
}

// 内部 channels (ReflectAgent 编排专用)
var (
	reflectActorInputCh = compose_v2.LastValue[*schema.Message]("__reflect_actor_input__") // 当前轮 actor input
	reflectActorOutCh   = compose_v2.LastValue[*schema.Message]("__reflect_actor_out__")   // 上轮 actor output
	reflectOriginalCh   = compose_v2.LastValue[*schema.Message]("__reflect_original__")    // 原始目标
	reflectRoundCh      = compose_v2.Sum("__reflect_round__")                              // 已完成轮数
	reflectExitedCh     = compose_v2.OnceFlag("__reflect_exited__")                        // 退出信号
	reflectFinalCh      = compose_v2.LastValue[*schema.Message]("__reflect_final__")       // 最终产出
	// R33 拆图新增：verdict/critique 跨 evaluate→feedback 节点传递（superstep
	// commit 持久化——feedback 中断 Resume 时不重评 evaluator 的关键）
	reflectCritiqueCh = compose_v2.LastValue[string]("__reflect_critique__") // 评估器批评全文
	reflectOpinionCh  = compose_v2.LastValue[string]("__reflect_opinion__")  // fn 档 evaluate 阶段已产的 Opinion
	// reflectActorSignalsCh 存**本轮** actor 做出的上行信号（转交 / 路由决定），等评估通过（或预算
	// 耗尽、以这一轮兜底）时才由 evaluate 节点交出——被评审否掉的那一轮，产出被下一轮取代，它做出的
	// 决定也随之作废。不留这一手的话 actor 第一轮转交、被否、第二轮自己作答并通过，对话照样被转走，
	// 跟最终产出说的不是一回事（实测）。
	reflectActorSignalsCh = compose_v2.LastValue[compose_v2.Signals]("__reflect_actor_signals__")
)

// GetReflectAgentRound 从 compose_v2.ChannelManager 读已完成 reflection 轮数 (Sub-5 audit Round 2 视角 G fix).
func GetReflectAgentRound(cm *compose_v2.ChannelManager) int32 {
	if cm == nil {
		return 0
	}
	round, _ := compose_v2.Get(cm.Channels(), reflectRoundCh)
	return int32(round)
}

// NewReflectAgentBuilder 创建 ReflectAgentBuilder。默认 maxRounds=5。
func NewReflectAgentBuilder() *ReflectAgentBuilder {
	return &ReflectAgentBuilder{maxRounds: 5}
}

// WithName 设名。
func (b *ReflectAgentBuilder) WithName(name string) *ReflectAgentBuilder {
	b.name = name
	return b
}

// WithActor 设 actor 函数（必填）。
func (b *ReflectAgentBuilder) WithActor(fn ReflectActorFn) *ReflectAgentBuilder {
	b.actor = fn
	return b
}

// WithReflector 设 Reflector 函数。与 EvalFunc 同设时 Reflector 生效（compose
// 低阶层静默优先级；agent_v2 高阶层构造期 panic 拒绝多档同配不会到这一步）。
func (b *ReflectAgentBuilder) WithReflector(fn Reflector) *ReflectAgentBuilder {
	b.reflector = fn
	return b
}

// WithFeedbackFn 设 feedback 节点的反馈生成函数（可选，R33 拆图引入）——
// 仅"未达标且还有下一轮"时被调（典型：agent_v2 把 reflectorAgent 包装成它）。
func (b *ReflectAgentBuilder) WithFeedbackFn(fn ReflectFeedbackFn) *ReflectAgentBuilder {
	b.feedbackFn = fn
	return b
}

// WithEvalFunc 设简化版评估函数。
func (b *ReflectAgentBuilder) WithEvalFunc(fn EvalFunc) *ReflectAgentBuilder {
	b.evalFn = fn
	return b
}

// WithMaxRounds 设最大轮数（默认 5）。
func (b *ReflectAgentBuilder) WithMaxRounds(n int) *ReflectAgentBuilder {
	if n > 0 {
		b.maxRounds = n
	}
	return b
}

// WithAgentHooks 设业务 hook。
func (b *ReflectAgentBuilder) WithAgentHooks(h hook.Hooks) *ReflectAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
func (b *ReflectAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if b.actor == nil {
		return nil, fmt.Errorf("reflect_agent: actor required (call WithActor)")
	}
	hooks := b.hooks
	// nil 防护: typed nil hook.Hooks 走 isFrameworkNil 退化 NoOpHooks——业务直接
	// NewReflectAgentBuilder().WithActor(...).Compile() 不调 WithAgentHooks 时, actor /
	// reflector / evalFn 业务 error 路径调 `hooks.OnError` 直接 nil deref panic.
	// 与 7 编排统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "reflect_agent"
	}
	maxRounds := b.maxRounds
	reflector := b.reflector
	feedbackFn := b.feedbackFn
	evalFn := b.evalFn

	// R33 拆图：每轮 3 节点（actor / evaluate / feedback）
	bld := compose_v2.NewBuilder().WithName(name).WithMaxVisit(maxRounds*3 + 5)
	compose_v2.WithChannel(bld, reflectActorInputCh)
	compose_v2.WithChannel(bld, reflectActorOutCh)
	compose_v2.WithChannel(bld, reflectOriginalCh)
	compose_v2.WithChannel(bld, reflectRoundCh)
	compose_v2.WithChannel(bld, reflectExitedCh)
	compose_v2.WithChannel(bld, reflectFinalCh)
	compose_v2.WithChannel(bld, reflectCritiqueCh)
	compose_v2.WithChannel(bld, reflectOpinionCh)
	compose_v2.WithChannel(bld, reflectActorSignalsCh)

	// actor 节点：拿 input → 跑 actor → 写 output
	actorFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		input, _ := compose_v2.Get(chs, reflectActorInputCh)
		orig, _ := compose_v2.Get(chs, reflectOriginalCh)
		round, _ := compose_v2.Get(chs, reflectRoundCh)

		// 第 1 轮 input 等于 original；后续是上一轮 feedback 节点拼装好的
		if input == nil {
			input = orig
		}
		if input == nil {
			return nil, fmt.Errorf("reflect_agent: actor 拿不到 input（caller 应通过 compose_v2.WithInitialWrites 写入）")
		}

		info := &hook.AgentInfo{Name: name, Type: "reflect", Round: int32(round)}
		// 8-method 协议：OnAgentStart 整个调用 1 次, 由 agent_v2.ReflectAgent.Run/Resume
		// top-level fire——actor 节点不再每轮触发 hook。

		// **R29 audit 视角4 HIGH-2 fix**：actor ctx 加 round NS 隔离让跨多轮 actor
		// 内部 sub-agent 的 InterruptID 自然不同（5 维 graphNS 含 round_<N>）。
		// 修前 actor 跨多轮共享同 ctx graphNS → sub-agent 内 ask_user 算 InterruptID
		// 全相同 → compose_v2.ResumeContext 跨轮误读上轮答案 → LLM 看到"用户对新问题答了上轮答案"。
		// 对照 LoopAgent 已用同模式（round_<N> 编入 ns）。
		actorCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), fmt.Sprintf("round_%d", round)))
		// 本轮做出的上行信号先收下、存进 channel，评估通过才交出（见 reflectActorSignalsCh）。
		var out *schema.Message
		sigs, err := compose_v2.CaptureSignals(actorCtx, func(c context.Context) error {
			var actorErr error
			out, actorErr = b.actor(c, input)
			return actorErr
		})
		if err != nil {
			// **R25 audit MED-4 fix**：ctx.Err() 同 compose_v2.InterruptError 同属控制流
			if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
				hooks.OnError(ctx, info, &schema.CommError{Code: "actor_error", Message: err.Error()})
			}
			return nil, fmt.Errorf("actor round %d: %w", round, err)
		}
		if out == nil {
			out = schema.AssistantMessage("")
		}
		return []compose_v2.ChannelWrite{
			compose_v2.Write(reflectActorOutCh, out),
			compose_v2.Write(reflectActorSignalsCh, sigs),
		}, nil
	}
	bld.AddNode("actor", actorFn)
	bld.AddEdge(compose_v2.StartNode, "actor")

	// evaluate 节点（R33 拆图）：只跑评估（Reflector / EvalFunc）→ 写 verdict
	// 进 channel（superstep commit 持久化）→ 判退出（含 budget，在 feedback
	// **之前**——耗尽轮反馈生成不白跑）。
	evaluateFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		out, _ := compose_v2.Get(chs, reflectActorOutCh)
		orig, _ := compose_v2.Get(chs, reflectOriginalCh)
		round, _ := compose_v2.Get(chs, reflectRoundCh)
		nextRound := round + 1

		matched := false
		critique := ""
		opinion := ""

		// **R30 视角A HIGH-1 + LOW-1 fix**：与 actor 路径对称（已 push round_<N>），
		// 评估也 push round_<N> NS 让跨多轮 evaluator 嵌套 sub-agent 的 InterruptID
		// 5 维 graphNS 维度自然隔离。
		evalCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), fmt.Sprintf("round_%d", round)))

		if reflector != nil {
			op, err := reflector(evalCtx, orig, out)
			if err != nil {
				// review-v6 M-10 fix: 业务错 fire OnError, 控制流 (Interrupt / ctx.Err)
				// 不 fire 避免 SLO/告警假阳性.
				if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
					info := &hook.AgentInfo{Name: name, Type: "reflect", Round: int32(round)}
					hooks.OnError(ctx, info, &schema.CommError{Code: "reflector_error", Message: err.Error()})
				}
				return nil, fmt.Errorf("reflector round %d: %w", round, err)
			}
			// 四编排 review RF-LOW-1：Reflector 返 (nil, nil) 是协议错——静默当
			//"不过 + 无意见"会烧满 maxRounds 轮 LLM 调用。fail-fast 指向实现错误。
			if op == nil {
				return nil, fmt.Errorf("reflector round %d: returned (nil, nil) — Reflector 必须返非 nil *ReflectOpinion 或 error", round)
			}
			matched = op.GoalMatched
			critique = op.Critique
			opinion = op.Opinion
		} else if evalFn != nil {
			m, evalErr := evalFn(evalCtx, out)
			if evalErr != nil {
				// HITL / ctx.Err / 业务错统一 propagate（同 Reflector 路径）
				if _, isInt := compose_v2.IsInterrupt(evalErr); !isInt && ctx.Err() == nil {
					info := &hook.AgentInfo{Name: name, Type: "reflect", Round: int32(round)}
					hooks.OnError(ctx, info, &schema.CommError{Code: "eval_error", Message: evalErr.Error()})
				}
				return nil, fmt.Errorf("eval round %d: %w", round, evalErr)
			}
			matched = m
		} else {
			// 没设评估器 → 默认接受第 1 轮输出
			matched = true
		}

		writes := []compose_v2.ChannelWrite{compose_v2.Write(reflectRoundCh, 1)}

		// 退出判定（含 budget，置于 feedback 之前——RF-M4：耗尽轮 reflectorAgent
		// 不被调）
		exhausted := !matched && nextRound >= maxRounds
		if matched || exhausted {
			// final 标 annotation 让业务区分"达标退出"vs"预算耗尽兜底"（RF-M4）。
			// clone 防污染 actor 原消息（out 同时存在于 reflectActorOutCh）。
			final := out
			if final != nil {
				cloned := *final
				anns := make(map[string]any, len(final.Annotations)+2)
				for k, v := range final.Annotations {
					anns[k] = v
				}
				anns["reflect_goal_matched"] = matched
				anns["reflect_rounds"] = int(nextRound)
				cloned.Annotations = anns
				final = &cloned
			}
			writes = append(writes,
				compose_v2.Write(reflectFinalCh, final),
				compose_v2.Write(reflectExitedCh, true),
			)
			// 这一轮的产出成了最终产出 ⟹ 它做出的决定这时才交出，跟着本节点的提交往上走。被否掉的
			// 那些轮的已被后来的轮次覆盖、永不交出（见 reflectActorSignalsCh）。
			if sigs, ok := compose_v2.Get(chs, reflectActorSignalsCh); ok {
				compose_v2.EmitSignals(ctx, sigs)
			}
			return writes, nil
		}

		// 继续循环：verdict/critique/opinion 进 channel（superstep commit——
		// feedback 内中断时 Resume 只重跑 feedback，evaluator 不重评，RF-H1 关键）
		writes = append(writes,
			compose_v2.Write(reflectCritiqueCh, critique),
			compose_v2.Write(reflectOpinionCh, opinion),
		)
		return writes, nil
	}
	bld.AddNode("evaluate", evaluateFn)
	bld.AddEdge("actor", "evaluate")

	// feedback 节点（R33 拆图）：仅"未达标且还有下一轮"可达——可选跑
	// ReflectFeedbackFn（reflectorAgent 包装）产改进意见 → 合成下一轮 actor input。
	feedbackNodeFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		out, _ := compose_v2.Get(chs, reflectActorOutCh)
		orig, _ := compose_v2.Get(chs, reflectOriginalCh)
		round, _ := compose_v2.Get(chs, reflectRoundCh)
		critique, _ := compose_v2.Get(chs, reflectCritiqueCh)
		opinion, _ := compose_v2.Get(chs, reflectOpinionCh)

		// 注意：evaluate 节点已把 round +1 commit，此处 round 是"已完成轮数"，
		// 本节点属于第 round-1 轮的收尾——ns 用 round-1 保持与同轮 actor/evaluate
		// 一致（InterruptID 5 维隔离按轮对齐）。
		fbRound := round - 1
		if fbRound < 0 {
			fbRound = 0
		}
		fbCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), fmt.Sprintf("round_%d", fbRound)))

		if feedbackFn != nil {
			op, err := feedbackFn(fbCtx, orig, out, critique)
			if err != nil {
				if _, isInt := compose_v2.IsInterrupt(err); !isInt && ctx.Err() == nil {
					info := &hook.AgentInfo{Name: name, Type: "reflect", Round: int32(fbRound)}
					hooks.OnError(ctx, info, &schema.CommError{Code: "feedback_error", Message: err.Error()})
				}
				return nil, fmt.Errorf("feedback round %d: %w", fbRound, err)
			}
			opinion = op
		}
		// 无 feedbackFn 时：fn 档用 evaluate 阶段已产 Opinion；Evaluator 档无
		// reflectorAgent 时 fallback 用 critique 全文（比留空多一行、信息量质变）。
		if opinion == "" {
			opinion = critique
		}

		origContent := ""
		if orig != nil {
			origContent = orig.Content
		}
		outContent := ""
		if out != nil {
			outContent = out.Content
		}
		// **不标 DerivedInputAnnotation**（对比 Sequential 的 stepInput）：这条虽然也是
		// 拼的，但里头的**评审反馈是不可重算的**——它来自 reflector / evaluator，那条分支
		// 已被 WithoutHistoryCtx 摘除，critique 本身不在历史里。不记这条，历史里就只剩
		// 「actor 说了 A，然后又说了 B」，看不出中间发生过什么、B 为什么跟 A 不一样。
		//
		// 判据是「拼进去的东西是不是已经在历史里」，不是「是不是框架拼的」：Sequential 拼
		// 的两样（原始目标 + 上一步产出）都在，纯重复；这里的第三样不在。
		nextInput := (schema.UserMessage(fmt.Sprintf(
			"原始目标：%s\n\n上一轮你的回答：\n%s\n\n评审反馈：\n%s\n\n请改进。",
			origContent, outContent, opinion,
		)))
		return []compose_v2.ChannelWrite{compose_v2.Write(reflectActorInputCh, nextInput)}, nil
	}
	bld.AddNode("feedback", feedbackNodeFn)
	bld.AddEdge("feedback", "actor")

	// ConditionalEdge: exited=true → End；否则 feedback 产反馈后回 actor
	exitChoice := func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		exited, _ := compose_v2.Get(chs, reflectExitedCh)
		if exited {
			return compose_v2.EndNode, nil
		}
		return "feedback", nil
	}
	bld.AddConditionalEdge("evaluate", exitChoice)

	return bld.Compile()
}

// ReflectAgentOriginalChannel 暴露原始目标 channel 让 caller compose_v2.WithInitialWrites 写入。
func ReflectAgentOriginalChannel() *compose_v2.Channel[*schema.Message] {
	return reflectOriginalCh
}

// ReflectAgentInputChannel 暴露 actor input channel——caller 也用此写入第 1 轮 input。
func ReflectAgentInputChannel() *compose_v2.Channel[*schema.Message] {
	return reflectActorInputCh
}

// ReflectAgentFinalMessage 从 cm 拿最终接受的 actor 输出。
//
// R33 拆图后 final 消息 Annotations 带 "reflect_goal_matched"(bool) +
// "reflect_rounds"(int)——业务区分"达标退出"vs"maxRounds 耗尽兜底"。
func ReflectAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, reflectFinalCh)
	return out
}
