package agent_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
	"crew-go/model"
	"crew-go/schema"
)

// BasicAgentModelFn 是 BasicAgent 编排的 LLM 调用函数。
//
// 入参：消息历史
// 输出：assistant 消息（可能含 tool_call 触发 tools 节点 / 不含则终止）
//
// **audit Round 9 Bug A2 文档化**：modelFn 必须 idempotent / 可重入——框架在
// compose_v2.InterruptError 中段保存 cp 后 Resume 重跑时会再次调用相同 in，业务勿在 modelFn
// 闭包内持 stream session ID 等"已 send 过"状态假设；流式聚合状态不持久化随 cp
// （streamOrGenerate 内的 partial accumulator 跨 Resume 重跑时丢失，业务 modelFn
// 应以"从头跑全 stream"为前提设计）。
//
// **两个消息参数，不是一个**：`preamble` 是 agent 的前言（[BasicAgentPreambleOutput].System——
// 提示词模板渲染结果 + system prompt 含 skill 正文），`msgs` 是会被上下文压缩处置的对话流。
// 自己实现 modelFn 时**必须把 preamble 拼到请求最前**，而且要拼在你自己的压缩 / 裁剪之后
// ——那正是它单独作为一个参数而不是混在 msgs 里的原因。
//
// 它曾经只经 ctx 递给框架自己的 modelFn，于是业务用公开 builder 时
// `BasicAgentPreambleOutput.System` **一条都到不了**且不报错（实测：业务 modelFn 只收到
// `[user "hi"]`）。做成参数之后，「忘了它」在类型上就不成立。
type BasicAgentModelFn func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error)

// BasicAgentToolFn 是 BasicAgent 编排的 tool 调用函数。
//
// 入参：assistant 输出的 tool_calls，以及每个调用自己的 ctx——callCtxs[i] 是 OnToolStart(i) 采纳之后的 ctx，跟
// toolCalls 一一对应。调用 i 的执行（工具体、工具中间件、它调起的嵌套 agent）用 callCtxs[i]；ctx 只用于这一批整体的
// 工作（如 fanout 事件）。
// 输出：每个 tool_call 对应的 tool_result 消息（顺序对齐）
//
// 每个调用一个 ctx，是因为钩子返回的 ctx 只作用于它包住的那次调用：OnToolStart(i) 放在 ctx 上的值（span、按调用的
// 凭证）只该在调用 i 的执行和 OnToolEnd(i) 里可见。曾经整批共用一个 ctx、每个钩子的返回值都赋回它：b 的 OnToolStart
// 收到 a 放的值，整批工具都拿到最后一个 OnToolStart 的值——a 的工具体带着 b 的值在跑，按惯例在 OnToolStart 开 span 的
// 业务钩子把 a 的工作挂到 b 的 span 下（实测）。
type BasicAgentToolFn func(ctx context.Context, toolCalls []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error)

// BasicAgentPreambleInput 是 BasicAgent preamble 节点的输入——caller 通过
// `compose_v2.WithInitialWrites(BasicAgentPreambleInputWrites(...))` 注入。
//
// **why 单独 struct 而非直接传 *schema.Message**：未来扩展（如 systemPromptSeed /
// 业务自定义元数据）不破协议。当前只含 user input 一个字段。
type BasicAgentPreambleInput struct {
	UserInput *schema.Message
}

// BasicAgentPreambleFn 是 BasicAgent 编排的 preamble 节点函数——graph 首段跑
// 业务准备消息（典型用法：store.Search / HistoryCompactor / Skill 预激活拼装到
// system prompt 后输出完整初始 messages）。
//
// 入参：caller 通过 compose_v2.WithInitialWrites 写入的 PreambleInput
// 输出：见 [BasicAgentPreambleOutput]——**两条流**，按「这段内容每轮能不能由配置重算」分。
//
// **idempotent 约定**（同 BasicAgentModelFn / BasicAgentToolFn）：业务 preamble 必须
// 可重入——业务回调（store.Search / HistoryCompactor / SkillBox.ActivateSkill）抛
// compose_v2.InterruptError 时框架自动 SaveNS cp，Resume 时**完整重跑** preamble。业务回调
// 必须保证多次调用返同样结果（除非业务自己读 compose_v2.ResumeContext 主动改写）；副作用
// 业务自己幂等（典型如 audit log 业务自己去重）。
//
// **why 节点而非 compose_v2.Run 入口同步调用**：业务回调抛 compose_v2.InterruptError 时走 graph 内
// SaveNS 协议自动存 cp，Resume 路径自然支持（修 ISSUE-74）。compose_v2.Run 入口同步调用
// 抛 compose_v2.InterruptError 时 cp 还没机会存，Resume 找不到 cp → HITL 链断。
type BasicAgentPreambleFn func(ctx context.Context, in *BasicAgentPreambleInput) (*BasicAgentPreambleOutput, error)

// BasicAgentPreambleOutput 是 preamble 的产出——**两条流，不是一条**。
//
// 分流的判据是**「这段内容每轮能不能由配置重算」**，不是 role：
//
//   - `System`：能重算的那些（`WithPromptTemplate` 渲染结果、`WithSystemPrompt` 拼上
//     skill hint / skill body）。它们**不进被压缩的消息流**，而是在每次调用 LLM 之前
//     transient prepend——跟 AGENTS.md 注入同一处、同一理由。
//   - `Messages`：不能重算的那些（长期记忆注入、从历史读回的 prior、用户本轮输入）。
//     它们进被压缩的消息流，正常参与四层压缩。
//
// ⚠️ **为什么必须分开，而不是让压缩层「认出」System 那半**：认出来要有判据，而判据只能
// 从手上的东西里借。原先借的是**前导位置**——「从 messages[0] 起连续的 system 就是
// agent 的 system prompt」。模板一旦渲染出 few-shot（`System → Placeholder → User`，
// 那正是 `prompt` 包 godoc 教的写法），真 sysPrompt 就排在 few-shot **之后**，判据当场失效：
// 实测紧急压缩把它直接丢掉、AutoCompact 把它连同 skill 正文摘成一段散文，**全程 `err == nil`**。
// few-shot 自己更是从来没被任何机制保护过。
//
// 分开之后**判据消失**，不是变准了——以后谁再往消息流最前面塞东西都不会误伤，因为
// 「agent 是什么」根本不在那条流里。
//
// ⚠️ **代价（如实说）**：`System` 那半对 `hook.Hooks.OnModelStart` 的 `input.Messages`
// **不可见**——它在压缩之后、`callLLM` 内部才拼上去。AGENTS.md 注入今天已经是这个待遇，
// 这次把 system prompt 也归了进去。要看 agent 的 persona 请读 agent 配置，别扫消息流。
type BasicAgentPreambleOutput struct {
	// System 是每次调用 LLM 前 transient prepend 的那段，不进被压缩的消息流、不进 checkpoint
	// 的消息通道（它自己那条 channel 会进 cp——Resume 后模型仍拿得到，见 basicSysPreambleCh）。
	System []*schema.Message
	// Messages 是进被压缩消息流的那段（记忆注入 / prior / 本轮 input）。
	Messages []*schema.Message
}

// BasicAgentPostambleFn 是 BasicAgent 编排的 postamble 节点函数——graph 末段跑
// 业务收尾回调（典型用法：store.Put 持久化最终 assistant 消息 / 业务自定义
// 审计写入 / final hook trigger）。
//
// 入参见 BasicAgentPostambleInput——**Messages 和 Final 是两种不同的东西**：
// 前者是含 prior 的全部累积消息，后者才是本次调用的产出。想写 Memory 用 Final，
// **别从 Messages 里倒扫找**（那会找到上一次调用的答复）。
// 输出：error——nil 表示成功，compose_v2.InterruptError / ctx.Err / 业务错都 propagate
//
// **idempotent 约定**（同 BasicAgentPreambleFn）：业务 postamble 必须可重入——
// store.Put 抛 compose_v2.InterruptError 时框架自动 SaveNS cp，Resume 时**完整重跑** postamble。
// 业务回调多次调用应返同样结果；副作用业务自己去重（典型 store.Put 业务实现
// 应支持幂等写或读 compose_v2.ResumeContext 区分首次/续跑）。
//
// **why 节点而非 BasicAgent.Run 入口同步调用**（修 ISSUE-75）：之前 commitFinal 跑
// 在 graph 之外，graph_loop defer cleanupOnSuccess 已删整 sid namespace 的 cp，
// 此时业务 store.Put 抛 compose_v2.InterruptError 让 caller Resume 找不到 cp → HITL 链断。
// 节点化让 postamble 走 graph 内 SaveNS 协议，cleanupOnSuccess 不触发（runErr != nil）
// → caller Resume 时 cm.Restore 重跑 postamble 自然续跑。与 ISSUE-74 preamble 完全
// 对称。
type BasicAgentPostambleFn func(ctx context.Context, in *BasicAgentPostambleInput) error

// BasicAgentPostambleInput 是 postamble 节点的入参。
//
// 两个字段是**两种不同的东西**，别混：
//   - Messages：本次调用累积的全部消息，**含 prior**（preamble 读进来的历史）。
//     给「要看全貌」的收尾逻辑用（统计、审计、按整段做压缩）。
//   - Final：**本次调用的最终产出**，决策点写的（见 basicFinalCh）。nil = 本轮没产出。
//     给「要那一个答案」的收尾逻辑用。
//
// 从 Messages 里倒扫去找 Final 是**错的**——那个列表含 prior，找出来的可能是上一次
// 调用的答复。这正是拆成两个字段的原因。
//
// Interjections 是这个任务里接进消息流的运行中插话（见 [Inbox]），按接入顺序；同样不含 prior。
type BasicAgentPostambleInput struct {
	Messages      []*schema.Message
	Final         *schema.Message
	Interjections []*schema.Message
}

// BasicAgentBuilder 是 BasicAgent 编排：经典 ReAct 循环（model ↔ tools 默认 2 节点; 业务
// 通过 WithPreamble + WithPostamble 可扩展至 4 节点 preamble + model + tools + postamble.
// **注**: agent_v2 wrapper (agent_v2.NewBasicAgent) 生产路径**始终注入** preamble + postamble,
// 业务生产看到的真是 4 节点拓扑 — 详 agent_v2/basic_agent.go godoc + ISSUE-74/75).
//
// 拓扑：
//
//	compose_v2.StartNode → model → conditional( compose_v2.EndNode 或 tools )
//	tools → model (循环回)
//
// 终止条件：model 返无 tool_call 的 assistant 消息。
//
// 能力（均已 first-class 落地）：
//   - 业务级 hook.Hooks：WithHooks 完整集成
//   - Resume / HITL：compose_v2 cp round-trip + InterruptError + WithResumeUserInputs
//   - Streaming：compose_v2.EventSink ctx 注入（业务 Send 实时投递）
//   - 4 层压缩：WithMicroCompact / WithAutoCompact / WithReactiveCompact + WithStore
//   - skill：WithSkills（DefaultSkillBox / fork / listing）
//   - retry：RetryConfig（各编排 WithXxxRetryConfig 接入）
//
// 4 层压缩 wrap 示例（业务代码）：
//
//	// 业务自己包装 modelFn 串联 AutoCompact + ReactiveCompact retry
//	//
//	// ⚠️ **压缩只作用在 msgs，preamble 原样透传**——那正是它单独成参的理由：
//	// agent 的前言每轮可由配置重算，不该占着被压缩的名额，更不该被压缩层丢掉。
//	// 把它拼进 msgs 再压缩，等于把「判据消失」这件事又倒回去。
//	wrappedModel := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
//	    // 第 2 层：进入前自动摘要（token 阈值触发）
//	    msgs, _ = agent_v2.AutoCompactRun(ctx, autoCfg, msgs)
//	    out, err := originalModel(ctx, preamble, msgs)
//	    if err != nil && agent_v2.IsPromptTooLongError(err) {
//	        // 第 3 层：报 prompt-too-long 时压缩并重试
//	        msgs, _ = agent_v2.ReactiveCompactRun(ctx, reactCfg, msgs, err)
//	        return originalModel(ctx, preamble, msgs)
//	    }
//	    return out, err
//	}
//
//	// 业务自己包装 toolsFn 串联 MicroCompact
//	wrappedTools := func(ctx context.Context, tcs []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error) {
//	    results, err := originalTools(ctx, tcs, callCtxs)
//	    if err != nil { return nil, err }
//	    // 第 1 层：tools 节点末段清旧 tool_result
//	    // 注意：MicroCompact 作用在累积 messages，不在 tool_results 单批
//	    // 通过 hook.OnToolEnd 或下一轮 OnModelStart 内做
//	    return results, nil
//	}
//
//	g, _ := NewBasicAgentBuilder(wrappedModel, wrappedTools).Compile()
//
// 已知限制：MicroCompact 作用在跨轮累积 messages 不在单批 tool results——业务
// 应在 OnModelStart hook 内（下轮入口）调用，或 Phase 4 把这个 wrap 内置到框架。
type BasicAgentBuilder struct {
	name      string
	model     BasicAgentModelFn
	tools     BasicAgentToolFn
	preamble  BasicAgentPreambleFn  // ISSUE-74 修：可选 preamble 节点（graph 首段 setup）
	postamble BasicAgentPostambleFn // ISSUE-75 修：可选 postamble 节点（graph 末段收尾，如 store.Put）
	maxRounds int
	hooks     hook.Hooks // 业务级 hook（OnAgentStart/End + OnModelStart/End + OnToolStart/End + OnError）
	// modelConfig 算这次模型调用的请求配置（模型名与按次参数），交给 OnModelStart 的 ModelInput.Config；nil 时不填。
	modelConfig func(ctx context.Context) *schema.ModelConfig
	// backgroundNotes 给模型节点这一轮要送进对话的后台任务结束通知（BasicAgent 开了后台任务时设，见
	// background_task.go）；nil = 没有。
	backgroundNotes func(ctx context.Context, msgs []*schema.Message) []*schema.Message
	// finishToolResult 在 OnToolEnd 与批内裁决之后按大小处理还没处理的 tool_result（开了转存时设，见
	// agent_v2/tool_render.go）；failed / code 是钩子之后的结论。nil = 没有要处理的。
	finishToolResult func(ctx context.Context, toolName string, msg *schema.Message, failed bool, code string)
	// finalizeToolMemo 在这一批处理完（OnToolEnd、批内裁决、按大小处理）之后把结果定进这次执行的工具账（见
	// agent_v2 的 finalizeToolMemo）；hookSignals[i] 是第 i 个调用的 OnToolEnd 钩子做出的决定。nil = 不定（自写
	// toolsFn 时没有框架的工具账）。
	finalizeToolMemo func(ctx context.Context, toolCalls []schema.ToolCall, results []*schema.Message, hookSignals []compose_v2.Signals)
	// turnGates / turnGateMaxVetoes 见 WithTurnGates：回合结束闸门（空 = 不建闸门节点）。
	turnGates         []TurnGateFunc
	turnGateMaxVetoes int
	// identityName 非空时进图身份（compose_v2.GraphBuilder.WithIdentityPrefix）：BasicAgent 显式给了名字时设，见
	// buildComposeGraph。空 = 图身份只有结构指纹。
	identityName string
}

// ReturnDirectAnnotation 是 tool result message 上的 marker——某工具声明"调用后立即
// 终止 ReAct 循环、不再回 LLM"（return-direct 语义）。
//
// 协议链：业务工具返 `tool.Result.Metadata[tool.ReturnDirectMetaKey]=true` →
// agent_v2 makeToolsFn 检测后在 tool result message 打此 annotation → 本包 toolsNode
// 读此 annotation 写 basicReturnDirectCh → tools 出口条件边短路到 terminalTarget。
//
// **典型用途**：HandoffAgent 的 transfer 工具（一调即转交、不让原 agent 继续唠叨）、
// exit 类工具（return-direct：工具一调即直接出口不回 model）。
//
// **零副作用保证**：无工具设此 marker 时 basicReturnDirectCh 恒未写（compose_v2.OnceFlag 零值
// false），tools 出口条件边恒走 "model" 分支——与原 `AddEdge("tools","model")` 完全一致。
const ReturnDirectAnnotation = "__return_direct__"

// ReturnDirectMintedAnnotation 记「这次 return-direct 的工具是不是**框架那件控制工具**」
// （transfer_to_agent / route_to，bool）。写侧在 makeToolsFn 里用
// `isFrameworkControlToolIn(dispatchToolkit, name)` 判——手上有 toolkit 和真实实例的那一层判，
// 消费侧读**结构化事实**。
//
// ⚠️ **为什么不能在消费侧按名字判**（audit F1，实测坐实）：名字是**开放集**——业务注册一件叫
// `respond` / `plan` / `route_to` 的工具就撞上，`OnToolStart` 改写
// `input.Name` 是 [消费] 档、一个归一化 hook 就够。实测
// 撞名工具「体真跑了、短路真发生了、产出被丢了」，err=nil 无告警。而 `tool_memo.go` 的
// `frameworkControlTool` godoc 早写明这条教训：「判据是封闭标记，不是工具名——那些名字
// 业务占得到」。同一个判定这里当时又按名字写了一遍。
const ReturnDirectMintedAnnotation = "__return_direct_minted__"

// finalOutputRetryAnnotation 标「这次 final_output 调用**没有产出通过 schema 校验的答复**」（bool），
// 即一次失败的尝试。只在 toolsNode 一处、`OnToolEnd` 之后判：参数不是合法 JSON、schema 校验失败、
// 校验通过却被钩子改成失败，都算；钩子把失败改成成功**也算**——答复有效与否归框架（同框架控制
// 工具：钩子把失败改成成功不会凭空变出一个转交），那次没经过校验、不成为答复。它不是终止信号，是
// 「本轮必须回 model 回炉」——toolsNode 据它给短路让路：同批的业务 return-direct 工具这一轮不短路，
// 否则回炉边死、`WithOutputSchema` 的承诺（最终答复是合法 JSON）静默破。预算耗尽时
// `StructuredOutputError.LastError` 也按它取最后一次失败的原因——「算不算一次失败的尝试」与「报出来的
// 原因」是同一个判据；钩子改成成功的那次报钩子之前的失败（finalOutputFailureAnnotation）。
//
// 曾经分两处打：schema 失败在 makeToolsFn 打、钩子否决在 toolsNode 打，参数非法哪处都不打——两次
// 参数非法耗尽预算时 LastError 为空，跟业务 exit 工具同批时业务工具短路收尾、报「模型未产出结构化
// 输出」（均实测）。收成一处之后又有一版把「钩子改成成功」当成不算失败：那次既不是失败也不是答复，
// 同批的业务工具照样短路、报「未产出」，预算耗尽时原因为空（审计实测）。
const finalOutputRetryAnnotation = "__final_output_retry__"

// finalOutputFailureAnnotation 是一次失败的 final_output 尝试「报出来的原因」，只在模型看到的内容不是
// 失败原因时打：钩子把失败改成了成功，模型看到钩子给的内容，原因记钩子之前那次失败。
const finalOutputFailureAnnotation = "__final_output_failure__"

// finalOutputAttemptAnnotation 标「这条结果是框架结构化输出那件 final_output 的一次调用」（bool），由派发层
// （makeToolsFn）盖：它手上有 toolkit 和真实实例，按封闭标记认那件工具（见 frameworkFinalOutput）；参数
// 非法（工具没跑起来）、校验失败、校验通过都盖。toolsNode 收尾时「算不算一次结构化输出的尝试」「是不是
// 结构化答复」只读它，postamble 取答复也读它。
//
// **不按工具名认**：没配 WithOutputSchema 时 final_output 这个名字业务占得到，那件就是普通业务工具。按名字
// 认的时候，业务那件同名工具成功了也被记成一次失败的尝试，同批的业务 exit 工具不再短路、产出被丢，模型
// 被多调一次，err == nil（审计实测）；它失败时、它自己是 return-direct 工具时同理。
const finalOutputAttemptAnnotation = "__final_output_attempt__"

// isFinalOutputAttempt 判断这条工具结果是不是框架结构化输出那件 final_output 的一次调用。
func isFinalOutputAttempt(m *schema.Message) bool {
	if m == nil {
		return false
	}
	v, _ := m.Annotations[finalOutputAttemptAnnotation].(bool)
	return v
}

// toolSearchCallAnnotation 标「这条结果来自框架那件 tool_search」（bool）。写侧同 finalOutputAttemptAnnotation：
// makeToolsFn 按派发的实例认、盖章，tools 节点只读章——业务自己写的同名工具不带它。tools 节点据它判这一批算不算
// 「调过工具」（见 basicToolsUsedCh）。
const toolSearchCallAnnotation = "__tool_search_call__"

func isToolSearchCall(m *schema.Message) bool {
	if m == nil {
		return false
	}
	v, _ := m.Annotations[toolSearchCallAnnotation].(bool)
	return v
}

// toolErrorCodeAnnotation / toolErrorMessageAnnotation 把**工具调用失败**的结构化信息
// 从 makeToolsFn 跨层带到本包 toolsNode 的 hook fire 层（issue-95 finding 3）。
//
// **为什么需要跨层带**：makeToolsFn 手上有完整的 `tool.Result{Success, Error{Code,Message}}`，
// 但它只把错误**格式化成 content 字符串**（`[code] message`）返回 `[]*schema.Message`；
// toolsNode 拿到的就只剩 message，结构化信息已经没了 → 构造 `schema.ToolCallResult` 时
// 只能硬编码 `Success: true`。而 observability 正是按 `!Success` 把工具 span 记成失败（`error.type`）
// ——生产方恒 true ⟹ 该 metric 对工具失败**恒为 0**（framework 内部生产者/消费者拧着）。
//
// **不导出**：框架同时是生产者和消费者；业务读失败信息走 `OnToolEnd` 的
// `Result.Success/Error/ErrorCode` 类型化字段，不必依赖 annotation 键名。
//
// **值类型必须是 string**（不用 struct/map）：message 会随 channel 进 cp，annotation
// 走 JSON round-trip——`map[string]string` 回来会变 `map[string]any` 让类型断言失效。
//
// **presence 即失败信号**（用 comma-ok 判，不看值）：`tool.Failure("", "msg")` 的 code
// 是空串，靠"值非空"判会漏。
const (
	toolErrorCodeAnnotation    = "__tool_error_code__"
	toolErrorMessageAnnotation = "__tool_error_message__"
	// toolDeniedAnnotation 标「这条 tool_result 是审批拒绝合成的、工具没有执行」（presence 即信号）。它不是失败标注：
	// 拒绝是用户或审批策略的决定，算进错误率会污染告警；toolsNode 据它在 OnToolEnd 的 ToolOutput.Extra 上标
	// ToolOutcomeDenied（见 ToolOutcomeExtraKey）。
	toolDeniedAnnotation = "__tool_denied__"
)

// ToolTimeoutErrCode 是框架给超时的工具调用合成的失败结果的错误码（`tool.Result.Error.Code`，OnToolEnd 的
// `Result.ErrorCode`）。observability 按它把这次调用记成 timeout。
const ToolTimeoutErrCode = "tool_timeout"

// ToolOutcomeExtraKey 是 OnToolEnd 的 `ToolOutput.Extra` 里标这次调用怎么收尾的键，值目前只有 [ToolOutcomeDenied]；没有
// 这个键时按 `Result.Success` 判成败。
const ToolOutcomeExtraKey = "outcome"

// ToolOutcomeDenied 标「这次调用被审批拒绝、没有执行」：`Result.Success` 为 true（拒绝不是失败——是用户或审批策略的
// 决定，计进错误率会污染告警），可它也不是一次成功的执行。observability 把它记成 blocked。
const ToolOutcomeDenied = "denied"

// ToolReplayedExtraKey 标「这次调用的产出是从工具账重放的」（OnToolEnd 的 `ToolOutput.Extra`，值为 true）：这一批上次
// 没整体成功（别的调用中断或失败），这个调用那时已经跑完、在那一次的 OnToolEnd 收过尾（见 [ToolCallErrCode]），续跑时
// 不再执行、原样交出当时的产出。OnToolStart / OnToolEnd 照常触发、改写照常消费；按调用计数或计时的钩子据它跳过——
// observability 不再记样本。
const ToolReplayedExtraKey = "replayed"

// ToolRedispatchedExtraKey 标「这一批 tool_call 是续跑时再次派发的」（OnToolStart 的 `ToolInput.Extra`，值为 true）：这一批
// 上次已经交给工具执行过（内容在那一次定下，见 fixToolCallContent），没整体成功、续跑重跑 tools 节点时同一批的每个调用
// 再触发一次 OnToolStart。把调用转发给前端的钩子据它不再重开同一个调用（agui：同一个 id 再发 TOOL_CALL_START 是重开、
// 参数追加）。只看「是不是同一批的再次派发」，跟 toolCallId 无关——provider 在不同轮次复用同一个 ID 的不是重开。
const ToolRedispatchedExtraKey = "redispatched"

// ToolModelRunIDExtraKey 标「这个 tool_call 出自哪次模型调用」（OnToolStart 的 `ToolInput.Extra` 与 OnToolEnd 的
// `ToolOutput.Extra`，值是那次调用的 run_id，与它流式片段的 run_id 同一个）。一次模型调用被撤回时（reset_stream，见
// streamOrGenerate 与 compose_v2.StreamAttempts），前端适配层据此连同它的工具调用与结果一起撤回——不能按 tool_call ID 认：provider
// 会在一个 run 里复用 ID，按 ID 认会把别的调用的工具调用与结果一起撤掉（审计实测）。模型的答复上没有 run_id 时不带。
const ToolModelRunIDExtraKey = "model_run_id"

// ToolCallErrCode 是一批 tool_call 没整体成功时，硬失败的那个调用（工具返回 error、panic、中间件出错）交给 OnToolEnd
// 的失败结果的错误码；它自身超时（错误链上有 context.DeadlineExceeded、调用方还没放弃）时用 [ToolTimeoutErrCode]。也是
// 这时 OnError 的 Code。
//
// 一批 tool_call 没整体成功时，已经有结局的调用照样各自触发 OnToolEnd，Extra 带 [ToolBatchFailedExtraKey]：跑完的
// （成功、业务失败、被审批拒绝、从工具账重放的）交它的结果，硬失败的交框架合成的失败结果；等人回答、等审批而挂起的，
// 以及被停下的（调用方 ctx 结束、软取消）不触发。成功路径上某个调用的 OnToolEnd 钩子报错或中断（这一批随之作废）时，
// 排在它后面的调用同样这样收尾。这时这批的结果整体作废，改写不被消费，钩子返回的错误与 panic 也不改变这一批的结局
// （只记日志）。续跑时跑完的那几个从工具账重放，OnToolEnd 再触发一次，带 [ToolReplayedExtraKey]。
const ToolCallErrCode = "tool_call_error"

// ToolBatchFailedExtraKey 标「这一批 tool_call 没整体成功，这次 OnToolEnd 的结果不进历史」（OnToolEnd 的
// `ToolOutput.Extra`，值为 true；规则见 [ToolCallErrCode]）。续跑时这个调用要么从工具账重放（跑完的），要么重新执行
// （硬失败的、不进工具账的框架工具），那一次的 OnToolEnd 才是进历史、给模型看的结果——给用户展示结果的钩子（agui）
// 据它跳过、等那一次；按执行计数或计时的钩子照常算这一次（它确实执行了）。
const ToolBatchFailedExtraKey = "batch_failed"

// ModelCallTimeoutErrCode / ModelContentFilteredErrCode 是模型调用失败时 OnError 的 Code：这次调用自身超时（错误链上有
// context.DeadlineExceeded，或者节点超时把它停下）、provider 内容过滤拦下了它（model.ErrContentFiltered）。其余失败是
// model_call_error。observability 按前两种把这次调用记成 timeout / blocked。
const (
	ModelCallTimeoutErrCode     = "model_call_timeout"
	ModelContentFilteredErrCode = "model_content_filtered"
)

// modelCallErrCode 按模型调用失败的错误给 OnError 的 Code（见 ModelCallTimeoutErrCode）。
func modelCallErrCode(callCtx context.Context, err error) string {
	var nodeTimeout *compose_v2.NodeTimeoutError
	switch {
	case errors.Is(err, model.ErrContentFiltered):
		return ModelContentFilteredErrCode
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &nodeTimeout) || errors.As(context.Cause(callCtx), &nodeTimeout):
		return ModelCallTimeoutErrCode
	default:
		return "model_call_error"
	}
}

// toolOutputFor 把一条 tool_result 还原成交给 OnToolEnd 的 ToolOutput：失败的结构化信息从标注里取回，审批拒绝标上
// ToolOutcomeDenied，从工具账重放的标上 ToolReplayedExtraKey。
//
// **issue-95 finding 3 fix**：曾经硬编码 `Success: true`——业务工具 `tool.Failure(code,msg)` 明确声明的失败在钩子边界
// 被抹平，观测层对工具失败恒记成功。makeToolsFn 经标注把 code / message 带过来（见常量 godoc）。
//
// **Output 对失败也保持 = r.Content（即 `[code] msg`）**：OnToolEnd 改写回写会按 `(ErrorCode, Error)` 重算 content，
// 两者必须重合，否则钩子什么都不改也会把 tool_result 内容改掉。两侧都调 [toolErrorContent]，重合由实现保证；机械对账见
// `tool_error_content_single_source_test.go`。
func toolOutputFor(r *schema.Message, callID string, replayed bool) *schema.ToolOutput {
	res := &schema.ToolCallResult{Output: r.Content, Success: true}
	if r.Annotations != nil {
		// comma-ok 判 presence 不判值——code 可能是空串（tool.Failure("", msg)）
		if raw, failed := r.Annotations[toolErrorCodeAnnotation]; failed {
			res.Success = false
			res.ErrorCode, _ = raw.(string)
			res.Error, _ = r.Annotations[toolErrorMessageAnnotation].(string)
		}
	}
	to := &schema.ToolOutput{CallId: callID, Result: res}
	if _, denied := r.Annotations[toolDeniedAnnotation]; denied {
		// 被审批拒绝、没有执行：Success 照旧为 true（不是失败），观测层据 Extra 把它记成 blocked
		to.Extra = map[string]any{ToolOutcomeExtraKey: ToolOutcomeDenied}
	}
	if replayed {
		if to.Extra == nil {
			to.Extra = map[string]any{}
		}
		to.Extra[ToolReplayedExtraKey] = true
	}
	return to
}

// retractModelAnswer 撤回一份作废的模型答复已交给前端的那些（按它的 run_id 发 reset_stream）；答复上没有 run_id 时什么也不做。
func retractModelAnswer(ctx context.Context, out *schema.Message, agentName string) {
	if out == nil {
		return
	}
	runID, _ := out.Annotations[compose_v2.RunIDAnnotationKey].(string)
	sink := compose_v2.GetEventSink(ctx)
	if runID == "" || sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type:     "reset_stream",
		Content:  "model answer discarded and will be regenerated",
		Metadata: map[string]any{"reason": "answer_discarded", "run_id": runID, "agent": agentName},
	})
	compose_v2.ForgetStream(ctx, runID) // 撤过了：所在的尝试作废时不再撤一次
}

// withModelRunID 在 Extra 上标这个 tool_call 出自哪次模型调用（见 ToolModelRunIDExtraKey）；runID 为空时原样返回。
func withModelRunID(extra map[string]any, runID string) map[string]any {
	if runID == "" {
		return extra
	}
	if extra == nil {
		extra = map[string]any{}
	}
	extra[ToolModelRunIDExtraKey] = runID
	return extra
}

// endSettledToolCalls 在一批 tool_call 没整体成功时，给 ends[from:] 里已经有结局的调用触发 OnToolEnd（规则见
// ToolCallErrCode），Extra 带 ToolBatchFailedExtraKey。各调用的结局来自框架的 toolsFn（toolBatch.ends），或者成功路径上
// 钩子中途报错时剩下的那几个；业务自写的 toolsFn 交不出，这时一个都不触发。
//
// 钩子的改写不被消费、做出的上行信号作废（这批的结果整体作废）；它返回的错误与 panic 只记日志——这一批已经以别的错误
// 收尾，只在这里出错的钩子不该把一个能回答的提问变成失败。
func endSettledToolCalls(ctx context.Context, hooks hook.Hooks, info *hook.AgentInfo, toolCalls []schema.ToolCall,
	callCtxs []context.Context, ends []toolCallEnd, from int) {
	for i := from; i < len(ends); i++ {
		e := ends[i]
		if i >= len(toolCalls) || i >= len(callCtxs) {
			break
		}
		var to *schema.ToolOutput
		switch {
		case e.msg != nil:
			to = toolOutputFor(e.msg, toolCalls[i].ID, e.replayed)
		case e.err != nil && !compose_v2.IsRetryFutile(ctx, e.err):
			// 硬失败：错误链上的超时是这次调用自己的（调用方放弃了的话上面那个判断已经把它挡在外面）
			code := ToolCallErrCode
			if errors.Is(e.err, context.DeadlineExceeded) {
				code = ToolTimeoutErrCode
			}
			msg := e.err.Error()
			to = &schema.ToolOutput{CallId: toolCalls[i].ID, Result: &schema.ToolCallResult{
				Output: toolErrorContent(code, msg), Success: false, ErrorCode: code, Error: msg}}
		default:
			continue // 挂起（中断）或被停下：这次调用没有结局
		}
		if to.Extra == nil {
			to.Extra = map[string]any{}
		}
		to.Extra[ToolBatchFailedExtraKey] = true
		endOneSettled(ctx, hooks, info, toolCalls[i].Function.Name, to, callCtxs[i], i)
	}
}

// endOneSettled 给一个已有结局的调用触发失败路径上的 OnToolEnd：钩子的 panic 当场兜住，跟返回的错误一样只记日志。
func endOneSettled(ctx context.Context, hooks hook.Hooks, info *hook.AgentInfo, toolName string, to *schema.ToolOutput,
	callCtx context.Context, idx int) {
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(ctx, slog.LevelError, "agent_v2: 这一批 tool_call 没整体成功，给已有结局的调用收尾时 OnToolEnd panic（只记日志）",
				panicerr.New(r), "tool", toolName, "call_id", to.CallId)
		}
	}()
	_, hookErr := compose_v2.CallHook(callCtx, func(c context.Context) (context.Context, error) {
		var newCtx context.Context
		var err error
		_, _ = compose_v2.CaptureSignals(toolHookCtx(c, "OnToolEnd", toolName, to.CallId, idx),
			func(cc context.Context) error {
				newCtx, err = hooks.OnToolEnd(cc, info, to)
				return err
			})
		return newCtx, err
	})
	if hookErr != nil {
		slog.WarnContext(ctx, "agent_v2: 这一批 tool_call 没整体成功，给已有结局的调用收尾时 OnToolEnd 报错（只记日志）",
			"tool", toolName, "call_id", to.CallId, "err", hookErr)
	}
}

// markToolDenied 给审批拒绝合成的 tool_result 打上 toolDeniedAnnotation，返回入参 m。
func markToolDenied(m *schema.Message) *schema.Message {
	if m == nil {
		return m
	}
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	m.Annotations[toolDeniedAnnotation] = true
	return m
}

// markToolFailure 给 tool_result message 打上失败 marker——见上方 annotation 常量 godoc。
// 同时置上 tool_result 块的 IsError：原生支持失败标记的 provider（Anthropic 的 is_error）据它发
// wire 字段，marker 是给框架与观测层的，模型看不见。返回入参 m 便于链式写在 return 里。
func markToolFailure(m *schema.Message, code, message string) *schema.Message {
	if m == nil {
		return m
	}
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	m.Annotations[toolErrorCodeAnnotation] = code
	m.Annotations[toolErrorMessageAnnotation] = message
	for _, cb := range m.ContentBlocks {
		if cb != nil && cb.Toolresult != nil {
			cb.Toolresult.IsError = true
		}
	}
	return m
}

// failedToolResult 合成一条**失败的** tool_result：内容给模型看，marker 给观测层看。
//
// # 为什么是一个函数而不是两句
//
// 「合成失败结果」这件事需要同时做两件（造消息 + 打 marker），拆成两句写就会有人只写
// 第一句——实测过：框架给工具超时合成的 tool_result 漏了 marker，于是 `OnToolEnd` 收到
// `Success=true`、工具 span 记成 success、`gen_ai.execute_tool.duration` 不带 `error.type`。**超时是最需要被
// 观测到的工具失败**，偏偏是唯一漏标的那条。收成一个入口之后，想漏都难。
//
// 谁该用它：框架判定**这次调用失败了**并合成 tool_result 的地方——今天是参数非法 /
// 未解锁 / 超时三处。业务工具自己返 `tool.Failure` 那条路走 res.Error 分支（内容格式
// 不同，marker 由那边打）。
//
// ⚠️ **审批拒绝不走这里**（`approval.go` 用裸 `ToolResultMessage`）：那是用户的决定、
// 不是错误，计进错误率会污染告警。判据是「这次调用**失败了**吗」而不是「它**没执行**
// 吗」——拒绝的语义是后者。
//
// 但「不算失败」≠「算成功」：拒绝合成的 tool_result 打 toolDeniedAnnotation，OnToolEnd 的 `ToolOutput.Extra` 带
// [ToolOutcomeDenied]，observability 记成 blocked（span 不标 Error、带 `crew.tool.outcome=denied`），跟「转账
// 成功了」分得开。
func failedToolResult(tcID, code, message string) *schema.Message {
	// **message 传裸文本、不要自带 `[code]` 前缀**：前缀由这里统一拼进 content，
	// 而 annotation 里存的是裸 message。两处合成曾自带前缀（`invalid_arguments` /
	// `tool_not_revealed`），于是 annotation 里也带着——OnToolEnd 改写路径按
	// `[code] message` 重拼 content 时就成了 `[x] [x] ...` 双前缀（实测）。
	// 不变量：content == "[code] " + message，annotation.message == message。
	return markToolFailure(schema.ToolResultMessage(tcID, toolErrorContent(code, message)), code, message)
}

// renderHookedToolResult 定下 OnToolEnd 之后模型看到的那条 tool_result 内容，以及它算不算失败——
// 两件事由**同一份** Result 推出。框架按「算不算失败」判这次调用做出的转交 / 路由决定作不作废、框架
// 控制工具短不短路，模型看到的必须是同一个结论。曾经判失败只看 `Success`、渲染失败格式却还要
// `Error` 非空：钩子整体换掉 Result 忘了填 Success（零值 false）时，模型看到的是正常内容，决定却
// 作废了（审计实测）。
//
// before 是钩子之前那份。`Success` 为假一律渲染成失败格式；没给 Error 时拿 Output 当失败说明（钩子
// 给的内容不丢）——除非 Output 还是钩子之前那段已经渲染好的失败文本（工具自己声明的失败、说明为空），
// 那时说明就是空的，否则会拼成双前缀。钩子什么都没改时渲染结果与原内容逐字相同：工具声明的失败本来
// 就是按同一个 toolErrorContent 拼的。
//
// errMsg 是失败时说明那一段（content 去掉 `[code] ` 前缀之后的部分），给失败标注用。
func renderHookedToolResult(before schema.ToolCallResult, after *schema.ToolCallResult) (content string, failed bool, errMsg string) {
	if after.Success {
		return after.Output, false, ""
	}
	msg := after.Error
	if msg == "" && (before.Success || after.Output != before.Output) {
		msg = after.Output
	}
	return toolErrorContent(after.ErrorCode, msg), true, msg
}

// toolErrorContent 是「失败的 tool_result 里那段文本」的**唯一**拼法：`[code] message`。
//
// # 为什么必须只有一处
//
// 这个格式有**三个**参与者，而其中一个是必须逐字复现前两个的消费者：
//
//   - 生产者：框架判定失败时（[failedToolResult]）
//   - 生产者：业务工具返 `tool.Failure` 时（`basic_agent.go` 的 `res.Error` 分支）
//   - **消费者**：`OnToolEnd` 之后按 `(ErrorCode, Error)` **无条件重算** content
//     （见下方 `to.Result` 那段）——它必须拼出跟前两个一模一样的串，否则
//     **业务 hook 什么都不改，tool_result 的内容也会被悄悄改掉**。
//
// 三处手写时漂移的症状是静默的，而这次的双前缀 bug 正是这么来的：一个生产者的入参
// 被调用点污染，消费者照常重算，于是前缀出现两次。收成一处之后，漂移的可能性从
// 「三份各写各的」降到「一份实现 + 三个调用点」。
//
// ⚠️ 加第四个调用点前先想清楚它是不是**真的失败**：审批拒绝（用户的决定）与
// dangling/abort 的占位收尾都**不走这里**，理由见 [failedToolResult] 的 godoc。
func toolErrorContent(code, message string) string {
	return "[" + code + "] " + message
}

// 内部 channels
var (
	basicMessagesCh = compose_v2.Append[*schema.Message]("__basic_agent_messages__")
	basicRoundCh    = compose_v2.Sum("__basic_agent_round__") // 跟踪 ReAct 轮次，OnModelStart/End AgentInfo.Round 用

	// basicRevealedCh 是工具渐进披露的**已解锁集**——`tool_search` 每检索一次就往里
	// 追加解锁的工具名。它是唯一的副本：bind list 与执行闸都只读它（见 revealedNames）。
	//
	// **它必须是 channel，不能是别的**：
	//   - 会话对象（RunSession）不进 checkpoint ⟹ 换 session 的续跑之后是空的，
	//     fail-closed 把模型正要调的工具拦下，而失败**静默**（模型收到「未解锁」自己
	//     换个说法收尾，final 看着完全正常）。这是它原先的形态，实测工具被执行 0 次。
	//     后来会话 KV 上还留过一份缓存，跟它分叉、又活得比任务长，已删。
	//   - 节点私有状态（NodeStates）的键含 visit ⟹ **每轮一个新格子**，装不了跨轮累积的
	//     东西。（memo 用它是对的——那本来就是「本轮哪些 tc 跑完了」。）
	//   - 消息 annotation 会被压缩层动：实测「只留末尾 N 条」这类截断直接切掉。
	//
	// channel state 随 checkpoint 走、跨轮累积、不受消息重写影响——这三条正好是它要的。
	// Append 而非 LastValue：解锁是单调累加的，多个 tool_search 各写各的、天然可合并。
	//
	// ⚠️ **它只覆盖「跨轮」**：tool_search 与中断工具**同批**时，整批 writes 回滚 ⟹ 这条
	// 写也回滚，那个场景靠的仍是「tools 节点整批重跑、工具体自己再记一次」（tool_search
	// 还在 memo 豁免名单里，所以重跑得了）。行为安全（这一批的解锁记录随那次执行丢掉、
	// 重跑时重新记），但「不依赖名单」这个目标在同批那半没达成——登记在 doc/code/todo.md。
	basicRevealedCh = compose_v2.Append[string]("__basic_agent_revealed__")

	// basicFinalOutputAttemptsCh：**本次调用**里框架那件 final_output 实际被调了多少次。
	//
	// 由 tools 节点在 OnToolEnd 之后按派发层盖的 finalOutputAttemptAnnotation 数——跟回炉标记同一处、
	// 同一把尺。曾经由 model 节点按模型原始的 tool_call 名数：`OnToolStart` 把别的工具改名成 final_output
	// 时预算不生效（实测预算 0、每次校验失败，模型被调 7 次才以轮次超限收尾），反向改名时报「已尝试 1 次」
	// 而框架那件一次没跑。
	//
	// 结构化输出的重试预算靠它裁决。原先是扫消息列表数 final_output tool_call，而那个
	// 列表**含 prior**——开了会话历史层时上几轮的尝试会被算进本轮预算，**模型压根不被
	// 调用就抛 StructuredOutputError**（实测：Run 1 有过一次校验失败回炉，Run 2 换个全新
	// 问题、模型第一次就给合法 JSON，仍报「已尝试 2 次」）。
	//
	// 原注释说「数历史而非独立 counter」的动机是「cp 持久，跨 Resume 累积不重置」——
	// 那个顾虑对**普通变量**成立，对 channel 不成立：channel 本来就在 cp 里，跨 Resume
	// 自然累积，而新 Run 从零开始。既满足动机又不越界。
	basicFinalOutputAttemptsCh = compose_v2.Sum("__basic_agent_fo_attempts__")
	basicPreambleInputCh       = compose_v2.LastValue[*BasicAgentPreambleInput]("__basic_agent_preamble_input__")

	// basicSysPreambleCh：preamble 算出来的「agent 是什么」那一段（见
	// [BasicAgentPreambleOutput].System），由 model 节点在**每次**调用 LLM 前 prepend。
	//
	// **为什么必须是 channel**（跟 basicRevealedCh 同一组理由）：preamble 只跑首轮，
	// 而它的产出每轮都要用。放会话对象上则换 session 的续跑之后是空的——模型突然
	// 失去 persona 且**静默**（它照样会答，只是不再遵守任何指令）。channel state 随
	// checkpoint 走，这一条正是它要的。
	//
	// LastValue 而非 Append：preamble 一次算完整段，重跑覆盖即可；Append 会让
	// Resume 重跑 preamble 时累积成两份（`basicMessagesCh` 那边正是靠「一次性写完整」
	// 绕开同一个坑）。
	basicSysPreambleCh = compose_v2.LastValue[[]*schema.Message]("__basic_agent_sys_preamble__")
	// basicReturnDirectCh：compose_v2.OnceFlag——某 return-direct 工具被调用后写 true，tools 出口
	// 条件边据此短路终止 ReAct（见 ReturnDirectAnnotation）。
	basicReturnDirectCh = compose_v2.OnceFlag("__basic_agent_return_direct__")
	// basicToolsUsedCh：这个任务里模型「调过工具」了（tools 节点写，随存档走、跨 Resume 累积；新的 Run 从假开始）。
	// model 节点据它复位调用方强制的 tool_choice=required。按模型发起过调用算、不看结果：失败的、被拒的、调了不存在
	// 名字的都算。唯一不算的是**第一批**全是框架那件 tool_search、且解锁出了新工具的（见 basicSearchGraceCh）：检索
	// 只是把别的工具解锁出来，算的话开了渐进披露时强制额度就花在它上面，解锁的工具一次没调、模型凭自己知识作答
	// （审计实测）。只豁免一批：检索什么都没解锁的照算——不然检索不到时模型每轮只能再检索，一路被逼到软降落轮
	// （审计实测 6 次模型调用、算的话 2 次）；每批解锁一个都不算也不行——工具库比轮次预算大时（MCP Hub 几十上百个
	// 工具，关键词检索几乎什么词都命中）同样一路被逼到软降落（审计实测 8 个被延迟工具、预算 6 轮）。豁免一批之后
	// 多强制的至多一轮，跟工具库多大无关。业务自己写的同名工具照常算（按派发的实例认，见 toolSearchCallAnnotation）。
	basicToolsUsedCh = compose_v2.OnceFlag("__basic_agent_tools_used__")
	// basicSearchGraceCh：这个任务里已经豁免过一批检索（tools 节点写，随存档走）。之后任何一批都算「调过工具」。
	basicSearchGraceCh = compose_v2.OnceFlag("__basic_agent_search_grace__")
	// basicFinalRoundCh：最近一次模型调用是不是软降落轮（轮次预算的最后一轮，见 [withFinalRound]）。model 节点每轮写
	// （真假都写），tools 节点读出来交给工具函数：那一轮模型硬调的工具不执行。「是不是最后一轮」只在 model 节点判一次——
	// tools 节点按 round 自己再算一遍的话同一个判断就有两份，而且两边读到的 round 差一（model 节点写完才 +1）。随存档走：
	// 中断续跑直接进 tools 节点时，读到的仍是产出这批调用的那一轮的值。
	basicFinalRoundCh = compose_v2.LastValue[bool]("__basic_agent_final_round__")
	// basicRepeatChainCh：工具重复调用检测的计数链（见 repeatToolState），tools 节点成功后写；model / 闸门节点接进
	// 用户插话时写一条空链（插话之后的重复不算原地打转，见 Inbox）。三个节点不在同一超步，单写入者成立。
	basicRepeatChainCh = compose_v2.LastValue[*repeatToolState]("__basic_agent_repeat_chain__")
	// basicGateVetoesCh：这个任务里回合结束闸门已经否决过几次（闸门节点写，随存档走）。只在配了闸门时注册。
	basicGateVetoesCh = compose_v2.Sum("__basic_agent_gate_vetoes__")

	// basicCompactionCh：这个任务里上一次 AutoCompact 的结果（消息流的前 Upto 条由哪几条代表），模型节点
	// 成功后写、下一轮读，随存档走——同一个 Run 里不再每轮把整段消息流重新摘要。单写入者（只有模型节点写）。
	// 见 compaction_memo.go。
	basicCompactionCh = compose_v2.LastValue[*compactionMemo]("__basic_agent_compaction__")

	// basicFinalCh：**本次调用的最终产出**，由决策点在产生它的那一刻写入。
	//
	// 为什么要有它（而不是事后从 basicMessagesCh 倒扫找）：`basicMessagesCh` 里
	// **同时装着 prior（preamble 读进来的历史）和本轮产出**，倒扫找「最后一条无
	// tool_call 的 assistant」在本轮以 return-direct 收尾时会越过边界，把**上一次调用的
	// 答复**当成这次的答案。实测过，且三处一起中招：调用方拿到别人的答案、被再存一份
	// 进长期记忆（同一句话自我复制）、写进 append-only 历史。
	//
	// 加这条 channel 不是引入新范式——**其余 6 个编排本来就是这么做的**
	// （seqAgentLastOutputCh / routingAgentResultCh / handoffAgentOutputCh /
	// loopAgentLastOutCh / parAgentFinalCh / reflectFinalCh 全是「决策点写、出口读」），
	// 只有 BasicAgent 在倒扫，而它恰恰是唯一需要处理「prior 和本轮产出混在一起」的。
	//
	// **它结构上装不进 prior**：只有 model / tools 节点会写它。这是「防御」和「结构上
	// 不可能」的区别。
	//
	// 存的是**决策点产出的原始消息**：model 路径是那条 assistant，return-direct 路径是
	// 那条 tool_result（role=tool）。转成对外形态由消费方做——「要不要把它当结构化 JSON」
	// 这类判断只有消费方知道。
	basicFinalCh = compose_v2.LastValue[*schema.Message]("__basic_agent_final__")

	// basicInterjectionsCh：这个任务里接进消息流的运行中插话（见 Inbox），按接入顺序。model / 闸门节点接进
	// 插话时写，收尾时交给 postamble（BasicAgentPostambleInput.Interjections）——不让收尾回消息列表里按标记
	// 扫：那个列表含 prior，上一个任务的插话同样带着标记。
	basicInterjectionsCh = compose_v2.Append[*schema.Message]("__basic_agent_interjections__")
)

// GetBasicAgentRound 从 compose_v2.ChannelManager 读最后一轮 ReAct 轮次 — caller (agent_v2
// 层 BasicAgent.Run/Resume) 用此 helper 在 compose_v2.Run 返回后拿到 round 数,
// update 顶层 hook.AgentInfo.Round 让 OnAgentEnd hook 业务侧拿到真实最后轮值
// (不是 compose_v2.Run 入口初始 Round=0).
//
// Sub-5 audit HIGH-1 fix: 之前 agent_v2/basic_agent.go::compose_v2.Run defer OnAgentEnd
// 用 compose_v2.Run 入口 info{Round: 0} → 业务钩子在 OnAgentEnd 读到的轮次恒为 0
// 业务监控失真. 本 helper 让 agent_v2 layer 能从 graph state 拿真 round 数.
//
// cm 为 nil 时返 0 (业务防御性调用安全).
func GetBasicAgentRound(cm *compose_v2.ChannelManager) int32 {
	if cm == nil {
		return 0
	}
	round, _ := compose_v2.Get(cm.Channels(), basicRoundCh)
	return int32(round)
}

// NewBasicAgentBuilder 创建 BasicAgentBuilder——必须传 model + tools 函数。
//
// tools 可空（无 tool_call 场景，BasicAgent 退化为单轮 LLM 调用——但通常用
// 现有 SequentialBuilder 单步更直接）。
//
// **audit¹⁴ HIGH-1 撤回**: 加 isFrameworkNil panic 破坏 TestBasicAgentBuilder_Validators
// 现有 idiom (`NewBasicAgentBuilder(nil, nil)` 让 Compile validate 报 error). 业务 init 期允许
// untyped nil 由 Compile 阶段统一报 error (不是 panic). R29 教训九复证: 修复完成 ≠ 修对.
// typed nil interface 陷阱在此处低 ROI — Compile 内会 nil check 都报 error.
func NewBasicAgentBuilder(model BasicAgentModelFn, tools BasicAgentToolFn) *BasicAgentBuilder {
	return &BasicAgentBuilder{model: model, tools: tools}
}

// WithMaxRounds 设最大 ReAct 轮数防 LLM 死循环（默认 20）。
func (b *BasicAgentBuilder) WithMaxRounds(n int) *BasicAgentBuilder {
	b.maxRounds = n
	return b
}

// WithName 设 BasicAgent 名字——用于 hook AgentInfo + 日志。
func (b *BasicAgentBuilder) WithName(name string) *BasicAgentBuilder {
	b.name = name
	return b
}

// WithPreamble 设 preamble 节点函数——graph 首段跑业务准备消息（ISSUE-74 修）。
//
// 不传时 graph 拓扑保持现状 `compose_v2.StartNode → model → ...`（向后兼容）；
// 传入时 graph 拓扑变为 `compose_v2.StartNode → preamble → model → ...`，preamble 只跑首轮
// （tools→model 不经 preamble）。
//
// 业务在 preamble 内调 store.Search / HistoryCompactor / Skill 预激活等回调，产出
// [BasicAgentPreambleOutput] 的**两条流**：`Messages` 写进 basicMessagesCh（被四层压缩处置），
// `System` 写进独立 channel、作为 [BasicAgentModelFn] 的 `preamble` 参数交给 modelFn。
//
// ⚠️ **放错字段不会报错，只会静默变味**：把 system prompt 放进 `Messages`，它就成了
// 可被压缩层丢弃/摘要的普通消息；放进 `System` 则永不被压缩、但**你自己的 modelFn 必须
// 记得把它拼到请求最前**（它是个入参，躲不掉，但拼不拼、拼在哪由你决定）。
//
// **why preamble 节点而非 BasicAgent.Run 入口同步调用**：业务回调抛 compose_v2.InterruptError
// 时走 graph 内 SaveNS 协议自动存 cp，Resume 自然支持。compose_v2.Run 入口同步调用抛
// compose_v2.InterruptError 时 cp 没机会存，Resume 找不到 cp → HITL 链断（ISSUE-74）。
func (b *BasicAgentBuilder) WithPreamble(fn BasicAgentPreambleFn) *BasicAgentBuilder {
	b.preamble = fn
	return b
}

// WithPostamble 设 postamble 节点函数——graph 末段跑业务收尾回调（ISSUE-75 修）。
//
// 不传时 graph 拓扑保持现状 `model → ConditionalEdge → compose_v2.EndNode|tools`（向后兼容）；
// 传入时 graph 拓扑变为 `model → ConditionalEdge → postamble→compose_v2.EndNode | tools`，
// postamble 只在终止路径跑（无 tool_call 时）。
//
// 业务在 postamble 内调 store.Put / 业务自定义审计写入等回调，从入参 finalMsgs
// 拿最终 messages（含 last assistant）。回调抛 compose_v2.InterruptError 时框架自动 SaveNS cp，
// caller Resume 时 cm.Restore 重跑 postamble 节点自然续跑。
//
// **why postamble 节点而非 BasicAgent.Run 入口同步调用**：之前 commitFinal 跑在 graph
// 之外，graph_loop defer cleanupOnSuccess 已删整 sid 命名空间的 cp，此时 store.Put
// 抛 compose_v2.InterruptError 让 caller Resume 找不到 cp → HITL 链断（ISSUE-75）。节点化让
// postamble 走 graph 内 SaveNS 协议，cleanupOnSuccess 不触发（runErr != nil） →
// Resume 重跑 postamble 自然续跑。与 ISSUE-74 preamble 完全对称。
func (b *BasicAgentBuilder) WithPostamble(fn BasicAgentPostambleFn) *BasicAgentBuilder {
	b.postamble = fn
	return b
}

// WithAgentHooks 设业务 hook 实例——节点函数在生命周期触发回调。
//
// 时机：
//   - **OnAgentStart（8-method 协议）**：本 builder **不再触发**——OnAgentStart 整个
//     调用 1 次, 由 agent_v2.{编排}.Run/Resume top-level fire (跟 OnAgentEnd 单一所有权
//     对称). 业务直接用 NewBasicAgentBuilder 想要 agent-level hook
//     必须改用 agent_v2.NewBasicAgent. 原因: compose_v2.NodeFunc 闭包内 ctx 修改无法返出 compose_v2.Run scope,
//     业务 OTel push span pattern 失效. 每轮喂 LLM 的完整消息流观测走 OnModelStart.
//   - **OnAgentEnd（ISSUE-82 后协议变化）**：本 builder **不再触发**——agent-level
//     lifecycle 的"agent 整体结束"语义由 agent_v2 wrapper (`agent_v2.BasicAgent`)
//     的 compose_v2.Run defer 唯一负责（单一所有权设计）。compose_v2 直接用 builder
//     的业务（如 example/channel_smoke）若要 OnAgentEnd 必须自己 wrap defer，
//     **或**改用 agent_v2.BasicAgent。修改原因：原 ConditionalEdge cond fn 内
//     fire 与 agent_v2 compose_v2.Run defer 重复造成业务双触发 + cond fn 不跑 compose_v2.InterruptError
//     路径漏 fire。详见 doc/issue/issue-82-onagentend-double-trigger.md。
//   - OnModelStart/End：model 节点 fn 调用前后
//   - OnToolStart/End：tools 节点内每个 tool_call 前后
//   - OnError：节点错误时
//
// OnModelEndStream 在 agent_v2/basic_agent.go::streamOrGenerate 接入（ISSUE-73 P1）——
// 业务 hook wrap reader 可观测/转换/审计每个 chunk；聚合不受影响。
func (b *BasicAgentBuilder) WithAgentHooks(h hook.Hooks) *BasicAgentBuilder {
	b.hooks = h
	return b
}

// WithModelConfig 设算请求配置的函数：每次模型调用前调它，结果放进交给 OnModelStart 的 ModelInput.Config（本次
// 请求的模型名、温度等，观测层据此记 gen_ai.request.model 与请求参数）。它是 [只读] 视图：框架发请求不读它，钩子
// 改了不影响调用。BasicAgent 用 model.RequestConfig 按「按次的 model.WithModel 覆盖 > 客户端的 model.ModelNamer」算。
func (b *BasicAgentBuilder) WithModelConfig(fn func(ctx context.Context) *schema.ModelConfig) *BasicAgentBuilder {
	b.modelConfig = fn
	return b
}

// WithTurnGates 给这张 ReAct 图加回合结束闸门（语义见 [WithTurnGate]）：模型自然收尾时先过闸门节点，闸门的
// 反馈接进消息流、回到 model。gates 为空时不建闸门节点、不注册它的 channel，图跟不配时完全一样。
// maxVetoes <= 0 用 [DefaultTurnGateMaxVetoes]。
func (b *BasicAgentBuilder) WithTurnGates(gates []TurnGateFunc, maxVetoes int) *BasicAgentBuilder {
	b.turnGates = append([]TurnGateFunc(nil), gates...)
	b.turnGateMaxVetoes = maxVetoes
	return b
}

// isPendingUserInput 报 m 是不是一条模型还没回应的、框架接进消息流的用户输入：运行中插话或闸门反馈。
// 消息流以它结尾时 ReAct 还不能结束，得再调一次模型（见 model / 闸门节点的出边）。只认这两个标记，不按
// 角色认：钩子把答复改写成 user 角色的消息时，照旧当作收尾。
func isPendingUserInput(m *schema.Message) bool {
	if m == nil || m.Role != schema.RoleUser || m.Annotations == nil {
		return false
	}
	inter, _ := m.Annotations[InterjectionAnnotation].(bool)
	gate, _ := m.Annotations[TurnGateFeedbackAnnotation].(bool)
	return inter || gate
}

// Compile 编译为 compose_v2.Graph。
func (b *BasicAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if b.model == nil {
		return nil, fmt.Errorf("basic_agent: Model required")
	}
	maxRounds := b.maxRounds
	if maxRounds <= 0 {
		maxRounds = 20
	}

	model := b.model
	tools := b.tools
	hooks := b.hooks
	modelConfig := b.modelConfig
	// nil 防护: 业务自定义 hook pointer receiver + 传 nil pointer 时 hooks 是 typed nil
	// (interface 持非 nil type tag), `hooks == nil` 返 false 漏 fail-fast → 下游
	// hooks.OnModel* / OnTool* / OnError 调用 nil deref panic
	// (OnAgentStart/End 已移 wrapper top-level, BasicAgent 内不直接调).
	// 与 ParallelAgent / 其他 5 编排 (Sequential/Loop/Routing/Reflect/Handoff) 共
	// 7 编排统一 isFrameworkNil 协议 (builder 层).
	//
	// **跨包分层** (R30 audit B-MED-3 备注): agent_v2 wrapper 层 With*Hooks 独立 8 编排
	// 同款 (含 Plan + compose_v2.Graph) — 不同语义层独立检查, 见 agent_v2/config.go::compose_v2.WithHooks godoc.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "basic_agent"
	}
	backgroundNotes, finishToolResult, finalizeToolMemo := b.backgroundNotes, b.finishToolResult, b.finalizeToolMemo

	modelNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicMessagesCh)
		round, _ := compose_v2.Get(chs, basicRoundCh)
		// **每个任务至多一个软降落轮**（同一个预算下；续跑时调大了预算，新预算的最后一轮另算）：上一次模型调用就是
		// 软降落轮（basicFinalRoundCh），而 round 已越过预算 ⟹ 软降落轮用过了，不再调模型、直接报轮次耗尽。只有续跑
		// （含自动续上的存档）走得到这里：顶层 Run 上访问计数跟 round 同步，引擎的 maxVisit 先拦下；续跑时引擎的访问
		// 计数从 0 数，round 却随存档接着数。不挡的话，不认 tool_choice=none 的 provider 在软降落轮之后每一轮都硬调工具、
		// 被执行闸拦下（见 makeToolsFn）、又回到这里，直到本次续跑的访问上限用完：白调 maxRounds+1 次、没有收尾答复
		// （审计实测）。
		//
		// 只看「round 越过预算」不够：续跑时把预算调得比已用的轮数还小（WithResumeMaxRounds），round 是直接跳过预算的，
		// 软降落一次都还没给过——这时这一次就是软降落轮，照常调模型（审计实测：只看 round 的话一次都不调就报错）。
		//
		// 存档照常留下，跟引擎的 maxVisit 同款：调大这个 agent 的预算再续跑还能接着跑（顶层用 WithResumeMaxRounds；编排
		// 里的成员续跑不收按次覆盖，要用更大的 WithMaxRounds 重建成员）。放在 OnModelStart 之前：这一轮不调模型，模型
		// 钩子也不触发。
		if lastFinal, _ := compose_v2.Get(chs, basicFinalRoundCh); maxRounds > 0 && round > maxRounds && lastFinal {
			return nil, fmt.Errorf("basic agent %q: 轮次预算 %d 轮已用完、软降落轮也已用过，不再调用模型（要接着跑，调大这个 agent 的轮次预算再续跑）: %w",
				name, maxRounds, &compose_v2.MaxIterationsError{Iterations: round})
		}
		// 这一轮是不是软降落轮（轮次预算的最后一轮，见下面软降落那段）：只在这里判一次，写进 basicFinalRoundCh 交给 tools
		// 节点。要调模型时按 round 与本次的预算算；交出记录里那份答复时（下面）改用记录里的值。
		finalRound := maxRounds > 0 && round >= maxRounds
		// 这次执行里那次模型调用的记录（见 agent_v2 的 modelCallRecord）。定下了的（这一轮上一次已经处理完、只是那次
		// 执行在提交之前作废了）不再处理：不调模型、钩子不触发、不重记会话历史，交出定下的写入与决定。
		rec, hasRec := modelCallOf(ctx)
		if hasRec {
			finalRound = rec.FinalRound // 交出的是记录里那份答复：按它生成时是不是软降落轮（见 modelCallRecord.FinalRound）
		}
		if hasRec && rec.Final {
			compose_v2.EmitSignals(ctx, rec.Signals)
			noteHistoryRecorded(ctx, modelHistoryRecords(rec.Notes, rec.Inter, rec.Boundary, rec.Out, rec.Late))
			return append(basicModelWrites(rec.Notes, rec.Inter, rec.Out, rec.Late, rec.Compaction, rec.Binding), compose_v2.Write(basicFinalRoundCh, finalRound)), nil
		}
		// 只有原样的（上一次 OnModelEnd 中断或被叫停）沿用那次的答复、不再调模型，把 OnModelEnd 处理完。原样只在两种
		// 结局下留着：处理完（覆盖成定下的那份）、因为等人或被叫停停下；别的结局——钩子报错拒掉、把答复置空、panic
		// （钩子、事件投递）——一律作废，续跑重新生成。拿不准这份答复还能不能用时宁可重新生成：内容触发的钩子 panic
		// 留着原样的话，续跑永远过不去（审计实测）。
		keepRecord := !hasRec // 还没有记录时没东西可作废
		defer func() {
			if !keepRecord {
				// 这份答复作废、续跑重新生成：它已经交给前端的那些在这一刻撤回（同 streamOrGenerate 流中途失败时）。不撤的话
				// 续跑从存档续、编排成员重试从存档续时，前端留着被拒掉的那份（审计实测）
				if r, ok := modelCallOf(ctx); ok && r != nil {
					retractModelAnswer(ctx, r.Out, name)
				}
				clearModelCall(ctx)
			}
		}()
		// 运行中插话（见 Inbox）：这次调模型之前取走收件箱里到了的——工具结果之后、OnModelStart 与四层压缩之前（钩子
		// 与压缩都看得见它）。取走即记进节点私有态，这次执行随后失败 / 中断时续跑读回。沿用原样时用那一次的：模型是看着
		// 它们答的（这期间新到的留到下一次取）。
		var inter []*schema.Message
		if hasRec {
			inter = rec.Inter
		} else {
			var err error
			if inter, err = takeInterjections(ctx); err != nil {
				return nil, fmt.Errorf("interjections: %w", err)
			}
		}
		// 后台任务的结束通知（见 background_task.go）：追加在消息流末尾、排在 OnModelStart 之前（钩子看得到它），
		// 随这一轮的产出一起写进消息流与会话历史。送达不在这里记——之后的节点在已提交的消息流里看到它才算，
		// 这一轮没成就下次再发。定下了的用当时那几条：模型是看着它们答的。
		var notes []*schema.Message
		switch {
		case hasRec:
			notes = rec.Notes
		case backgroundNotes != nil:
			notes = backgroundNotes(ctx, msgs)
		}
		if len(notes)+len(inter) > 0 {
			msgs = append(append(append(make([]*schema.Message, 0, len(msgs)+len(notes)+len(inter)), msgs...), notes...), inter...)
		}
		// 工具渐进披露的已解锁集：channel 是权威副本（随 cp 走），经 ctx 递给 modelFn
		// ——它的签名是 (ctx, msgs)、拿不到 channel，见 withRevealedFromChannel。
		// 没有也推（nil）：盖掉从调用方继承来的那份——父 agent 模型回调里调起的子 agent 不该拿到父的揭示集。
		revealed, _ := compose_v2.Get(chs, basicRevealedCh)
		ctx = withRevealedFromChannel(ctx, revealed)
		info := &hook.AgentInfo{Name: name, Type: "base", Round: int32(round)}

		// hook: OnModelStart（hook 返 Interrupt/ctx.Err 不上报 OnError）
		// 8-method 协议：OnAgentStart 不在此触发——它整个调用 1 次, 由
		// agent_v2.BasicAgent.Run/Resume top-level fire（跟 OnAgentEnd 同函数 scope,
		// 业务 OTel ctx-push pattern 真生效）。每轮喂 LLM 的完整消息流观测走 OnModelStart。
		// 把本次调用累计的 final_output 尝试次数透给 modelFn 做预算裁决——它拿不到
		// channels，而让它自己数消息就得扫含 prior 的列表（正是要摆脱的做法）。
		foAttempts, _ := compose_v2.Get(chs, basicFinalOutputAttemptsCh)
		ctx = withFinalOutputAttempts(ctx, foAttempts)
		// 「agent 是什么」那一段：channel 是权威副本（随 cp 走），作为 ModelInput 的
		// **独立字段**交给 hook——不跟 Messages 合并。
		//
		// 合并过一版（打 marker 拼进去、hook 返回后按 marker 劈回来），实测两处静默错：
		// 业务插在前言**之前**的消息劈完跑到了它**之后**（跨半的相对顺序在分区时丢掉）；
		// 业务造一条新 system 想顶掉前言时**两条都发出去**。两者都是「N=1 对、N≥2 静默错」。
		// 见 schema.ModelInput.Preamble 的 godoc。
		sysPre, _ := compose_v2.Get(chs, basicSysPreambleCh)

		modelIn := &schema.ModelInput{Preamble: sysPre, Messages: msgs}
		if modelConfig != nil {
			modelIn.Config = modelConfig(ctx)
		}
		if hasRec {
			// 沿用原样：这一遍没有模型调用，按调用计数、计时的钩子据此不再记一次（见 hook.ModelResponseReusedKey）
			modelIn.Extra = map[string]any{hook.ModelResponseReusedKey: true}
		}
		streamMsgs := msgs // 钩子改写之前的消息流——压缩边界只在钩子没动它时写（见下）
		// 上一次摘要的结果（见 basicCompactionCh）；modelFn 这一轮又摘要了就填 out。
		memoIn, _ := compose_v2.Get(chs, basicCompactionCh)
		memo := &compactionMemoCtx{in: memoIn}
		var out, boundary *schema.Message
		var binding *toolBinding // 这一轮交给模型的工具，没有裁剪时为 nil（见 toolBinding）
		// 处理这次模型调用时做出的上行信号收在一起（钩子与模型里做出的，多数为空）：定下时连同产出记下，续跑重放定下的
		// 那一轮时交出当时那份（那时钩子不再触发）。
		sigs, err := compose_v2.CaptureSignals(ctx, func(segCtx context.Context) error {
			// 按 compose_v2.CallHook 的规则采纳：钩子返回别处缓存的 ctx 时，模型的答复照样记进
			// 这次 Run 的会话历史、之后的转交照样交给这次执行（曾经直接换成钩子的，两样都静默丢，实测）。
			//
			// 采纳之后的 ctx 只用于这次模型调用与 OnModelEnd（钩子返回的 ctx 只作用于它包住的那次调用，同工具钩子，
			// 见 BasicAgentToolFn）；节点后面的工作（记历史、写 channel）用节点自己的 ctx。
			callCtx, startErr := compose_v2.CallHook(segCtx, func(c context.Context) (context.Context, error) {
				return hooks.OnModelStart(modelHookCtx(c, "OnModelStart"), info, modelIn)
			})
			if startErr != nil {
				if compose_v2.MustPropagate(segCtx, startErr) {
					keepRecord = true // 等人或被叫停：沿用的原样留着
				}
				if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && segCtx.Err() == nil {
					hooks.OnError(modelHookCtx(segCtx, "OnError"), info, &schema.CommError{Code: "model_start_error", Message: startErr.Error()})
				}
				return fmt.Errorf("OnModelStart: %w", startErr)
			}
			// 两个字段都重新读（都是 [消费] 档，hook 可能改）：
			// 前言作为 modelFn 的 `preamble` 入参往下传，由它在四层压缩全跑完之后才拼到
			// 请求上；Messages 就是被压缩的那条流。
			// 钩子可能留下 nil（往末尾 append 一个零值、整段换成含 nil 的切片）：压缩层分组时跳过 nil、按条数算位置的
			// 地方就差一位（备忘的 Upto 会多算一条，之后模型输入里出现找不到声明者的 tool_result），provider 的消息
			// 转换遇到 nil 也会 panic。在这里去掉，下面每一层都不必再防。
			msgs = dropNilMessages(modelIn.Messages)
			// hook 可能往 Preamble 里塞 nil（`append` 一个零值、或整段换成含 nil 的切片）。
			// 它会一路走到 provider 的消息转换里——实测真 openai client 在那里 panic。
			modelIn.Preamble = dropNilMessages(modelIn.Preamble)

			// **软降落**：这是轮次预算的最后一轮 ⟹ 标记 ctx，让 modelFn 关掉这一轮的工具调用（见 [withFinalRound]）；
			// 同一个值写进 basicFinalRoundCh，tools 节点据它不执行这一轮模型硬调的工具。
			//
			// round 从 0 开始数（Sum channel），每完成一次模型调用 +1；maxVisit = maxRounds+1，所以一个任务里调模型时
			// round 取值 0..maxRounds（顶层 Run 由引擎的 maxVisit 保证，续跑由节点开头那道闸保证）。
			// `round >= maxRounds` 即最后那一次。
			// 那一格本来就是为「无 tool 的 final 调用」预留的，只是原先靠模型自己选择不调工具。
			//
			// 这两个标记每轮都推（为假也推）：这一轮的模型回调里调起的子 agent 要拿到假，不是父的真。
			callCtx = withFinalRound(callCtx, finalRound)
			// 这个任务里模型「调过工具」没有（见 basicToolsUsedCh；随存档走，中断续跑重跑这一轮时读到
			// 的还是同一个值）。
			toolsUsedSoFar, _ := compose_v2.Get(chs, basicToolsUsedCh)
			callCtx = withToolsUsed(callCtx, toolsUsedSoFar)
			callCtx = withCompactionMemo(callCtx, memo)
			if hasRec {
				// 上一次模型调用成功之后、OnModelEnd 处理完之前停下了（中断、取消）：沿用那次的答复，不再调模型——钩子
				// 问过人的话，用户答的就是这一份（见 modelCallRecord）
				out, memo.out, boundary, binding = rec.Out, rec.Compaction, rec.Boundary, rec.Binding
			} else {
				var err error
				// modelFn 绑工具时把这一轮交给模型的那些填进来（见 toolBinding）
				sink := &toolBinding{}
				out, err = model(withToolBindingSink(callCtx, sink), modelIn.Preamble, msgs)
				if sink.Restricted {
					binding = sink
				}
				if err != nil {
					// audit Round 7 Bug 3 修复：compose_v2.InterruptError 是控制流不是错误, 不上报 OnError
					if _, isInt := compose_v2.IsInterrupt(err); !isInt {
						hooks.OnError(callCtx, info, &schema.CommError{Code: modelCallErrCode(callCtx, err), Message: err.Error()})
					}
					return fmt.Errorf("model: %w", err)
				}
				if out == nil {
					err := fmt.Errorf("model returned nil message")
					hooks.OnError(callCtx, info, &schema.CommError{Code: "model_nil_output", Message: err.Error()})
					return err
				}
				// 压缩边界在这一刻定（它只看这一轮的摘要与 OnModelStart 钩子动没动消息列表）：沿用原样续跑时 OnModelStart
				// 再触发一次，它这次动没动跟那次答复无关。
				//
				// **只在 OnModelStart 钩子没动消息列表时写**：压缩是在钩子改写之后的那份上做的，钩子插进来的、
				// 改写过的消息会跟着进替代历史、在下一次 Run 里当成历史重放（每轮插一条「当前时间」的钩子，
				// 下一次 Run 就看到两条）。钩子动了就不写，下一次 Run 照旧读全量。
				if memo.out != nil && sameMessageSlice(streamMsgs, msgs) {
					boundary = buildCompactionBoundary(memo.out, msgs, GetHandoffHistoryMode(ctx))
				}
				// 模型调用成功那一刻记下原样：OnModelEnd 因为要等人或被叫停停下时，续跑沿用这一份（见 modelCallRecord）。
				// 记在交给 OnModelEnd 之前——存下的字节是钩子改写之前当场序列化的。
				saveModelCall(ctx, &modelCallRecord{Notes: notes, Inter: inter, Out: out, Compaction: memo.out, Boundary: boundary, Binding: binding, FinalRound: finalRound})
				keepRecord = false
			}

			// hook: OnModelEnd
			// **Round 8 实际业务集成验证 fix (真 deepseek 暴露)**: 真 LLM (openai/deepseek
			// client) 把 token usage 填在 Message.Metadata.Usage (model/openai/client.go:256),
			// framework 必须复制到 ModelOutput.Usage 让业务 OnModelEnd hook 读 out.Usage 拿到
			// token. 之前 `ModelOutput{Message: out}` 漏复制 → 真 LLM 下 out.Usage 永 nil →
			// observability token metric (gen_ai.llm.tokens.total) + 业务 cost 计算全失效.
			// mock 模式 + Sub-5c 单测直接构造 ModelOutput{Usage:...} 绕开此真实路径未暴露 —
			// 8 轮 audit + 单测全绿没抓到, 真 LLM 端到端集成验证才暴露 (印证 CLAUDE.md
			// "mock 闭包绕开真实路径不算端到端覆盖").
			modelOut := &schema.ModelOutput{Message: out}
			if hasRec {
				modelOut.Extra = map[string]any{hook.ModelResponseReusedKey: true}
			}
			// 这是哪次模型调用（见 hook.ModelRunIDKey）：钩子可能换掉 Message，按消息标注就认不出了
			if rid, _ := out.Annotations[compose_v2.RunIDAnnotationKey].(string); rid != "" {
				if modelOut.Extra == nil {
					modelOut.Extra = map[string]any{}
				}
				modelOut.Extra[hook.ModelRunIDKey] = rid
			}
			// 一次模型调用的用量只交给 OnModelEnd 一次：沿用原样时，上一次 OnModelEnd 停下之前排在前面的钩子已经收到过
			// （观测链装在最前），再带一次就是一次调用报两次。
			if out.Metadata != nil && out.Metadata.Usage != nil && !hasRec {
				modelOut.Usage = out.Metadata.Usage
			}
			// OnModelEnd 的返回值不往后流（同 OnToolEnd / OnNodeEnd）。
			_, hookErr := compose_v2.CallHook(callCtx, func(c context.Context) (context.Context, error) {
				return hooks.OnModelEnd(modelHookCtx(c, "OnModelEnd"), info, modelOut)
			})
			if hookErr != nil {
				// 等人或被叫停（中断、取消）：留着原样，续跑沿用；钩子报错拒掉了这份答复：作废（见上面的 defer），续跑重新生成。
				if compose_v2.MustPropagate(segCtx, hookErr) {
					keepRecord = true
				}
				if _, isInt := compose_v2.IsInterrupt(hookErr); !isInt && segCtx.Err() == nil {
					hooks.OnError(modelHookCtx(callCtx, "OnError"), info, &schema.CommError{Code: "model_end_error", Message: hookErr.Error()})
				}
				return fmt.Errorf("OnModelEnd: %w", hookErr)
			}
			preHookMsg := out      // 非 nil（:328 已保证 model 自身输出非 nil）
			out = modelOut.Message // hook 可能替换
			if out == nil {
				// **对称 :328（model 自身 nil）+ :475（OnToolEnd 置 nil）守卫**：OnModelEnd 的
				// Message 是 [消费] 档业务可改写，但不得置 nil——ReAct 循环需 assistant 消息续跑。
				// 漏防时 hook 置 nil 走两条坏路径：(1) 下方 reset_stream 的 len(out.Content) 解
				// 引用 nil panic（块内 :360 已守卫 out != nil，此处遗漏）；(2) 即便不 panic，
				// 下游 compose_v2.Write(basicMessagesCh, [nil]) 让 cond fn GetToolCalls(nil) 静默终止对话、
				// 最终答复变空。统一 fail-fast 返干净 error 让业务定位是自己 hook 置了 nil。
				err := fmt.Errorf("OnModelEnd hook set Message to nil")
				hooks.OnError(callCtx, info, &schema.CommError{Code: "model_end_nil_message", Message: err.Error()})
				return err
			}
			// out / preHookMsg 现都保证非 nil——条件简化 + 下方 len(out.Content) 安全
			//
			// 钩子换上的消息仍是这次模型调用的答复：盖上这次调用的 run_id（下面的改写信号、tools 节点给工具调用标的出处都从
			// 它取）。最自然的写法 `out.Message = &schema.Message{...}` 不带标注，曾经改写信号的 run_id 为空、前端与对端对不上
			// 原来那条消息，没脱敏的原文留着（审计实测，经 A2A 一跳宿主拿到的也是原文）。盖在消息与标注表的拷贝上，钩子交来的
			// 对象不动：护栏钩子常返回一条共享的固定回复（out.Message = blocked），在它身上盖章会让并发的调用互相改写、
			// 各自的改写信号对不上自己那条（-race 实测）。
			if rid, _ := preHookMsg.Annotations[compose_v2.RunIDAnnotationKey].(string); rid != "" {
				if cur, _ := out.Annotations[compose_v2.RunIDAnnotationKey].(string); cur != rid {
					ann := make(map[string]any, len(out.Annotations)+1)
					for k, v := range out.Annotations {
						ann[k] = v
					}
					ann[compose_v2.RunIDAnnotationKey] = rid
					stamped := *out
					stamped.Annotations = ann
					out = &stamped
				}
			}
			if out != preHookMsg || out.Content != preHookMsg.Content {
				if sink := compose_v2.GetEventSink(segCtx); sink != nil {
					runID := ""
					if out.Annotations != nil {
						if v, ok := out.Annotations[compose_v2.RunIDAnnotationKey].(string); ok {
							runID = v
						}
					}
					// 不是作废、是替换：带上新答复的正文（replacement），前端与对端用它替换那条消息。只有清片段的信号时，适配层
					// 无从得知新内容，按作废处理会让整条答复消失（审计实测）
					sink.Send(compose_v2.Event{
						Type:    "reset_stream",
						Content: "OnModelEnd hook 改写 Message，前端应清老 chunks buffer + 渲染新 final.Content",
						Metadata: map[string]any{
							"reason":      "hook_overrode_message",
							"run_id":      runID,
							"agent":       name,
							"new_len":     len(out.Content),
							"replacement": out.Content,
						},
					})
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		// 模型给出了最终答复、而这一轮之后还能再调模型：再看一眼收件箱，答复期间到的插话接在答复后面，出边据此
		// 回到 model（见 isPendingUserInput）——迟到的话不留到下一次 Run。最后一轮不取（没有下一轮回应它，留在
		// 收件箱里给调用方）。取在定下这一轮之前：定下的那份连同它们一起重放。
		var late []*schema.Message
		if len(schema.GetToolCalls(out)) == 0 && !finalRound {
			all, err := takeInterjections(ctx)
			if err != nil {
				return nil, fmt.Errorf("interjections: %w", err)
			}
			late = all[len(inter):]
		}

		// 会话历史（opt-in）：本轮 LLM 产出（连同接进来的插话——那是用户的原话）。放在这里而不是节点入口——
		// 节点入口那时还不知道这一轮会不会失败/中断，写了就得在 Resume 重跑时再写一遍。
		//
		// 这一轮真摘要了一次时，先记一条压缩边界（这一刻的完整替代历史，模型调用那一刻定的，见上），再记本轮产出——
		// 下一次 Run 从这条边界往后读，见 compaction_boundary.go；排列见 modelHistoryRecords。
		//
		// 定下这一轮（见 modelCallRecord），在记会话历史之前：重放时据它跳过记历史——这次执行之后作废的话，续跑时
		// 历史里已经有这一份。
		saveModelCall(ctx, &modelCallRecord{Notes: notes, Inter: inter, Late: late, Out: out, Compaction: memo.out, Boundary: boundary, Binding: binding, Final: true, Signals: sigs, FinalRound: finalRound})
		keepRecord = true
		compose_v2.EmitSignals(ctx, sigs)
		recordHistory(ctx, historyOwner(ctx, name), name, modelHistoryRecords(notes, inter, boundary, out, late))
		return append(basicModelWrites(notes, inter, out, late, memo.out, binding), compose_v2.Write(basicFinalRoundCh, finalRound)), nil
	}

	toolsNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if tools == nil {
			return nil, fmt.Errorf("tools called but BasicAgent has no tools fn")
		}
		// **已解锁集在这里也要灌一次**，不能只在 modelNode 灌。
		//
		// 中断续跑是从存档恢复 frontier **直接进本节点**的，modelFn 这一轮根本没跑；
		// 而续跑若没带上原 session（跨进程 / 新请求的常态），会话里那份缓存是空的。
		// 于是本节点的 fail-closed 检查会把模型正要调的、上一轮明明解锁过的工具拦下
		// ——而失败是静默的：模型收到「未解锁，请先检索」，自己换个说法收尾。
		//
		// 实测形态：延迟披露的工具**自己中断**（工具内 Ask / 审批 gate）时，用户答完
		// 续跑，那件工具的函数体一次都不再执行，final 却看着一切正常。
		revealed, _ := compose_v2.Get(chs, basicRevealedCh)
		ctx = withRevealedFromChannel(ctx, revealed) // 没有也推，理由同 model 节点
		// 产出这批调用的那次模型调用是不是软降落轮（见 basicFinalRoundCh）：是的话工具函数不执行这一批（框架那件
		// final_output 除外，见 makeToolsFn）。为假也推，理由同 model 节点——父 agent 软降落轮的模型回调里调起的子 agent，
		// 自己的工具要照常执行。
		finalRound, _ := compose_v2.Get(chs, basicFinalRoundCh)
		ctx = withFinalRound(ctx, finalRound)
		// 这一批 tool_call 出自哪一轮的绑定（见 toolBinding）：执行闸只放行那一轮交给模型的业务工具。从 channel 读，
		// 续跑直接进本节点时也是当初那份。没有也推（nil），盖掉从调用方继承来的那份。
		binding, _ := compose_v2.Get(chs, basicToolBindingCh)
		ctx = withToolBinding(ctx, binding)
		// 这一批里检索解锁的名字记在这次执行上（工具体拿不到 channel 写入口），节点成功后写进 basicRevealedCh；
		// 节点失败或中断时随这次执行一起丢掉，续跑重跑这一批时检索重新记（tool_search 不进工具账）。每次执行一份新的，
		// 也盖掉从调用方继承来的那份——父 agent 工具里调起的子 agent 不该记进父的这一批。
		turn := &revealTurn{}
		ctx = withRevealTurn(ctx, turn)
		// 重复调用的计数链同理：从 channel 拷一份给这一批，节点成功后写回（见 repeatToolState）。
		repeatChain, _ := compose_v2.Get(chs, basicRepeatChainCh)
		repeat := newRepeatTurn(repeatChain)
		ctx = withRepeatTurn(ctx, repeat)
		msgs, _ := compose_v2.Get(chs, basicMessagesCh)
		round, _ := compose_v2.Get(chs, basicRoundCh)
		info := &hook.AgentInfo{Name: name, Type: "base", Round: int32(round)}

		if len(msgs) == 0 {
			return nil, fmt.Errorf("tools: no messages in history")
		}
		last := msgs[len(msgs)-1]
		toolCalls := schema.GetToolCalls(last)
		if len(toolCalls) == 0 {
			return nil, fmt.Errorf("tools: last message has no tool_calls")
		}
		// 这一批出自哪次模型调用（见 ToolModelRunIDExtraKey）
		modelRunID, _ := last.Annotations[compose_v2.RunIDAnnotationKey].(string)

		// 每个 tool_call 触发 OnToolStart——消费 hook 对 Name / ArgumentsJSON 的改写
		// 后再调用业务工具（参数注入 / 重定向）。CallId 是 tool_call ↔ tool_result
		// 配对锚点，hook 改它会破坏配对（OpenAI 422），故传给 hook 观测但不回写消费。
		//
		// **Phase 1 fix**: 业务 tool 实现 tool.Categorizer 接口时, agent_v2.BasicAgent
		// 入口已通过 compose_v2.WithToolCategoryLookup push lookup 函数到 ctx, 这里 fire OnToolStart
		// 之前查 lookup 填 ti.Extra["category"] 让 observability 创对应 span kind
		// (retrieval / embedding / guardrail). lookup 返空字符串时业务
		// 未实现 Categorizer, ti.Extra 不填, observability 走默认 tool kind.
		categoryLookup := compose_v2.GetToolCategoryLookup(ctx)
		// 这一批上一次已经处理完、只是那次执行在提交之前作废了的那几个调用（见 agent_v2 的 fixedToolResults）：续跑时不再
		// 处理——工具不执行（工具账命中）、OnToolStart / OnToolEnd 都不触发，结论与钩子做出的决定用定下时那份。
		fixedCalls, fixedHookSigs := fixedToolResults(ctx, toolCalls)
		isFixed := func(i int) bool { return i < len(fixedCalls) && fixedCalls[i] }
		// unfixed 去掉定下了的那几个：它们的 OnToolStart 没触发，失败路径上也不补 OnToolEnd（见 endSettledToolCalls）
		unfixed := func(ends []toolCallEnd) []toolCallEnd {
			out := make([]toolCallEnd, len(ends))
			for j := range ends {
				if !isFixed(j) {
					out[j] = ends[j]
				}
			}
			return out
		}
		// 每个调用持有自己的 ctx：OnToolStart(i) 采纳之后的 ctx 只用于调用 i 的执行与 OnToolEnd(i)。每个 OnToolStart
		// 都从节点的 ctx 起算，看不到别的调用的钩子放的值；节点后面的工作用节点自己的 ctx（见 BasicAgentToolFn）。
		callCtxs := make([]context.Context, len(toolCalls))
		// 这一批上次已经派发过（内容已定下）：续跑时的再次派发，OnToolStart 带上标记（见 ToolRedispatchedExtraKey）
		redispatched := toolCallsDispatchedBefore(ctx)
		for i := range toolCalls {
			if isFixed(i) {
				callCtxs[i] = ctx
				continue
			}
			ti := &schema.ToolInput{
				Name:          toolCalls[i].Function.Name,
				ArgumentsJSON: toolCalls[i].Function.Arguments,
				CallId:        toolCalls[i].ID,
			}
			if redispatched {
				ti.Extra = map[string]any{ToolRedispatchedExtraKey: true}
			}
			ti.Extra = withModelRunID(ti.Extra, modelRunID)
			if categoryLookup != nil {
				if cat := categoryLookup(ti.Name); cat != "" {
					if ti.Extra == nil {
						ti.Extra = make(map[string]any)
					}
					ti.Extra["category"] = cat
				}
			}
			// 业务钩子不是节点体：每次钩子调用在自己的相位里跑（见 toolHookCtx）；采纳钩子返回的
			// ctx 时把节点体自己的那套相位铺回来。
			callCtx, startErr := compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
				return hooks.OnToolStart(toolHookCtx(c, "OnToolStart", toolCalls[i].Function.Name, ti.CallId, i), info, ti)
			})
			if startErr != nil {
				if _, isInt := compose_v2.IsInterrupt(startErr); !isInt && ctx.Err() == nil {
					hooks.OnError(toolHookCtx(ctx, "OnError", toolCalls[i].Function.Name, ti.CallId, i), info, &schema.CommError{Code: "tool_start_error", Message: startErr.Error()})
				}
				return nil, fmt.Errorf("OnToolStart: %w", startErr)
			}
			// 消费改写：Name / ArgumentsJSON 写回 toolCalls 让下游 tools() 用改写值调用。
			// toolCalls 是 GetToolCalls 返的副本，改它不污染历史 assistant message。
			// CallId 不回写（配对锚点只读）。
			toolCalls[i].Function.Name = ti.Name
			toolCalls[i].Function.Arguments = ti.ArgumentsJSON
			callCtxs[i] = callCtx
		}
		// 改写只在这一批第一次交给工具执行时生效：之后的执行换回那一次定下的内容（见 fixToolCallContent）。下面的
		// 工具执行、OnToolEnd、这批决定的裁决都用这一份。
		fixToolCallContent(ctx, toolCalls, schema.GetToolCalls(last))

		// 调用业务 tools fn。盖章：这里跑的工具的产出会被本节点记成 tool_result（见
		// withToolResultRecorder）——AgentAsTool 据此不在边界补记历史。只盖在工具这一段上（这一批的 ctx 与
		// 每个调用执行用的 ctx）：钩子里的调用不是工具本身，交给 OnToolEnd 的 callCtxs 不带章。
		// 收集器：框架自己的 toolsFn 把每个调用收到的上行信号登记在这里，OnToolEnd 跑完之后再定（见
		// tool_batch_signals.go）。
		batch := &toolBatch{}
		execCtxs := make([]context.Context, len(callCtxs))
		for i, c := range callCtxs {
			execCtxs[i] = withToolBatch(withToolResultRecorder(c), batch)
		}
		results, err := tools(withToolBatch(withToolResultRecorder(ctx), batch), toolCalls, execCtxs)
		if err != nil {
			// 这一批没整体成功：已经有结局的调用先各自收尾（OnToolEnd），挂起的、被停下的不收（见 endSettledToolCalls）
			endSettledToolCalls(ctx, hooks, info, toolCalls, callCtxs, unfixed(batch.ends), 0)
			// audit Round 7 Bug 3: compose_v2.InterruptError 是控制流, 不上报 OnError (HITL ask_user)
			if _, isInt := compose_v2.IsInterrupt(err); !isInt {
				// 业务自写的 toolsFn 交不出各调用的结局，OnError 是钩子给 OnToolStart 里开的东西（span 等）收尾的唯一机会。
				// 只有一个调用时失败的就是它，交它的 ctx；好几个调用时分不出是哪一个，交节点的。
				errCtx := ctx
				if len(callCtxs) == 1 {
					errCtx = callCtxs[0]
				}
				hooks.OnError(toolHookCtx(errCtx, "OnError", "", "", -1), info, &schema.CommError{Code: ToolCallErrCode, Message: err.Error()})
			}
			return nil, fmt.Errorf("tools: %w", err)
		}

		// 每个 tool_result 触发 OnToolEnd——消费 hook 对 Result 的改写后写回 LLM 历史
		// （结果脱敏 / 注入缓存）。CallId 是配对锚点，传给 hook 观测但不回写消费。
		//
		// failedAfterHook 记钩子改写之后这次调用成没成——它做出的转交 / 路由决定算不算看的是这个。
		// hookSignals 是每次 OnToolEnd 做出的上行信号，排在这批调用的决定之后交（见循环后）。
		failedAfterHook := make([]bool, len(toolCalls))
		// codeAfterHook 记钩子之后的错误码（钩子可能改了它；消息上的失败标注是钩子之前的）；pendingSize 记哪些结果的
		// 正文还没按大小处理（开了转存时，见 agent_v2/tool_render.go），钩子与批内裁决之后统一处理。
		codeAfterHook := make([]string, len(toolCalls))
		pendingSize := make([]bool, len(toolCalls))
		for i, r := range results {
			if i < len(pendingSize) && r != nil {
				pendingSize[i], _ = r.Annotations[toolResultSizePendingAnnotation].(bool)
			}
		}
		// preHookFailure 记钩子之前那次失败的内容：钩子把失败改成成功时，报出来的原因用它。
		preHookFailure := make([]string, len(toolCalls))
		hookSignals := make([]compose_v2.Signals, 0, len(toolCalls))
		for i, r := range results {
			if i >= len(toolCalls) {
				break
			}
			// 还原成交给 OnToolEnd 的 ToolOutput（失败的结构化信息、审批拒绝、从工具账重放的，见 toolOutputFor）
			to := toolOutputFor(r, toolCalls[i].ID, i < len(batch.ends) && batch.ends[i].replayed)
			to.Extra = withModelRunID(to.Extra, modelRunID)
			res := to.Result
			before := *res // 钩子之前那份（值拷贝：钩子可能就地改 res）
			if !before.Success {
				preHookFailure[i] = before.Output // 失败时 Output 就是渲染好的 `[code] msg`
			}
			// 定下了的不再处理（OnToolStart 也没触发，见上）：结论与钩子做出的决定用定下时那份。
			if isFixed(i) {
				hookSignals = append(hookSignals, fixedHookSigs[i])
				failedAfterHook[i], codeAfterHook[i] = !before.Success, before.ErrorCode
				continue
			}
			// 钩子里做出的决定单独收：它是看过这次调用的结果之后才做的，要排在这批调用的决定之后交。
			// 直接交进本节点这次执行的话，批的决定在全部 OnToolEnd 跑完之后才交，「后交的为准」会让
			// 工具更早做出的决定盖掉钩子更晚做出的（审计实测：工具定 alpha、钩子看过结果改道 beta，
			// 走的是 alpha）。
			//
			// OnToolEnd(i) 收的是这个调用自己的 ctx（OnToolStart(i) 采纳之后的），它的返回值不往后流——下一个调用的
			// OnToolEnd、节点后面的工作都看不到（同 OnNodeEnd / OnGraphEnd）。
			var hookSigs compose_v2.Signals
			_, hookErr := compose_v2.CallHook(callCtxs[i], func(c context.Context) (context.Context, error) {
				var newCtx context.Context
				var err error
				hookSigs, _ = compose_v2.CaptureSignals(toolHookCtx(c, "OnToolEnd", toolCalls[i].Function.Name, to.CallId, i),
					func(cc context.Context) error {
						newCtx, err = hooks.OnToolEnd(cc, info, to)
						return err
					})
				return newCtx, err
			})
			if hookErr != nil {
				// 这一批随之作废：排在后面的调用还没收到 OnToolEnd，按失败路径收尾（不然续跑时它们从工具账重放、带着重放
				// 标记，却从没收过尾——它们唯一一次真实执行就没人记了）。先收尾再报 OnError：OnError 不知道是哪个调用出的错，
				// 钩子会把还在飞的调用都算上，收尾在后的话它们跑成功了也被记成失败
				rest := make([]toolCallEnd, len(toolCalls))
				for j := i + 1; j < len(toolCalls) && j < len(results); j++ {
					rest[j] = toolCallEnd{msg: results[j], replayed: j < len(batch.ends) && batch.ends[j].replayed}
				}
				endSettledToolCalls(ctx, hooks, info, toolCalls, callCtxs, unfixed(rest), i+1)
				if _, isInt := compose_v2.IsInterrupt(hookErr); !isInt && ctx.Err() == nil {
					hooks.OnError(toolHookCtx(callCtxs[i], "OnError", toolCalls[i].Function.Name, to.CallId, i), info, &schema.CommError{Code: "tool_end_error", Message: hookErr.Error()})
				}
				return nil, fmt.Errorf("OnToolEnd: %w", hookErr)
			}
			hookSignals = append(hookSignals, hookSigs)
			// 消费改写：用改写后的 Result 重写 results[i] 的 content（in-place 改
			// Message.Content + Toolresult.Content，保留 Annotations 不丢 marker，
			// 避免破坏 4 层压缩跨层防护）。CallId 不回写（配对锚点只读）。
			if to.Result != nil {
				content, failed, errMsg := renderHookedToolResult(before, to.Result)
				r.Content = content
				if len(r.ContentBlocks) > 0 && r.ContentBlocks[0].Toolresult != nil {
					r.ContentBlocks[0].Toolresult.Content = content
					// 失败标记跟着模型看到的结论走（钩子可能把失败改成成功，反之亦然）
					r.ContentBlocks[0].Toolresult.IsError = failed
				}
				// 失败标注跟着钩子之后的结论走：它们随消息进存档与会话历史，留着钩子之前那份的话，钩子做的脱敏对它们
				// 不生效——钩子把带秘密的大失败脱敏成一句话、或改成成功之后，标注里还是原文（审计实测）。
				if failed {
					markToolFailure(r, to.Result.ErrorCode, errMsg)
				} else if r.Annotations != nil {
					delete(r.Annotations, toolErrorCodeAnnotation)
					delete(r.Annotations, toolErrorMessageAnnotation)
				}
				failedAfterHook[i], codeAfterHook[i] = failed, to.Result.ErrorCode
			} else {
				failedAfterHook[i], codeAfterHook[i] = !before.Success, before.ErrorCode
			}
		}

		// 这批 tool_call 做出的决定在这里定：钩子改写之后是失败的作废，同一件事上第一个生效、后面
		// 给出不同值的那几个换成失败结果（见 tool_batch_signals.go）。在记历史与写回之前——被盖掉的
		// 那几个要以失败结果进历史、给模型看。
		preBatch := append([]*schema.Message(nil), results...)
		if batch.filled {
			finishToolBatchSignals(ctx, toolCalls, results, batch.perCall, failedAfterHook)
		}
		// 按大小处理（转存 / 截断）在钩子与裁决之后：转存的是钩子改写之后、模型被允许看的那份。被裁决换成失败结果的
		// 那几条（带着原来的结果）照样处理，用它自己的失败标注——那是在钩子之后打的。
		if finishToolResult != nil {
			for i, r := range results {
				if i >= len(toolCalls) || !pendingSize[i] || r == nil {
					continue
				}
				failed, code := failedAfterHook[i], codeAfterHook[i]
				if r != preBatch[i] {
					_, failed = r.Annotations[toolErrorCodeAnnotation]
					code, _ = r.Annotations[toolErrorCodeAnnotation].(string)
				}
				finishToolResult(ctx, toolCalls[i].Function.Name, r, failed, code)
			}
		}
		// 这一批处理完了：结果定进工具账（见 agent_v2 的 finalizeToolMemo）。被裁决换掉的那几条定下的是替换后的失败结果——
		// 不定的话账里留着它转存前、钩子脱敏前的原文，节点之后作废时随出口存档落盘（审计实测）。
		if finalizeToolMemo != nil {
			finalizeToolMemo(ctx, toolCalls, results, hookSignals)
		}
		// 钩子做出的决定排在这批调用的决定之后交，按钩子的先后（理由见循环里收的那一处）。
		for _, s := range hookSignals {
			compose_v2.EmitSignals(ctx, s)
		}

		// 会话历史（opt-in）：本轮全部工具结果。**必须在整节点成功之后**——tools 是
		// 整节点原子的，任一工具抛中断/报错都会让这批 writes 全部作废、Resume 时全部
		// 重跑。在 runOne 里逐个工具写就会在中断场景下重复入库。
		//
		// 这次执行上一次已经记过（处理完之后在提交之前作废、续跑重放）就不再记：结果已定下、跟那次记的是同一份
		// （见 agent_v2 的 finalizeToolMemo）。不记这一笔的话同一批结果在历史里两份，下一次 Run 的 prior 里一个
		// 调用配两条结果（审计实测）。按 Rerunnable 记：被否决重做的那一批是新结果，照常记。
		if done, _ := compose_v2.GetStepResult[bool](ctx, basicToolsHistoryStep); !done {
			if err := compose_v2.SaveStepResult(ctx, basicToolsHistoryStep, true, compose_v2.Rerunnable(), compose_v2.WithStreams(nil)); err != nil {
				slog.WarnContext(ctx, "agent_v2: 记「这一批已记过会话历史」失败，这次执行之后作废时续跑会再记一份", "err", err)
			}
			recordHistory(ctx, historyOwner(ctx, name), name, results)
		}

		writes := []compose_v2.ChannelWrite{compose_v2.Write(basicMessagesCh, results)}
		// 本轮 tool_search 解锁的工具进 channel——权威副本随 checkpoint 走，续跑后
		// 不必依赖消息历史还在（见 basicRevealedCh）。
		unlockedNew := false
		if names := newlyRevealed(turn.list(), revealed); len(names) > 0 {
			writes = append(writes, compose_v2.Write(basicRevealedCh, names))
			unlockedNew = true
		}
		if st := repeat.result(); st != nil {
			writes = append(writes, compose_v2.Write(basicRepeatChainCh, st))
		}
		// return-direct 检测：某工具声明调用后立即终止 ReAct（如 handoff transfer / exit）。
		// 任一 tool result 带 ReturnDirectAnnotation → 写 compose_v2.OnceFlag，tools 出口条件边短路。
		// **两件事分开扫，别挤在一个循环里**：
		//   - 「要不要短路 ReAct」：任一 return-direct 结果即可，撞见就够；
		//   - 「本轮的答案是不是某个工具的产出」：**只有 final_output 成立**，而它可能排在
		//     这批结果的任意位置。
		//
		// 挤在一起写过一版：短路那半 `break` 得早，把覆盖判断一起带走了 ⟹ final_output
		// **只有排在第一个 return-direct 位置时才生效**。同一轮 `[业务RD工具, final_output]`
		// 时模型明明产出了合法 JSON、校验也过了，却抛 StructuredOutputError（实测）。
		var rdFinal *schema.Message
		// rdAnswer 是**按语义挑出来的赢家**（final_output 恒胜 > 业务 exit 工具），不是按位置。
		var rdAnswer *schema.Message
		rdAnswerIsFinalOutput := false
		// final_output 这次有没有产出通过 schema 校验的答复——没有就是一次失败的尝试（见
		// finalOutputRetryAnnotation）。哪些结果是它的调用按派发层盖的事实认（finalOutputAttemptAnnotation），
		// 不按名字。通过校验的结果带着 makeToolsFn 打的 return-direct 标；钩子否决了的看 failedAfterHook。
		for i, r := range results {
			if i >= len(toolCalls) || !isFinalOutputAttempt(r) {
				continue
			}
			if rd, _ := r.Annotations[ReturnDirectAnnotation].(bool); rd && !failedAfterHook[i] {
				continue // 通过了校验、钩子也没否决：这就是答复
			}
			if r.Annotations == nil {
				r.Annotations = map[string]any{}
			}
			r.Annotations[finalOutputRetryAnnotation] = true
			if !failedAfterHook[i] && preHookFailure[i] != "" {
				r.Annotations[finalOutputFailureAnnotation] = preHookFailure[i]
			}
		}
		// 本批里框架那件 final_output 被调了几次——结构化输出的重试预算靠它裁决（见
		// basicFinalOutputAttemptsCh）。按章数，跟上面打回炉标记同一处、同一把尺。
		foAttempts := 0
		for i, r := range results {
			if i < len(toolCalls) && isFinalOutputAttempt(r) {
				foAttempts++
			}
		}
		if foAttempts > 0 {
			writes = append(writes, compose_v2.Write(basicFinalOutputAttemptsCh, foAttempts))
		}
		// 这一批算不算「调过工具」（见 basicToolsUsedCh）：有 tool_search 以外的调用就算；全是 tool_search 时，
		// 这一批解锁出了新工具、且这个任务还没豁免过一批，才不算（豁免记进 basicSearchGraceCh）。按整批判、不按
		// 单次检索：同一批两次检索命中同一个工具时，第二次「什么也没解锁」，按单次判会把这一批算成调过、强制额度
		// 在模型还没调那件工具时就没了。
		graced, _ := compose_v2.Get(chs, basicSearchGraceCh)
		toolsUsed := !unlockedNew || graced
		for i, r := range results {
			if i < len(toolCalls) && !isToolSearchCall(r) {
				toolsUsed = true
				break
			}
		}
		if toolsUsed {
			writes = append(writes, compose_v2.Write(basicToolsUsedCh, true))
		} else {
			writes = append(writes, compose_v2.Write(basicSearchGraceCh, true))
		}
		// needsRetry：同批有**失败的 final_output**——它不是终止信号，是「本轮必须
		// 回 model 重试」。判定在 RD 过滤**之前**：失败的 final_output 多半没有 RD 标。
		needsRetry := false
		for i, r := range results {
			if r == nil || r.Annotations == nil {
				continue
			}
			if retry, ok := r.Annotations[finalOutputRetryAnnotation].(bool); ok && retry {
				needsRetry = true
			}
			if rd, ok := r.Annotations[ReturnDirectAnnotation].(bool); !ok || !rd {
				continue
			}
			// final_output 校验通过了、却被 OnToolEnd 改成失败（业务追加的校验拒掉了它）：等同校验失败——
			// 回炉，不短路、不当答复。不这样的话被否决的那次照样被挑成答复收尾，最终答复是失败文本、不是
			// JSON，`WithOutputSchema` 的承诺静默破（审计实测）。模型下一轮看到的是渲染好的失败结果，按它重给；
			// 这次照样算一次尝试（tools 节点按派发层的章计数，见 basicFinalOutputAttemptsCh）。
			if isFinalOutputAttempt(r) && i < len(failedAfterHook) && failedAfterHook[i] {
				continue // 上面已记成一次失败的尝试（needsRetry 也已置位）
			}
			// 框架那件控制工具（transfer_to_agent / route_to）的结果在钩子之后是失败的：它做出的决定
			// 已作废，不短路——模型被告知这一次没成，该轮到它接着说话。不跳过的话，钩子拦下的转交作废了，
			// ReAct 却照样短路收尾，最终答复是空的（实测）。
			//
			// **业务自己的 return-direct 工具不在此列**：它声明失败（或被钩子改成失败）时，失败内容就是
			// 它给出的答复（「无法处理，请联系人工」），照常短路——tool.ReturnDirectMetaKey 的契约没有
			// 失败例外。这里曾对所有工具都跳过，业务 exit 工具一声明失败模型就被再调一次、答复被顶掉
			// （审计实测）。
			if isFrameworkControlResult(r) && i < len(failedAfterHook) && failedAfterHook[i] {
				continue
			}
			if rdFinal == nil {
				rdFinal = r
			}
			// 「return-direct ⟹ 工具返回值即答案」对**哪些工具**成立，判据是
			// **它短路之后还有没有别人来说话**：
			//
			//   - 框架铸的 `transfer_to_agent` / `route_to` 是**控制动作**，跳过。
			//     ⚠️ **真正承重的理由是历史可见性，不是「答复来源」**（失重实验证实，
			//     audit F2）：把这道跳过整个拆掉，Handoff / Routing 的**答复行为完全不变**
			//     ——转交决定走上行信号，编排读的是信号不是这条 channel。唯一红的是
			//     `TestHandoffAgent_ModelB_HistoryDefault_SeesPredecessor`：取了 transfer
			//     的产出，Rewrite 模式下后接 agent 看到的就是「已转交给 billing」这句
			//     机械文本，**而不是前接 agent 真正说的话**——接力语义断在历史那一维上。
			//     按「答复来源」推演会得出「这道跳过没用」，然后删掉它。
			//   - `final_output` 的契约就是「我的产出即答案」。
			//   - **业务自己的 return-direct 工具**（`tool.ReturnDirectMetaKey` godoc 里的
			//     「exit 类工具」）短路之后**没有任何人再说话**——不认它的话最终答复是
			//     **空的**，而且无错无告警。
			//
			// ⚠️ **`final_output` 与业务工具之间不靠位置挑**（一度写成「第一个命中就
			// break」，业务工具排在前面就把 `final_output` 遮蔽掉——
			// `TestStructuredOutput_FinalOutputOrderIndependent` 钉的）。
			// **两个业务工具之间仍取第一个**——那一档没有语义可依据，任何选择都任意
			// （audit F4 实测：`[alpha,beta]`→alpha、`[beta,alpha]`→beta，这是现状不是
			// 承诺；真要收紧方向是构造期/运行期拒绝同批多个业务 RD，不是再发明一个次序）。
			minted, _ := r.Annotations[ReturnDirectMintedAnnotation].(bool)
			if isFinalOutputAttempt(r) {
				rdAnswer = r
				rdAnswerIsFinalOutput = true
				// final_output 恒胜——但**不 break**：needsRetry 的扫描要看完全批
				// （失败的 final_output 可能排在它后面）。
			} else if !minted && rdAnswer == nil {
				// 业务 exit 工具候补——被后面的 final_output 覆盖掉是对的。
				// 判据读写侧盖的「框架铸造」事实，不查名字名单：业务注册 `respond` /
				// `route_to` 这类撞名工具时 minted=false ⟹ 走这档，产出即答复（修复
				// 撞名产出被丢）；真的框架 transfer / route 才 minted=true 走下面那条。
				rdAnswer = r
			}
		}
		// **短路的决定挪到看完全批之后**（曾在循环内第一命中就写——那时还不知道同批
		// 有没有校验失败的 final_output）。三档：
		//
		//   1. 成功的 final_output 在场 ⟹ 正常短路收尾（恒胜档既有语义）。
		//   2. 校验失败的 final_output 在场（且无成功的）⟹ **不短路、不写答复**——回炉赢。
		//      业务 return-direct 说的是「这轮成功收尾」，而配了 [WithOutputSchema] 的
		//      agent「收尾」的定义被 schema 收紧：没有合法 JSON 就不算收尾完。短路在这里
		//      抢跑会掐死 tools→model 的回炉边，实测后果：模型第 2 轮永不被调、postamble
		//      抛 StructuredOutputError 且文案把它诊断成「自由文本终止」（模型明明调了
		//      final_output）。回炉后业务工具的 tool_result 仍在消息流里，模型下一轮
		//      补一个合法 final_output 即可。
		//   3. 都没有 ⟹ 业务 return-direct 正常短路。
		//
		// 同类产品对「失败×短路」各选了简单极端（失败也短路成终态 / 失败直接错出图），
		// 但**没有一家有「失败 ⟹ 回炉重试」的工具**——那是 final_output 特有的构造，
		// 冲突语义只能按我们自己的承诺定：`WithOutputSchema` 承诺最终答复是合法 JSON。
		if rdFinal != nil && (rdAnswerIsFinalOutput || !needsRetry) {
			writes = append(writes, compose_v2.Write(basicReturnDirectCh, true))
		} else if needsRetry {
			rdAnswer = nil // 本轮没收尾，别把业务产出写成答复
		}
		if rdAnswer != nil {
			writes = append(writes, compose_v2.Write(basicFinalCh, rdAnswer))
		}
		return writes, nil
	}

	// **maxVisit = maxRounds + 1**（修 per-node-vs-总预算 混淆）：compose_v2.maxVisit 是
	// **per-node** 上限（graph_loop.go `visits[id]++; if visits[id] > maxVisit`），model 节点
	// 每 ReAct 轮访问 1 次。BasicAgent **无优雅轮次退出**（model 条件边只判 tool_calls，不判
	// 轮次），故此 maxVisit 是 ReAct 循环唯一硬约束——触顶抛 MaxIterationsError。
	//
	// 原 `maxRounds * 2` 把 per-node 上限误当**总步数预算**（心智：2 节点/轮 × maxRounds 轮 =
	// 总访问数）——但本引擎是 per-node 计数，model 单节点已只数 model 访问，`*2` 直接把 model
	// 上限翻倍 → 实际能跑 2*maxRounds 次 LLM 调用（默认 maxRounds=10 → 20 次）才报错，与文档
	//"最大 maxRounds 轮"差 2 倍。
	//
	// `+1` 而非精确 maxRounds：容最后一轮 model 在 tool 循环后再做**一次无 tool 的 final 收尾**
	// 调用（maxRounds 个 tool 轮 + 1 个 final 轮）——不 +1 会把第 maxRounds 轮的 final 答复截掉。
	// LoopAgent(`maxIter+5`) / ReflectAgent(`maxRounds*3+5`) 各有优雅轮次退出条件，其 maxVisit 是
	// 永不触发的 dead headroom，不受此修影响（本修仅针对无优雅退出的 BasicAgent）。
	bld := compose_v2.NewBuilder().WithMaxVisit(maxRounds + 1)
	if b.identityName != "" {
		bld = bld.WithIdentityPrefix(b.identityName)
	}
	bld = compose_v2.WithChannel(bld, basicMessagesCh)
	bld = compose_v2.WithChannel(bld, basicRoundCh)
	bld = compose_v2.WithChannel(bld, basicRevealedCh)
	bld = compose_v2.WithChannel(bld, basicToolBindingCh)
	bld = compose_v2.WithChannel(bld, basicFinalOutputAttemptsCh)
	bld = compose_v2.WithChannel(bld, basicReturnDirectCh)
	bld = compose_v2.WithChannel(bld, basicToolsUsedCh)
	bld = compose_v2.WithChannel(bld, basicSearchGraceCh)
	bld = compose_v2.WithChannel(bld, basicFinalRoundCh)
	bld = compose_v2.WithChannel(bld, basicRepeatChainCh)
	bld = compose_v2.WithChannel(bld, basicCompactionCh)
	bld = compose_v2.WithChannel(bld, basicFinalCh)
	bld = compose_v2.WithChannel(bld, basicInterjectionsCh)

	bld.AddNode("model", modelNode)
	bld.AddNode("tools", toolsNode)

	// **ISSUE-74 修**：preamble 节点（可选）夹在 compose_v2.StartNode 和 model 之间。业务通过
	// WithPreamble 注入；preamble 内业务回调抛 compose_v2.InterruptError 时走 graph 内 SaveNS
	// 协议自动存 cp，Resume 自然支持（修复 compose_v2.Run 入口同步调用抛 Interrupt 时 cp 不存的
	// HITL 链断 bug）。preamble 只跑首轮（tools→model 直接回，不经 preamble）。
	preambleFn := b.preamble
	if preambleFn != nil {
		bld = compose_v2.WithChannel(bld, basicPreambleInputCh)
		bld = compose_v2.WithChannel(bld, basicSysPreambleCh)
		preambleNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, basicPreambleInputCh)
			if in == nil {
				return nil, fmt.Errorf("basic_agent preamble: PreambleInput nil（caller 应通过 " +
					"compose_v2.WithInitialWrites(BasicAgentPreambleInputWrites(...)) 注入）")
			}
			out, err := preambleFn(ctx, in)
			if err != nil {
				// compose_v2.InterruptError / ctx.Err / 业务错统一 propagate——graph_loop defer SaveNS
				// 自动存 cp（含本节点 frontier + PendingInterrupts），Resume 自然支持。
				return nil, err
			}
			// **plan 7.3 关键设计**：一次性 compose_v2.Write 完整 messages，避免增量 compose_v2.Write 后
			// Resume 重跑 preamble 时 compose_v2.Append channel 累积造成 messages 重复。
			//
			// 会话历史由 preambleFn 内部只记 UserInput 一条——**不能在这里整批记**：
			// msgs 里含 system / StoreMemory 注入 / 从历史读回的 prior，整批写等于每轮
			// 把全部历史再抄一遍。见 BasicAgent.preambleFn。
			//
			if out == nil {
				return nil, fmt.Errorf("basic_agent preamble: 返回了 nil output（约定：err == nil 时必须给一个非 nil 的 *BasicAgentPreambleOutput）")
			}
			// 两条流分开写：System 那条进 basicSysPreambleCh（压缩层看不见、model 节点
			// 每轮 prepend），其余进 basicMessagesCh（正常参与四层压缩）。
			// 见 BasicAgentPreambleOutput 的 godoc。
			writes := []compose_v2.ChannelWrite{compose_v2.Write(basicMessagesCh, out.Messages)}
			if sys := adoptPreamble(out.System); len(sys) > 0 {
				writes = append(writes, compose_v2.Write(basicSysPreambleCh, sys))
			}
			return writes, nil
		}
		bld.AddNode("preamble", preambleNode)
		bld.AddEdge(compose_v2.StartNode, "preamble")
		bld.AddEdge("preamble", "model")
	} else {
		bld.AddEdge(compose_v2.StartNode, "model")
	}

	// **ISSUE-75 修**：postamble 节点（可选）夹在终止路径和 compose_v2.EndNode 之间。业务通过
	// WithPostamble 注入；postamble 内业务回调（store.Put 等）抛 compose_v2.InterruptError 时
	// 走 graph 内 SaveNS 协议自动存 cp，Resume 自然支持（修复 BasicAgent.Run 入口
	// commitFinal 同步调用抛 Interrupt 时 cleanupOnSuccess 已删 cp 的 HITL 链断 bug）。
	// postamble 只在终止路径跑（无 tool_call 时），与 ISSUE-74 preamble 完全对称。
	postambleFn := b.postamble
	terminalTarget := compose_v2.NodeID(compose_v2.EndNode)
	if postambleFn != nil {
		postambleNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			finalMsgs, _ := compose_v2.Get(chs, basicMessagesCh)
			rawFinal, _ := compose_v2.Get(chs, basicFinalCh)
			interjections, _ := compose_v2.Get(chs, basicInterjectionsCh)
			// 本次调用的 final_output 尝试次数（同 model 节点）：收尾时没有结构化答复要报它，而
			// finalMsgs 含 prior，数它会把会话历史里上几个回合的尝试也算进来（实测）。
			foAttempts, _ := compose_v2.Get(chs, basicFinalOutputAttemptsCh)
			ctx = withFinalOutputAttempts(ctx, foAttempts)
			if err := postambleFn(ctx, &BasicAgentPostambleInput{
				Messages:      finalMsgs,
				Final:         presentFinal(rawFinal),
				Interjections: interjections,
			}); err != nil {
				// compose_v2.InterruptError / ctx.Err / 业务错统一 propagate——graph_loop defer SaveNS
				// 自动存 cp（含本节点 frontier + PendingInterrupts），Resume 自然支持。
				// cleanupOnSuccess 不触发（runErr != nil）→ cp 保留供 Resume 续跑。
				return nil, err
			}
			return nil, nil
		}
		bld.AddNode("postamble", postambleNode)
		bld.AddEdge("postamble", compose_v2.EndNode)
		terminalTarget = "postamble"
	}

	// **回合结束闸门**（见 WithTurnGate）：配了才建节点。模型自然收尾 → 闸门节点 → 有反馈（或答复之后又到了插话）
	// 回 model，没有就去 terminalTarget。图结构只随静态配置变（闸门是构造期选项），Run 与 Resume 是同一张图。
	gateTarget := terminalTarget
	if len(b.turnGates) > 0 {
		bld = compose_v2.WithChannel(bld, basicGateVetoesCh)
		gates := b.turnGates
		maxVetoes := b.turnGateMaxVetoes
		if maxVetoes <= 0 {
			maxVetoes = DefaultTurnGateMaxVetoes
		}
		gateNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			msgs, _ := compose_v2.Get(chs, basicMessagesCh)
			round, _ := compose_v2.Get(chs, basicRoundCh)
			vetoes, _ := compose_v2.Get(chs, basicGateVetoesCh)
			// 轮次预算用完（刚才那次已是最后一次能调模型的机会）：放行。插话留在收件箱。
			if len(msgs) == 0 || (maxRounds > 0 && round > maxRounds) {
				return nil, nil
			}
			// 模型答复之后又到的插话：先回去回应它，这一次不问闸门、不算一次否决
			inter, err := takeInterjections(ctx)
			if err != nil {
				return nil, fmt.Errorf("interjections: %w", err)
			}
			if len(inter) > 0 {
				recordHistory(ctx, historyOwner(ctx, name), name, inter)
				return []compose_v2.ChannelWrite{
					compose_v2.Write(basicMessagesCh, inter),
					compose_v2.Write(basicRepeatChainCh, &repeatToolState{}),
					compose_v2.Write(basicInterjectionsCh, inter),
				}, nil
			}
			if vetoes >= maxVetoes {
				return nil, nil
			}
			feedback, err := runTurnGates(ctx, gates, msgs, vetoes)
			if err != nil {
				return nil, err
			}
			if len(feedback) == 0 {
				return nil, nil
			}
			if vetoes+1 >= maxVetoes {
				// 单独一条、不进会话历史（见 newTurnGateLastChance）；recordHistory 按标记跳过它
				feedback = append(feedback, newTurnGateLastChance())
			}
			recordHistory(ctx, historyOwner(ctx, name), name, feedback)
			return []compose_v2.ChannelWrite{
				compose_v2.Write(basicMessagesCh, feedback),
				compose_v2.Write(basicGateVetoesCh, 1),
			}, nil
		}
		bld.AddNode(turnGateNodeID, gateNode)
		bld.AddConditionalEdge(turnGateNodeID, func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			msgs, _ := compose_v2.Get(chs, basicMessagesCh)
			if len(msgs) > 0 && isPendingUserInput(msgs[len(msgs)-1]) {
				return "model", nil
			}
			return terminalTarget, nil
		})
		gateTarget = turnGateNodeID
	}

	bld.AddConditionalEdge("model", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		msgs, _ := compose_v2.Get(chs, basicMessagesCh)
		if len(msgs) == 0 {
			return terminalTarget, nil
		}
		last := msgs[len(msgs)-1]
		// 答复后面接着模型还没回应的用户输入（答复期间到的插话）：再调一次模型
		if isPendingUserInput(last) {
			return "model", nil
		}
		if len(schema.GetToolCalls(last)) == 0 {
			// **ISSUE-82 fix**: 删原 OnAgentEnd 触发——cond fn 是路由判断不是终止信号，
			// compose_v2.InterruptError 时 cond fn 不跑 → 漏 fire；agent_v2.BasicAgent.Run defer
			// 直接调 hooks.OnAgentEnd 已**唯一源**兜底所有退出路径 (success / Interrupt /
			// panic / ctx.Err). audit² 删 SafeOnAgentEnd wrapper — framework compose_v2.Run 入口
			// outer defer recover 兜底 panic.
			//
			// **单一所有权决策**: agent-level lifecycle hook 只由 agent_v2 wrapper 唯一
			// fire——compose_v2 BasicAgent 自己**不 fire** agent-level hook (cond fn 漏
			// compose_v2.InterruptError 路径), 避免两处重复触发。graph 层只 fire graph-level hook,
			// 与 agent-level 是不同层不冲突。compose_v2 直接业务 (example/channel_smoke
			// 等) 不传 WithAgentHooks 不受影响；想要 agent-level lifecycle 必须用
			// agent_v2 wrapper。
			//
			// 配了回合结束闸门时先过闸门（gateTarget），没配就是 terminalTarget。
			return gateTarget, nil
		}
		return "tools", nil
	})
	// **return-direct**：某工具声明"一调即出"（ReturnDirectAnnotation → basicReturnDirectCh）
	// 时短路到 terminalTarget（postamble/End），不回 model——避免 transfer/exit 后 LLM 继续
	// 生成。无此类工具时 basicReturnDirectCh 恒未写 → 恒走 "model"，与原无条件
	// AddEdge("tools","model") 行为完全一致（零副作用，不影响其他编排的 ReAct 循环）。
	bld.AddConditionalEdge("tools", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if rd, _ := compose_v2.Get(chs, basicReturnDirectCh); rd {
			return terminalTarget, nil
		}
		return "model", nil
	})

	return bld.Compile()
}

// toolHookCtx 是交给工具钩子（OnToolStart / OnToolEnd / OnError）的 ctx：每次钩子调用是一个相位
// （compose_v2.EnterPhase）——自己的私有态一格、自己的提问与嵌套调用计数、自己的相位名，名字是
// 钩子名 + 工具名 + 所属 tool_call（工具名与 callID 为空、idx 为 -1 表示整批）。工具名在
// OnToolStart 那里是它改写之前的名字（相位在钩子跑之前就定了），之后是实际执行的名字——这一批
// 第一次交给工具执行时定下的那个（见 fixToolCallContent），续跑时不变。这一维让 OnToolEnd 记过的
// 结果只认那个工具、不依赖那份记录：改写曾经在续跑时生效，那时新工具的结果读到旧工具的记录、没过
// 审计就放行（实测）。现在它是冗余的第二道，没有测试覆盖。工具体的分域同样带工具名。
//
// 钩子跑在 tools 节点体里、拿的是节点体的 ctx，却不是节点体：
//
//   - 私有态不分开的话，钩子里一次 `compose_v2.SaveNodeState` 就把 tools 节点自己记的工具账覆盖了，
//     续跑时已执行过的工具再执行一遍（实测）；
//   - 计数不分开的话，两个 tool_call 的钩子带同一个 call key 调同一个 agent（按订单号预检）落在
//     同一个位置上，整个 Run 终局失败（实测）；
//   - 相位名进提问的身份：provider 不回 tool_call ID 时，钩子里的提问与 tool_call #0 工具体里的提问
//     不再撞成同一个 ID（实测曾终局失败）。
//
// 钩子重跑时（tools 节点续跑）相位相同，钩子里记下的从这里读回。
func toolHookCtx(ctx context.Context, hook, toolName, callID string, idx int) context.Context {
	return compose_v2.EnterPhase(ctx, "tool_hook\x00"+hook+"\x00"+toolName+"\x00"+callID+"\x00"+strconv.Itoa(idx))
}

// modelHookCtx 让 model 节点的一次钩子调用在自己的相位里跑（同 toolHookCtx：自己的提问与嵌套调用计数、相位名进提问
// 身份、自己那一格节点私有态）。各钩子各一格：原样作废、重新调模型时只推进 OnModelEnd 那一格的重做代际（见
// clearModelCall），OnModelStart 问的是输入、不跟着换。
func modelHookCtx(ctx context.Context, hook string) context.Context {
	return compose_v2.EnterPhase(ctx, "model_hook\x00"+hook)
}

// BasicAgentInputWrites 是 BasicAgent 启动 helper——把 user message 写到内部 channel。
//
// **未启用 preamble 模式**用此 helper：caller 在 BasicAgent.Run 入口同步 build 完整
// messages 后通过此 helper 一次性写入 basicMessagesCh。
//
// **启用 preamble 模式**应改用 BasicAgentPreambleInputWrites——把 user input 写
// preamble channel 让 preamble 节点拼装。
func BasicAgentInputWrites(userMsg *schema.Message) []compose_v2.ChannelWrite {
	return []compose_v2.ChannelWrite{compose_v2.Write(basicMessagesCh, []*schema.Message{userMsg})}
}

// BasicAgentPreambleInputWrites 是 BasicAgent preamble 模式启动 helper——把
// PreambleInput 写到 basicPreambleInputCh，让 preamble 节点拼装完整初始 messages
// 后写到 basicMessagesCh（ISSUE-74 修）。
//
// 用法：
//
//	g, _ := NewBasicAgentBuilder(modelFn, toolsFn).WithPreamble(preambleFn).Compile()
//	compose_v2.Run(ctx, g, cm, compose_v2.WithInitialWrites(
//	    BasicAgentPreambleInputWrites(&BasicAgentPreambleInput{
//	        UserInput: schema.UserMessage("hi"),
//	    })...,
//	))
func BasicAgentPreambleInputWrites(in *BasicAgentPreambleInput) []compose_v2.ChannelWrite {
	return []compose_v2.ChannelWrite{compose_v2.Write(basicPreambleInputCh, in)}
}

// BasicAgentMessages 取累积的所有 messages（user + assistant + tool_result）。
func BasicAgentMessages(chs compose_v2.Channels) []*schema.Message {
	msgs, _ := compose_v2.Get(chs, basicMessagesCh)
	return msgs
}

// BasicAgentFinalMessage 取**本次调用的最终产出**，nil = 本轮没有产出（graph 未完成 /
// 模型没答出东西）。
//
// 读的是 `basicFinalCh`——决策点在产生它的那一刻写进去的（见该 channel 的声明处）。
// **不再倒扫消息列表**：那个列表含 prior，倒扫在 return-direct 收尾时会拿到上一次调用
// 的答复。
//
// **return-direct 收尾时返回什么，按谁收的尾分三档**（与 [tool.ReturnDirectMetaKey]
// 的 godoc 是同一条契约，改任何一边都要同步另一边——这两处已经矛盾过一次，audit F3）：
//
//   - `final_output`：**工具的规范 JSON 即答案**，带原 tool_result 的 annotations
//     （业务据 `ReturnDirectToolAnnotation` 可知是它收的尾）。
//   - **业务自己的 return-direct 工具**（exit 类）：**工具产出即答案**（2026-08-28 起）。
//     它短路之后没有别人再说话，不取产出的话答复是空的。
//   - **框架铸的** `transfer_to_agent` / `route_to`：返回**模型那条 assistant**（它在
//     转交/路由之前说的话）——真实理由是历史可见性：取了 transfer 的产出，Rewrite 模式
//     下后接 agent 看到的就是机械文本而不是前接真正说的话。判据是写侧盖的
//     `ReturnDirectMintedAnnotation`（铸造事实），**不是名字**——那些名字业务占得到。
func BasicAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	raw, _ := compose_v2.Get(chs, basicFinalCh)
	return presentFinal(raw)
}

// presentFinal 把决策点写下的原始消息转成对外的最终答复形态。
//
// model 路径写的就是 assistant，原样返；return-direct 路径写的是 tool_result
// （role=tool），转成同内容的 assistant——对外契约一直是「最终答复是 assistant」，
// 一条 role=tool 会让所有下游（a2a / agui / 父编排）都得特判。
func presentFinal(raw *schema.Message) *schema.Message {
	if raw == nil || raw.Role != schema.RoleTool {
		return raw
	}
	out := schema.AssistantMessage(raw.Content)
	if len(raw.Annotations) > 0 {
		out.Annotations = make(map[string]any, len(raw.Annotations))
		for k, v := range raw.Annotations {
			out.Annotations[k] = v
		}
	}
	return out
}

// adoptPreamble 把业务 preambleFn 交出来的前言**接管**成框架自己的一份：逐条浅拷贝 + 去 nil。
//
// **拷贝是承重的，不是防御性洁癖**：`prompt.Placeholder` 的 godoc 明写它把
// `vars[key]` 里的 `[]*schema.Message` **原样插入**——那是业务自己持有、且很可能在多个
// agent 之间复用的指针。前言是 [消费] 档，业务在 `OnModelStart` 里就地改 `m.Content`
// （框架自己的示例就这么写）会顺着这条别名写穿回去。实测：一个配了 hook 的 agent 跑完，
// **另一个从没配过那个 hook 的 agent 收到了被改过的 few-shot**，全程 `err == nil`。
//
// 只拷一层：`Message` 里的引用类型字段（`ToolCalls` / `Metadata` 等）仍与业务共享。
// 前言这条路上它们本来就该是空的，真要动那些字段的业务自负——**别把这句读成「深拷贝」**。
func adoptPreamble(msgs []*schema.Message) []*schema.Message {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		c := *m
		out = append(out, &c)
	}
	return out
}

// dropNilMessages 去掉 nil 元素，全非 nil 时原样返回（不白拷一遍）。
func dropNilMessages(msgs []*schema.Message) []*schema.Message {
	n := 0
	for _, m := range msgs {
		if m == nil {
			n++
		}
	}
	if n == 0 {
		return msgs
	}
	out := make([]*schema.Message, 0, len(msgs)-n)
	for _, m := range msgs {
		if m != nil {
			out = append(out, m)
		}
	}
	return out
}
