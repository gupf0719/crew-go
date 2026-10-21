package agent_v2

import (
	"context"
	"errors"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// RunOption 是 Agent.Run 的可选参数——仅本次调用生效，不修改构造时配置。
type RunOption func(*runConfig)

type runConfig struct {
	orchHooks OrchestrationHooks

	// per-call 覆盖构造期 Config（仅本次 Run 生效，不进 cp、不跨 Resume）。
	// v1→v2 迁移（commit ad2743e 删 v1 agent/ 包）丢失的 ISSUE-18 能力此处补回。
	model        model.ChatModel // nil = 不覆盖
	systemPrompt *string         // nil = 不覆盖（指针区分 "" 与未设）
	maxRounds    int             // <=0 = 不覆盖（与 WithMaxRounds 语义一致）
	modelOpts    []model.Option  // per-request model.Option（thinking 等 provider 参数按次控制）；空=不注入

	// handoffGrant 是上层交给**本次调用**的转交能力授予（见 handoffGrant）。
	// 无导出 option 可写它——业务构造不出（类型与字段都不导出）。
	//
	// **不进 overrides()**：那三元组会被 rejectCompositeOverride 拒，而 grant 恰恰
	// 必须能传给编排（编排要把它重新发给自己的成员）。两者是不同的东西：per-call
	// 覆盖是「改本层配置」，grant 是「授予本层一项能力」。
	handoffGrant *handoffGrant
	// routeGrant 是上层交给本次调用的路由能力授予（见 routeGrant），规矩同 handoffGrant。
	routeGrant *routeGrant

	// callKey 见 WithCallKey——只有 RunSubAgent 读它。
	callKey string
	// rerunnable 见 WithRerunnable——只有 RunSubAgent 读它。
	rerunnable bool

	// callDigest 是 RunSubAgent 替被调方算好的调用方输入摘要（见 withNestedCallDigest），编排的
	// runGraph 经 inputDigestOption 交给引擎。无导出 option 可写它。
	callDigest string
	// callDigestIsIdentity：摘要就是这次调用的身份（不带 call key 的嵌套调用），见
	// compose_v2.WithInputIdentity。
	callDigestIsIdentity bool
}

// withNestedCallDigest 把调用方输入的摘要交给被调方——按内容定身份的嵌套调用用（只按位置定
// 身份的调用——全历史输入的 AgentAsTool、skill fork——不交）。
//
// 摘要在调用方这一侧算（见 inputDigest）：那是调用方交来的输入，还没经过被调方自己的
// OnAgentStart 钩子；被调方的钩子往输入里加点会变的东西，不该算成父节点给了不同的输入。
func withNestedCallDigest(digest string, isIdentity bool) RunOption {
	return func(c *runConfig) {
		c.callDigest = digest
		c.callDigestIsIdentity = isIdentity
	}
}

// inputDigestOption 把 RunSubAgent 交来的摘要转成引擎选项。没交（顶层调用、编排的成员）时
// 摘要为空，等于没给。
func (c *runConfig) inputDigestOption() compose_v2.RunOption {
	if c.callDigestIsIdentity {
		return compose_v2.WithInputIdentity(c.callDigest)
	}
	return compose_v2.WithInputDigest(c.callDigest)
}

// WithCallKey 给一次嵌套调用一个由业务声明的身份。只对 [RunSubAgent] 生效——直接传给
// `Agent.Run` 时被忽略。
//
// # 默认身份
//
// 不给 key 时，嵌套调用的身份是「位置 + 输入内容」：同一个位置上输入不同的调用天然是不同的
// 调用，各有自己的存档与完成记录——输入不同的调用之间，身份跟调用的次序、有没有跳过别的调用
// 都无关。父节点重跑时，已完成的调用直接拿回记下的结果（不再执行），没完成的续上自己的存档。
//
// 输入完全相同的几次只能按发起次序区分：那种调用别自己按进度跳过（已完成的框架已经记住，全部
// 照调一遍即可）——跳过其中一次，后面那次就拿到它的记录。要跳过或重排，给每次一个 key。
// 带 key 也只解决身份：被跳过的那次调用当初做出的转交与路由决定（发给上层 HandoffAgent /
// RoutingAgent 的上行信号）只在被再调一次、拿回时交出，跳过就丢了——有这种决定的调用照调一遍。
//
// # 什么时候需要 key
//
// 输入里有**每次重跑都会变、却不代表是另一件事**的东西（请求号、时间戳、每次重新生成的
// 措辞）时。按内容定身份的话，这样的调用每次重跑都是一次新调用：已完成的会再做一遍，挂着的
// 提问会换一个身份重问。给 key 之后身份就是（位置, key），输入变了照样认作同一次调用（打一条
// 告警）。
//
// # key 怎么选
//
// 选这次调用处理的那件事的身份：条目 ID、任务号、文件路径——重跑时对同一件事给出同一个 key，
// 对不同的事给出不同的 key。**别用会随调用集合变的东西**（比如剩余列表的下标）：那样的 key
// 在两次执行之间会指向不同的事，框架无从发现。同一次执行（节点执行 / 一个 tool_call / 一次图钩子
// 或工具钩子调用 / 被调方在自己图外的那一段）里、同一个位置上（同一个名字）两次调用给了同一个
// key 报 compose_v2.ErrRunAborted（它们会共用一个身份）——`compose_v2.RunWithRetry` 的重试不算
// 两次；手写的循环里每次都算一次新的调用，带同一个 key 就会撞上这条。⚠️ 一个 agent 的
// `OnAgentStart` 与 `OnAgentEnd` 同在「它在图外的那一段」里，两个钩子里对同一个 agent 别用同一个
// key（`OnAgentEnd` 没有错误返回值，撞上时那次调用拿到的错只能自己记日志）。
//
// 空串等于没给。
func WithCallKey(key string) RunOption {
	return func(c *runConfig) {
		c.callKey = key
	}
}

// WithRerunnable 声明这次嵌套调用的结果是父节点的**产出**：父节点的条件出边按
// `compose_v2.CondFailRerunFrom` 否决、要求父节点重新产出时，这次调用的完成记录跟着作废，重跑时
// 重新调用。只对 [RunSubAgent] 生效——直接传给 `Agent.Run` 时被忽略。
//
// # 默认不作废
//
// 不声明时，已完成的调用在父节点怎么重跑都原样拿回，里面的副作用不重放。框架分不出一次调用
// 是「被判定的那份产出」还是「顺带做成的事」（扣款、发通知、问过人的把关）：同一个节点里常常
// 两种都有，作废的是前一种，后一种再做一遍就是重复扣款（实测）。所以作废要调用方点名。
//
// 典型用法：节点里调 planner 拿方案，条件边校验方案、不合格按 CondFailRerunFrom 要求重做——给
// planner 这一次调用声明 WithRerunnable，节点里别的调用照常。
//
// 只有**否决**会作废：条件边被外面停下（软取消、陈旧写回、调用方 ctx 已经死了）不算，中断 /
// 失败之后的续跑也不算，那些情况下记录照常拿回。「否决」包括本节点里经 [RunSubAgent] 调起的图的
// 入口条件边否决它交出去的输入（节点因此失败）。代际按分域算：调用发生在某个 tool_call、某个
// 钩子相位里时，同一个节点里别的分域（兄弟 tool_call 里嵌套图的入口被否决）不影响它。
//
// # 重做的是整次调用
//
// 否决之后重做的那一次是**另一次调用**：身份里带上调用所在分域的重做代际
// （compose_v2.RerunGeneration），存档槽、被调方在它自己的图之外记下的东西（它的 agent 级钩子、
// 业务自研 agent 在自己的 Run 里发起的嵌套调用、它的图的入口条件边）、它问的问题都换成新的。
// 被调方是图的时候它本来就整个重跑（成功时存档已清）；这样被调方是业务自研 agent 的时候也一样
// ——不然外面这次重新执行了，里面真正干活的那次调用却原样拿回，产出不变，续跑多少次都一样
// （实测）。所以别给里面有扣款的整次调用点名，把产出那一步单独拎出来调。
//
// **只有父节点直接发起的那一层点名才算数**：外层调用是普通记录时整次拿回、被调方根本不执行，
// 它里面某一次调用的声明走不到。要让里层那一次重做，外层那次也得点名。
//
// 同样的效果 AgentAsTool 给不了（`tool.Tool.Call` 没有地方传这个选项）：节点体里产出要被条件边
// 判的那一次，用本 helper 调。
func WithRerunnable() RunOption {
	return func(c *runConfig) {
		c.rerunnable = true
	}
}

// WithOrchHooks 为本次 Run 注入 OrchestrationHooks（图级观测/streaming）。
//
// hook 触发时机（详见 OrchestrationHooks 定义）：
//   - OnGraphStart/End：整次 Run 入口/出口
//   - OnNodeStart/End：每个节点执行前后
//   - OnNodeEvent：节点内 chunk / step_dispatch 等实时事件投递
func WithOrchHooks(h OrchestrationHooks) RunOption {
	return func(c *runConfig) {
		// audit²¹ HIGH-1 fix: isFrameworkNil 检 typed nil OrchestrationHooks (协议二十一复证).
		// 业务 `var h *MyOrchHooks; WithOrchHooks(h)` typed nil → 后续 hooks.OnGraphStart 远 stack nil deref.
		// untyped nil 等价 silent skip (by design idiom 保留).
		if isFrameworkNil(h) {
			return
		}
		c.orchHooks = h
	}
}

// WithRunModel 仅对本次 Run 覆盖构造期的 model（如临时切更强模型重试一次）。
//
// **语义**：仅本次 Run 生效、不进 cp、不自动跨 Resume（model interface 不可序列化无法
// 存 cp 重放；与 LangGraph per-invocation config / eino per-call option 同款 by-design）。
// HITL 中断后 Resume 默认回退构造期 model；业务想 Resume 也用覆盖 model 须显式传
// WithResumeModel（对齐 eino/langgraph：resume 是一次正常调用、可重传 per-call）。
//
// typed nil 防御：业务 `var m *MyModel; WithRunModel(m)` 时 silent skip 回退构造期
// model（跟 WithOrchHooks 协议一致；区别于构造期 WithModel 的 fail-fast panic——
// per-call 有安全回退，不 crash Run）。
//
// 覆盖的是主 ReAct 推理 model；不影响 skill fork 的 fallback model（那个仍用构造期 model）。
func WithRunModel(m model.ChatModel) RunOption {
	return func(c *runConfig) {
		if isFrameworkNil(m) {
			return
		}
		c.model = m
	}
}

// WithRunModelOptions 仅对本次 Run 注入 per-request model.Option（thinking / reasoning
// 等 provider 专属参数按次控制），透传到本次 Run 的每次主推理 model 调用（含 ReactiveCompact
// retry）。业务页面开关场景：这次开 thinking、下次不开，无需重建 client。
//
// **语义**：仅本次 Run 生效、不进 cp、不自动跨 Resume（对齐 WithRunModel；Resume 想续用
// 同款按次参数须显式传 WithResumeModelOptions）。**仅 BasicAgent 生效**——composite 编排
// 收到 per-call 覆盖 fail-fast（见 rejectCompositeOverride），业务应在构造期设或用具体 BasicAgent。
//
// 覆盖的是主 ReAct 推理 model 调用；不影响 AutoCompact 摘要 / skill fork 的 model。
//
// 例外一条：`model.WithToolChoice(schema.ToolChoiceRequired)` 只管到模型第一次调工具为止，之后的轮次复位成
// 不指定（`WithAllowedTools` 的白名单照旧）——每轮都强制的话模型永远不能以文本收尾，一路烧到轮次预算的最后
// 一轮。「调过工具」按模型发起过调用算、不看结果（失败、被拒的也算）；只有第一批全是框架的 tool_search、且解锁出新
// 工具的不算：它只是把别的工具解锁出来，开了渐进披露时第一轮往往只能调它，强制要留给解锁出来的那件。只豁免这一批：
// 检索什么都没解锁的、之后再检索的照算——不然检索不到、或工具库比轮次预算大时，模型一路检索烧到最后一轮。
// 这一轮没有工具可绑时（软降落轮、白名单滤光）也摘掉它。
func WithRunModelOptions(opts ...model.Option) RunOption {
	return func(c *runConfig) {
		c.modelOpts = opts
	}
}

// WithRunSystemPrompt 仅对本次 Run 覆盖构造期的 system prompt。传 "" 可显式清空。
//
// 仅本次 Run 生效、不进 cp。**与 model/maxRounds 语义不同**：systemPrompt 由 preamble
// 首轮拼进 messages 并固化进 cp，后续轮次 / Resume 续跑都用这一份（不重新注入）——故
// **无 WithResumeSystemPrompt**（Resume 改不动已固化的 system；三方亦无此 option：eino
// 无改 instruction 口子、langgraph 是 dynamic prompt 架构非 option）。
func WithRunSystemPrompt(s string) RunOption {
	return func(c *runConfig) {
		c.systemPrompt = &s
	}
}

// WithRunMaxRounds 仅对本次 Run 覆盖构造期的 maxRounds（ReAct 最大轮数）。n<=0 忽略。
//
// 编译期注入 graph 的 WithMaxVisit（maxVisit 不进 graph ID hash，故 per-call 改它
// 不触发 Resume 的 GraphID 冲突）。语义：仅本次 Run 生效、不自动跨 Resume。
//
// **每段 Run/Resume 各自约束**（audit Finding 2）：框架 visit 计数每次 compose_v2.Run
// 从 0 重置（不从 cp.NodeVisits 续），且 Resume 默认回退构造期 maxRounds——故 per-call
// maxRounds 约束的是"本段未中断的连续轮数"。要让 cap 跨 HITL 续上，Run 传
// WithRunMaxRounds(n) 的同时 Resume 也传 WithResumeMaxRounds(n)（对齐 eino resume 重传
// WithRuntimeMaxSteps）。需要全局硬上限则在构造期 WithMaxRounds 设兜底值。
func WithRunMaxRounds(n int) RunOption {
	return func(c *runConfig) {
		if n > 0 {
			c.maxRounds = n
		}
	}
}

// applyRunOptions 应用 RunOption 列表，返回最终 cfg。
func applyRunOptions(opts []RunOption) runConfig {
	cfg := runConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// runOverridesCtxKey 是 per-call 覆盖值的 ctx key。
type runOverridesCtxKey struct{}

// runOverrides 是 per-call 覆盖值（model / systemPrompt / maxRounds）的 ctx 载体——
// Run 入口 push，graph 内节点（buildComposeGraph / makeModelFn / buildInitialMessages）
// 读取并优先于构造期 a.config。
type runOverrides struct {
	model        model.ChatModel
	systemPrompt *string
	maxRounds    int
	modelOpts    []model.Option
}

// overrides 抽出 per-call 覆盖三元组（runConfig / resumeConfig 共用 ctx 透传载体）。
func (c runConfig) overrides() runOverrides {
	return runOverrides{model: c.model, systemPrompt: c.systemPrompt, maxRounds: c.maxRounds, modelOpts: c.modelOpts}
}

// isZero 返回 per-call 覆盖是否全空（无任何覆盖）。
func (o runOverrides) isZero() bool {
	return o.model == nil && o.systemPrompt == nil && o.maxRounds <= 0 && len(o.modelOpts) == 0
}

// ErrCompositeOverride 是 composite 编排收到 per-call 覆盖时的 fail-fast 错误——业务可
// errors.Is 判定。
var ErrCompositeOverride = errors.New("per-call override (model/system-prompt/max-rounds/model-options) is unsupported on composite orchestrations: set it at construction (WithModel/WithSystemPrompt/WithMaxRounds) or on a BasicAgent")

// rejectCompositeOverride 对 composite 编排 fail-fast：per-call 覆盖（WithRunModel /
// WithRunSystemPrompt / WithRunMaxRounds 及 Resume 对应项）只在 BasicAgent 生效——框架级
// resolveModel / resolveSystemPrompt / resolveMaxRounds 自动读这三个 ctx 值，而 composite
// 从不把它们透传给 inner step，传了是静默 no-op（footgun）。直接报错，让业务在构造期
// （WithModel / WithSystemPrompt / WithMaxRounds）设，或在具体 BasicAgent 上用 per-call 覆盖。
func rejectCompositeOverride(o runOverrides) error {
	if o.isZero() {
		return nil
	}
	return ErrCompositeOverride
}

// withRunOverrides 把 per-call 覆盖值 push 进 ctx（Run 和 Resume 入口共用）。
//
// **总是 push（即使本次无覆盖）**——必须覆盖父 agent 经 ctx 透传下来的 override，防
// per-call 覆盖泄漏到嵌套子 agent（父 BasicAgent 用 WithRunModel 后经 AgentAsTool 调
// 子 BasicAgent，子沿用父 ctx）。per-call 语义是"仅本层本次调用"，与 orchHooks 的 ctx
// 继承（WithCtxOrchHooks 让子 agent 继承父 hooks）**相反**——子 agent 的覆盖只来自它
// 自己的 Run/ResumeOption，无则各 resolveXxx 拿到 zero override 回退构造期默认。
// （不 short-circuit：若本次无覆盖就不 push，父 override 会残留在 ctx 泄漏给子。）
func withRunOverrides(ctx context.Context, o runOverrides) context.Context {
	return context.WithValue(ctx, runOverridesCtxKey{}, o)
}

// resolveModel / resolveSystemPrompt / resolveMaxRounds：per-call 覆盖优先，否则构造期
// 默认。Run 入口 push runConfig.overrides()，Resume 入口 push resumeConfig.overrides()
// （对齐 eino/langgraph：resume 也能传 per-call）——无覆盖时拿 zero override 回退 base。
func resolveModel(ctx context.Context, base model.ChatModel) model.ChatModel {
	if o, ok := ctx.Value(runOverridesCtxKey{}).(runOverrides); ok && o.model != nil {
		return o.model
	}
	return base
}

func resolveSystemPrompt(ctx context.Context, base string) string {
	if o, ok := ctx.Value(runOverridesCtxKey{}).(runOverrides); ok && o.systemPrompt != nil {
		return *o.systemPrompt
	}
	return base
}

func resolveMaxRounds(ctx context.Context, base int) int {
	if o, ok := ctx.Value(runOverridesCtxKey{}).(runOverrides); ok && o.maxRounds > 0 {
		return o.maxRounds
	}
	return base
}

// resolveModelOpts 返回本次 Run/Resume 的 per-request model.Option（WithRunModelOptions /
// WithResumeModelOptions 注入）——modelFn 读它串进 llm.Generate/Stream。无覆盖返 nil。
func resolveModelOpts(ctx context.Context) []model.Option {
	if o, ok := ctx.Value(runOverridesCtxKey{}).(runOverrides); ok {
		return o.modelOpts
	}
	return nil
}

// orchHooksCtxKey 是 agent_v2 层 ctx-aware OrchHooks 的 ctx key。
// 与 compose_v2.GetOrchHooks 独立存（两层签名不同）。
type orchHooksCtxKey struct{}

// WithCtxOrchHooks 注入 agent_v2.OrchestrationHooks 到 ctx——业务想让嵌套 sub-agent
// 调用自动继承父 hooks 时调（streaming 跨子图边界不丢）。
//
// 与 RunOption WithOrchHooks 共存：
//   - RunOption: 显式给某次 Run 注入
//   - ctx-aware: 沿 ctx 链透传给所有下游 sub-agent.Run 调用
//
// 优先级：RunOption > ctx > nil。
func WithCtxOrchHooks(ctx context.Context, h OrchestrationHooks) context.Context {
	// audit²¹ HIGH-3 fix: isFrameworkNil 检 typed nil (协议二十一复证, 与 RunOption 协议对称).
	if isFrameworkNil(h) {
		return ctx
	}
	return context.WithValue(ctx, orchHooksCtxKey{}, h)
}

// GetCtxOrchHooks 从 ctx 取 agent_v2.OrchestrationHooks——nil 表示业务未注入。
func GetCtxOrchHooks(ctx context.Context) OrchestrationHooks {
	if h, ok := ctx.Value(orchHooksCtxKey{}).(OrchestrationHooks); ok {
		return h
	}
	return nil
}

// extractOrchHooksOrCtx 拿 OrchHooks——RunOption 优先 / ctx fallback。
//
// 嵌套场景关键：父 编排 sub-agent.Run(ctx, ...) 时如果 ctx 含 OrchHooks，子
// agent 自动继承——streaming 跨子图边界不丢。
//
// 用法：编排 wrapper 在 Run 入口调用，把结果传给 compose_v2.WithHooks RunOption：
//
//	hooks := extractOrchHooksOrCtx(ctx, runCfg.orchHooks)
//	if hooks != nil {
//	    runOpts = append(runOpts, compose_v2.WithHooks(adaptHooks(hooks)))
//	}
func extractOrchHooksOrCtx(ctx context.Context, optsHooks OrchestrationHooks) OrchestrationHooks {
	// review-v6 M-19 fix: 用 isFrameworkNil 防 typed nil — 业务自定义 OrchestrationHooks
	// 实现传 `var h *MyHooks; ...` 时是 typed nil interface (非 untyped nil).
	// 当前 WithOrchHooks 已防 typed nil, 这里是 defense in depth — 未来加新 With*
	// option 忘 isFrameworkNil 时第二层保护. 与其他防御协议对称.
	if !isFrameworkNil(optsHooks) {
		return optsHooks
	}
	return GetCtxOrchHooks(ctx)
}

// ResumeOption 是 Agent.Resume 的可选参数。
type ResumeOption func(*resumeConfig)

type resumeConfig struct {
	action     ResumeAction
	userInputs map[string]*schema.Message // by InterruptID
	userText   string                     // 非寻址型「用户这轮说了什么」
	orchHooks  OrchestrationHooks
	session    *RunSession // 续传给 framework 让事件累积到同一对象

	// per-call 覆盖（对齐 eino/langgraph：resume 本身是一次正常调用，能重传 per-call；
	// 仅本次 Resume 生效、不进 cp）。无则回退构造期默认（不继承上次 Run 的覆盖）。
	//
	// **不含 systemPrompt**：它首轮由 preamble 固化进 cp.messages，Resume 续跑用 cp 的、
	// 改不动；且三方无对应 option（eino 无改 instruction 口子、langgraph dynamic prompt
	// 非 option），故不提供 WithResumeSystemPrompt。
	model     model.ChatModel // nil = 不覆盖
	maxRounds int             // <=0 = 不覆盖
	modelOpts []model.Option  // per-request model.Option（对齐 WithRunModelOptions）；空=不注入
}

// overrides 抽出 per-call 覆盖（与 runConfig.overrides 共用 ctx 透传载体）。systemPrompt
// 恒 nil——Resume 不覆盖 systemPrompt（见 resumeConfig 字段注释）。
func (c resumeConfig) overrides() runOverrides {
	return runOverrides{model: c.model, maxRounds: c.maxRounds, modelOpts: c.modelOpts}
}

// WithResumeAction 设置 Resume 三态：Continue (默认) / Replan / Abort。
func WithResumeAction(a ResumeAction) ResumeOption {
	return func(c *resumeConfig) {
		c.action = a
	}
}

// WithResumeUserInputs 用 map[InterruptID]Message 答多 pending interrupts。
//
// 单 pending 场景下取唯一答案；多 pending 时按 InterruptID 配对。Run 1 中断时
// 框架在 InterruptError.Query.InterruptID 自动填稳定 ID（fnv64a hex 16 chars），
// caller 把答案按此 ID 包装传回。
func WithResumeUserInputs(inputs map[string]*schema.Message) ResumeOption {
	return func(c *resumeConfig) {
		c.userInputs = inputs
	}
}

// WithResumeUserText 传「用户这轮补的指示」——**非寻址型**负载，不指向任何一个提问。
//
// 典型用途：Replan。用户中途说「换个方向」，planner 要拿这句话重新规划，但它不在节点里、
// 没有自己的 InterruptID，按 ID 取不到东西。
//
// # 跟 WithResumeUserInputs 是两条独立通道
//
//   - `WithResumeUserInputs`：**寻址型**——这条答案是给某个具体提问的，按 InterruptID 投递
//   - `WithResumeUserText`：**非寻址型**——谁想据此改主意谁读（`compose_v2.GetResumeUserText`）
//
// **别再用假 ID 把非寻址型负载塞进前者**（业务曾约定 `"_replan_"` 之类的 sentinel key）。
// 那么做有两条实测代价：假 ID 跟真挂起并存时 `ask_user` 会反复重抛中断**死循环**；而且
// 逼出了一个「取 map 里任意一条」的读法，那个读法会**吃到别人的答案**（实测：用户答任务 A
// 的 ID，节点执行了任务 B 那件事，`err == nil`）。
func WithResumeUserText(text string) ResumeOption {
	return func(c *resumeConfig) {
		c.userText = text
	}
}

// WithResumeOrchHooks 为本次 Resume 注入 OrchestrationHooks。
//
// **audit²¹ HIGH-2 fix**: isFrameworkNil 检 typed nil (协议二十一复证).
func WithResumeOrchHooks(h OrchestrationHooks) ResumeOption {
	return func(c *resumeConfig) {
		if isFrameworkNil(h) {
			return
		}
		c.orchHooks = h
	}
}

// WithResumeSession 续传 caller 的 RunSession 让事件累积到同一对象（避免
// Resume 重新 NewRunSession 后 caller 看到事件链断层）。
func WithResumeSession(s *RunSession) ResumeOption {
	return func(c *resumeConfig) {
		c.session = s
	}
}

// WithResumeModel 仅对本次 Resume 覆盖构造期的 model（对齐 eino/langgraph：resume 是
// 一次正常调用，可重传 per-call model）。不传则用构造期 WithModel 的 model（per-call
// 覆盖不自动跨调用持久化，需 resume 时显式重传）。typed nil silent skip。
func WithResumeModel(m model.ChatModel) ResumeOption {
	return func(c *resumeConfig) {
		if isFrameworkNil(m) {
			return
		}
		c.model = m
	}
}

// WithResumeMaxRounds 仅对本次 Resume 覆盖构造期的 maxRounds。n<=0 忽略。
//
// 配合 WithRunMaxRounds 让 cap 跨 HITL 续上（audit Finding 2）：业务 Run 传
// WithRunMaxRounds(n)、Resume 也传 WithResumeMaxRounds(n) 即可在中断恢复后续用同一
// 上限（框架 visit 计数每次 compose_v2.Run 从 0 重置，故每段 Run/Resume 各自约束 n 轮）。
func WithResumeMaxRounds(n int) ResumeOption {
	return func(c *resumeConfig) {
		if n > 0 {
			c.maxRounds = n
		}
	}
}

// WithResumeModelOptions 仅对本次 Resume 注入 per-request model.Option（对齐
// WithRunModelOptions；per-call 覆盖不自动跨调用持久化，Resume 想续用须显式重传）。
// 强制的 tool_choice=required 同样只管到模型第一次调工具（第一批解锁了新工具的 tool_search 不算）为止——按任务算：Run 里已经调过的，
// 续跑时传进来的 required 也不再生效。
func WithResumeModelOptions(opts ...model.Option) ResumeOption {
	return func(c *resumeConfig) {
		c.modelOpts = opts
	}
}

// applyResumeOptions 应用 ResumeOption 列表。
func applyResumeOptions(opts []ResumeOption) resumeConfig {
	cfg := resumeConfig{action: ResumeActionContinue}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithToolTimeout 给**单个业务工具**调用一个 deadline（仅 BasicAgent；0 = 不设，默认）。
//
// # 它解决什么
//
// 在这之前，框架能施加影响的唯一时间点是调用方**整个 Run** 的 ctx deadline——一个卡住的
// 工具能把整轮拖死，而业务想单独限制某次工具调用没有着力点。
//
// # ⚠️ 它给的是 deadline，不是「掐断」
//
// 实现是把带 deadline 的 ctx 交给工具。**只有 select 了 `ctx.Done()` 的工具会被解开**
// ——HTTP 调用、DB 查询、沙箱执行这类走标准库的天然响应，而一个纯 CPU 循环或者忽略 ctx
// 的实现不会。Go 没有安全地中止一个 goroutine 的办法，框架**不会**丢下它自己往前走
// （那样工具的副作用会在无人观测的时刻发生，比等它更糟）。
//
// 参照的那家在这个问题上更极端：同步节点**根本不允许**设 timeout，编译期直接拒——
// 「给不了就不给这个选项」。Go 这边给不了那么干净的二分（同一个工具接口下，是否响应 ctx
// 由实现决定、框架看不出来），所以取的是**让失效可见**：工具超过 deadline 还返回时，
// 框架 `slog.Warn` 点名这个工具说「它没响应 ctx，这次调用实际没被保护」。
//
// **一个不生效又不出声的超时，比没有超时更坏**——业务会以为有保护。
//
// # 作用域
//
// 只裹**业务**工具。框架自带工具（ask_user / activate_skill / final_output / …）豁免：
// 它们要么在等人（超时把 HITL 打断）、要么是框架自己的同步逻辑。
//
// 超时表现为工具返回 `context.DeadlineExceeded`——那是**这次工具调用**的失败，不是控制流
// 信号（调用方的 ctx 没死），所以按普通工具错处理：转成 tool_result 给模型看，让它换条路。
func WithToolTimeout(d time.Duration) Option {
	return func(c *Config) { c.ToolTimeout = d }
}
