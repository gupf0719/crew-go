package agent_v2

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/prompt"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/store"
	"crew-go/tool"
)

// Config 保存 BasicAgent / PlanAgent 共享的配置。
//
// 不持有 sync.Mutex / atomic 字段——构造期 immutable，运行时通过 ctx /
// 节点 channels 透传 per-Run 状态。
type Config struct {
	Name string
	// nameExplicit 记「Name 是业务给的还是自动生成的」。
	//
	// 自动生成的名字每次构造都不一样，当日志标签没问题，当**持久身份**就不行了——
	// 会话历史按归属 agent 名读回，名字一换就全部读不回来。见 validateHistoryConfig。
	nameExplicit bool
	Description  string // 一句话描述：供 LLMPlanner 自动注入 planner prompt
	SystemPrompt string
	// AgentsMDPaths：WithAgentsMD 配置的 AGENTS.md 文件路径——model 调用时 transient
	// 注入为 harness 指令层（排 SystemPrompt 任务层之前），不持久 / 每轮重注入 / 不被压缩。
	AgentsMDPaths []string
	Model         model.ChatModel
	Toolkit       *tool.Toolkit
	Hooks         hook.Hooks
	MaxRounds     int

	// HandoffTransferOptIn：业务用 [WithHandoffTransfer] 声明「这个 agent 允许把对话
	// 转出去」。**跟授予是两把不同的闸**：授予回答「这次调用里，谁有资格转」（编排给的），
	// 本字段回答「这个 agent 愿不愿意转」（构造它的人给的）。两把都开才注册 transfer 工具
	// ——业务据此把某个候选做成**终点**（收得到、转不出去）。
	HandoffTransferOptIn bool
	// RouteToolOptIn：业务用 [WithRouteTool] 声明「这个 agent 愿意替 RoutingAgent 定路由」。
	// 跟路由授予是两把闸，两把都开才注册 route_to（见 routeGrant）。
	RouteToolOptIn bool
	// AskUserOptIn：业务用 [WithAskUser] 声明「这个 agent 可以停下来问人」。ask_user 跟结构化输出的
	// final_output 一样由框架每次 Run 注入（见 withFrameworkTools），不写进 Toolkit——那是业务传进来的
	// 对象，可能被几个 agent 共用。
	AskUserOptIn bool

	// Store 跨 thread / 跨用户的长期 KV 持久化抽象（用户偏好 / 长期事实 /
	// 共享黑板 / 幂等记录）—— crew-go 第三个持久化层，跟 Checkpointer 并列。
	// （历史：原 Memory 接口已被 Store 取代，PR 3.3 commit `e6505f9` 落地）
	//
	// BasicAgent.Run 入口会自动 `store.WithContext(ctx, Store)` 让下游所有节点 /
	// hook / tool 通过 `store.FromContext(ctx)` 取到（详见 doc/v2/store.md §5）。
	//
	// nil = 不挂 store；业务调 store.FromContext(ctx) 返 nil 需自查降级。
	//
	// **8 编排 wrapper 不重复接 WithStore**——业务给底层 BasicAgent 传 WithStore，
	// 或在 Run 入口显式 `ctx = store.WithContext(ctx, st)` 让整树共享（RFC §5）。
	Store store.Store

	// History 是 WithHistory 装配的会话历史配置（ISSUE-98）——按会话（ThreadID）
	// 留下完整消息流，跨 Run 活着。nil = 没开（opt-in，不配零开销）。
	//
	// 跟 StoreMemory 是两层：那层存「提炼后的知识」按相似度检索片段、会改写会删除；
	// 这层存「逐字时序回放」只追加不改。见 HistoryStore godoc。
	History *historyConfig

	// StoreMemoryNS / StoreMemory 是 RFC §6 "WithStoreMemory helper" 配置——
	// 框架在 preamble 自动 store.Search(ns, query=lastUserInput) 注入到 LLM messages
	// 头部 + postamble 自动 Put 保存本轮新消息。是已删 Memory 接口的 Store-backed
	// 等价物（PR 3.3 commit `e6505f9` 落地，Memory 接口已删）。
	//
	// **PR 3.3+ 真消费**：BasicAgent.buildInitialMessages 子步调
	// `store.Search(ctx, StoreMemoryNS, ...)` 注入历史 + commitFinal 内
	// 调 `store.Put(ctx, StoreMemoryNS, key, last.Content)` 保存最终答复的**文本**
	// （2026-09-07 起存文本：那时存整条 message 时 in-memory 与持久后端读回来的类型不一致，
	// 喂给 LLM 的东西也跟着分叉；store 按值的 JSON 形状存之后类型已经一致，文本仍是注入时最直接的
	// 形状。Content 为空则跳过）
	// （写入者维在 ns 上防成员之间撞键，见 StoreMemoryConfig.WriterID）。
	//
	// nil StoreMemory = 不启用 helper。
	StoreMemoryNS store.Namespace
	StoreMemory   *StoreMemoryConfig

	// Skill 集成 skill 系统——modelNode 内部自动注入 activate_skill 工具，
	// 让 LLM 通过 tool-call 激活专项技能（拿到对应 prompt 指导）。
	// nil = 不启用。
	Skill *skill.SkillSource

	// SkillSystemPrompt 业务自定义 skill 触发提示——替代默认中文 hint
	// 「当你判断当前任务适合使用某个专项技能时，请先调用 activate_skill 工具…」。
	//
	// 仅当 Skill 启用（Skill != nil && Skill.SkillBox != nil）时生效；
	// 空字符串 = 用框架默认中文 hint。
	//
	// 典型使用场景：英文 prompt 业务 / 多语言 / 自定义触发措辞 / 加 skill 调用约定
	// （如「优先选 description 含 SQL 的 skill」）。
	SkillSystemPrompt string

	// SkillListingBudgetChars 控制 skill listing 总字符预算（activate_skill 工具
	// description 里列 skill 时的总长上限）。0 = 用框架默认 SkillListingBudgetChars (4000)。
	//
	// 三档降级策略：全 fits → 每条描述按预算截断 → maxDescLen < 20 时降级仅名字。
	//
	// **典型场景**：业务接 50+ skill 时调小（避免 listing 占满 context window）；
	// 或业务用大 context window（如 Claude 200K）调大让 listing 更详细。
	SkillListingBudgetChars int

	// SkillListingPerEntryChars 单条 skill description 硬上限（即使总预算够也截断）。
	// 0 = 用框架默认 SkillListingPerEntryChars (200)。
	//
	// 防御「业务写超长 SKILL.md description 单条吃掉整个 listing」的场景。
	SkillListingPerEntryChars int

	// SkillSelector adaptive / pre_match 模式的预激活配置。
	// 仅当 Skill.Options.ActivationMode 为 adaptive / pre_match 时生效；
	// toolCall 模式下不消费此字段。
	// nil = 用框架默认（无 LLM 仲裁、MinScore=0、MaxCandidates=8）。
	SkillSelector *SkillSelectorConfig

	// SkillSelectionHook adaptive / pre_match 模式预激活后业务订阅 SkillSelection
	// 的回调——做 telemetry / dashboard / 自定义日志。nil = 不订阅（仅 slog warn）。
	// 仅 adaptive / pre_match 模式生效；toolCall 模式不调（无预激活流程）。
	SkillSelectionHook SkillSelectionHook

	// SkillBodyMaxChars adaptive / pre_match 模式注入 sysPrompt 的 skill body
	// 字符上限——业务 SKILL.md body 过长时截断（带 trailing "…"）。0 = 用框架默认
	// 20000 char (~5K token，对齐 Claude POST_COMPACT_MAX_TOKENS_PER_SKILL=5_000)。
	//
	// **why 必要**：adaptive 注入 sysPrompt → AutoCompact/ReactiveCompact 都跳过
	// system token 估算（设计正确：sys 不被压缩）→ 大 SKILL.md body 直接打满 ctx
	// window 没人防御。本字段是该路径的唯一上限。
	SkillBodyMaxChars int

	// ToolResultMaxBytes 是**单条业务工具结果**进消息流前的字节上限，超了从中间挖空
	// （保留头尾 + 一条说明被删多少的标记）。<= 0 关闭（默认关闭）。
	//
	// **why 在这里拦而不是等压缩层**：压缩层是按「组」丢的，一条巨型工具结果会把它所在的
	// 整组变得非丢不可；而它若又落在受保护的组里（比如同组带着 skill 正文 marker），
	// 压缩层就只剩「整组丢」和「整组留」两个都不能选的动作。堵在源头之后，那个死局基本消失。
	//
	// **框架工具恒豁免**（按实例判，见 frameworkMintedTool）：它们的结果是结构化的、且有的承重——
	// `final_output` 装的就是最终答案本身，截它等于把答案改坏。
	ToolResultMaxBytes int

	// ToolResultSpill 是超大工具输出转存配置（nil = 关闭）——超过阈值的业务工具结果全文存进 store，消息流里
	// 只留说明 + 头尾预览 + 取回 id，并注入 read_tool_output。经 [WithToolResultSpill] 设，见 tool_spill.go。
	ToolResultSpill *ToolResultSpillConfig

	// BackgroundTasks 是长时间运行工具的后台执行配置（nil = 关闭）——点名的工具一次调用跑过 YieldAfter 还没返回
	// 就转入后台、先给模型一个任务 id，结束后作为通知送进对话，并注入 background_task。经 [WithBackgroundTasks]
	// 设，只在顶层生效，见 background_task.go。
	BackgroundTasks *BackgroundTasksConfig

	// SkillModelResolver 是 P2.B 解析 skill frontmatter `model: <name>` 字段为
	// `model.ChatModel` 实例的回调（业务实现）。
	//
	// 仅 fork 模式 + skill.Model 非空时调用——inline 模式整 Run 共享一个 model 无切换边界。
	// 返 (nil, nil) 视为"用 parent model 兜底"；返 err 时 fork 调用失败给 LLM。
	SkillModelResolver SkillModelResolver

	// SkillSubAgentResolver 是 P2.C 解析 skill frontmatter `agent: <name>` 字段为
	// `Agent` 实例的回调（业务实现）。
	//
	// 仅 fork 模式 + skill.Agent 非空时调用——空 Agent 时框架
	// 默认创建新 BasicAgent（SystemPrompt = skill body）。返 (nil, nil) 视为兜底默认；
	// 返 err 时 fork 调用失败给 LLM。
	SkillSubAgentResolver SkillSubAgentResolver

	// InlineSkillBody 启用 P2.D inline skill body 协议（仅 toolCall 模式生效）。
	//
	// **行为差异**：
	//   - 默认 false：activate_skill tool_result content 直接是 skill body（current）。
	//     skill body 进 tool_result wrapper，AutoCompact splitProtectedGroups 按 API
	//     round 整组保留（保 tool_use ↔ tool_result 配对完整）
	//   - true：activate_skill tool_result 退化为 terse "已激活，见下条 system reminder";
	//     skill body 作为额外 UserMessage 注入对话流（带 SkillBodyAnnotation marker），
	//     语义上 body 是"普通对话内容"而非"tool wrapper"
	//
	// **why P2.D**：参考 Claude Code SkillTool.newMessages 设计——skill body 应该是
	// 第一类对话内容（user 发的"system_reminder"），不是 tool wrapper。tool_result 仅
	// 作激活 ack。LLM 看到的对话流更自然，AutoCompact 可按 user-group 边界正常分组。
	//
	// **token 收益**：tool_result 短了（~30 char vs N00 char body），但 body 仍在 user
	// message 里所以总量持平。主要收益是**语义清晰** + 跨层 marker 协议自然衔接。
	InlineSkillBody bool

	// ToolSearch 启用工具级渐进披露（nil = 关闭）——工具多（尤其 MCP Hub 聚合多 server）时
	// deferred 工具初始不进 LLM bind list，LLM 调 tool_search 检索后才揭示。见 tool_search.go。
	ToolSearch *ToolSearchConfig

	// ToolFilter 每轮 model 调用前裁剪本轮可见的**业务工具**（nil = 不裁，全部 bind）。
	// 回调返回本轮允许的业务工具名白名单，框架在 bind 前过滤——返 nil 表示不裁、返子集
	// 表示只 bind 这些业务工具（未命中的名忽略）。**框架工具（ask_user / activate_skill /
	// tool_search / transfer_to_agent / route_to / ...）恒保留、不受白名单影响**（与审批层
	// 豁免一致，防业务漏带导致搜索/技能/问用户/路由静默失效）。判的是实例不是名字（见
	// frameworkMintedTool）：业务工具恰好叫这些名字时照常受白名单约束。
	// ⚠️ **「恒保留」只对 [ToolFilter] 这条路成立**——配了结构化输出时，软降落轮（轮次预算最后一轮，见
	// [withFinalRound]）另有一道过滤，那里框架工具全部摘掉、**只留 `final_output`**（没配时那一轮工具集照旧，
	// 用 tool_choice=none 关掉工具调用）。两道过滤的语义相反：这道防「业务白名单漏带把问用户/技能静默关掉」，
	// 那道要的正是摘掉它们。用同一个 helper 表达两者曾让软降落的白名单一行没生效，故各有各的载体。
	// input 是本轮发给 LLM 的最后一条**真实**消息（首轮通常是 user 输入，round 2+ 通常是
	// tool_result）——框架合成的记账文本（见 UnresolvedCallsRecordAnnotation）会被跳过，
	// 否则 Handoff 共享视图下它恰好排在末尾、业务拿到的是框架记账而不是对话内容；框架以 user
	// 角色追加的提示（后台任务通知、重复调用提醒、inline skill 正文，见 IsFrameworkNote）同样跳过；
	// 每轮都跑（含 Resume 后的轮）。用于"分类后裁工具"等场景（RoutingAgent classifier 分类 →
	// filter 返子集是典型用法）。与 ToolSearch（LLM 主动搜解锁）正交可叠加。
	// **执行侧同样拦**：模型照样发了调被裁掉的业务工具的 tool_call（凭上文记得的名字）时，工具不执行，模型收到
	// [ToolNotAvailableErrCode] 失败结果；判的是产出这一批 tool_call 的那一轮交给模型的工具（随存档走，续跑时不重算回调）。
	// 软降落轮摘掉的工具（含框架工具）同理。
	ToolFilter func(ctx context.Context, input *schema.Message) []string

	// ToolMiddlewares 包住每次**业务**工具调用（观测 / 短路 / 挂起）。先注册的最外。
	// 契约与顺序见 [ToolMiddleware] —— 尤其「next 至多调一次」和「框架工具不过链」两条。
	ToolMiddlewares []ToolMiddleware

	// ToolTimeout 是**单个业务工具**调用的 deadline（0 = 不设，默认）。见 [WithToolTimeout]。
	ToolTimeout time.Duration

	// ModelRetry 是一次模型调用失败时整次重来的策略（零值 = 不重试，默认）。见 [WithModelRetry]。
	ModelRetry compose_v2.RetryConfig

	// RepeatTool 配置工具重复调用检测（nil / 未启用 = 零开销）。见 [WithRepeatToolReminder]。
	RepeatTool *RepeatToolConfig

	// PromptTemplate 提示词模板（nil = 用 WithSystemPrompt 字符串）——设了则 preamble
	// 渲染它并 prepend 到对话最前（system + 可选 few-shot / 历史占位）。变量由
	// PromptTemplateVarsFn 在 preamble 时按 ctx / 当轮 input 动态提供。与 WithSystemPrompt
	// 正交：模板消息在前、SystemPrompt 系统消息在后（一般二选一，用模板时把 SystemPrompt 留空）。
	PromptTemplate *prompt.Template
	// PromptTemplateVarsFn 渲染 PromptTemplate 时的变量提供器（nil = 无变量）。在 preamble
	// 节点渲染时调，可按 ctx / 当轮 input 动态算变量（如当前时间 / 用户画像 / few-shot 列表）。
	// preamble 只首轮跑（ISSUE-74）：fresh Run 调一次、渲染结果随 messages 进 cp；Resume 时仅
	// 当中断点在 preamble 才重渲染，中断点在 model/tools 则从 cp 恢复、不重调（结果已固化）。
	PromptTemplateVarsFn func(ctx context.Context, input *schema.Message) map[string]any

	// OutputSchema 结构化输出配置（nil = 关闭，最终答复自由文本）——设了则最终答复走
	// final_output 工具 + 自校验 + ReAct 回炉（仅 BasicAgent）。经 WithOutputSchema 设；final_output
	// 由框架每次 Run 注入，不写进 Toolkit（见 withFrameworkTools）。见 structured_output.go。
	OutputSchema *outputConfig

	// ApprovalMode 工具执行审批模式（默认 ApprovalAuto 全放行、零开销）。见 WithApprovalMode + approval.go。
	ApprovalMode ApprovalMode
	// ApprovalHeuristic 业务审批启发式 seam（可选，nil = 仅按 ApprovalMode）。见 WithApprovalHeuristic。
	ApprovalHeuristic ApprovalHeuristic
	// ApprovalScope 把一次工具调用归一化成「这算哪件事」的指纹（可选，nil = 参数原文精确匹配）。
	// 决定 approve_always 这条规则覆盖多大范围。见 WithApprovalScope。
	ApprovalScope ApprovalScope
	// ApprovalPrompt 替换写进系统提示的审批说明（nil = 默认文案，指向空串 = 不写）。见 WithApprovalPrompt。
	ApprovalPrompt *string
	// TurnGates 回合结束闸门（空 = 不设，图里没有闸门节点）。见 WithTurnGate。
	TurnGates []TurnGateFunc
	// TurnGateMaxVetoes 一个任务里闸门最多否决几次（<= 0 用 DefaultTurnGateMaxVetoes）。见 WithTurnGateMaxVetoes。
	TurnGateMaxVetoes int
	// approvalOwner 是这个 agent 实例在 approve_always 规则里的身份，构造时铸（见 NewBasicAgent，在选项之后）。规则只对
	// 答应它的那个 agent 生效：工具名只在一个 agent 的工具表里唯一，别的 agent 同名的可能是另一个工具（见
	// approvalRuleKey）。
	approvalOwner string

	// MaxHistoryPairs 跨 Run 历史对硬上限——为防多轮对话累积爆 context 的兜底
	// 截断。**默认 defaultMaxHistoryPairs=20**（defaultConfig 设，默认开启）；业务
	// WithMaxHistoryPairs(0) 显式禁用。
	//
	// 非 0 时在 prior 注入前删超出部分（按 2x 估算保留最后 MaxHistoryPairs 对）。
	// **prior 的来源看有没有开会话历史**：开了是历史层的逐字完整流（含工具往返），
	// 没开是 session events。前者意味着截断可能切在 tool_call 和它的结果之间，所以
	// 切点走配对感知（见 truncatePriorPreservingMarkers），带 marker 的消息还会连同
	// 声明它的 assistant 一起抽出保护。
	//
	// 与 4 层 compact 关系：兜底层（无 LLM、无 token 估算），HistoryCompactor 缺
	// 时才生效；HistoryCompactor 接管后这个值降级为 keepPairs 提示。
	MaxHistoryPairs int

	// HistoryCompactor 自定义历史压缩函数——对消息列表做 token-aware 的智能截断
	// （比 MaxHistoryPairs 更聪明）。设此函数后 MaxHistoryPairs 作 keepPairs 提示。
	// nil = 不启用，回退到 MaxHistoryPairs 简单截断。
	HistoryCompactor HistoryCompactor

	// EmitCpEvent 是否 fire cp.save / cp.load / cp.cleanup EventSink event
	// (Phase 2 Sub-3). 默认 false opt-in.
	// true 时透传 compose_v2.WithEmitCpEvent 让 framework cp 操作 (SaveNS / LoadNS /
	// DeleteSubtree) wrap fire 含 SessionID/NS/Status/ChannelsCount/Duration/Ok/Err.
	EmitCpEvent bool

	// 4 层 context compaction（任一为 nil 等价关闭对应层）。
	MicroCompact    *MicroCompactConfig    // 第 1 层：tool_result 末段清白名单
	AutoCompact     *AutoCompactConfig     // 第 2 层：modelNode 入口 LLM 摘要
	ReactiveCompact *ReactiveCompactConfig // 第 3 层：prompt-too-long 兜底重试
}

// HistoryCompactor 自定义历史压缩函数。
//
// 入参：messages（当前累积消息）+ keepPairs（caller 期望保留的 user/assistant 对数）
// 出参：压缩后的消息列表 + error
//
// **入参可能含 tool_call / tool_result**：开了会话历史之后，prior 是逐字完整流，工具
// 往返就在里面（没开历史时只有首尾，这也是这份契约以前没提它的原因）。实现不必自己保
// 配对——出口有一层按声明重建配对块的兜底，怎么切都不会送出非法形状；但**切断配对是有
// 代价的**：被切散的那一轮，结果会被替换成「没返回结果」的占位，模型看到的是"这次工具
// 没成"。想保住内容就别把 assistant 和它的 tool_result 分开。
//
// **报错会让整个 Run 失败**（它跑在 preamble 节点里）。历史是旁路、压缩不是——压不动
// 就返回原列表，别返 error。
//
// **idempotent 约定**：可能因 retry / preamble Resume 被多次调用，实现勿持有外部
// 状态。preamble HITL 场景：业务可抛 `compose_v2.InterruptError` 做 HITL gate
// （ISSUE-74 修后），cp 自动保存，Resume 重跑业务取 ResumeContext 答案续跑。
// 详见 `doc/v2/hitl-resume.md` §12 BasicAgent preamble HITL。
type HistoryCompactor func(ctx context.Context, messages []*schema.Message, keepPairs int) ([]*schema.Message, error)

// SkillModelResolver 把 skill frontmatter `model: <name>` 字段解析为 model.ChatModel
// 实例（P2.B）。返 (nil, nil) = "用 parent model 兜底"；返 (nil, err) = fork 失败。
//
// 业务典型实现：维护 modelName → ChatModel 字典（如 {"gpt-4": gpt4Client, "haiku": haikuClient}）。
type SkillModelResolver func(modelName string) (model.ChatModel, error)

// SkillSubAgentResolver 把 skill frontmatter `agent: <name>` 字段解析为 Agent 实例
// （P2.C）。返 (nil, nil) = "用框架默认 BasicAgent 兜底"；返 (nil, err) = fork 失败。
//
// 业务典型实现：维护 agentName → Agent 字典，sub-agent 可预配置 hooks / toolkit / memory。
type SkillSubAgentResolver func(agentName string) (Agent, error)

// Option 是配置 BasicAgent / PlanAgent 的功能选项。
type Option func(*Config)

// defaultMaxHistoryPairs 是 MaxHistoryPairs 的默认值——跨 Run 历史对硬上限兜底截断
// 阈值，**默认开启**（防多轮复用 session 累积爆 ctx）。业务 WithMaxHistoryPairs(0) 可显式
// 禁用。basic_agent.go 的 HistoryCompactor keepPairs hint 复用同一常量避免魔法数字 drift。
const defaultMaxHistoryPairs = 20

func defaultConfig() Config {
	// Name 自动生成唯一值——共享 session 下区分多 agent 产出者
	return Config{
		Name:            "agent_v2_" + newUUID()[:8],
		MaxRounds:       10,
		MaxHistoryPairs: defaultMaxHistoryPairs, // 默认开启历史截断兜底（防多轮复用 session ctx 爆）
		Hooks:           hook.NoOpHooks{},
	}
}

// WithName 设置 agent 名（用于 hook AgentInfo + 日志 + cp 寻址）。
func WithName(name string) Option {
	return func(c *Config) {
		c.Name = name
		c.nameExplicit = name != ""
	}
}

// WithDescription 设置 agent 职责描述，供 LLMPlanner / SupervisorAgent 自动
// 注入 planner prompt（对照 compose/agent.WithDescription）。
//
// 推荐写法：一句话说明本 agent 适合处理什么任务。
// 例：WithDescription("调研资料、列事实，必要时调用 search 工具")
func WithDescription(desc string) Option {
	return func(c *Config) { c.Description = desc }
}

// WithSystemPrompt 设置 system prompt——LLM 调用前作为首条 system 消息。
func WithSystemPrompt(prompt string) Option {
	return func(c *Config) { c.SystemPrompt = prompt }
}

// WithAgentsMD 配置 AGENTS.md 项目级操作指令文件（多文件按序拼接）。
//
// 每次 model 调用时 transient 注入为 **harness 指令层**（排在 WithSystemPrompt 任务层
// 之前），实现「操作规范 vs 任务/人设」二层分离（类比 Claude Code CLAUDE.md 这类
// harness 级默认指令）。
//
// **transient 机制**：内容只在 LLM 调用末段 prepend，**不写进持久
// 消息历史 / 不进 cp**——所以①不被 4 层压缩 summarize（每轮在压缩之后注入）②Resume 自动
// 重注入（每轮都注）③无需幂等 marker / 压缩保护。缺文件静默跳过。
func WithAgentsMD(paths ...string) Option {
	return func(c *Config) { c.AgentsMDPaths = paths }
}

// WithModel 设置 LLM ChatModel。
//
// **audit⁶ HIGH-2 fix**: isFrameworkNil 检 typed nil (协议六复证 — audit² isAgentNil
// 只防 Agent, model.ChatModel 等其他 framework interface 全漏). 业务
// `var m *MyModel; WithModel(m)` typed nil → Generate() 远 stack nil deref.
func WithModel(m model.ChatModel) Option {
	return func(c *Config) {
		if isFrameworkNil(m) {
			panic("WithModel: typed nil ChatModel (含 Go interface 陷阱)")
		}
		c.Model = m
	}
}

// WithToolkit 设置 tool 注册表——BasicAgent ReAct 循环内可调的所有 tool。
//
// `*tool.Toolkit` 是具名指针, `tk == nil` (untyped) 是 "不挂工具" by-design idiom — 不防御.
func WithToolkit(tk *tool.Toolkit) Option {
	return func(c *Config) { c.Toolkit = tk }
}

// WithToolMiddleware 追加工具调用中间件——包住每次业务工具调用，业务不碰 Tool 对象。
//
// 契约见 [ToolMiddleware]。最常见的两个用途：
//
//	// 观测：注意它跑在 fanout goroutine 里，持状态要自己加锁
//	agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
//	    return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
//	        start := time.Now()
//	        res, err := next(ctx, name, args)
//	        slog.InfoContext(ctx, "tool done", "name", name, "ms", time.Since(start).Milliseconds())
//	        return res, err
//	    }
//	})
//
//	// 短路：不调 next 直接给结果（会被记进 memo 并在续跑时重放，别返「本次特有」的东西）
//	return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
//	    if hit, ok := cache.Get(name, args); ok {
//	        return hit, nil
//	    }
//	    return next(ctx, name, args)
//	}
//
// **工具在一个 tool_call 里只执行一次**：同步执行的工具成功返回之后，结果记在这个 tool_call 里。中间件在 next 之后
// 挂起（问人确认结果）、或在 next 之后返回 error，续跑 / 重试时 next 交回记下的那一份、工具不再执行——跟这次传给
// next 的参数无关。交回的 Output：类型用 `compose_v2.RegisterValueType` 注册过的是原类型；字符串原样；别的是交给
// 模型的那段文本（`%v`），类型变成 string——模型看到的跟没停过时一样，中间件要做类型断言就注册那个类型。超过
// [WithToolTimeout] 之后才返回的成功照样按超时收尾。Metadata 编不成 JSON 时记不下（告警），续跑会再执行一次。工具
// 返回 Go error 的那次不记。所以「校验产出、不合格就重来」别在 next 之后返 error（拿回的还是同一份），
// 返 `tool.Failure`，让模型看到失败、重发一个 tool_call（新的一次调用）。可后台的调用（[WithBackgroundTasks]）不记。
//
// ⚠️ 跟嵌套调用相关的一件事（工具是 AgentAsTool、或工具体里经 RunSubAgent 调别的 agent 时）：
//
//   - **往 args 里注入每次都变的值**（trace_id、时间戳），而它落进了交给被调方的输入（AgentAsTool
//     配了自定义 schema 时整个 args 都是输入；默认 schema 下只有 `input` 字段是），被调方的输入就每次
//     都不同，按内容定身份就是一次新调用：续跑时被调方从头跑、换一个 ID 重问、永远收敛不了。要么别
//     注入进那部分（放 ctx 里、或放 `input` 之外的字段），要么给 AgentAsTool 声明身份
//     （[WithAgentToolCallKey]）。
func WithToolMiddleware(mws ...ToolMiddleware) Option {
	return func(c *Config) {
		for _, m := range mws {
			if m != nil {
				c.ToolMiddlewares = append(c.ToolMiddlewares, m)
			}
		}
	}
}

// WithToolFilter 设每轮业务工具裁剪回调——每次 model 调用前框架调 fn 拿本轮允许的业务
// 工具名白名单，在 bind 前过滤（fn 返 nil = 不裁、返子集 = 只 bind 这些业务工具）。框架
// 工具（ask_user/activate_skill/tool_search/...）恒保留、不受影响。fn 为 nil 不挂。
//
// ⚠️ **轮次预算的最后一轮**（软降落，见 [withFinalRound]）fn 照常被调用，但那一轮模型不调工具：
// 没配 WithOutputSchema 时照常按 fn 的白名单 bind，只是 tool_choice=none 关掉了工具调用；配了时工具
// **只剩 `final_output`**（框架那件），fn 的白名单不生效。那一轮模型硬调的工具一律不执行。典型
// 用法：RoutingAgent classifier 分类意图后只开相关工具子集。详见 Config.ToolFilter。
//
// 裁掉的工具在执行侧同样拦：模型硬调时不执行，拿到 [ToolNotAvailableErrCode] 失败结果——模型能调的就是这一轮交给
// 它的那些。
func WithToolFilter(fn func(ctx context.Context, input *schema.Message) []string) Option {
	return func(c *Config) {
		if fn == nil {
			return
		}
		c.ToolFilter = fn
	}
}

// WithPromptTemplate 用提示词模板生成 system + 可选 few-shot 消息（替代 WithSystemPrompt
// 字符串拼接）。varsFn 在 preamble 时按 ctx / 当轮 input 提供模板变量（nil = 无变量）。
// tmpl 为 nil 不挂。模板渲染结果 prepend 到对话最前；详见 Config.PromptTemplate。
func WithPromptTemplate(tmpl *prompt.Template, varsFn func(ctx context.Context, input *schema.Message) map[string]any) Option {
	return func(c *Config) {
		if tmpl == nil {
			return
		}
		c.PromptTemplate = tmpl
		c.PromptTemplateVarsFn = varsFn
	}
}

// WithHooks 设置业务 hook (OnAgentStart/End + OnModelStart/End + OnToolStart/End + OnError).
// nil 等价于 hook.NoOpHooks.
//
// **audit⁶ MED-1 fix**: isFrameworkNil 检 typed nil hook (协议六复证). 业务
// `var nilHooks *MyHooks; WithHooks(nilHooks)` 时 h != nil 但 underlying nil →
// 退化 NoOp (by design idiom 保留).
//
// **跨包 isFrameworkNil 协议分层** (R30 audit B-MED-3 备注澄清两套 "N 编排" 数字):
//   - **agent_v2 wrapper 层 8 编排** (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan/Graph
//     各自 With*Hooks): 业务 wrapper 入口检 typed nil hook 防下游 deref panic
//   - **compose_v2 builder 层 7 编排** (BasicAgentBuilder + 6 高阶 builder: Sequential/
//     Parallel/Loop/Routing/Reflect/Handoff; 不含 Plan + Graph builder): 基础层 builder
//     入口检 typed nil 防 framework 内部调用 deref. 见 `agent_v2/basic_builder.go::Compile`
//   - 两层数字不同是 by-design (不同语义层独立检查), 不是 stale.
//
// **panic 语义**: hook panic 由框架兜住、不让 goroutine 崩——多数转成这次调用的错误，调用成功之后的
// OnAgentEnd 只作观测、panic 就地兜住记一条 ERROR（见 hook 包文档「panic 语义」）.
func WithHooks(h hook.Hooks) Option {
	return func(c *Config) {
		if isFrameworkNil(h) {
			h = hook.NoOpHooks{}
		}
		c.Hooks = h
	}
}

// WithRepeatToolReminder 开启工具重复调用检测：模型连续用同样参数调同一个工具到
// 阈值时，框架往对话流里塞一条提醒，让它有机会自己拐弯。
//
//	agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true})
//
// 不开时零开销（不算键、不建链）。判据与取舍见 [RepeatToolConfig]；提醒**不进会话历史**，
// 理由见 [RepeatToolReminderAnnotation]。
//
// ⚠️ **它不是 maxRounds 的替代品**，是那道悬崖之前的一级台阶：撞满轮次仍然抛
// [MaxIterationsError]，这里只是让模型在那之前听见一句话。
func WithRepeatToolReminder(cfg *RepeatToolConfig) Option {
	return func(c *Config) { c.RepeatTool = cfg }
}

// WithHooksChained 把 h **链到已配的 hook 之前**，而不是覆盖它。
//
// [WithHooks] 是覆盖写（后写赢），所以「业务先配了自己的 hook、框架或工具函数再追加
// 一个」这个形态用它会**静默丢掉业务那份**。`observability.Install` 正是这个形态：
// 它要在业务已有的 opts 后面追加 obs 的 hook，而业务大概率也传了 WithHooks。
//
// **顺序是 h 在前**（`NewChainHooks(h, 已配的)`）：观测类 hook 要最早 fire start、
// 最晚 fire end（defer 语义），且前面的 hook panic 时不该顺带吃掉它的 end。
// 详见 `observability.Observability.AgentHooks` 的顺序说明。
//
// 已配的是 nil / NoOp 时退化成单独一个 h（`NewChainHooks` 自己过滤 nil）。
func WithHooksChained(h hook.Hooks) Option {
	return func(c *Config) {
		if isFrameworkNil(h) {
			return
		}
		if isFrameworkNil(c.Hooks) {
			c.Hooks = h
			return
		}
		c.Hooks = hook.NewChainHooks(h, c.Hooks)
	}
}

// WithStore 配置 BasicAgent 的 store 实例——跨 thread 长期 KV 持久化层。
//
// 内部实现：把 store 实例存进 Config.Store；BasicAgent.Run 入口
// `store.WithContext(ctx, c.Store)` push 到 ctx 让所有下游 modelFn / toolsFn 节点、
// 业务 hook、业务 tool 通过 `store.FromContext(ctx)` 取到。
//
// **8 编排 wrapper 不接 WithStore**（RFC §5 设计选择）——业务给底层 BasicAgent 传，
// 或在 Run 入口显式 `ctx = store.WithContext(ctx, st)` 让整个嵌套树共享。
//
// **typed nil 防护**：业务 `var st *MyStore; WithStore(st)` 时 st != nil 但
// underlying nil → 后续 st.Get() 远 stack nil deref（跟 WithModel 同款
// isFrameworkNil 协议六复证）。
func WithStore(st store.Store) Option {
	return func(c *Config) {
		if isFrameworkNil(st) {
			panic("WithStore: typed nil Store (含 Go interface 陷阱)")
		}
		c.Store = st
	}
}

// StoreMemoryConfig 配置 WithStoreMemory helper 行为。
//
// helper 编译成 preamble Search 注入 + postamble Put 保存，业务零代码接入「长期记忆
// 自动注入」。
//
// # 曾经有过一个 Filter 字段，已删（2026-08-24）
//
// 它把一个 `map[string]any` 转发给 `store.Search(WithFilter(...))`，而那个 filter 的
// 语义是「**把 value 当 map 按字段比较**」——框架自动写进去的 value 恒是一条消息，
// 不是 map ⟹ 类型断言失败 ⟹ **框架自己写的记忆 100% 被剔除**。
//
// ⚠️ 它不是「完全不工作」，这才是它隐蔽的地方：业务自己 `Put` 进同一个 ns 的
// `map[string]any` 照常命中。所以在混合 ns 上一试，看到的是「有结果啊」，实际
// agent 自己的记忆一条不剩（实测：不配 Filter 业务 1 条 + 框架 1 条；配了 Filter
// 业务 1 条 + 框架 **0** 条）。
//
// **要「只回忆一部分」，用下面两条现成的路，别把 filter 加回来**：
//
//   - 按**谁写的**收窄：写入者维在 namespace 上（见 [StoreMemoryConfig.WriterID]），
//     业务 `Search(基准ns/写入者)` 即可；要彻底隔离就给不同 agent 配**兄弟** ns。
//   - 按**内容相关性**收窄：给 store 配 [store.IndexConfig]，注入时按本轮问题做语义
//     检索（框架已经把最后一条 user 输入当 query 传下去了）。
type StoreMemoryConfig struct {
	// SearchLimit 是 store.Search 返回的 Item 数上限，默认 5。
	//
	// **业务从旧 MemoryConfig.MaxReadMessages 迁移注意**：
	//   旧字段是 prompt 注入的 message 对数；新字段是 store Item 数。
	//   仅当每 Item.Value 恰好是一条 schema.Message 时 1:1 等价。
	//   业务 Value 含 `{"content": "...", "metadata": {...}}` 富结构时单 Item 可能远
	//   大于一条 message——必须实测典型 query 返 Item 总 token 数再校准 SearchLimit，
	//   避免 prompt 撑爆触发压缩。
	SearchLimit int

	// WriterID 是写入这条记忆的**身份**，**作为 namespace 段追加在基准 ns 后面**
	// （`基准ns/<WriterID>`）。空 = 用编排注册位置。
	//
	// # 为什么要有这一维
	//
	// **键里没有写入者**：现在是 `<RunID>_assistant_<内容指纹>`，而 RunID 只区分「哪一次
	// 任务」、同一次任务里的多个成员共享它。指纹这一维是**内容**的函数，不是身份的——
	// 两个成员产出同一句话就该是同一条事实，指纹不该、也不能替身份维干活。
	//
	// ⚠️ 这一维当初的论证是另一套（键末位曾是「各自 session 的事件条数」这个序号，
	// 并行分支毫无关系地就撞在一起、后写静默覆盖）。**那个撞键已经由内容指纹消掉了**，
	// 但这一维仍然必要——理由换成了下面这条「只有 ns 参与检索」，它跟撞键无关。
	//
	// 放 ns 而不放 key，是因为只有 ns 参与检索：`Search(基准ns)` 照旧拿到全部（共享
	// 语义不变），`Search(基准ns/身份)` 能收窄到单个写入者。key 维不参与检索，放那儿
	// 防得住撞键但换不来收窄能力。
	//
	// # 什么时候需要显式给
	//
	// 缺省的位置是**编排结构的函数**：改分支名、加一层 wrapper、挪层次，都会让这个
	// agent 换一个 ns，旧记忆读不回来（跟会话历史层的归属同款代价）。跨版本要稳定
	// 读回历史记忆的，在这里显式给一个不随重构漂的名字。
	//
	// # 取值约束
	//
	// 它当 ns 段用，所以受 [store.Namespace] 的字符约束：**不能含 `.` 或 NUL**。`/`
	// 可以有，会展开成多层（`"a/b"` → `基准ns/a/b`），但不能拼出空段（`"a//b"` /
	// `"/x"` / `"x/"` 都非法）。装配期 fail-fast，不合规当场 panic。
	//
	// ⚠️ 唯一性框架不校验（它不知道你还有哪些 agent 会写这个 ns）。两个 agent 用同一个
	// WriterID 就退回撞键。
	WriterID string
}

// WithStoreMemory 配置 BasicAgent 的 Store-backed 长期记忆——RFC §6 "WithStoreMemory
// helper 替代 Memory 接口"。
//
// 业务零代码接入：
//
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithStore(st),
//	    agent_v2.WithStoreMemory(store.NS("users", uid, "memories"),
//	        &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
//	)
//	// preamble 自动 store.Search(ns) 注入 LLM messages 头 + postamble 自动 store.Put
//
// **WithStoreMemory 隐式要求 WithStore 同实例已挂**——helper 内 buildInitialMessages
// 子步调 store.FromContext(ctx)，ctx 无 store 时 fail-fast 返 ErrStoreNotConfigured。
//
// # 同一个 ns = 共享记忆（读写都共享）
//
// 注入按 namespace 搜，而 `Search` 的语义是「本 ns + **全部后代**」。所以给多个 agent
// 配同一个 ns，它们**互相读得到对方写的记忆**（实测：A 写的进了 B 的 prompt）。
//
// 这是有意的——`store.md` 把「多 agent 共享黑板」列为正当用途，上面那个示例的 ns 也是
// 按**用户**分而不是按 agent 分。
//
// ⚠️ **这是 2026-08 才真正生效的语义**：在那之前非顶层的 agent 一条都不写（一道防键
// 撞车的开关），所以「配同一个 ns」看起来像隔离的——那只是功能没生效。
//
// # 写入者维在 ns 上，可以事后收窄
//
// 框架保存时会在你给的 ns 后面追加一段**写入者位置**（编排注册名，或 [StoreMemoryConfig.WriterID]
// 显式给的）。所以：
//
//   - `Search(ns)`——全部写入者的记忆（**注入走的就是这条**，共享语义不变）
//   - `Search(ns/写入者)`——只有那一个写入者的
//
// 注入侧还会在**真有多个写入者时**对每个写入者各超采一份、回来一起按相关性排，
// 免得 `SearchLimit` 截断变成按名字排序的偏袒（`Search` 的跨 backend 契约是
// `(ns, key)` 字典序——直接截，名字排末位的成员自己的记忆一条都进不去）。
// **这不是给每人留配额**：拿回来之后谁相关谁进。单写入者不多花查询。
//
// # 要隔离，给**兄弟** ns
//
//	agentA: store.NS("users", uid, "mem_a")
//	agentB: store.NS("users", uid, "mem_b")
//
// ⚠️ **不能靠「在共享 ns 底下再加一层」隔离**——`NS("users", uid, "memories", "agentA")`
// 是 `NS("users", uid, "memories")` 的后代，配了后者的 agent 照样搜得到它。Search 覆盖
// 全后代，隔离必须发生在**分叉点**上。
//
// 想共享一部分、私有一部分：两个 agent 各配自己的私有兄弟 ns，共享那部分业务自己
// Get/Put。
//
// **PR 3.3+ 真接入**：BasicAgent.buildInitialMessages 调 store.Search 注入历史 +
// commitFinal 调 store.Put 保存最终答复的**文本**（2026-09-07 起；Content 为空则跳过）。Memory 接口已删（PR 3.3
// commit `e6505f9`）。
//
// nil cfg = 用默认 StoreMemoryConfig（SearchLimit=5 — 限 store.Search 返 Item 数上限 5,
// 非"不过滤"）。
func WithStoreMemory(ns store.Namespace, cfg *StoreMemoryConfig) Option {
	return func(c *Config) {
		if err := ns.Validate(); err != nil {
			panic("WithStoreMemory: invalid namespace: " + err.Error())
		}
		c.StoreMemoryNS = ns
		// WriterID 当 ns 段用，字符约束跟 ns 一样。装配期拒——这是调用方**显式给的值
		// 自身非法**，它手上的信息完全够自查，报错合法（对照：拿「你的意图」去撞「框架
		// 发现的状态」那种守卫不合法）。运行时才发现的话，那时的选择只剩「静默丢记忆」
		// 或「让整个 Run 失败」，两个都比装配期 panic 差。
		if cfg != nil && cfg.WriterID != "" {
			probe := append(append(store.Namespace{}, ns...), strings.Split(cfg.WriterID, "/")...)
			if err := probe.Validate(); err != nil {
				panic("WithStoreMemory: invalid WriterID " + strconv.Quote(cfg.WriterID) +
					" (它会作为 namespace 段追加在基准 ns 后面): " + err.Error())
			}
		}
		if cfg == nil {
			cfg = &StoreMemoryConfig{}
		}
		if cfg.SearchLimit <= 0 {
			cfg.SearchLimit = 5
		}
		c.StoreMemory = cfg
	}
}

// WithMaxRounds 设置最大 ReAct 轮数（默认 10）防 LLM 死循环。
func WithMaxRounds(n int) Option {
	return func(c *Config) {
		if n <= 0 {
			n = 1
		}
		c.MaxRounds = n
	}
}

// WithMicroCompact 启用第 1 层压缩——tool 节点完成后清白名单内的旧 tool_result
// 内容（保配对结构）。nil 关闭。
func WithMicroCompact(cfg *MicroCompactConfig) Option {
	return func(c *Config) {
		if cfg == nil {
			c.MicroCompact = nil
			return
		}
		copied := *cfg
		c.MicroCompact = &copied
	}
}

// WithAutoCompact 启用第 2 层压缩——modelNode 入口估算 token 超阈值时调用
// cfg.Model 生成结构化摘要替代早期消息。nil 关闭。
//
// 必填：cfg.Model + cfg.ModelContextWindow（其它字段有默认值）。
//
// 必须显式 `copied.breakers = &sync.Map{}`——
// autocompact.go:78 godoc 早就承诺会做但代码漏掉，导致 getOrCreateBreakerEntry
// 第 100-102 行 `cfg.breakers == nil → return nil`，整个断路器路径被绕过 →
// LLM 持续失败时不跳，反复浪费 quota。
//
// breakerMaxFailures 也必须从 cfg copy 出来——getOrCreateBreakerEntry 用此值
// lazy 创建 per-session breaker 实例。
func WithAutoCompact(cfg *AutoCompactConfig) Option {
	return func(c *Config) {
		if cfg == nil {
			c.AutoCompact = nil
			return
		}
		// audit⁹ HIGH-2 fix: cfg.Enabled=true 时 cfg.Model 必填且不能 typed nil. AutoCompact
		// 路径 summarize 调 cfg.Model.Generate, typed nil 远 stack nil deref.
		if cfg.Enabled && isFrameworkNil(cfg.Model) {
			panic("WithAutoCompact: typed nil cfg.Model with Enabled=true (含 Go typed nil ChatModel 陷阱)")
		}
		copied := *cfg
		copied.breakers = &sync.Map{}
		copied.breakerMaxFailures = cfg.MaxConsecutiveFailures
		c.AutoCompact = &copied
	}
}

// WithReactiveCompact 启用第 3 层压缩——LLM 报 prompt-too-long 时按 token gap
// 删最旧消息组并重试。nil 关闭。
func WithReactiveCompact(cfg *ReactiveCompactConfig) Option {
	return func(c *Config) {
		if cfg == nil {
			c.ReactiveCompact = nil
			return
		}
		copied := *cfg
		c.ReactiveCompact = &copied
	}
}

// WithSkills 集成 skill 系统——modelNode 内部自动注入 activate_skill 工具，
// 让 LLM 能通过 tool-call 激活专项技能（拿到对应 prompt 指导）。
//
// src.SkillBox nil 时 no-op。框架会在 system prompt 后追加一段提示告诉 LLM
// 何时该调 activate_skill 工具。
//
// 用法：
//
//	box, err := skill.NewDefaultSkillBox("./skills")  // (rootDir string, opts ...SkillBoxOption) (*DefaultSkillBox, error)
//	if err != nil { /* handle */ }
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("research"),
//	    agent_v2.WithModel(m),
//	    agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
//	)
//
// 目录式 SkillBox 就用上面的 `skill.NewDefaultSkillBox(dir, opts...)`；从预解析内容
// 装配走 `skill.NewInMemorySkillBox(name, contents, opts...)`。
func WithSkills(src skill.SkillSource) Option {
	return func(c *Config) {
		// audit⁶ MED-1 fix: isFrameworkNil 检 typed nil SkillBox (协议六复证). 旧
		// `if src.SkillBox == nil` 漏 typed nil interface → 业务 `var nilBox *MyBox;
		// WithSkills(skill.SkillSource{SkillBox: nilBox})` 漏检, 后续 ResolveByName
		// nil deref. typed nil 等价 untyped nil silent skip (by design idiom 保留).
		if isFrameworkNil(src.SkillBox) {
			return
		}
		copied := src
		c.Skill = &copied
	}
}

// WithSkillSystemPrompt 业务自定义 skill 触发提示——覆盖框架默认中文 hint。
//
// 仅当通过 WithSkills 启用 skill 系统时生效；空字符串 = 用框架默认 hint。
//
// 典型场景：
//   - 英文 prompt 业务（默认 hint 是中文）
//   - 加业务约定（如「优先用 description 含 'SQL' 的 skill」）
//   - 多语言切换
//
// 用法：
//
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithModel(m),
//	    agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
//	    agent_v2.WithSkillSystemPrompt(
//	        "When the user request matches a specialized skill, "+
//	        "first call activate_skill to load the skill's playbook, "+
//	        "then act on the loaded instructions.",
//	    ),
//	)
func WithSkillSystemPrompt(prompt string) Option {
	return func(c *Config) { c.SkillSystemPrompt = prompt }
}

// WithSkillSelector 配置 adaptive / pre_match 模式的预激活策略。
//
// **激活模式选择**：业务通过 `WithSkills(skill.SkillSource{SkillBox: box, Options:
// &skill.SkillRuntimeOptions{ActivationMode: ...}})` 选模式：
//   - toolCall（V1 默认）：注入 activate_skill 工具，LLM 推理时自决——**无需** WithSkillSelector
//   - adaptive：runtime MatchSkills 预筛 → 多候选可调 LLM 仲裁；强烈推荐配 WithSkillSelector(Model)
//   - pre_match：runtime 预筛即决，多候选直接 top1；可不配 WithSkillSelector
//
// cfg.Model nil 时 adaptive 多候选退化为直接 top1（行为等价 pre_match）。
//
// **adaptive / pre_match 模式下被忽略的设置**：
//   - `WithSkillSystemPrompt`：仅 toolCall 模式生效。adaptive 路径 skill body 直接进
//     sysPrompt，无需 toolCall hint。业务想给 router LLM 自定义 prompt 用
//     SkillSelectorConfig.SystemPrompt。
//
// 用法：
//
//	agent_v2.WithSkills(skill.SkillSource{
//	    SkillBox: box,
//	    Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModeAdaptive},
//	}),
//	agent_v2.WithSkillSelector(agent_v2.SkillSelectorConfig{
//	    Model:    routerModel,  // 推荐用快速 model（小尺寸 / 低延迟）
//	    MinScore: 0.3,
//	}),
func WithSkillSelector(cfg SkillSelectorConfig) Option {
	return func(c *Config) {
		copied := cfg
		c.SkillSelector = &copied
	}
}

// WithSkillSelectionHook 让业务订阅 adaptive / pre_match 模式的 skill 选择结果。
//
// 何时调用：buildInitialMessages 调 selectSkillAdaptive 后立即调用，无论选中
// 与否（`selection.Skill nil` 也调用，让业务知道"无候选 / 分数过低"原因）。
// **不调用**的情况：control flow 错误（InterruptError / ctx.Err）propagate 时
// 走 BasicAgent.Run 的 error 路径，hook 跳过——business 应该接 error 处理。
//
// 仅 adaptive / pre_match 模式生效；toolCall 模式无预激活流程不调用。
//
// 用法：
//
//	agent_v2.WithSkillSelectionHook(func(ctx context.Context, sel agent_v2.SkillSelection) {
//	    if sel.Skill == nil {
//	        log.Printf("no skill activated: strategy=%s reason=%s", sel.Strategy, sel.Reason)
//	        return
//	    }
//	    metrics.Counter("skill_activated").With("name", sel.Skill.Name, "strategy", sel.Strategy).Inc
//	})
func WithSkillSelectionHook(hook SkillSelectionHook) Option {
	return func(c *Config) {
		// audit⁸ HIGH-1 fix: isFrameworkNil 检 typed nil func (协议八复证).
		// 业务 `var fn SkillSelectionHook; WithSkillSelectionHook(fn)` typed nil →
		// 后续 selectionHook(ctx, sel) 远 stack nil deref panic.
		if isFrameworkNil(hook) {
			panic("WithSkillSelectionHook: nil hook (含 Go typed nil func 陷阱)")
		}
		c.SkillSelectionHook = hook
	}
}

// WithSkillBodyMaxChars adaptive / pre_match 模式注入 sysPrompt 的 skill body
// 字符上限。0 或负数 = 用框架默认 20000 char (~5K token)——不支持"-1 禁用截断"
// 语义（业务想禁用截断请显式设大值如 1<<30）。
//
// **why 必要**：adaptive 把 active.Prompt 拼到 sysPrompt → AutoCompact/ReactiveCompact
// 设计上都跳过 system token 估算（sys 不被压缩）→ 大 SKILL.md body 直接打满 ctx
// window 没人防御。本字段是该路径的唯一上限。
//
// 截断带 trailing "…" + UTF-8 安全（复用 truncateWithEllipsis）。
//
// 用法：
//
//	agent_v2.WithSkillBodyMaxChars(50000)  // 业务 SKILL.md body 较短允许更大上限
//	agent_v2.WithSkillBodyMaxChars(8000)   // 紧 ctx window 业务调小
func WithSkillBodyMaxChars(n int) Option {
	return func(c *Config) { c.SkillBodyMaxChars = n }
}

// WithToolResultMaxBytes 给**单条业务工具结果**设字节上限，超了从中间挖空
// （保留头尾 + 一条说明被删了多少字节的标记，见 [Config.ToolResultMaxBytes]）。
//
// n <= 0 关闭。**默认关闭**——它会改变模型看到的内容，该由业务按自己的工具产出规模决定。
//
// **框架工具恒豁免**：`final_output` 装的是最终答案本身、`activate_skill` 的正文另有
// `WithSkillBodyMaxChars` 管，截它们只会把事情弄坏。
//
// **截在工具返回那一刻，钩子之后不再截**：`OnToolEnd` 看到的是截过的结果；钩子改写的 `Result`（[消费]档）原样进消息流，
// 改得再大也不截——这个上限管的是工具产出，不是对钩子的硬保证。开了它之后 MicroCompact 卸载出去的也是截过的那份
// （带截断标记）。
//
// 用法：
//
//	agent_v2.WithToolResultMaxBytes(50_000)  // 允许较大的工具产出
//	agent_v2.WithToolResultMaxBytes(8_000)   // 工具会吐大文件/长命令输出时收紧
func WithToolResultMaxBytes(n int) Option {
	return func(c *Config) {
		c.ToolResultMaxBytes = n
	}
}

// WithSkillModelResolver 注册 P2.B 解析 skill frontmatter `model: <name>` 字段为
// model.ChatModel 实例的回调。
//
// 仅 fork 模式 + skill.Model 非空时调用——inline 模式整 Run 共享一个 model 无切换边界。
//
// 用法：
//
//	gpt4 := openai.NewChatModel(openai.WithModel("gpt-4"))
//	haiku := openai.NewChatModel(openai.WithModel("claude-haiku"))
//	models := map[string]model.ChatModel{"gpt-4": gpt4, "haiku": haiku}
//	agent_v2.WithSkillModelResolver(func(name string) (model.ChatModel, error) {
//	    if m, ok := models[name]; ok { return m, nil }
//	    return nil, fmt.Errorf("unknown model %q", name)
//	})
func WithSkillModelResolver(resolver SkillModelResolver) Option {
	return func(c *Config) {
		// audit⁸ HIGH-1 fix: isFrameworkNil 检 typed nil func (协议八复证).
		if isFrameworkNil(resolver) {
			panic("WithSkillModelResolver: nil resolver (含 Go typed nil func 陷阱)")
		}
		c.SkillModelResolver = resolver
	}
}

// WithSkillSubAgentResolver 注册 P2.C 解析 skill frontmatter `agent: <name>` 字段
// 为 Agent 实例的回调。
//
// 仅 fork 模式 + skill.Agent 非空时调用。返 nil = 框架默认创建
// 新 BasicAgent（SystemPrompt = skill body + 父 agent 的 model）。
//
// **R16 audit MED-1 关键限制（HITL）**：默认 fallback BasicAgent 不注 toolkit /
// 不注 ask_user——sub-agent 内 LLM 不能主动抛 InterruptError，HITL 协议在 fallback
// 路径完全不可达。业务想 fork + HITL **必须** 通过本 resolver 自构造 sub-agent +
// 显式注 `WithAskUser()`：
//
//	hitlSubAgent := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("ask"), agent_v2.WithModel(m),
//	    agent_v2.WithAskUser(),  // 必须显式注内置 ask_user 工具
//	)
//
// **R16 audit LOW-1 并发警示**：返**单例** sub-agent 时业务需保证 sub-agent 内
// stateful 字段（Store backend / Hooks / 业务 toolkit 内部状态）并发安全——典型
// fan-out / ParallelAgent 多 branch 同时调 fork skill 时同 sub-agent 实例并发跑。
// **fan-out 场景推荐**：closure 内每次 new 一份独立实例：
//
//	WithSkillSubAgentResolver(func(name string) (agent_v2.Agent, error) {
//	    return agent_v2.NewBasicAgent(...), nil  // 每次新建避免并发竞态
//	})
//
// **R23 audit LOW-4 状态累积警示**：即使非并发，sub-agent 含 Store / stateful
// toolkit / hooks 含计数器等状态时**也建议 closure 内每次 new 实例**——业务想
// sub-agent 是 stateless 但单例多次 fork 调用会累积状态：sub-agent.commitFinal 调
// store.Put 跨 N 次 fork 累积，sub-agent 内部 hook 计数器跨调用累积，silent 泄漏。
//
// ⚠️ 每次新建的代价：resolver 在每次 fork（含续跑）都被调，新建的实例是新的 approve_always 身份——子 agent 里答的
// 「一直允许」只撑到下一次中断或下一次 fork，之后同样的调用再问一次（方向安全）。要它在一个任务里一直记得，就让
// resolver 返回同一个实例（并保证上面那条并发安全）。
//
// 用法：
//
//	researchAgent := agent_v2.NewBasicAgent(...)
//	agents := map[string]agent_v2.Agent{"research": researchAgent}
//	agent_v2.WithSkillSubAgentResolver(func(name string) (agent_v2.Agent, error) {
//	    if a, ok := agents[name]; ok { return a, nil }
//	    return nil, fmt.Errorf("unknown agent %q", name)
//	})
func WithSkillSubAgentResolver(resolver SkillSubAgentResolver) Option {
	return func(c *Config) {
		// audit⁸ HIGH-1 fix: isFrameworkNil 检 typed nil func (协议八复证).
		if isFrameworkNil(resolver) {
			panic("WithSkillSubAgentResolver: nil resolver (含 Go typed nil func 陷阱)")
		}
		c.SkillSubAgentResolver = resolver
	}
}

// WithInlineSkillBody 启用 P2.D inline skill body 协议（仅 toolCall 模式生效）。
//
// 默认 false 走老路径（skill body 进 activate_skill tool_result content）；true 时
// tool_result 退化为 terse 激活 ack，skill body 作为 [system_reminder] UserMessage
// 注入对话流（带 SkillBodyAnnotation marker，4 层压缩跨层一致防护）。
//
// **what 改变**：
//   - tool_result content：从 "<skill body 全文>" → "Skill 'X' activated; see next system reminder for content"
//   - 新增 UserMessage：`[system_reminder] Skill 'X' content:\n<body>`，标 SkillBodyAnnotation marker
//   - tool_result 不再标 SkillBodyAnnotation marker（marker 跟随 body 走）
//
// **跨层契约**：AutoCompact splitProtectedGroups + ReactiveCompact isProtectedGroup
// 都按 marker 检测——marker 在 UserMessage 上时保护其所在 API round group（含本身
// + 后续 assistant 回复）。tool_call group（terse ack）可以被 summarize，损失小。
//
// **何时不用**：
//   - 业务依赖 tool_result.Output 直接拿 skill body 做后处理
//   - adaptive / pre_match 模式：本字段无效（body 已通过 sysPrompt 注入）
//
// 用法：
//
//	agent_v2.WithInlineSkillBody(true)
func WithInlineSkillBody(enabled bool) Option {
	return func(c *Config) { c.InlineSkillBody = enabled }
}

// WithToolSearch 启用工具级渐进披露。典型：`WithToolSearch(&agent_v2.ToolSearchConfig{
// Enabled: true, Deferred: mcp.IsRemoteTool})` 延迟披露所有 MCP 工具。见 ToolSearchConfig。
func WithToolSearch(cfg *ToolSearchConfig) Option {
	return func(c *Config) { c.ToolSearch = cfg }
}

// WithSkillListingBudget 控制 skill listing 字符预算——用于 activate_skill 工具
// description 里列 skill 时的两个尺寸上限。
//
// totalChars：listing 总字符预算上限；0 = 用框架默认 (4000 char)
// perEntryChars：单条 description 硬上限；0 = 用框架默认 (200 char)
//
// 超总预算时走三档降级（全 fits → 每条按 maxDescLen 截断 → 仅名字）。
// 仅当通过 WithSkills 启用 skill 系统时生效。
//
// 用法：
//
//	// 业务用大 context window，让 listing 更详细
//	agent_v2.WithSkillListingBudget(8000, 400)
//
//	// 业务接 100+ skill 且 context window 紧
//	agent_v2.WithSkillListingBudget(2000, 100)
func WithSkillListingBudget(totalChars, perEntryChars int) Option {
	return func(c *Config) {
		c.SkillListingBudgetChars = totalChars
		c.SkillListingPerEntryChars = perEntryChars
	}
}

// WithMaxHistoryPairs 设跨 Run 历史对硬上限——为防多轮对话累积爆 context 的兜底。
//
// **默认 defaultMaxHistoryPairs=20**（defaultConfig 设，默认开启）：不调本 option 时，
// 复用同 session 的跨 Run 历史超过 20 对（40 条）即从头截断、只保留最后 20 对。**显式
// 禁用**传 WithMaxHistoryPairs(0)（n<0 也归零）。想要 token-aware 智能截断改用
// WithHistoryCompactor（接管后本值作 keepPairs 提示）。只截 prior 跨 Run 历史，不动本轮
// input / system / StoreMemory；单次 Run（prior 空）不受影响。
func WithMaxHistoryPairs(n int) Option {
	return func(c *Config) {
		if n < 0 {
			n = 0
		}
		c.MaxHistoryPairs = n
	}
}

// WithHistoryCompactor 设自定义历史压缩函数——接管 MaxHistoryPairs 简单截断。
//
// nil 关闭，回退 MaxHistoryPairs。
func WithHistoryCompactor(fn HistoryCompactor) Option {
	return func(c *Config) {
		// audit⁸ HIGH-1 fix: isFrameworkNil 检 typed nil func (协议八复证).
		if isFrameworkNil(fn) {
			panic("WithHistoryCompactor: nil fn (含 Go typed nil func 陷阱)")
		}
		c.HistoryCompactor = fn
	}
}

// WithEmitCpEvent 启用 cp.save / cp.load / cp.cleanup EventSink event fire (Phase 2 Sub-3).
//
// 默认 false opt-in. true 时 BasicAgent.Run 内透传 compose_v2.WithEmitCpEvent
// 让 framework cp 操作 wrap fire 含 SessionID/NS/Status/Duration/Ok/Err.
// 业务订阅 OrchestrationHooks.OnNodeEvent 按 event.Type=cp 分发处理 *CpAction.
func WithEmitCpEvent(enabled bool) Option {
	return func(c *Config) {
		c.EmitCpEvent = enabled
	}
}
