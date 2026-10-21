// Package skill 定义 skill 核心类型与接口。
// 依赖方向：schema ← skill ← agent ← compose
package skill

// SkillDescriptor 定义 skill 的用户可见元数据。
// 从 SKILL.md 的 YAML frontmatter 解析。
//
// # 每个字段的 godoc 都带一个处置标记
//
//   - **[框架消费]**——框架读它并据此行动，写了就生效。
//   - **[业务自负]**——框架解析它但**不据此行动**，理由写在该字段自己的 godoc 里。
//     写了不会自动生效，业务要自己读 `SkillDescriptor` 去用。
//
// ⚠️ 加新字段**必须**带标记，机械对账见 `TestSkillDescriptor_EveryFieldDeclaresDisposition`。
//
// # 为什么需要这套标记
//
// 曾经有四个字段「解析了、清洗了、进了公开结构体，而框架一次都没读过」，而它们在结构体里
// 长得跟真正生效的字段一模一样——业务只能靠 grep 消费点反推「这个写了到底有没有用」。
// 其中两个（`multi-turn` / `max-turns`）预设「skill 激活后跨轮持续活着」这个我们**没有的
// 状态**，另两个（`inputSchema` / `outputSchema`）解析器里根本没有对应分支、恒零值，
// 四个已一并删除——**不解析的字段，不可能变成「解析了但没兑现」**。
//
// 留下来的两个 [业务自负] 各有明确判据（见各自 godoc），不是「还没做」。
//
// **DisableModelInvocation / UserInvocable 二维**（参考 Claude Code）：
// 两字段正交分别控制"LLM 端可见性"与"用户端可调用性"，组合 4 状态：
//
//	| DisableModelInvocation | UserInvocable | 含义 |
//	|---|---|---|
//	| false (默认) | true (默认) | 双端可用——常规 skill |
//	| false | false | 仅 LLM 端可用——LLM 自决调用，业务不直接暴露 |
//	| true | true | 仅用户端可用——业务/CLI 程序化调用，LLM 看不到 |
//	| true | false | 双端禁用——通常是 deprecated skill 临时下线但保留文件 |
//
// **DisableModelInvocation 在 v2 的强制点**：
//   - toolCall 模式：skillTool.Description() listing 过滤 + skillTool.Call() 拒绝
//   - adaptive / pre_match 模式：selectSkillAdaptive 候选过滤
//
// **UserInvocable 在 v2 的强制点**：
//   - 暂无强制点（v2 无 slash command / CLI）。预留字段供业务上层 UI / API
//     做 user-facing 列表过滤，框架不做强制。
type SkillDescriptor struct {
	// Name [框架消费] skill 的激活键（`activate_skill({skill})` 按它查）。
	Name string `json:"name"`
	// Description [框架消费] 进 listing，是 LLM 判断「要不要激活」的主要依据。
	Description string `json:"description"`
	// WhenToUse [框架消费] 进匹配文本（`buildMatchText`）与 listing。
	WhenToUse string `json:"whenToUse,omitempty"`
	// Tags [框架消费] 进匹配文本，参与关键词打分。
	Tags []string `json:"tags,omitempty"`
	// Examples [框架消费] 进匹配文本，参与关键词打分。
	Examples []string `json:"examples,omitempty"`
	// AllowedTools [业务自负] 对应 SKILL.md frontmatter `allowed-tools`（标准里标着 Experimental、
	// 「各实现的支持程度可能不同」）。
	//
	// ⚠️ **本框架解析它但不强制**，而且这不是「还没做」——它在我们的执行模型里**没有良
	// 定义的语义**：
	//   - `context: fork` 默认路径：框架给子 agent 的构造是 name + system prompt + model，
	//     **一个业务工具都不给**。安全期望（「只让它用 read_file」）已经被超额满足，
	//     没有可裁的东西。
	//   - `context: fork` + `WithSkillSubAgentResolver`：那是**业务自己的 agent**，
	//     框架伸手改它的工具集越界了。
	//   - `context: inline`：body 注入当前对话流，父 agent 的工具原样保留。这里理论上
	//     可裁，但「这个 skill 还活着」是个我们没有的状态——inline 的 body 注入一次就完了，
	//     没有作用域边界可挂。
	//
	// 想按轮裁工具用 `agent_v2.WithToolFilter`——那是**有明确作用域**的机制（每轮 model
	// 调用前问一次）。解析期会对非空 AllowedTools 出声（见 [ParseSkillMD]），免得业务
	// 以为写了就生效。
	AllowedTools []string `json:"allowedTools,omitempty"`

	// DisableModelInvocation [框架消费] 禁止 LLM 端调用此 skill（默认 false 允许）。
	// 对应 SKILL.md frontmatter `disable-model-invocation: true|false`。
	DisableModelInvocation bool `json:"disableModelInvocation,omitempty"`

	// Context [框架消费] 定义 skill 执行模式（P2.C 新增；空 = "inline"）：
	//   - "inline"（默认）：skill body 作为 prompt 注入当前 agent 对话流（toolCall mode：
	//     tool_result content / inline body：UserMessage；adaptive：sysPrompt）
	//   - "fork"：spawn 独立 sub-agent，body 作为子 agent system prompt，
	//     LLM 通过 activate_skill 的 `query` 参数传子任务
	//
	// 对应 SKILL.md frontmatter `context: inline|fork`。**其余值一律在解析期拒绝**
	// （见 [ParseSkillMD]）——写错的 `context:` 不会静默退化成 inline。
	//
	// ⚠️ 曾经有第三档 `"fork_with_context"`（fork + 父历史复制）。它被接受、却走 fork
	// 同一分支不复制历史、无任何信号——业务写它拿到的是普通 fork 且 `err == nil`。
	// 保留它的理由记的是「预留 frontmatter 兼容性」，2026-08-26 逐个查过**那个兼容对象
	// 不存在**：`context` 整个键**不在 agentskills.io 标准里**（标准的 6 个字段是
	// name/description/license/compatibility/metadata/allowed-tools，厂商扩展指定放
	// `metadata`）；写这个键的两家里，一家只认 `fork` 一个值、另一家是这第三档的**发明者**
	// 且为实现它废弃了自己一整个构造函数。也就是说它的兼容对象只有那一家，而我们抄进来
	// 还不兑现语义。已删——写它现在会在解析期报错。
	Context string `json:"context,omitempty"`

	// Agent [框架消费] 指定 fork 模式下使用的 sub-agent 名（P2.C 新增）——业务通过
	// `WithSkillSubAgentResolver` 注册的 resolver 把 name 解析为 Agent 实例。
	// 空 = 默认创建新 BasicAgent（用 SystemPrompt = skill body）。
	// 仅 Context == "fork" 时生效。
	//
	// 对应 SKILL.md frontmatter `agent: <name>`。
	Agent string `json:"agent,omitempty"`

	// Model [框架消费] 指定 skill 执行用的 LLM 名（P2.B 新增）——业务通过
	// `WithSkillModelResolver` 注册的 resolver 把 name 解析为 model.ChatModel 实例。
	// 空 = 沿用父 agent 的 model。
	//
	// 对应 SKILL.md frontmatter `model: <name>`。
	//
	// **仅 fork 模式有意义**——inline 模式下整 Run 共享一个 model，无切换边界；
	// fork 模式 sub-agent 独立可用不同 model（如 SQL skill 走 GPT-4 / 翻译 skill 走轻量）。
	Model string `json:"model,omitempty"`

	// UserInvocable [业务自负] 允许用户端（业务上层 UI / CLI / 程序化 API）调用此 skill（默认 true 允许）。
	// 对应 SKILL.md frontmatter `user-invocable: true|false`。
	// **注意**：v2 框架不做强制，业务自行用此字段过滤 user-facing 列表。
	//
	// **why no omitempty**（R12 audit LOW-2）：本字段语义"默认 true 显式可写 false"——
	// 跨系统消费方（DB / Web UI / 其他语言）必须能区分"业务显式 false"vs"未声明（沿
	// 用默认 true）"。加 omitempty 后 false 会被序列化时 strip → 消费方拿到无字段
	// JSON 与"未声明"无区别，必须自己重判默认 → 不同消费方默认行为可能不一致 →
	// 跨边界语义漂移。不加 omitempty 让 true / false 都强制持久化，消费方看到字段
	// 即权威值，无需重判默认。
	UserInvocable bool `json:"userInvocable"`
}

// SkillResource 描述 skill 目录中的一个资源文件。
type SkillResource struct {
	Path         string `json:"path"`
	ResourceType string `json:"resourceType,omitempty"`
	Description  string `json:"description,omitempty"`
}

// SkillActionDescriptor 描述 skill 暴露的一个可执行 action。
type SkillActionDescriptor struct {
	Name            string         `json:"name"`
	Description     string         `json:"description,omitempty"`
	Runtime         string         `json:"runtime,omitempty"` // 当前仅 "go"（进程内函数，经 WithSkillActionFunc 注册）
	ArgumentsSchema map[string]any `json:"argumentsSchema,omitempty"`
	Examples        []string       `json:"examples,omitempty"`
}

// ActiveSkill 描述一个已激活 skill 在当前 AgentLoop 中的展开视图。
type ActiveSkill struct {
	Skill     SkillDescriptor         `json:"skill"`
	RootDir   string                  `json:"rootDir"`
	Prompt    string                  `json:"prompt"`
	Resources []SkillResource         `json:"resources,omitempty"`
	Actions   []SkillActionDescriptor `json:"actions,omitempty"`
}

// SkillExecutionResult 表示一次 action 执行结果。
type SkillExecutionResult struct {
	Success   bool           `json:"success"`
	ExitCode  int            `json:"exitCode,omitempty"`
	Stdout    string         `json:"stdout,omitempty"`
	Stderr    string         `json:"stderr,omitempty"`
	Output    string         `json:"output,omitempty"`
	Error     string         `json:"error,omitempty"`
	ErrorCode string         `json:"errorCode,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// SkillMatchResult 表示一次 skill 预匹配结果。
type SkillMatchResult struct {
	Skill   SkillDescriptor `json:"skill"`
	Score   float64         `json:"score"`
	Reason  string          `json:"reason,omitempty"`
	Signals map[string]any  `json:"signals,omitempty"` // runtime 内部匹配信号摘要
}

// SkillActivationMode 定义 skill 激活方式。
type SkillActivationMode string

const (
	// SkillActivationModeAdaptive 默认推荐。
	// 每次 LLM 调用前先做 skill 预匹配：无候选不激活，单候选直接激活，
	// 多候选时将候选 metadata 注入 messages 并让 LLM 做结构化选择（V2 实现）。
	SkillActivationModeAdaptive SkillActivationMode = "adaptive"

	// SkillActivationModePreMatch 只使用 agent 侧预匹配结果激活 skill，
	// 不让 LLM 做 skill 选择（V2 实现）。
	SkillActivationModePreMatch SkillActivationMode = "pre_match"

	// SkillActivationModeToolCall 注入内部 activate_skill tool，
	// 由 LLM 通过 tool call 激活 skill（V1 默认实现）。
	SkillActivationModeToolCall SkillActivationMode = "tool_call"
)

// SkillRuntimeOptions 定义 Agent 使用 skill 时的运行配置。
type SkillRuntimeOptions struct {
	// ActivationMode skill 激活方式，默认 SkillActivationModeToolCall（V1）。
	ActivationMode SkillActivationMode `json:"activationMode,omitempty"`
}

// SkillSource 定义一个 skill 来源，一个 SkillSource 通常对应一个 skill root 目录。
type SkillSource struct {
	// SkillBox skill 来源容器，必填。
	SkillBox SkillBox
	// Options 当前 skill 来源在 Agent 中使用时的运行配置，nil 使用默认值。
	Options *SkillRuntimeOptions
}
