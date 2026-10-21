// Package scenarios 提供 PlanAgent demo 的可复用场景注册中心。
//
// 拆出原因：原本 example/orchestration/plan_chat 下 8 个 scenario_*.go 通过 init() 注册到
// main 包私有 registry，只 CLI demo 能用。example/orchestration/plan_chat_web 想复用这些场景
// 但无法 import main 包。抽到中立公共包 example/scenarios 后，plan_chat CLI /
// plan_chat_web / channel_chat_web 三个 demo 共用同一份场景定义，改一处三边同步
// （ISSUE-72 R7 后续：按用户需求把 scenario sub-agent 注入 web）。
//
// Scenario 自身不绑定 LLM / hook：通过 Builder 函数延迟构造，传 model + hooks
// 让上层 demo 决定 mock 还是真 LLM、绑何种 hook（CLI 走 trace、web 走 SSE sink）。
//
// 各场景 sub-agent 通过 agent_v2.WithDescription 声明职责，PlannerPrompt 不再硬编码
// "可选 agent: xxx" 列表（ISSUE-72 R7：interface contract pattern）——加 / 删
// 专家只动 builder，无需改 PlannerPrompt。
package scenarios

import (
	"sort"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
)

// Scenario 描述一个 PlanAgent demo 场景：sub-agents、planner 提示语、对话脚本。
//
// 所有 scenario 统一走 InputRefs 路径——sub-agent 的 input 由 Planner 在 PlannerPrompt
// 里通过 refs 字段显式声明依赖（ISSUE-47 Supervisor 范式）。fan-in 节点（如 arbiter）
// 也靠 Planner 把所有相关 step 的 index 列进 refs，框架不再有针对单 step 的覆盖钩子。
type Scenario struct {
	Name           string                    // 场景标识（CLI flag / web query 用）
	Description    string                    // 一行描述（启动时打印 / 前端展示）
	PlannerPrompt  string                    // planner system prompt（决定 replan 行为，不含 sub-agent 列表）
	SubAgents      map[string]agent_v2.Agent // 专家 map；各 agent 通过 WithDescription 自描述
	Fallback       agent_v2.Agent            // PlanStep.SubAgentName 为空时的兜底
	DialogueScript []string                  // 建议的用户输入序列（demo 时方便复制）
	ReplanHint     string                    // 该场景下 replan 应该如何变化的一句话提示
}

// Builder 由各 scenario_*.go 通过 init() 注册到全局 registry。
//
// extraOpts 透给每个 sub-agent 的 NewBasicAgent —— 让上层 demo 注入 demo 特有的
// 通用配置（如 web demo 的 4 层压缩 WithMicroCompact / WithAutoCompact /
// WithReactiveCompact）。CLI demo 不需要时传 nil 即可。
type Builder func(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario

var registry = map[string]Builder{}

// Register 把场景 builder 加入全局 registry。重名直接覆盖（demo 无并发场景）。
func Register(name string, b Builder) {
	registry[name] = b
}

// Build 按场景名构造 Scenario。返回 (Scenario, false) 表示场景不存在。
//
// extraOpts 会被各 scenario builder 透给每个 sub-agent 构造——典型用法：web demo
// 传压缩配置让所有 sub-agent 启用 4 层压缩，CLI demo 不传保持轻量。
func Build(name string, m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) (Scenario, bool) {
	b, ok := registry[name]
	if !ok {
		return Scenario{}, false
	}
	return b(m, hooks, extraOpts...), true
}

// Has 检查场景是否已注册。不触发构造，避免提前要求 model/hooks 非空。
func Has(name string) bool {
	_, ok := registry[name]
	return ok
}

// Names 返回所有已注册场景名（字典序）。CLI 展示可选项 / web 前端 dropdown 用。
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// withExtra 拼接 base + extraOpts 给 NewBasicAgent。
// 各 scenario builder 用法：agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{...}, extraOpts)...)
func withExtra(base []agent_v2.Option, extra []agent_v2.Option) []agent_v2.Option {
	if len(extra) == 0 {
		return base
	}
	out := make([]agent_v2.Option, 0, len(base)+len(extra))
	out = append(out, base...)
	out = append(out, extra...)
	return out
}

// RefsPromptHelp 是各 scenario PlannerPrompt 的统一附录，教 LLM 用 refs 字段
// 声明每步的数据依赖（ISSUE-47）。各 scenario 通过 + RefsPromptHelp 拼接。
const RefsPromptHelp = `

【依赖声明 refs】每步必须有 "refs" 字段（数组），声明这一步需要哪些**已完成步骤**的**原文**作为输入：
- 独立任务（不需要前面任何步骤的产出）→ refs: []
- 依赖前面已完成步骤 → refs 元素可以是：
    · agent 名字符串（推荐）：refs: ["coder","critic"]  → 取最近一个匹配 agent 的步骤
    · 0-based int 下标：refs: [0,2,4]                   → 精确指定，对应"已完成步骤"前的 [N]
    · 两者可混用：refs: ["critic", 0]                   → 最近一个 critic + 第 0 步

何时必须用 int：想要**多个**同 agent 的历史版本（例如 code_review 想看 v1/v2/v3 所有 coder 产出），
string 只能取最近一个，此时必须改用 int 列表 refs: [1,3,5]。

⚠️【节奏建议 · 每次只输出 1 步】PlanAgent 是 replan-每步 范式 —— 框架每执行完一步就回来调你重规划，
所以一次返多步是 token 浪费（除了 remaining[0]，框架会丢弃剩余）。请按以下方式输出：
- 没有 executed → 输出 [{第一步 refs:[]}]，单 step
- 有 executed → 输出 [{下一步 refs:[...指向已完成步骤的 agent]}]，单 step
- 目标已达成 → 输出 []（收尾）

刻意不要这样写：[{researcher refs:[]}, {writer refs:["researcher"]}]
（researcher 还没 executed 时 writer 的 refs:["researcher"] 指向同批 sibling —— 框架会在执行 writer 时
真正解析这个 ref；但因为框架只跑 remaining[0]=researcher，writer 这个 step 永远不会被执行，纯属浪费。）

判断依据是"被分配的 sub-agent 完成任务时是否需要看前面步骤的原文"，不是"我的 desc 文字里要不要提到前面"。
不要在 desc 里复制前面步骤的产出原文——设了 refs 框架会自动附上。
框架不会做任何提纯/摘要——若担心原文太长，请在 plan 里插一个 summarizer 步骤，而不是在 desc 里手抄摘要。
agent 名拼错或不存在（凭空捏造）会立即报错，请严格使用"已完成步骤"里 () 中显示的 agent 名。`
