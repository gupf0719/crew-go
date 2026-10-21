package agent_v2

import (
	"context"
	"log/slog"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// AskUserToolName 是内置 ask_user tool 的工具名（HITL）。
//
// 名字以 ask_ 前缀避免与用户自定义 user_ 类工具混淆；与 OpenAI tool-call 命名规范
// 保持下划线风格。
const AskUserToolName = "ask_user"

// NewAskUserTool 返回内置 ask_user 工具——LLM 调用时返回 *compose_v2.InterruptError
// 带 AskQuery，触发框架挂起 + 保存 cp，等 user 答复。
//
// **设计**（参考旧 agent.NewAskUserTool）：
//   - tool 形态封装让 LLM 主动选择何时问——比节点内 imperative call 更灵活
//     （LLM-in-the-loop），同时保留 InterruptError + WithResumeUserInputs map
//     by InterruptID 的精确配对（HITL 多 ask 共存场景）
//
// 工作流：
//  1. LLM 决定调 ask_user(question, options) 提问 user
//  2. 工具内部调 NextAnswerSlot(ctx) 一次取定「本站点下一个提问位」的 stableID
//     （身份按 InterruptSite 派生，内含 idx，多 ask 不串台）
//  3. 工具返 (nil, *InterruptError{Query: {Question, Options, InterruptID: stableID}})
//  4. 框架冒泡 InterruptError 到外层 Run，自动 SaveNS cp 含 PendingInterrupts
//  5. caller 拿 InterruptError 后展示 Query.Question / Options 给 user
//  6. user 答复后 caller 调 ag.Resume(sessionID, WithResumeUserInputs(map[string]*schema.Message{
//     ie.Query.InterruptID: schema.UserMessage("答案")}))
//  7. 框架按 ID 配对答案，下次 ask_user 调用时 NextAnswerSlot 在同一位置命中取出 →
//     返 tool.Success(answer) → LLM 看 tool_result 继续推理（二阶段语义自动幂等）
//
// 典型注册：
//
//	tk := tool.NewToolkit()
//	tk.Register(agent_v2.NewAskUserTool())
//	ag := agent_v2.NewBasicAgent(
//	    agent_v2.WithToolkit(tk),
//	    agent_v2.WithSystemPrompt("不确定 user 意图时调 ask_user 而非猜测"),
//	)
//
// 或用便利 option：agent_v2.WithAskUser()
//
// 返回的工具带「框架铸的」标记：审批、ToolFilter、渐进披露、中间件与超时这些闸据它豁免（问人这件事不该
// 再要审批），业务手动注册它时同样生效。业务自己写一件叫 ask_user 的工具不带这个标记，按业务工具对待。
func NewAskUserTool() tool.Tool {
	return markMinted(newAskUserFuncTool())
}

func newAskUserFuncTool() *tool.FuncTool {
	return tool.NewFuncTool(
		AskUserToolName,
		"向用户提问并等待回答。在你需要 user 提供信息、做选择或确认方向时调用，不要自己瞎猜。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"question": map[string]any{
					"type":        "string",
					"description": "要问 user 的具体问题，越明确越好。",
				},
				"options": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": "可选的备选答案；不填则 user 自由文本回答。",
				},
			},
			"required": []string{"question"},
		},
		askUserToolFunc,
	)
}

// askUserToolFunc 是 ask_user tool 的执行体。
//
// **二阶段语义 + 多 pending 不串台**：
//   - 工具入口调 NextAnswerSlot(ctx)：它内部 tick 一格本节点计数器取序号 idx，
//     并按 InterruptSite 算稳定 ID（编入 graphNS / nodeID /
//     实例身份或 visit / tc 下标 / idx）——同节点同序号跨 retry/Resume 总是同 ID；
//     不同序号不同 ID（多 pending 配对答案不串台）
//   - 第一次（无 ResumeAnswer）→ 抛 InterruptError(Query{InterruptID:stableID, ...})
//   - Resume 后（GetResumeAnswer(ctx, stableID) 命中）→ 返用户答案给 LLM
//
// **audit Bug E 修复**：之前用 GetResumeAnswerSingle（已删） 取 map 第一条，多 pending 时
// 第二次 ask_user 调用会拿到第一次的答案串台。改用按 stableID 精确配对。
//
// 二阶段语义：节点 re-run 时直接返 resume value 而非再抛。
// caller 用 WithResumeUserInputs map[stableID]string 按 ID 配对。
func askUserToolFunc(ctx context.Context, args map[string]any) (*tool.Result, error) {
	question, _ := args["question"].(string)
	var options []string
	if raw, ok := args["options"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				options = append(options, s)
			}
		}
	} else if raw, ok := args["options"].([]string); ok {
		options = raw
	}

	// 身份的各维度组合保证（`NextAnswerSlot` 内部按 `InterruptSite` 派生）：
	//   - 多节点不串台（nodeID 维度，audit Round 2 Bug #1 修复）
	//   - 多 pending 不串台（idx 维度）
	//   - 跨节点多次访问不串台（visit 维度，audit Round 3 Bug #1 修复——Loop /
	//     ReAct 多轮 / PlanAgent 循环回 plan 节点等场景）
	//   - 同超步同 NodeID 多并发实例不串台（**InstanceID 维度**，ISSUE-96——SendFrom
	//     同 Target 投递多次时多个 worker 并发执行场景。此前用 branchIdx=frontier 里的
	//     位置，部分失败时 frontier 缩水导致位置重排、身份漂移）
	//   - 同一 tools 节点实例内 N 个并发 tool_call 不串台（**ToolCallIdx 维度**——它们
	//     跑在同一节点实例里、InstanceID 全相同，只能靠这维区分）
	// 身份与答案一次取定（`NextAnswerSlot` 内部 tick 一次计数器——分成「先算 ID
	// 再拿它读」两步的话，第二处只能猜那个计数值）
	stableID, ansMsg, ok := compose_v2.NextAnswerSlot(ctx)
	if ok && ansMsg != nil {
		return tool.Success(ansMsg.Content), nil
	}

	// 首次调用 → 抛 InterruptError，Query.InterruptID 已填稳定 ID 让 caller 配对
	return nil, &compose_v2.InterruptError{
		Reason: "ask_user",
		Query: &compose_v2.AskQuery{
			// 跟 approval.go 同款：单个中断走「原样透出」不经 MergeInterrupts，
			// 只填 ie.Reason 的话 cp 里读不出这条挂起是什么——单/多路径行为分叉。
			Reason:      "ask_user",
			Question:    question,
			Options:     options,
			InterruptID: stableID,
		},
	}
}

// WithAskUser opt-in 便利 option——让这个 agent 拿到 ask_user 工具。
//
// 工具由框架每次 Run 注入，不写进 [WithToolkit] 传进来的那个 toolkit，所以跟 WithToolkit 的先后无关。
// 曾经直接注册进业务的 toolkit：几个 agent 共用一个 toolkit 时，没配它的 agent 也拿到了 ask_user（实测）。
// 业务 toolkit 里已有同名工具时框架那件生效（构造期告警一次）。
func WithAskUser() Option {
	return func(c *Config) { c.AskUserOptIn = true }
}

// warnAskUserOverride 在构造期告警「框架的 ask_user 顶掉了业务 toolkit 里的同名工具」——只顶这个 agent
// 每次 Run 拼出来的那份工具表，业务的 toolkit 不动。
func warnAskUserOverride(cfg *Config) {
	if cfg.AskUserOptIn && cfg.Toolkit != nil && cfg.Toolkit.Get(AskUserToolName) != nil {
		slog.Warn("framework ask_user overrides business tool with the same name", "agent", cfg.Name)
	}
}
