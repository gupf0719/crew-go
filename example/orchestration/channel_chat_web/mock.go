package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"crew-go/model"
	"crew-go/schema"
)

// mockChatModel 为 -mock 模式提供可预测的 LLM 输出，让 web demo 在
// 外部 LLM 服务不可达时也能跑出完整 plan → exec → replan → final 流程。
//
// ISSUE-72 R7 后 web demo 支持 8 个 scenario（default / code_review / feasibility /
// fault / budget / drill / arbiter / writing_team），每个 scenario 的 sub-agent 池
// 不同。mock 走两层策略：
//
//  1. **planner**：解析 user prompt 里"可选 agent"段拿到本场景的实际 agent 列表，
//     按已完成步数 cycle 输出 [{next_agent}]——避免 mock 硬编码具体 agent 名。
//  2. **sub-agent**：按 system prompt 关键词匹配返回模拟内容；新 scenario 的特殊
//     prompt 走通用 [mock] 回显，不影响 plan→exec→replan 流转节奏。
//
// Mutex 用指针持有（state 字段）——WithTools 需要返回带 tools 的新实例
// 但共享同一计数器；值拷贝 mutex 会触发 govet 告警，所以把可变状态拆出单独结构体。
type mockChatModel struct {
	state *mockState
	tools []*schema.ToolInfo
}

type mockState struct {
	mu           sync.Mutex
	plannerCalls int
}

func newMockModel() *mockChatModel {
	return &mockChatModel{state: &mockState{}}
}

var _ model.ChatModel = (*mockChatModel)(nil)

func (m *mockChatModel) Generate(ctx context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 给 mock 加 ~500ms 延迟，模拟真 LLM 一轮调用 1-3s 节奏：
	//   1. 让用户能在浏览器看清各 step 流转，而不是一闪而过
	//   2. 给 HITL 中断留足窗口（点 ⏸ 后下一轮 hook 触发抛 InterruptError）
	// ctx 取消时立即返回，不阻塞退出。
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	sys, lastUser := extractSysUser(messages)

	switch {
	case strings.Contains(sys, "聊天入口助手"):
		// chat_entry：根据消息历史决定调 tool 还是直接回答
		return m.chatEntryResponse(messages, lastUser), nil
	case strings.Contains(sys, "动态重规划者"):
		return m.plannerResponse(lastUser), nil
	case strings.Contains(sys, "资料调研员") || strings.Contains(sys, "技术可行性调研员"):
		return schema.AssistantMessage(
			"[mock researcher] 资料要点：\n- 调度器基于 GMP 模型，P 绑定 M 运行 G\n- goroutine 栈按需增长，初始 2KB\n- 抢占调度基于信号与协作点",
		), nil
	case strings.Contains(sys, "文案作者") || strings.Contains(sys, "技术写作者"):
		return schema.AssistantMessage(
			"[mock writer] 基于上述要点：Go 调度器通过 GMP 模型把数以万计的 goroutine 高效多路复用到少量 OS 线程上……（以下省略 mock 内容）",
		), nil
	case strings.Contains(sys, "审稿人"):
		return schema.AssistantMessage(
			"[mock critic] 建议：\n1. 引入抢占信号的细节（信号 SIGURG）\n2. 补充 netpoller 与 syscall 的交互\n3. 代码示例避免抽象过度",
		), nil
	case strings.Contains(sys, "代码工程师") || strings.Contains(sys, "Go 工程师"):
		return schema.AssistantMessage(
			"[mock coder] 示例代码：\n```go\nfunc main() {\n    runtime.GOMAXPROCS(0) // 默认绑 CPU 核数\n    go func() { /* worker */ }()\n}\n```",
		), nil
	case strings.Contains(sys, "翻译员"):
		return schema.AssistantMessage(
			"[mock translator] Translation: Go's scheduler uses the GMP model to multiplex goroutines onto OS threads.",
		), nil
	case strings.Contains(sys, "摘要专家") || strings.Contains(sys, "最终总结者"):
		return schema.AssistantMessage(
			"[mock summarizer] 关键要点：\n- GMP 模型核心\n- 抢占调度基于信号\n- netpoller 与 syscall 交互",
		), nil
	case strings.Contains(sys, "对比分析员"):
		return schema.AssistantMessage(
			"[mock comparator] 对比表：\n| 维度 | A | B |\n|---|---|---|\n| 调度 | 抢占 | 协作 |\n| 栈 | 动态 | 固定 |",
		), nil
	case strings.Contains(sys, "代码审查员"):
		// code_review scenario 的 critic：必须 VERDICT 开头让 planner 走 PASS / FAIL 分支
		return schema.AssistantMessage(
			"[mock critic] VERDICT: PASS\n代码已无明显问题。",
		), nil
	case strings.Contains(sys, "结论撰写者"):
		return schema.AssistantMessage("[mock concluder] 结论：可行，建议直接采用方案 A。"), nil
	case strings.Contains(sys, "日志检查员"):
		return schema.AssistantMessage("[mock log_inspector] 故障定位线索：DB schema 不一致。建议 db_inspector 排查 orders 表。"), nil
	case strings.Contains(sys, "DBA"):
		return schema.AssistantMessage("[mock db_inspector] schema 不匹配代码期望——orders 表缺 shipping_addr 字段。"), nil
	case strings.Contains(sys, "代码检查员"):
		return schema.AssistantMessage("[mock code_inspector] db/orders.go:88 引用了 shipping_addr，与 schema 不一致。"), nil
	case strings.Contains(sys, "修复方案撰写者"):
		return schema.AssistantMessage("[mock fixer] 方案：ALTER TABLE orders ADD COLUMN shipping_addr VARCHAR(255);"), nil
	case strings.Contains(sys, "机票搜索员"):
		return schema.AssistantMessage("[mock flight] 800 元"), nil
	case strings.Contains(sys, "酒店搜索员"):
		return schema.AssistantMessage("[mock hotel] 1650 元"), nil
	case strings.Contains(sys, "活动规划员"):
		return schema.AssistantMessage("[mock activity] 180 元"), nil
	case strings.Contains(sys, "预算审计"):
		return schema.AssistantMessage("[mock budgeter] TOTAL: 2630 元 / BUDGET: 2000 元\nSTATUS: OVER by 630 元\n建议：换 budget 酒店，或砍 activity"), nil
	case strings.Contains(sys, "codebase 概览员"):
		return schema.AssistantMessage("[mock overview] AcmeShop 后端 monorepo，核心业务在 service/。建议钻取 /service/order。"), nil
	case strings.Contains(sys, "模块钻取员"):
		return schema.AssistantMessage("[mock driller] /service/order 含 handler.go + repo.go，repo.go 有 N+1 查询问题。已足够，不需要继续。"), nil
	case strings.Contains(sys, "信息源 A"):
		return schema.AssistantMessage("[mock searcher_a] 结论：8KB（来自官方 blog）"), nil
	case strings.Contains(sys, "信息源 B"):
		return schema.AssistantMessage("[mock searcher_b] 结论：2KB（社区资料）"), nil
	case strings.Contains(sys, "信息源 C"):
		return schema.AssistantMessage("[mock searcher_c] 结论：源码 _StackMin=2048 但 newproc 实际 8192"), nil
	case strings.Contains(sys, "仲裁者"):
		return schema.AssistantMessage("[mock arbiter] AGREE\n综合源 A/B/C：实际启动栈 8KB（newproc 分配 _FixedStack=8192）。"), nil
	}
	return schema.AssistantMessage("[mock] " + preview(lastUser, 80)), nil
}

// availableAgentsRE 匹配 webPlanner 注入的"可选 agent"段，每行 "  - <name>: ..." 抽 name。
var availableAgentsRE = regexp.MustCompile(`(?m)^\s*-\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[:：]?`)

// chatEntryResponse mocks chat_entry BasicAgent 的路由决策。按 system prompt
// 区分 mode：
//   - Normal mode（"Normal mode" 字样）→ 第 1 次调 ask_translator 演示「单步直调」
//   - Plan mode（"Plan mode" 字样）→ 第 1 次调 default scenario 演示「Plan-Execute 范式」
//
// 第 2 次调（有 tool_result）→ 总结 tool 结果作终答。
//
// 真实 LLM 按 user input + tool description 选最合适的；mock 固定行为让 demo
// 流程稳定可复现，方便用户切换 mode 观察 normal vs plan 的差异（同一句输入，
// Normal 调单步 ask_translator，Plan 调重型 default scenario）。
func (m *mockChatModel) chatEntryResponse(messages []*schema.Message, lastUser string) *schema.Message {
	// 找当前 system 决定 mode
	sys := ""
	for _, msg := range messages {
		if msg.Role == schema.RoleSystem {
			sys = msg.Content
			break
		}
	}
	isPlanMode := strings.Contains(sys, "Plan mode")

	hasToolResult := false
	for _, msg := range messages {
		if msg.Role == schema.RoleTool {
			hasToolResult = true
			break
		}
	}
	if hasToolResult {
		modeLabel := "Normal"
		if isPlanMode {
			modeLabel = "Plan"
		}
		return schema.AssistantMessage(fmt.Sprintf("[mock chat_entry · %s mode] 已通过工具完成任务：%s", modeLabel, preview(lastUser, 60)))
	}
	// 第 1 次：按 mode 路由不同 tool 演示差异
	toolName := "ask_translator" // Normal mode：单步直调
	if isPlanMode {
		toolName = "default" // Plan mode：调 scenario PlanAgent 走 plan-execute
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc_chat_entry",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      toolName,
			Arguments: fmt.Sprintf(`{"input":%q}`, lastUser),
		},
	}})
}

func (m *mockChatModel) plannerResponse(userPrompt string) *schema.Message {
	executed := countExecuted(userPrompt)

	m.state.mu.Lock()
	m.state.plannerCalls++
	m.state.mu.Unlock()

	// 从 prompt 抽取 scenario 的实际 agent 列表
	agents := parseAvailableAgents(userPrompt)
	if len(agents) == 0 {
		// fallback：旧 default 三步流程，避免完全没列表时崩
		agents = []string{"researcher", "writer", "critic"}
	}

	if executed >= len(agents) {
		return schema.AssistantMessage("[]")
	}
	next := agents[executed]
	refs := "[]"
	if executed > 0 {
		// 引用上一步执行的 agent（作为本步的数据来源）
		refs = fmt.Sprintf(`[%q]`, agents[executed-1])
	}
	desc := fmt.Sprintf("[mock plan] 第 %d 步：%s 处理", executed+1, next)
	body := fmt.Sprintf(`[{"desc":%q,"agent":%q,"refs":%s}]`, desc, next, refs)
	return schema.AssistantMessage(body)
}

// parseAvailableAgents 从 webPlanner 的 prompt 抽取 "可选 agent" 段下的 agent 名列表。
// 例：
//
//	可选 agent（按目标和已完成步骤选最合适的）：
//	  - coder: ...
//	  - critic: ...
//
// 返回 ["coder", "critic"]。无该段返回空。
//
// 注意 delimiter 必须用 "\n已完成步骤" / "\n请输出" 而不是裸字符串——header 行
// "（按目标和已完成步骤选最合适的）" 也含 "已完成步骤"，裸 substring 会误截到第一个
// agent 之前。
func parseAvailableAgents(prompt string) []string {
	const marker = "可选 agent"
	idx := strings.Index(prompt, marker)
	if idx < 0 {
		return nil
	}
	// 从 marker 起截到下一个段落为止
	rest := prompt[idx:]
	for _, end := range []string{"\n已完成步骤", "\n请输出", "\n用户补充指示"} {
		if i := strings.Index(rest, end); i >= 0 {
			rest = rest[:i]
		}
	}
	matches := availableAgentsRE.FindAllStringSubmatch(rest, -1)
	out := make([]string, 0, len(matches))
	for _, mt := range matches {
		out = append(out, mt[1])
	}
	return out
}

func (m *mockChatModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *mockChatModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	// 返回共享 state 的新实例；mock 不真正使用工具定义，仅留字段。
	return &mockChatModel{state: m.state, tools: tools}, nil
}

// ── 辅助 ──

func extractSysUser(msgs []*schema.Message) (sys, user string) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleSystem && sys == "" {
			sys = msg.Content
		}
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			user = msgs[i].Content
			break
		}
	}
	return
}

// countExecuted 从 buildReplanPrompt 的文本里抽"已完成 N 步"的 N。
// 解析失败（例如 "无"）返回 0。
func countExecuted(prompt string) int {
	const key = "已完成 "
	idx := strings.Index(prompt, key)
	if idx < 0 {
		return 0
	}
	rest := prompt[idx+len(key):]
	n := 0
	for _, r := range rest {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}
