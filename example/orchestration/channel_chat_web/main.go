package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/example/scenarios"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/schema"
	"crew-go/tool"
)

// runSlot 持有 HITL 会话的可中断 + 可恢复状态（对照 plan_chat_web/main.go::runSlot）。
//
// 同一 session_id 可被多次 SSE 连接复用：第一次新建 → 跑到一半触发 interrupt →
// runtime save cp 到 cp[sess.RunID] → SSE 断开 → 用户点"继续/取消"再发一次
// GET /api/chat?session_id=xxx&action=...&text=... → 服务端按 action 走 Resume。
type runSlot struct {
	sess      *agent_v2.RunSession
	cp        *compose_v2.InMemoryCheckpointer
	interrupt atomic.Bool
	finished  atomic.Bool

	resumeAction agent_v2.ResumeAction
	resumeText   string
	// lastInterruptID 记录上次中断的稳定 ID，Resume 时按 ID 包成 UserInputs map。
	lastInterruptID string

	// Mode 切换：normal / plan，sticky 状态
	mode string
}

// runs 全局 session_id → *runSlot 注册表（单进程 demo 内存模式）。
var runs sync.Map

//go:embed index.html
var indexHTML []byte

// compactionDemoCtxWindow 是 AutoCompact 的 ModelContextWindow——demo / mock 模式
// 用低阈值（默认 32000 token）容易看到 AutoCompact 触发；生产改回真实 model 上限。
var compactionDemoCtxWindow = 32000

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	useMock := flag.Bool("mock", false, "使用离线 mock chat model（不调用真 LLM）")
	// iter-5-O: 加 -provider qwen|deepseek 跟 CLAUDE.md "测试 LLM 默认 qwen" 对齐.
	provider := flag.String("provider", "qwen", "真 LLM provider: qwen (默认, dashscope, 需 QWEN_API_KEY) / deepseek (需 DEEPSEEK_API_KEY); 仅 -mock=false 生效")
	cwin := flag.Int("compaction-window", 32000, "AutoCompact 模拟 ModelContextWindow")
	flag.Parse()
	compactionDemoCtxWindow = *cwin

	envloader.Load()

	var chatModel model.ChatModel
	if *useMock {
		chatModel = newMockModel()
		log.Printf("[channel_chat_web] mock 模式：不调用外部 LLM")
	} else {
		switch *provider {
		case "qwen":
			if os.Getenv("QWEN_API_KEY") == "" {
				log.Fatal("QWEN_API_KEY 未设置 (.env 加 QWEN_API_KEY=xxx), 或 -mock / -provider=deepseek")
			}
			chatModel, _ = llmtrace.NewQwenFallback()
			log.Printf("[channel_chat_web] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
		case "deepseek":
			apiKey := os.Getenv("DEEPSEEK_API_KEY")
			if apiKey == "" {
				log.Fatal("DEEPSEEK_API_KEY 未设置, 或 -mock / -provider=qwen")
			}
			chatModel = deepseek.NewClient(apiKey, "")
			log.Printf("[channel_chat_web] deepseek 模式")
		default:
			log.Fatalf("未知 -provider %q (支持 qwen / deepseek)", *provider)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("/api/chat", makeChatHandler(chatModel))
	mux.HandleFunc("/api/interrupt", interruptHandler)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("[channel_chat_web] 启动 http://localhost%s（compose_v2 + agent_v2 production binary）", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// makeChatHandler 为每个请求构造独立 sink + hooks；session_id 维度复用 sess + cp +
// interrupt flag（runSlot），支持 HITL 中断 + 恢复。
func makeChatHandler(chatModel model.ChatModel) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
		q := strings.TrimSpace(r.URL.Query().Get("q"))

		var slot *runSlot
		isResume := false
		switch {
		case sessionID != "":
			v, ok := runs.Load(sessionID)
			if !ok {
				http.Error(w, "session not found (likely already completed or never started)", http.StatusNotFound)
				return
			}
			slot = v.(*runSlot)
			if slot.finished.Load() {
				http.Error(w, "session already completed", http.StatusGone)
				return
			}
			slot.interrupt.Store(false)
			slot.resumeAction = parseResumeAction(strings.TrimSpace(r.URL.Query().Get("action")))
			slot.resumeText = strings.TrimSpace(r.URL.Query().Get("text"))
			if newMode := strings.TrimSpace(r.URL.Query().Get("mode")); newMode == "normal" || newMode == "plan" {
				slot.mode = newMode
			}
			isResume = true
		case q != "":
			mode := strings.TrimSpace(r.URL.Query().Get("mode"))
			if mode != "plan" {
				mode = "normal"
			}
			slot = &runSlot{
				sess: agent_v2.NewRunSession(),
				cp:   compose_v2.NewInMemoryCheckpointer(),
				mode: mode,
			}
			runs.Store(slot.sess.RunID, slot)
		default:
			http.Error(w, "missing q (new session) or session_id (resume)", http.StatusBadRequest)
			return
		}

		sink := newEventSink()
		orch := &orchAdapter{sink: sink}
		observer := &hookAdapter{sink: sink}
		intHook := &interruptHook{flag: &slot.interrupt}
		hooks := hook.NewChainHooks(observer, intHook)

		var wg sync.WaitGroup
		wg.Add(1)
		ctx, cancel := context.WithTimeout(r.Context(), 600*time.Second)
		defer cancel()
		ctx = compose_v2.WithCheckpointerCtx(ctx, slot.cp)

		go func() {
			defer wg.Done()
			defer sink.close()
			runOrResume(ctx, chatModel, slot, q, isResume, sink, orch, hooks)
		}()

		writeSSE(w, r, sink)
		wg.Wait()
	}
}

func parseResumeAction(s string) agent_v2.ResumeAction {
	switch s {
	case "replan":
		return agent_v2.ResumeActionReplan
	case "abort":
		return agent_v2.ResumeActionAbort
	default:
		return agent_v2.ResumeActionContinue
	}
}

func interruptHandler(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	v, ok := runs.Load(sessionID)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	slot := v.(*runSlot)
	if slot.finished.Load() {
		http.Error(w, "session already completed", http.StatusGone)
		return
	}
	slot.interrupt.Store(true)
	w.WriteHeader(http.StatusOK)
}

// runOrResume 是 chat handler goroutine 主体：每次连接重建 chat_entry agent +
// 8 sub-agents + 8 scenario PlanAgent（绑当前 sink 的 hooks），按 isResume 走 Run / Resume。
func runOrResume(
	ctx context.Context,
	m model.ChatModel,
	slot *runSlot,
	userInput string,
	isResume bool,
	sink *eventSink,
	orch agent_v2.OrchestrationHooks,
	hooks hook.Hooks,
) {
	ag := buildChatEntryAgent(m, hooks, sink, slot.mode)

	if isResume {
		sink.send("resume", map[string]any{"session_id": slot.sess.RunID, "mode": slot.mode})
	} else {
		sink.send("user_input", map[string]any{
			"q":          userInput,
			"session_id": slot.sess.RunID,
			"mode":       slot.mode,
		})
	}

	var (
		err    error
		events []*agent_v2.AgentEvent
	)
	if isResume {
		// 两条通道按语义分：
		//   - Continue：text 是**给某个具体提问的答案** ⟹ 配 lastInterruptID 走寻址通道
		//   - Replan：text 是**用户这轮补的指示**，不指向任何提问 ⟹ 走非寻址通道
		//
		// ⚠️ 曾经两者都塞进寻址通道、Replan 那半用假 key `"_replan_"` 顶替真 ID。
		// 那么做的代价实测过：假 ID 跟真挂起并存时 ask_user 反复重抛中断**死循环**。
		var userInputs map[string]*schema.Message
		var userText string
		if slot.resumeText != "" {
			if slot.resumeAction == agent_v2.ResumeActionReplan || slot.lastInterruptID == "" {
				userText = slot.resumeText
			} else {
				// ⚠️ **Continue 分支只走寻址通道，planner 因此看不见这句话——这是有意的**。
				//
				// 拆分之前它是看得见的：那时两种负载共用一个 map，planner 用「取任意一条」
				// 的读法顺手捞到了给 ask_user 的答案。但「批准」这种针对某个提问的回答，
				// 被 planner 当成重规划指示读本来就是噪音。
				//
				// 想让 planner 也看见（比如那句话确实同时是新指示），**两条都传**——
				// `plan_chat_web` 就是那个形态。调用方本来就知道自己的意图。
				userInputs = map[string]*schema.Message{
					slot.lastInterruptID: schema.UserMessage(slot.resumeText),
				}
			}
		}
		events, err = ag.Resume(ctx, slot.sess.RunID,
			agent_v2.WithResumeOrchHooks(orch),
			agent_v2.WithResumeAction(slot.resumeAction),
			agent_v2.WithResumeUserInputs(userInputs),
			agent_v2.WithResumeUserText(userText),
			agent_v2.WithResumeSession(slot.sess),
		)
	} else {
		events, err = ag.Run(ctx, slot.sess, schema.UserMessage(userInput), agent_v2.WithOrchHooks(orch))
	}

	if err != nil {
		var ie *compose_v2.InterruptError
		if errors.As(err, &ie) {
			// **单 query 简化**（本 example 单 BasicAgent ask_user 单点中断）：直接读
			// ie.Query 可。**生产业务遇到 ISSUE-81 toolsFn fanout 多 tc 同时抛 Interrupt
			// 或 ParallelAgent 多 branch 并发中断场景必须改 for q := range ie.AllQueries()
			// pattern**——否则漏处理兄弟 query 让对应 Resume 续不上。见
			// doc/v2/hitl-resume.md §2.1 / §2.4 业务统一 accessor 协议 + example/orchestration/parallel_hitl_smoke。
			if ie.Query != nil && ie.Query.InterruptID != "" {
				slot.lastInterruptID = ie.Query.InterruptID
			}
			payload := map[string]any{
				"session_id": slot.sess.RunID,
				"reason":     ie.Reason,
				"checkpoint": summarizeCheckpoint(ctx, slot.cp, slot.sess.RunID),
			}
			if ie.Query != nil {
				payload["query"] = ie.Query
			}
			sink.send("interrupted", payload)
			sink.send("done", map[string]any{"interrupted": true, "session_id": slot.sess.RunID})
			return
		}
		sink.send("error", map[string]any{"message": err.Error()})
		sink.send("done", nil)
		return
	}

	// Abort 路径
	if isResume && slot.resumeAction == agent_v2.ResumeActionAbort {
		slot.finished.Store(true)
		runs.Delete(slot.sess.RunID)
		sink.send("aborted", map[string]any{"session_id": slot.sess.RunID})
		sink.send("done", map[string]any{"aborted": true, "session_id": slot.sess.RunID})
		return
	}

	slot.finished.Store(true)
	runs.Delete(slot.sess.RunID)

	if msg := agent_v2.FinalMessage(events); msg != nil {
		data := map[string]any{"content": msg.Content}
		if rid, ok := msg.Annotations["run_id"].(string); ok && rid != "" {
			data["run_id"] = rid
		}
		sink.send("final", data)
	}
	sink.send("done", map[string]any{"history_size": len(slot.sess.History())})
}

// buildChatEntryAgent 构造 chat_entry BasicAgent，按 mode 装载不同 system prompt。
func buildChatEntryAgent(m model.ChatModel, hooks hook.Hooks, sink *eventSink, mode string) *agent_v2.BasicAgent {
	tk := tool.NewToolkit()

	for _, sa := range buildSubAgents(m, hooks) {
		tk.Register(agent_v2.AgentAsTool(sa.agent, agent_v2.WithAgentToolDescription(sa.description)))
	}

	for _, name := range scenarios.Names() {
		scn, ok := scenarios.Build(name, m, hooks, compactionExtraOpts(m)...)
		if !ok {
			continue
		}
		// LLMPlanner：每个 scenario 用独立的 BasicAgent 作 planner，注入 scn.PlannerPrompt
		// 作 system prompt，调真 LLM 输出 JSON plan steps。
		plannerAgent := agent_v2.NewBasicAgent(
			agent_v2.WithName("planner_"+name),
			agent_v2.WithSystemPrompt(scn.PlannerPrompt),
			agent_v2.WithModel(m),
			agent_v2.WithHooks(hooks),
			agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 3}),
		)
		pa := agent_v2.NewPlanAgent(
			newWebPlanner(plannerAgent, sink, scn.SubAgents),
			agent_v2.PlanName(name),
			agent_v2.PlanDescription(scn.Description),
			agent_v2.WithSubAgents(scn.SubAgents),
			agent_v2.WithExecInputBuilder(makeWebExecInput(sink)),
		)
		tk.Register(agent_v2.AgentAsTool(pa, agent_v2.WithAgentToolDescription("[scenario] "+scn.Description)))
	}

	systemPrompt := chatEntrySystemPromptNormal
	if mode == "plan" {
		systemPrompt = chatEntrySystemPromptPlan
	}

	extraOpts := compactionExtraOpts(m)
	baseOpts := []agent_v2.Option{
		agent_v2.WithName("chat_entry"),
		agent_v2.WithDescription("聊天入口 agent，按 mode 装不同 system prompt 引导 LLM 调对应工具"),
		agent_v2.WithSystemPrompt(systemPrompt),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
		agent_v2.WithMaxRounds(20),
	}
	return agent_v2.NewBasicAgent(append(baseOpts, extraOpts...)...)
}

// agentMeta 是 webPlanner 内部用的 sub-agent 元数据视图（对照 plan_chat_web/main.go）。
type agentMeta struct {
	Name        string
	Description string
}

// extractAgentMetas 从 subAgents map 抽取 metas 并字典序稳定排序。
func extractAgentMetas(subAgents map[string]agent_v2.Agent) []agentMeta {
	metas := make([]agentMeta, 0, len(subAgents))
	for name, ag := range subAgents {
		desc := ""
		if ag != nil {
			desc = ag.Description()
		}
		metas = append(metas, agentMeta{Name: name, Description: desc})
	}
	for i := 0; i < len(metas); i++ {
		for j := i + 1; j < len(metas); j++ {
			if metas[i].Name > metas[j].Name {
				metas[i], metas[j] = metas[j], metas[i]
			}
		}
	}
	return metas
}

// newWebPlanner 是基于 LLM 的动态 Planner——每次 plan 节点入口调 plannerAgent
// （内嵌 scenario PlannerPrompt 的 BasicAgent）输出 JSON 数组，解析为 PlanStep。
//
// 对照 plan_chat_web/main.go::newWebPlanner，主要差异：
//   - compose.GetResumeAnswerSingle → compose_v2.GetResumeUserText（非寻址通道，2026-08-06 换）
//   - agent.* → agent_v2.*
//   - prevRemaining diff 逻辑保持
func newWebPlanner(plannerAgent agent_v2.Agent, sink *eventSink, subAgents map[string]agent_v2.Agent) agent_v2.Planner {
	metas := extractAgentMetas(subAgents)
	var prevRemaining []agent_v2.PlanStep
	return func(ctx context.Context, sess *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		// HITL Resume：把 user 中途插话喂回 Planner prompt
		var resumeUserMsg string
		if ans, ok := compose_v2.GetResumeUserText(ctx); ok {
			resumeUserMsg = ans
		}
		prompt := buildReplanPrompt(input, executed, metas, resumeUserMsg)
		events, err := plannerAgent.Run(ctx, sess, schema.UserMessage(prompt))
		if err != nil {
			return nil, "", fmt.Errorf("planner: %w", err)
		}
		out := agent_v2.FinalMessage(events)
		if out == nil {
			return nil, "", fmt.Errorf("planner: empty output")
		}
		raw := extractJSONArray(out.Content)
		var items []struct {
			Desc  string            `json:"desc"`
			Agent string            `json:"agent"`
			Refs  []json.RawMessage `json:"refs"`
		}
		if err := json.Unmarshal([]byte(raw), &items); err != nil {
			return nil, "", fmt.Errorf("planner: parse json %q: %w", raw, err)
		}
		steps := make([]agent_v2.PlanStep, 0, len(items))
		for _, it := range items {
			refs := parseRawRefs(it.Refs, sink)
			steps = append(steps, agent_v2.PlanStep{
				Description:  it.Desc,
				SubAgentName: it.Agent,
				InputRefs:    refs,
			})
		}

		data := map[string]any{
			"executed": len(executed),
			"curr":     planToWire(steps),
			"prev":     planToWire(prevRemaining),
		}
		switch {
		case len(executed) == 0:
			data["kind"] = "initial"
		case len(steps) == 0:
			data["kind"] = "done"
		default:
			data["kind"] = "replan"
		}
		sink.send("plan", data)

		prevRemaining = steps
		if len(steps) == 0 {
			return nil, "", nil
		}
		return steps, "", nil
	}
}

// buildReplanPrompt 拼接 Planner 的 user prompt——original 目标 + 可选 agent 列表
// + 已完成步骤摘要 + 用户中途插话。对照 plan_chat_web/main.go::buildReplanPrompt。
func buildReplanPrompt(original string, executed []agent_v2.ExecutedStep, metas []agentMeta, resumeUserMsg string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "原始目标：%s\n\n", original)
	if len(metas) > 0 {
		b.WriteString("可选 agent（按目标和已完成步骤选最合适的）：\n")
		for _, m := range metas {
			if m.Description == "" {
				fmt.Fprintf(&b, "  - %s\n", m.Name)
			} else {
				fmt.Fprintf(&b, "  - %s: %s\n", m.Name, m.Description)
			}
		}
		b.WriteString("\n")
	}
	if len(executed) == 0 {
		b.WriteString("已完成步骤：（无，请给出初始计划）\n\n")
	} else {
		fmt.Fprintf(&b, "已完成 %d 步（refs 中可用 0-based [N] 或 agent 名引用）：\n", len(executed))
		for i, e := range executed {
			fmt.Fprintf(&b, "  [%d] (%s) %s\n      产出摘要：%s\n", i, e.Agent, e.Step, preview(e.Result, 200))
		}
		b.WriteString("\n")
	}
	if resumeUserMsg != "" {
		fmt.Fprintf(&b, "用户补充指示（请据此调整剩余步骤）：%s\n\n", resumeUserMsg)
	}
	b.WriteString("请输出剩余步骤的 JSON 数组（若已完成请输出 []）。")
	return b.String()
}

// parseRawRefs 把 LLM 输出 refs（int 或 string）转成 []agent_v2.RefSpec。
// 对照 plan_chat_web/main.go::parseRawRefs。
func parseRawRefs(raw []json.RawMessage, sink *eventSink) []agent_v2.RefSpec {
	if len(raw) == 0 {
		return nil
	}
	out := make([]agent_v2.RefSpec, 0, len(raw))
	for i, r := range raw {
		var asInt int
		if err := json.Unmarshal(r, &asInt); err == nil {
			out = append(out, agent_v2.IndexRef(asInt))
			continue
		}
		var asStr string
		if err := json.Unmarshal(r, &asStr); err == nil {
			out = append(out, agent_v2.AgentRef(asStr))
			continue
		}
		sink.send("warn", map[string]any{
			"msg": fmt.Sprintf("refs[%d] (%s) 既不是 int 也不是 string，跳过", i, string(r)),
		})
	}
	return out
}

// extractJSONArray 从 LLM 输出的字符串里抽出最外层 [...] JSON 数组——容忍前后
// markdown ``` 围栏 / 解释文本。
func extractJSONArray(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		if idx := strings.LastIndex(s, "```"); idx >= 0 {
			s = s[:idx]
		}
		s = strings.TrimSpace(s)
	}
	lb := strings.Index(s, "[")
	rb := strings.LastIndex(s, "]")
	if lb >= 0 && rb > lb {
		return s[lb : rb+1]
	}
	return s
}

// planToWire 把 PlanStep 压成简洁 JSON 格式（只保留 desc / agent / refs），
// 方便前端 diff 对比。
func planToWire(steps []agent_v2.PlanStep) []map[string]any {
	out := make([]map[string]any, 0, len(steps))
	for _, s := range steps {
		out = append(out, map[string]any{
			"desc":  s.Description,
			"agent": s.SubAgentName,
			"refs":  s.InputRefs,
		})
	}
	return out
}

// chatEntrySystemPromptNormal 是 Normal mode 的 system prompt。
const chatEntrySystemPromptNormal = `你是聊天入口助手（Normal mode）。

可用工具分两类（按 tool name 区分，按 tool description 选）：
- ask_xxx 系列（轻量单步专家）：翻译 / 摘要 / 写代码 / 查资料 / 审稿 / 对比 / 写结论
- start_xxx / 场景名（重型 Plan-Execute 流程）：完整任务流水线

调用策略：
- 简单 / 闲聊问题 → 直接回答
- 单步任务 → 调对应 ask_xxx
- 复杂多步任务 → 调 scenario PlanAgent
- 工具返回结果后总结或追问，不要无脑调全部`

// chatEntrySystemPromptPlan 是 Plan mode 的 system prompt。
const chatEntrySystemPromptPlan = `你是聊天入口助手（Plan mode）。

**当前为 Plan mode**：复杂任务一律走结构化 Plan-Execute 范式，不要用单步 ask_xxx 凑合。

可用工具分两类：
- scenario 系列（**Plan mode 优先选这个**）：scenario PlanAgent 包装的完整流水线
- ask_xxx 系列（仅简单查询用）

调用策略：
- 简单 / 闲聊 → 直接回答
- 单步纯文本 → 可以 ask_xxx
- 一切多步 / 结构化任务 → 必须调 scenario PlanAgent`

type subAgentMeta struct {
	agent       agent_v2.Agent
	description string
}

func buildSubAgents(m model.ChatModel, hooks hook.Hooks) []subAgentMeta {
	extra := compactionExtraOpts(m)
	mk := func(name, prompt, desc string, tk *tool.Toolkit) subAgentMeta {
		opts := []agent_v2.Option{
			agent_v2.WithName("ask_" + name),
			agent_v2.WithDescription(desc),
			agent_v2.WithSystemPrompt(prompt),
			agent_v2.WithModel(m),
			agent_v2.WithHooks(hooks),
		}
		if tk != nil {
			opts = append(opts, agent_v2.WithToolkit(tk))
		}
		opts = append(opts, extra...)
		return subAgentMeta{
			agent:       agent_v2.NewBasicAgent(opts...),
			description: desc,
		}
	}

	searchTool := tool.NewFuncTool(
		"search",
		"根据关键词检索资料",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"query": map[string]any{"type": "string"}},
			"required":   []string{"query"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			q, _ := args["query"].(string)
			return tool.Success(fmt.Sprintf("[mock search] %q 的要点：A/B/C", q)), nil
		},
	)
	searchTk := tool.NewToolkit()
	searchTk.Register(searchTool)

	return []subAgentMeta{
		mk("researcher",
			"你是资料调研员。收到子任务后必要时调用 search 工具，最终输出结构化的资料要点。",
			"[单步] 查资料、列事实——必要时调 search 工具",
			searchTk),
		mk("writer",
			"你是文案作者。基于已有资料要点，写出流畅的段落或教程章节。",
			"[单步] 整理资料、写流畅的段落或教程章节",
			nil),
		mk("critic",
			"你是审稿人。指出前序产出在事实、逻辑、结构上的问题，给出改进建议。",
			"[单步] 审查内容输出改进建议",
			nil),
		mk("coder",
			"你是代码工程师。基于上下文生成简短可运行的示例代码并配少量注释。",
			"[单步] 写代码示例",
			nil),
		mk("translator",
			"你是翻译员。把输入文本翻译为目标语言，保留术语原貌。无目标语言时默认中英互译。",
			"[单步] 中英互译",
			nil),
		mk("summarizer",
			"你是摘要专家。把上文压成 3-5 条 bullet。",
			"[单步] 摘要/提纯",
			nil),
		mk("comparator",
			"你是对比分析员。把上文里的多份内容并排对照。",
			"[单步] 对比分析",
			nil),
		mk("concluder",
			"你是结论撰写者。基于已有内容给出 1-3 句话的最终结论。",
			"[单步] 写最终结论",
			nil),
	}
}

// compactionExtraOpts 构造 4 层压缩配置——所有 sub-agent 启用，让 SSE 事件流里
// 能看到压缩触发。
func compactionExtraOpts(m model.ChatModel) []agent_v2.Option {
	microCfg := &agent_v2.MicroCompactConfig{
		Enabled:    true,
		KeepRecent: 3,
	}
	summaryMax := compactionDemoCtxWindow / 5
	if summaryMax < 50 {
		summaryMax = 50
	}
	summaryBuf := compactionDemoCtxWindow / 8
	if summaryBuf < 30 {
		summaryBuf = 30
	}
	// 同理缩放成本守卫的下限——默认 5000 是照 ~200K 窗口定的（约 1/40），直接用会让
	// demo 的可压缩区永远低于它 ⟹ 守卫在调摘要之前早退 ⟹ **压缩静默不发生**，而 demo
	// 还在打印「触发了」。低于 1 要夹住：该字段 <= 0 会被当成「没配」取回默认 5000。
	minCompactable := compactionDemoCtxWindow / 40
	if minCompactable < 1 {
		minCompactable = 1
	}
	autoCfg := &agent_v2.AutoCompactConfig{
		Enabled:              true,
		Model:                m,
		ModelContextWindow:   compactionDemoCtxWindow,
		SummaryMaxTokens:     summaryMax,
		SummaryBufferTokens:  summaryBuf,
		MinCompactableTokens: minCompactable,
		ReinjectRecentFiles:  true,
	}
	reactiveCfg := &agent_v2.ReactiveCompactConfig{
		Enabled:     true,
		MaxAttempts: 3,
	}
	return []agent_v2.Option{
		agent_v2.WithMicroCompact(microCfg),
		agent_v2.WithAutoCompact(autoCfg),
		agent_v2.WithReactiveCompact(reactiveCfg),
	}
}

// makeWebExecInput 构造 PlanAgent sub-agent 的 input message，trace 打印走 sink.send。
func makeWebExecInput(sink *eventSink) agent_v2.ExecInputBuilder {
	return func(
		_ context.Context,
		original string,
		executed []agent_v2.ExecutedStep,
		current agent_v2.PlanStep,
		resolvedRefs []int,
	) (*schema.Message, error) {
		stepIdx := len(executed)
		target := current.SubAgentName
		if target == "" {
			target = "(default)"
		}
		sink.send("step_dispatch", map[string]any{
			"idx":   stepIdx,
			"agent": target,
			"desc":  current.Description,
			"refs":  resolvedRefs,
		})

		var b strings.Builder
		fmt.Fprintf(&b, "原始目标：%s\n\n你的任务：%s", original, current.Description)
		for _, idx := range resolvedRefs {
			if idx < 0 || idx >= len(executed) {
				continue
			}
			e := executed[idx]
			fmt.Fprintf(&b, "\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result)
		}
		return schema.UserMessage(b.String()), nil
	}
}

// summarizeCheckpoint 提取指定 sessionID 的 cp 关键字段暴露给前端。
func summarizeCheckpoint(ctx context.Context, cp compose_v2.Checkpointer, sessionID string) map[string]any {
	saved, err := cp.LoadNS(ctx, sessionID, "")
	if err != nil || saved == nil {
		return nil
	}
	out := map[string]any{
		"id":             saved.ID,
		"created_at":     saved.CreatedAt,
		"channels_count": len(saved.Channels),
		"frontier":       saved.CurrentFrontier,
		"pending_inter":  len(saved.PendingInterrupts),
		"graph_id":       saved.GraphID,
		"status":         saved.Status.String(),
		"schema_ver":     saved.SchemaVer,
	}
	return out
}
