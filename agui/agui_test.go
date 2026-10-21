package agui_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// streamingModel 模拟真流式 LLM——Stream() 投递 N 个 chunk 后 EOF（照
// agent_v2/streaming_test.go 的 stub）。用来端到端验证 chunk→AG-UI TextMessage 映射。
type streamingModel struct {
	chunks []string
}

func (m *streamingModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	full := ""
	for _, c := range m.chunks {
		full += c
	}
	return schema.AssistantMessage(full), nil
}

func (m *streamingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](len(m.chunks))
	for _, c := range m.chunks {
		send(&schema.Message{Role: schema.RoleAssistant, Content: c})
	}
	schema.CloseSend(sr)
	return sr, nil
}

func (m *streamingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*streamingModel)(nil)

// parseSSE 把 SSE 响应文本解析成事件列表（每个 data 行的 JSON 对象）。
func parseSSE(raw string) []map[string]any {
	var events []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line[len("data: "):]), &ev); err == nil {
			events = append(events, ev)
		}
	}
	return events
}

func eventTypes(events []map[string]any) []string {
	types := make([]string, len(events))
	for i, ev := range events {
		types[i], _ = ev["type"].(string)
	}
	return types
}

func contains(types []string, want string) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

// TestSSEWriter_WireFormat 验证 SSE 编码——id 行 + camelCase JSON + 空行边界。
func TestSSEWriter_WireFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	sse, ok := agui.NewSSEWriter(rec)
	if !ok {
		t.Fatal("NewSSEWriter: recorder should support Flusher")
	}
	if err := sse.WriteEvent(agui.NewRunStarted("t1", "r1")); err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}
	out := rec.Body.String()

	if !strings.HasPrefix(out, "id: RUN_STARTED_") {
		t.Errorf("missing/incorrect id line: %q", out)
	}
	// AG-UI wire 关键：type + camelCase 字段（threadId/runId 不是 thread_id）
	for _, want := range []string{`"type":"RUN_STARTED"`, `"threadId":"t1"`, `"runId":"r1"`, `"timestamp":`} {
		if !strings.Contains(out, want) {
			t.Errorf("SSE frame missing %s: %q", want, out)
		}
	}
	if !strings.HasSuffix(out, "\n\n") {
		t.Errorf("frame not terminated by blank line: %q", out)
	}
}

// TestHandler_Streaming_EndToEnd 端到端验证：真 BasicAgent（mock 流式 model）经
// agui.Handler 产出的 AG-UI 事件流。断言事件序列 + chunk delta 拼回原文——证明
// chunk→TextMessageContent 映射真实（非 tautological）。
func TestHandler_Streaming_EndToEnd(t *testing.T) {
	stub := &streamingModel{chunks: []string{"hello ", "world", "!"}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("agui_e2e"),
			agent_v2.WithModel(stub),
			agent_v2.WithHooks(hooks), // adapter 作 hook.Hooks 注入（拿 LLM 收尾 / tool）
			agent_v2.WithMaxRounds(2),
		)
	})

	body := `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}

	events := parseSSE(rec.Body.String())
	types := eventTypes(events)
	if len(types) == 0 {
		t.Fatalf("no SSE events parsed from: %q", rec.Body.String())
	}

	// 首尾必须是 run 边界
	if types[0] != "RUN_STARTED" {
		t.Errorf("first event = %s, want RUN_STARTED (all: %v)", types[0], types)
	}
	if last := types[len(types)-1]; last != "RUN_FINISHED" {
		t.Errorf("last event = %s, want RUN_FINISHED (all: %v)", last, types)
	}

	// 流式 message 三件套都在
	for _, want := range []string{"TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END"} {
		if !contains(types, want) {
			t.Errorf("missing %s in event stream: %v", want, types)
		}
	}

	// chunk delta 拼回原文——证明映射真实传递了内容
	content := ""
	for _, ev := range events {
		if ev["type"] == "TEXT_MESSAGE_CONTENT" {
			d, _ := ev["delta"].(string)
			content += d
		}
	}
	if content != "hello world!" {
		t.Errorf("reassembled TextMessageContent delta = %q, want %q", content, "hello world!")
	}

	// 同一 run 的 message 应共享一个 messageId（= run_id）
	var msgID string
	for _, ev := range events {
		if ev["type"] == "TEXT_MESSAGE_START" {
			msgID, _ = ev["messageId"].(string)
		}
		if ev["type"] == "TEXT_MESSAGE_CONTENT" {
			if id, _ := ev["messageId"].(string); id != msgID {
				t.Errorf("TextMessageContent messageId = %q, want same as Start %q", id, msgID)
			}
		}
	}
	if msgID == "" {
		t.Error("TextMessageStart has empty messageId")
	}
}

// TestHandler_RejectsNonPost 验证非 POST 请求被拒。
func TestHandler_RejectsNonPost(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&streamingModel{}), agent_v2.WithHooks(hooks))
	})
	req := httptest.NewRequest(http.MethodGet, "/agui", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", rec.Code)
	}
}

// askThenAnswerModel 首轮调 ask_user（触发 HITL 中断），Resume 后（第 2 次 Generate）
// 出最终答案。Stream 返 error 强制走 Generate 路径，简化 HITL mock。
type askThenAnswerModel struct {
	mu    sync.Mutex
	calls int
}

func (m *askThenAnswerModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "au1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"你叫什么名字？"}`}},
		}), nil
	}
	return schema.AssistantMessage("你好，很高兴认识你！"), nil
}

func (m *askThenAnswerModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *askThenAnswerModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*askThenAnswerModel)(nil)

func postAGUI(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// outcomeType 取 RUN_FINISHED 的 outcome.type（success / interrupt）；无 outcome 视为 success。
func outcomeType(events []map[string]any) string {
	for _, ev := range events {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		if outcome, _ := ev["outcome"].(map[string]any); outcome != nil {
			t, _ := outcome["type"].(string)
			return t
		}
		return "success"
	}
	return ""
}

// findInterruptID 从 RUN_FINISHED(interrupt) 取第一个 interrupt 的 id。
func findInterruptID(events []map[string]any) string {
	for _, ev := range events {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		outcome, _ := ev["outcome"].(map[string]any)
		if outcome == nil || outcome["type"] != "interrupt" {
			continue
		}
		if its, _ := outcome["interrupts"].([]any); len(its) > 0 {
			it, _ := its[0].(map[string]any)
			id, _ := it["id"].(string)
			return id
		}
	}
	return ""
}

// TestHandler_HITL_InterruptResume 端到端验证 HITL 闭环：Run→中断→Resume→完成。
// 真 BasicAgent + WithAskUser + checkpointer，mock model 首轮调 ask_user、Resume 后答。
// 证明 Resume 缺口已补齐（前端答复能回来续跑）。
func TestHandler_HITL_InterruptResume(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	stub := &askThenAnswerModel{}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("agui_hitl"),
			agent_v2.WithModel(stub),
			agent_v2.WithAskUser(),
			agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(4),
		)
	}, agui.WithCheckpointer(cp))

	// 1. Run → 应中断（ask_user）
	rec1 := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"帮我"}]}`)
	events1 := parseSSE(rec1.Body.String())
	if ot := outcomeType(events1); ot != "interrupt" {
		t.Fatalf("first Run outcome = %q, want interrupt (events: %v)", ot, eventTypes(events1))
	}
	interruptID := findInterruptID(events1)
	if interruptID == "" {
		t.Fatal("no interruptId in RunFinished(interrupt)")
	}

	// 2. Resume（带答案）→ 应完成（success）+ 出最终答案
	resumeBody := `{"threadId":"t1","resume":[{"interruptId":"` + interruptID + `","status":"resolved","payload":"张三"}]}`
	rec2 := postAGUI(h, resumeBody)
	events2 := parseSSE(rec2.Body.String())
	if ot := outcomeType(events2); ot != "success" {
		t.Fatalf("Resume outcome = %q, want success (events: %v, body: %s)", ot, eventTypes(events2), rec2.Body.String())
	}
	if !contains(eventTypes(events2), "TEXT_MESSAGE_CONTENT") {
		t.Errorf("Resume should stream final answer TextMessage, got: %v", eventTypes(events2))
	}
}

// TestHandler_Resume_NoCheckpointer 验证没配 checkpointer 时 resume 请求发 RUN_ERROR（非 panic）。
func TestHandler_Resume_NoCheckpointer(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&streamingModel{}), agent_v2.WithHooks(hooks))
	}) // 无 WithCheckpointer
	rec := postAGUI(h, `{"threadId":"t1","resume":[{"interruptId":"x","status":"resolved","payload":"y"}]}`)
	if !contains(eventTypes(parseSSE(rec.Body.String())), "RUN_ERROR") {
		t.Errorf("resume without checkpointer should emit RUN_ERROR")
	}
}

// TestHandler_SequentialAgent 验证非 BasicAgent 编排（SequentialAgent 嵌套两个
// BasicAgent step）也能产出 AG-UI 事件流——**关键前提**：factory 里把 adapter（hooks）
// 挂到**每个 sub-agent**（不只顶层），嵌套编排的 LLM 输出才都能被 adapter 捕获。
// 这验证 agui 不限于 BasicAgent，任意编排只要工厂把 hooks 挂全即可。
func TestHandler_SequentialAgent(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		mkStep := func(name string, chunks []string) agent_v2.Agent {
			return agent_v2.NewBasicAgent(
				agent_v2.WithName(name),
				agent_v2.WithModel(&streamingModel{chunks: chunks}),
				agent_v2.WithHooks(hooks), // ← adapter 挂到每个 sub-agent
				agent_v2.WithMaxRounds(2),
			)
		}
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
			AddStep("s1", mkStep("step1", []string{"第一步", "完成。"})).
			AddStep("s2", mkStep("step2", []string{"第二步", "完成。"}))
	})

	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"走两步"}]}`)
	events := parseSSE(rec.Body.String())
	types := eventTypes(events)

	if len(types) == 0 || types[0] != "RUN_STARTED" || types[len(types)-1] != "RUN_FINISHED" {
		t.Fatalf("event stream boundary wrong: %v", types)
	}
	// 两个 sub-agent 的文本都应流出（chunk 走 OrchHooks ctx 嵌套继承 → TextMessage*）
	content, chunks := "", 0
	for _, ev := range events {
		if ev["type"] == "TEXT_MESSAGE_CONTENT" {
			d, _ := ev["delta"].(string)
			content += d
			chunks++
		}
	}
	if !strings.Contains(content, "第一步") || !strings.Contains(content, "第二步") {
		t.Errorf("expected both sub-agent outputs, got %q (events: %v)", content, types)
	}
	// **硬验证 sub-agent 真流式**：2 sub-agent × 2 chunk = 4 段 TextMessageContent。
	// 若 sub-agent 非流式（WithCtxOrchHooks 没让其继承 OrchHooks），则各一次性补发只 2 段。
	if chunks != 4 {
		t.Errorf("expected 4 streaming chunks (sub-agent 真流式), got %d —— WithCtxOrchHooks 应让 sub-agent 继承 adapter OrchHooks 逐段流式；只 2 段说明退回一次性补发", chunks)
	}
}

// cyclicBarrier 是 N 方循环屏障——强制并发 goroutine 逐步同步（防 Go scheduler 把
// 并发测试串行化掉，见 CLAUDE.md「thundering herd 测试需 WaitGroup 屏障」）。
type cyclicBarrier struct {
	mu   sync.Mutex
	cond *sync.Cond
	n    int // 参与方数
	cnt  int // 当前 generation 已到达数
	gen  int // generation 号
}

func newCyclicBarrier(n int) *cyclicBarrier {
	b := &cyclicBarrier{n: n}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *cyclicBarrier) wait() {
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.gen
	b.cnt++
	if b.cnt == b.n { // 最后一个到 → 放行本代、开新代
		b.cnt = 0
		b.gen++
		b.cond.Broadcast()
		return
	}
	for g == b.gen {
		b.cond.Wait()
	}
}

// lockstepModel 的 Stream 经屏障逐 chunk 产出——多 branch 并发流式时强制 chunk 交错
// （A0,B0,A1,B1…），复现单槽 activeMsgID 的交错缺陷（否则 Go scheduler 串行化两 branch
// 掩盖并发 bug）。用 unbuffered StreamReader 让 send 阻塞到 Recv，屏障同步 Recv 时机。
type lockstepModel struct {
	chunks []string
	bar    *cyclicBarrier
}

func (m *lockstepModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(strings.Join(m.chunks, "")), nil
}

func (m *lockstepModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](0) // unbuffered → send 阻塞到 Recv
	go func() {
		for _, c := range m.chunks {
			m.bar.wait() // 两 branch 到齐才各发下一 chunk
			send(&schema.Message{Role: schema.RoleAssistant, Content: c})
		}
		schema.CloseSend(sr)
	}()
	return sr, nil
}

func (m *lockstepModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*lockstepModel)(nil)

// verifyTextMessageSequence 按官方 AG-UI verify 的 activeMessages Map 语义校验事件序列
// （sdks/typescript/packages/client/src/verify/verify.ts）：START(id) 不能重复开 active 的
// id、CONTENT/END(id) 的 id 必须处于 active、run 结束无残留 active。单槽 activeMsgID 在
// 并发流式下会 END 错 message 或重复开合，命中这些断言——正是 fix 前应 fail 的点。
func verifyTextMessageSequence(t *testing.T, events []map[string]any) {
	t.Helper()
	active := map[string]bool{}
	startCount := map[string]int{}
	for i, ev := range events {
		typ, _ := ev["type"].(string)
		id, _ := ev["messageId"].(string)
		switch typ {
		case "TEXT_MESSAGE_START":
			startCount[id]++
			if active[id] {
				t.Errorf("event[%d] TEXT_MESSAGE_START: message %q 已 active（重复开合）", i, id)
			}
			active[id] = true
		case "TEXT_MESSAGE_CONTENT":
			if !active[id] {
				t.Errorf("event[%d] TEXT_MESSAGE_CONTENT: message %q 未 active（被别的 branch END 掉）", i, id)
			}
		case "TEXT_MESSAGE_END":
			if !active[id] {
				t.Errorf("event[%d] TEXT_MESSAGE_END: message %q 未 active", i, id)
			}
			delete(active, id)
		}
	}
	// **判别单槽 vs open-set 的核心断言**：并发流式下每条 message 应恰好开合一次（1 个
	// START + 1 个 END，中间多个 CONTENT）。单槽 activeMsgID 遇到别的 run_id 就 END 当前
	// 再 START，把一条 message 交错拆成多个 START/END 片段（startCount>1）——每片虽"合法"
	// （骗过上面的 active 检查，因 END 后 delete、再 START 不冲突），但前端会看到同一
	// message 反复开合、内容闪烁。open-set 让各 run_id 独立、恰好 1 次 START。
	for id, c := range startCount {
		if c != 1 {
			t.Errorf("message %q 出现 %d 次 TEXT_MESSAGE_START（应恰好 1；单槽 activeMsgID 交错把它拆成多段）", id, c)
		}
	}
	if len(active) > 0 {
		ids := make([]string, 0, len(active))
		for id := range active {
			ids = append(ids, id)
		}
		t.Errorf("run 结束仍有未收尾 message: %v", ids)
	}
}

// TestHandler_ParallelAgent_ConcurrentStreaming 回归测试并发编排的流式契约：ParallelAgent
// 下两个 sub-agent 并发流式时，adapter 的 per-run_id open-set 必须让每条 message 独立
// Start/Content/End，不被单槽交错开合。lockstepModel + 屏障强制两 branch chunk 逐个交错，
// 命中单槽缺陷；per-run_id fix 后事件序列符合官方 AG-UI TextMessage 契约。
func TestHandler_ParallelAgent_ConcurrentStreaming(t *testing.T) {
	bar := newCyclicBarrier(2)
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		mkBranch := func(name string, chunks []string) agent_v2.Agent {
			return agent_v2.NewBasicAgent(
				agent_v2.WithName(name),
				agent_v2.WithModel(&lockstepModel{chunks: chunks, bar: bar}),
				agent_v2.WithHooks(hooks),
			)
		}
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
			AddBranch("a", mkBranch("A", []string{"A1", "A2", "A3"})).
			AddBranch("b", mkBranch("B", []string{"B1", "B2", "B3"}))
	})

	// postAGUI 同步执行；屏障死锁（branch 未真并发）会 hang，加超时保护。
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"go"}]}`)
	}()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("并发流式 hang（屏障死锁或 branch 未真并发）")
	}

	events := parseSSE(rec.Body.String())
	// 核心断言：TextMessage 序列符合官方 activeMessages 契约（fix 前单槽交错必 fail）。
	verifyTextMessageSequence(t, events)

	// 两 branch 的文本增量都应过 wire（各自独立 message，内容不丢）。
	joined := ""
	for _, ev := range events {
		if ev["type"] == "TEXT_MESSAGE_CONTENT" {
			d, _ := ev["delta"].(string)
			joined += d
		}
	}
	for _, want := range []string{"A1", "A2", "A3", "B1", "B2", "B3"} {
		if !strings.Contains(joined, want) {
			t.Errorf("缺 chunk %q（并发流式内容丢失），got=%q", want, joined)
		}
	}
}

// TestHandler_HITL_Cancelled 验证 cancelled resume → 终止整个 run（对齐官方 AG-UI
// "user abandoned" 语义）：Run 触发 ask_user 中断，前端发 status=cancelled，handler 走
// ResumeActionAbort 清 cp 退出、发 RunFinished(cancelled)，不续跑（不进 model 次轮）、
// 清挂起 sess。
func TestHandler_HITL_Cancelled(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("x"),
			agent_v2.WithModel(&askThenAnswerModel{}),
			agent_v2.WithAskUser(),
			agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(5),
		)
	}, agui.WithCheckpointer(cp))

	// Run → 期望 ask_user 中断。
	iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if iid == "" {
		t.Fatal("期望 ask_user 中断")
	}

	// Resume with cancelled → 期望 abort：RUN_FINISHED 带 outcome.type=cancelled、不续跑。
	ev2 := parseSSE(postAGUI(h, `{"threadId":"c","resume":[{"interruptId":"`+iid+`","status":"cancelled"}]}`).Body.String())
	// **wire 合规关键断言**：RUN_FINISHED 必须带 outcome.type=cancelled。
	//
	// ⚠️ **这条断言 2026-09-21 整个翻转过**，翻转的理由值得留着——它是一个「哨兵钉住的
	// 事实本身变了」的实例，不是有人写错了：
	//
	//   - 原断言是「**不能**带 cancelled」，当时**完全正确**：那个快照的
	//     RunFinishedOutcome 判别联合只有 success/interrupt，塞 cancelled 会被前端 reject。
	//   - 1.0 定稿加了 cancelled 分支，同时把「省掉 outcome」这条路**明令禁止**：
	//     「MUST NOT report a stopped run as success — neither with the success outcome
	//     nor by omitting the outcome」，而规范另一处规定缺省 outcome **意味着成功**。
	//     ⟹ 原来那条合规路径变成了「把取消报成成功」。
	//
	// 所以别因为「注释说过不能发」就翻回去——去读 spec/1.0 的 events/lifecycle。
	foundCancelledOutcome := false
	for _, ev := range ev2 {
		if ev["type"] != "RUN_FINISHED" {
			continue
		}
		outcome, _ := ev["outcome"].(map[string]any)
		if outcome == nil {
			t.Errorf("RUN_FINISHED 缺 outcome——规范规定缺省 outcome 意味着**成功**，" +
				"于是取消会被前端读成一个正常完成的 run")
			continue
		}
		if outcome["type"] != "cancelled" {
			t.Errorf("RUN_FINISHED 的 outcome.type = %v, want cancelled", outcome["type"])
			continue
		}
		// cancelled 分支是 closed 的且只有 type 一个字段——不得带 interrupts。
		if _, has := outcome["interrupts"]; has {
			t.Errorf("cancelled outcome 不该带 interrupts（schema 是 closed 的）: %v", outcome)
		}
		foundCancelledOutcome = true
	}
	if !foundCancelledOutcome {
		t.Errorf("取消路径期望一条带 cancelled outcome 的 RUN_FINISHED，got %v", eventTypes(ev2))
	}
	// 取消语义走 Custom("cancelled")（官方 CUSTOM 合规）。
	foundCancel := false
	for _, ev := range ev2 {
		if ev["type"] == "CUSTOM" && ev["name"] == "cancelled" {
			foundCancel = true
		}
	}
	if !foundCancel {
		t.Errorf("cancelled abort 期望 Custom(name=cancelled) 携带取消语义，got %v", eventTypes(ev2))
	}
	if !contains(eventTypes(ev2), "RUN_FINISHED") {
		t.Errorf("cancelled abort 期望 RUN_FINISHED 终态，got %v", eventTypes(ev2))
	}
	// abort 不续跑：不该有 TOOL_CALL_RESULT（ask_user 命中答案后续跑的症状）。
	if contains(eventTypes(ev2), "TOOL_CALL_RESULT") {
		t.Errorf("abort 不应续跑（不该有 TOOL_CALL_RESULT）: %v", eventTypes(ev2))
	}

	// 挂起 sess 应已清理：再次 resume 同 threadId 返 RUN_ERROR（no session）。
	ev3 := parseSSE(postAGUI(h, `{"threadId":"c","resume":[{"interruptId":"`+iid+`","status":"resolved","payload":"x"}]}`).Body.String())
	if !contains(eventTypes(ev3), "RUN_ERROR") {
		t.Errorf("abort 后 sess 应已清理，再次 resume 应返 RUN_ERROR，got %v", eventTypes(ev3))
	}
}

// generateOnlyModel 只支持 Generate（Stream 返 error 强制降级）——验证非流式路径。
type generateOnlyModel struct {
	answer string
}

func (m *generateOnlyModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.answer), nil
}

func (m *generateOnlyModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *generateOnlyModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*generateOnlyModel)(nil)

// TestHandler_ParallelAgent_NonStreaming 验证并发编排下**非流式**（Generate fallback）
// sub-agent 的最终答案各自独立 message（不同 messageId），不因 OnModelEnd 兜底同一
// a.runID 而冲突到一条 message。依赖 basic_agent 在 Generate 路径也 stamp run_id
// （与 Stream 聚合路径一致），否则两 branch 补发都用 a.runID、message 交错错乱。
func TestHandler_ParallelAgent_NonStreaming(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		mkBranch := func(name, answer string) agent_v2.Agent {
			return agent_v2.NewBasicAgent(
				agent_v2.WithName(name),
				agent_v2.WithModel(&generateOnlyModel{answer: answer}),
				agent_v2.WithHooks(hooks),
			)
		}
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
			AddBranch("a", mkBranch("A", "答案A")).
			AddBranch("b", mkBranch("B", "答案B"))
	})
	events := parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"go"}]}`).Body.String())
	verifyTextMessageSequence(t, events)
	// 两 branch 的答案应在不同 messageId 下（各自独立 message，非补发到同一 a.runID）。
	msgIDs := map[string]bool{}
	for _, ev := range events {
		if ev["type"] == "TEXT_MESSAGE_START" {
			id, _ := ev["messageId"].(string)
			msgIDs[id] = true
		}
	}
	if len(msgIDs) < 2 {
		t.Errorf("并发非流式应有 2 条独立 message（不同 messageId），got %d: %v", len(msgIDs), msgIDs)
	}
}

// TestHandler_Resume_InvalidEntry 验证非法 resume entry（status 非 resolved/cancelled、
// 或 interruptId 空）fail-fast 发 RunError，而非静默当 resolved 注入答案续跑（对齐官方
// ResumeEntry enum 语义）。
func TestHandler_Resume_InvalidEntry(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	newHandler := func() http.Handler {
		return ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("x"),
				agent_v2.WithModel(&askThenAnswerModel{}),
				agent_v2.WithAskUser(),
				agent_v2.WithHooks(hooks),
				agent_v2.WithMaxRounds(5),
			)
		}, agui.WithCheckpointer(cp))
	}

	// 先 Run 触发 ask_user 中断拿 interruptId。
	h := newHandler()
	iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if iid == "" {
		t.Fatal("期望 ask_user 中断")
	}

	// 非法 status "cancel"（少 led，非合法枚举）→ 期望 RUN_ERROR、不续跑。
	ev := parseSSE(postAGUI(h, `{"threadId":"c","resume":[{"interruptId":"`+iid+`","status":"cancel","payload":"x"}]}`).Body.String())
	if !contains(eventTypes(ev), "RUN_ERROR") {
		t.Errorf("非法 status 期望 RUN_ERROR，got %v", eventTypes(ev))
	}
	if contains(eventTypes(ev), "TOOL_CALL_RESULT") {
		t.Errorf("非法 status 不应续跑（不该有 TOOL_CALL_RESULT）: %v", eventTypes(ev))
	}

	// 空 interruptId → 期望 RUN_ERROR。
	ev2 := parseSSE(postAGUI(h, `{"threadId":"c","resume":[{"interruptId":"","status":"resolved","payload":"x"}]}`).Body.String())
	if !contains(eventTypes(ev2), "RUN_ERROR") {
		t.Errorf("空 interruptId 期望 RUN_ERROR，got %v", eventTypes(ev2))
	}
}

// TestHandler_NilAgent 验证 factory 返回 nil agent 时 fail-fast 发 RUN_ERROR(code=nil_agent)，
// 而非 nil-interface deref panic 留半截流。断言 code=nil_agent 而非泛 RUN_ERROR——区分守卫
// 路径与 panic recover 兜底（双层防护，避免 sanity 被兜底路径误命中假绿）。
func TestHandler_NilAgent(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return nil // factory bug：某分支忘 return 真 agent
	})
	events := parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String())
	found := false
	for _, ev := range events {
		if ev["type"] == "RUN_ERROR" && ev["code"] == "nil_agent" {
			found = true
		}
	}
	if !found {
		t.Errorf("nil agent 期望 RUN_ERROR code=nil_agent（守卫，非 panic 兜底），got %v", events)
	}
}

// TestHandler_PanicRecover 验证 handler goroutine 上的 panic（agent.Run 之外，如 factory
// 构造 panic）被 ServeHTTP defer recover 补发 RUN_ERROR 终态，不留半截流 / 不 crash。
func TestHandler_PanicRecover(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		panic("factory boom") // factory 构造 panic（在 handler goroutine，非 agent.Run 内）
	})
	events := parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String())
	found := false
	for _, ev := range events {
		if ev["type"] == "RUN_ERROR" && ev["code"] == "panic" {
			found = true
		}
	}
	if !found {
		t.Errorf("handler panic 期望 recover 补发 RUN_ERROR code=panic，got %v", events)
	}
}

// blockingModel：首轮 ask_user 中断，次轮 Generate 阻塞在 block channel（模拟慢 Resume
// 保持 busy 窗口）——用于并发 busy 保护测试。
type blockingModel struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{} // 次轮进入时 close（通知测试已 busy）
	block   chan struct{} // 次轮阻塞在此，测试 close 放行
}

func (m *blockingModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	c := m.calls
	m.mu.Unlock()
	if c == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "bau1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"q?"}`}},
		}), nil
	}
	close(m.entered) // 已进次轮 = busy=true
	<-m.block        // 阻塞保持 busy 窗口
	return schema.AssistantMessage("done"), nil
}

func (m *blockingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *blockingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*blockingModel)(nil)

// recordingCP 包装 Checkpointer 记录 DeleteSubtree 调用——验证覆盖挂起 / evict 时清旧 cp。
type recordingCP struct {
	compose_v2.Checkpointer
	mu      sync.Mutex
	deleted []string
}

func (c *recordingCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.mu.Lock()
	c.deleted = append(c.deleted, sid)
	c.mu.Unlock()
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

func (c *recordingCP) deletedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deleted)
}

// TestHandler_ConcurrentResume_Busy 验证同 threadId 并发 resume 时 busy 保护：第一个占用
// （busy=true），第二个并发 resume 返 RUN_ERROR(code=busy)，避免同一挂起 sess 被两个
// Resume 并发驱动 + cp 双写。
func TestHandler_ConcurrentResume_Busy(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	m := &blockingModel{entered: make(chan struct{}), block: make(chan struct{})}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(m),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(cp))

	// Run → ask_user 中断。
	iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if iid == "" {
		t.Fatal("期望 ask_user 中断")
	}
	body := `{"threadId":"c","resume":[{"interruptId":"` + iid + `","status":"resolved","payload":"a"}]}`

	// goroutine A：resume，次轮 model 阻塞保持 busy。
	aDone := make(chan struct{})
	go func() {
		_ = postAGUI(h, body)
		close(aDone)
	}()
	<-m.entered // 等 A 进次轮 model（busy=true 窗口）

	// B：同 threadId 并发 resume → 期望 RUN_ERROR(code=busy)。
	bEv := parseSSE(postAGUI(h, body).Body.String())
	foundBusy := false
	for _, ev := range bEv {
		if ev["type"] == "RUN_ERROR" && ev["code"] == "busy" {
			foundBusy = true
		}
	}
	if !foundBusy {
		t.Errorf("并发 resume 同 threadId 期望第二个返 RUN_ERROR code=busy，got %v", eventTypes(bEv))
	}

	close(m.block) // 放行 A
	<-aDone
}

// TestHandler_ServeRun_DropsStaleCP 验证 serveRun 覆盖已有挂起会话（用户开新对话）时
// dropCP 清旧 cp 子树（DeleteSubtree），避免旧挂起 cp 孤儿泄漏。
func TestHandler_ServeRun_DropsStaleCP(t *testing.T) {
	rcp := &recordingCP{Checkpointer: compose_v2.NewInMemoryCheckpointer()}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&askThenAnswerModel{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(rcp))

	// Run1 中断（threadId=t，存挂起 cp）。
	iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"t","runId":"r1","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if iid == "" {
		t.Fatal("期望 ask_user 中断")
	}
	before := rcp.deletedCount()

	// Run2 同 threadId（新对话，覆盖挂起）→ 应 dropCP 旧挂起 cp。
	_ = postAGUI(h, `{"threadId":"t","runId":"r2","messages":[{"role":"user","content":"hi2"}]}`)
	if rcp.deletedCount() <= before {
		t.Errorf("serveRun 覆盖挂起会话应 dropCP 清旧 cp（DeleteSubtree），未被调（before=%d after=%d）", before, rcp.deletedCount())
	}
}

// TestHandler_MaxSessions_Evict 验证挂起会话超上限时 LRU 淘汰最旧空闲会话——防前端放弃
// 中断导致 sessions map 长跑 OOM。max=2 下存 3 挂起，最旧（t1）被淘汰、resume 返 no_session；
// 最新（t3）保留。
func TestHandler_MaxSessions_Evict(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&askThenAnswerModel{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(cp), agui.WithMaxSessions(2))

	// 线程名**故意跟时间顺序反着排**（z 最旧、a 次之、b 最新）：名字顺着排的话，
	// 「按最旧淘汰」和「按名字淘汰」给出同一个答案，这条断言就不再承重（实测把定序里
	// 的 touchedAt 比较删掉，用 t1/t2/t3 的版本照样绿）。
	iids := map[string]string{}
	for _, tid := range []string{"z-oldest", "a-mid", "b-newest"} {
		iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"`+tid+`","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
		if iid == "" {
			t.Fatalf("threadId %s 期望 ask_user 中断", tid)
		}
		iids[tid] = iid
	}

	// max=2：最旧的那个应被淘汰 → resume 返 RUN_ERROR(code=no_session)。
	ev1 := parseSSE(postAGUI(h, `{"threadId":"z-oldest","resume":[{"interruptId":"`+iids["z-oldest"]+`","status":"resolved","payload":"a"}]}`).Body.String())
	foundNoSession := false
	for _, ev := range ev1 {
		if ev["type"] == "RUN_ERROR" && ev["code"] == "no_session" {
			foundNoSession = true
		}
	}
	if !foundNoSession {
		t.Errorf("最旧的 z-oldest 应被淘汰（maxSessions=2），resume 期望 RUN_ERROR code=no_session，got %v", eventTypes(ev1))
	}

	// 最新的那个应保留 → resume 不返 no_session。
	ev3 := parseSSE(postAGUI(h, `{"threadId":"b-newest","resume":[{"interruptId":"`+iids["b-newest"]+`","status":"resolved","payload":"a"}]}`).Body.String())
	for _, ev := range ev3 {
		if ev["type"] == "RUN_ERROR" && ev["code"] == "no_session" {
			t.Errorf("最新的 b-newest 不应被淘汰，但 resume 返 no_session: %v", eventTypes(ev3))
		}
	}
}

// countingModel 记 Generate 调用次数——验证 client 断开短路后 agent 不跑。
type countingModel struct {
	mu    sync.Mutex
	calls int
}

func (m *countingModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	return schema.AssistantMessage("x"), nil
}

func (m *countingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *countingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *countingModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// failWriter 是一个 Write 恒失败的 ResponseWriter（模拟 client 已断开），实现 http.Flusher
// 让 NewSSEWriter 接受它。
type failWriter struct {
	header http.Header
}

func (f *failWriter) Header() http.Header {
	if f.header == nil {
		f.header = http.Header{}
	}
	return f.header
}
func (f *failWriter) Write(_ []byte) (int, error) { return 0, errors.New("client disconnected") }
func (f *failWriter) WriteHeader(_ int)           {}
func (f *failWriter) Flush()                      {}

// TestHandler_ClientDisconnect_ShortCircuit 验证 RunStarted 写失败（client 已断开）时短路
// 不跑 agent——省 LLM/工具开销。
func TestHandler_ClientDisconnect_ShortCircuit(t *testing.T) {
	m := &countingModel{}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(m), agent_v2.WithHooks(hooks))
	})
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(`{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`))
	h.ServeHTTP(&failWriter{}, req)
	if m.count() != 0 {
		t.Errorf("client 断开（RunStarted 写失败）应短路不跑 agent，但 model 被调 %d 次", m.count())
	}
}

// twoToolsModel 第一轮一次发两个 tool_call，第二轮给终局答复。
// 用来暴露「所有工具结果共用一个 messageId」——单工具时看不出来。
type twoToolsModel struct {
	mu    sync.Mutex
	calls int
}

func (m *twoToolsModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_a", Type: "function", Function: schema.FunctionCall{Name: "echo", Arguments: `{"s":"a"}`}},
			{ID: "tc_b", Type: "function", Function: schema.FunctionCall{Name: "echo", Arguments: `{"s":"b"}`}},
		}), nil
	}
	return schema.AssistantMessage("done"), nil
}

func (m *twoToolsModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *twoToolsModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*twoToolsModel)(nil)

// TestHandler_ToolCallResult_MessageIDUniqueInThread 钉住「每条工具结果有自己的 messageId」。
//
// 规范把唯一性写成了硬要求（1.0 basic/index）：「`messageId` identifies a message …
// it MUST be unique within its thread」，而 TOOL_CALL_RESULT 的 schema 描述解释了原因
// ——「Mints a tool message rather than appending to an existing one, **which is why it
// has its own messageId**」。同 id 发两次 = 声称铸造同一条消息两次，消费者会把后一条
// **追加进前一条**。
//
// ⚠️ **必须用两个工具**：这里曾无条件用 run id 当 messageId，单工具的用例**照样绿**
// ——那个量在 N=1 上不承重。
func TestHandler_ToolCallResult_MessageIDUniqueInThread(t *testing.T) {
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("echo", "echo",
			map[string]any{"type": "object", "properties": map[string]any{"s": map[string]any{"type": "string"}}},
			func(_ context.Context, args map[string]any) (*tool.Result, error) {
				s, _ := args["s"].(string)
				return tool.Success("ok:" + s), nil
			}))
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("x"),
			agent_v2.WithModel(&twoToolsModel{}),
			agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(5),
		)
	})

	ev := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"run-1","messages":[{"role":"user","content":"hi"}]}`).Body.String())

	seen := map[string]string{} // messageId -> toolCallId
	for _, e := range ev {
		if e["type"] != "TOOL_CALL_RESULT" {
			continue
		}
		mid, _ := e["messageId"].(string)
		tcid, _ := e["toolCallId"].(string)
		if mid == "" {
			t.Errorf("TOOL_CALL_RESULT 缺 messageId（schema required）: %v", e)
			continue
		}
		if prev, dup := seen[mid]; dup {
			t.Errorf("两条工具结果共用 messageId %q（toolCallId %q 与 %q）——"+
				"消费者会把后一条追加进前一条那条消息里", mid, prev, tcid)
		}
		seen[mid] = tcid
	}
	if len(seen) != 2 {
		t.Fatalf("期望两条 TOOL_CALL_RESULT，实际 %d 条：%v", len(seen), eventTypes(ev))
	}
}

// TestHandler_Resume_UsesWireRunID 钉住「续跑在 wire 上是一个**新** run」。
//
// 规范两条（1.0 basic/index + interrupt-resume）：「`runId` identifies one run.
// It MUST NOT be reused for another run on the same thread」、「continuing means a new run」。
//
// 这里曾把 `sess.RunID`（我们内部的「一个任务」身份，一次 Run + N 次 Resume 共用一枚、
// 是 checkpoint 主索引）直接当成 wire 上的 runId 发出去 ⟹ 续跑重播了被中断那个 run 的 id。
// **N=1（没中断过）正确，N≥2 静默违规**——正是「借来的量」那个形状。
//
// 断言选在「续跑事件带的是本次请求的 runId」上；只断言「两次 runId 不同」不够——
// 那在「续跑随便编一个新 id」的错误实现下也绿。
func TestHandler_Resume_UsesWireRunID(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("x"),
			agent_v2.WithModel(&askThenAnswerModel{}),
			agent_v2.WithAskUser(),
			agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(5),
		)
	}, agui.WithCheckpointer(cp))

	ev1 := parseSSE(postAGUI(h, `{"threadId":"t2","runId":"run-first","messages":[{"role":"user","content":"hi"}]}`).Body.String())
	iid := findInterruptID(ev1)
	if iid == "" {
		t.Fatal("期望 ask_user 中断")
	}

	// 续跑：客户端按规范给一个**新** runId。
	ev2 := parseSSE(postAGUI(h, `{"threadId":"t2","runId":"run-second","resume":[{"interruptId":"`+iid+`","status":"resolved","payload":"Bob"}]}`).Body.String())

	checked := 0
	for _, e := range ev2 {
		ty, _ := e["type"].(string)
		if ty != "RUN_STARTED" && ty != "RUN_FINISHED" {
			continue
		}
		got, _ := e["runId"].(string)
		if got != "run-second" {
			t.Errorf("%s 的 runId = %q, want \"run-second\"——续跑在 wire 上是一个新 run，"+
				"复用被中断那个 run 的 id 违反 MUST NOT be reused（got %q 多半是内部 sess.RunID 漏出去了）",
				ty, got, got)
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("续跑没发出 RUN_STARTED / RUN_FINISHED，无从断言：%v", eventTypes(ev2))
	}
}

// TestWireShape_AgainstSpec1_0 直接对事件构造器做 **wire 形状**断言——不跑 agent，
// 只看序列化出来的 JSON 对不对得上 1.0 schema 的 required / closed 约束。
//
// **为什么要有这一条**：其余用例都是「跑一遍看事件类型对不对」，而协议合规的失效往往
// 不改变事件类型，只改变**字段**——少一个 required、多一个会被 strip 的、或者一个
// omitempty 让必填字段在某个取值上消失。那些在按 `type` 断言的用例里全都看不见。
//
// 断言的依据逐条写在注释里，改之前先去读 `spec/1.0/schema.json` 对应的 $defs。
func TestWireShape_AgainstSpec1_0(t *testing.T) {
	marshal := func(v any) map[string]any {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return m
	}
	has := func(m map[string]any, k string) bool { _, ok := m[k]; return ok }

	t.Run("RUN_FINISHED cancelled 带 outcome 且不带 interrupts", func(t *testing.T) {
		// RunFinishedCancelledOutcome：required[type]、unevaluatedProperties:false。
		m := marshal(agui.NewRunCancelled("th", "r1"))
		oc, _ := m["outcome"].(map[string]any)
		if oc == nil {
			t.Fatal("缺 outcome——规范规定缺省 outcome 意味着成功，取消会被读成完成")
		}
		if oc["type"] != "cancelled" {
			t.Errorf("outcome.type = %v, want cancelled", oc["type"])
		}
		if has(oc, "interrupts") {
			t.Error("cancelled outcome 不该带 interrupts（schema 是 closed 的）")
		}
	})

	t.Run("RUN_FINISHED success 不带 interrupts", func(t *testing.T) {
		// 同理 closed：「a success with an interrupt still pending would be a contradiction」。
		oc, _ := marshal(agui.NewRunFinished("th", "r1"))["outcome"].(map[string]any)
		if oc == nil || oc["type"] != "success" {
			t.Fatalf("success outcome 形状不对: %v", oc)
		}
		if has(oc, "interrupts") {
			t.Error("success outcome 不该带 interrupts")
		}
	})

	t.Run("RUN_ERROR 不带 runId", func(t *testing.T) {
		// 1.0 的 RUN_ERROR 只有 type/message/code/usage 且 unevaluatedProperties:false
		// ——多发的字段被消费者 strip 并 warn，等于「我们以为在发、实际没发出去」。
		m := marshal(agui.NewRunError("boom", "some_code"))
		if has(m, "runId") {
			t.Error("RUN_ERROR 带了 runId——1.0 schema 是 closed 的，这个字段到不了前端")
		}
		if m["message"] != "boom" {
			t.Errorf("message = %v", m["message"])
		}
	})

	t.Run("CUSTOM.value 即使是 nil 也必须出现", func(t *testing.T) {
		// 1.0：required[type,name,value]，且「A required JSON payload can be null」。
		// 加 omitempty 会让业务传 nil 时整个字段消失。
		m := marshal(agui.NewCustom("cancelled", nil))
		if !has(m, "value") {
			t.Error("CUSTOM 传 nil value 时字段消失了——1.0 里 value 是 required（可以是 null）")
		}
	})

	t.Run("TOOL_CALL_RESULT 的 messageId 随 toolCallId 变", func(t *testing.T) {
		// messageId MUST be unique within its thread；TOOL_CALL_RESULT「mints a tool
		// message rather than appending to an existing one, which is why it has its
		// own messageId」。
		a := marshal(agui.NewToolCallResult("run-1/tc_a", "tc_a", "x"))
		b := marshal(agui.NewToolCallResult("run-1/tc_b", "tc_b", "y"))
		if a["messageId"] == b["messageId"] {
			t.Errorf("两条结果的 messageId 相同 (%v)——消费者会把后一条追加进前一条", a["messageId"])
		}
		for _, k := range []string{"messageId", "toolCallId", "content"} {
			if !has(a, k) {
				t.Errorf("TOOL_CALL_RESULT 缺 required 字段 %q", k)
			}
		}
	})
}
