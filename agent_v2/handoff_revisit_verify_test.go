package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// capturingRevisitModel 记录每次 Generate 收到的 input messages（验证 HIGH-1 复访悬空 tool_call）。
type capturingRevisitModel struct {
	responses []*schema.Message
	idx       int
	captured  [][]*schema.Message
	mu        sync.Mutex
}

func (m *capturingRevisitModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func (m *capturingRevisitModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*schema.Message, len(msgs))
	copy(cp, msgs)
	m.captured = append(m.captured, cp)
	resp := m.responses[m.idx]
	if m.idx < len(m.responses)-1 {
		m.idx++
	}
	return resp, nil
}

func (m *capturingRevisitModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, _ := m.Generate(ctx, msgs, opts...)
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

// findDanglingToolCall 检测 messages 里有无悬空 assistant(tool_call)——带 tool_call 的 assistant
// 后续无配对 role=tool 消息（同 callID）。返回悬空的 callID（空=无悬空）。
func findDanglingToolCall(msgs []*schema.Message) []string {
	resultIDs := make(map[string]bool)
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			resultIDs[schema.GetToolResultID(m)] = true
		}
	}
	var dangling []string
	for _, m := range msgs {
		if m == nil || m.Role != schema.RoleAssistant {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if !resultIDs[tc.ID] {
				dangling = append(dangling, tc.ID)
			}
		}
	}
	return dangling
}

// TestHandoffAgent_Revisit_DanglingTransferToolCall 验证 Agent D HIGH-1：
// frontdesk→billing→frontdesk 复访时，frontdesk 二次激活收到的 prior 是否含悬空
// transfer tool_call（hop1 的 Exit=assistant(transfer tc)，其 tool_result 不进 events）。
func TestHandoffAgent_Revisit_DanglingTransferToolCall(t *testing.T) {
	// frontdesk: hop1 transfer→billing；hop3 复访返回文本终止
	frontModel := &capturingRevisitModel{responses: []*schema.Message{
		transferToolCall("tc_f1", "billing"),
		schema.AssistantMessage("frontdesk 二次处理完成"),
	}}
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	// billing: hop2 transfer→frontdesk（触发复访）
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			transferToolCall("tc_b1", "frontdesk"),
		})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
		agent_v2.WithHandoffMaxHops(5), // 允许 ping-pong
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("我要账单"))
	t.Logf("Run err: %v", err)

	frontModel.mu.Lock()
	defer frontModel.mu.Unlock()
	t.Logf("frontdesk model 被调用 %d 次", len(frontModel.captured))
	for i, cap := range frontModel.captured {
		t.Logf("--- frontdesk 第 %d 次调用收到 %d 条 messages ---", i+1, len(cap))
		for j, m := range cap {
			tcs := schema.GetToolCalls(m)
			t.Logf("  [%d] role=%s tool_calls=%d content=%.40q", j, m.Role, len(tcs), m.Content)
		}
		if dangling := findDanglingToolCall(cap); len(dangling) > 0 {
			t.Errorf("❌ HIGH-1 确认：frontdesk 第 %d 次调用收到悬空 tool_call %v（真实 API 会 400）", i+1, dangling)
		}
	}
}

// TestStoreMemory_DanglingToolCallSanitized 验证 HIGH-1 fix 的 StoreMemory 旁路覆盖
// （audit FINDING 1）：Run 1 return-direct 出口（带悬空 tc 的 assistant）被 commitFinal
// st.Put 进长期记忆 → Run 2 **新 session** preamble Search 注入进 msgs（在 prior 之前，
// prior 层清洗覆盖不到）→ msgs 最终汇合层剥离兜底。
// 若有人把 dropDanglingToolCalls 挪回 prior 层，本测试 FAIL。
//
// **形状来源是业务自声明的 return-direct 工具，不是 transfer_to_agent**：后者在没有
// HandoffAgent 接的场合已改成软失败回灌（LLM 接着自己作答）⟹ 摆不出悬空 tc，这条用例
// 会退化成「什么都没发生」而恒真（实测过：Run 2 收到的是 `[stub exhausted]` 纯文本，
// 把 dropDanglingToolCalls 整个禁掉它照样绿）。同病三处，另两处在 record_leak_verify_test.go。
//
// ⚠️ 断言必须有正向那一半（下面 sawInjectedExit）：只断言「没有悬空 tc」的话，
// fixture 一旦腐化到「记忆压根没注入」，这条用例在空转状态下恒真。
func TestStoreMemory_DanglingToolCallSanitized(t *testing.T) {
	st := store.NewInMemoryStore()
	ns := store.NS("memories", "u1")

	// Run 1: 业务自声明 return-direct 工具一调即出，Exit = 带悬空 tc 的 assistant
	// → commitFinal 顶层 st.Put 进 store。
	run1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_x1", Type: "function",
				Function: schema.FunctionCall{Name: directExitToolName, Arguments: "{}"},
			}}),
		})),
		agent_v2.WithToolkit(directExitToolkit()),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
		agent_v2.WithMaxRounds(3),
	)
	if _, err := run1.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Run 2: 全新 session，同 store——preamble Search 注入 Run 1 存的记忆。
	capModel := &capturingRevisitModel{responses: []*schema.Message{
		schema.AssistantMessage("done"),
	}}
	run2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(capModel),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
		agent_v2.WithMaxRounds(3),
	)
	if _, err := run2.Run(context.Background(), agent_v2.NewRunSession(), // query 要跟 Run 1 留下的记忆有共同词——注入按相关性排（见 searchMemories）。
		// 这条用例验的是「注入的历史里没有悬空 tool_call」，前提是那条历史真被注入。
		schema.UserMessage("上次已收尾了吗")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	capModel.mu.Lock()
	defer capModel.mu.Unlock()
	if len(capModel.captured) == 0 {
		t.Fatal("Run 2 模型未被调用")
	}
	sawInjectedExit := false
	for i, cap := range capModel.captured {
		for j, m := range cap {
			t.Logf("  run2 call%d [%d] role=%s tool_calls=%d content=%.40q", i+1, j, m.Role, len(schema.GetToolCalls(m)), m.Content)
			// 正向那一半验的是**前提**（Run 1 的出口确实经 store 注入进来了），
			// 不是验修复——注入物在剥离前后形态不同（悬空 tc / `[历史记录]` 记账文本），
			// 认「本轮 user 之前有 assistant」即可。验成修复的话，剥离被破坏时这条会
			// 跟着响，把「fixture 坏了」和「防护坏了」两件事混成一个信号。
			// 按 **StoreInjectedAnnotation** 判，不按 role 判——注入物的 role 变过：
			// 2026-09-07 记忆改存文本后，注入走 `case string` ⟹ 包成 SystemMessage
			// （此前 in-memory 上是 assistant、持久后端上已经是 system，本来就不一致）。
			// marker 是这条注入路径唯一稳定的身份，按它判才不会随形态再红一次。
			if v, ok := m.Annotations[agent_v2.StoreInjectedAnnotation].(bool); ok && v {
				sawInjectedExit = true
			}
		}
		if dangling := findDanglingToolCall(cap); len(dangling) > 0 {
			t.Errorf("❌ Run 2 收到悬空 tool_call %v（真实 API 会 400）", dangling)
		}
	}

	// ⚠️ **这条用例的性质来回变过两次，记全**。
	//
	// 原本靠「Run 1 的 return-direct 出口经 store 注入进 Run 2」喂给 dropDanglingToolCalls，
	// 所以 `sawInjectedExit` 是**正向那一半**（防空测：注入物根本没进来的话，
	// 「没有悬空 tc」是恒真的）。
	//
	// 2026-08-24 记忆注入改成按相关性打分之后那条消息进不来了（出口 assistant 的 Content
	// 是空的、抽不出文本），断言一度反转成「不该被注入」。
	//
	// 2026-08-28 return-direct 的最终答复改为**取工具产出**——业务 exit 工具的出口消息
	// 现在**有内容、且不带 tool_call**，于是又能被打分注入了。断言**恢复正向语义**：
	// 前提必须成立（注入物进来了），而真实伤害由下面那句 `findDanglingToolCall` 独立守着
	// ——它才是验修复的那一半，两者分工不能混。
	//
	// （框架工具 transfer / route 走的仍是旧形态：它们是控制动作，答复来源是接手的 agent。）
	if !sawInjectedExit {
		t.Error("fixture 前提没成立：Run 1 的出口没有经 store 注入进 Run 2，" +
			"「没有悬空 tool_call」这句话在空输入上恒真、测不出东西")
	}
}
