package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// transferToolCall 构造一个调 transfer_to_agent 工具的 assistant message。
func transferToolCall(callID, target string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: callID,
		Function: schema.FunctionCall{
			Name:      agent_v2.HandoffTransferToolName,
			Arguments: `{"agent_name":"` + target + `"}`,
		},
	}})
}

func TestHandoffAgent_ActiveRequiresOuterCommit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resume bool
	}{{"failed_parent", false}, {"resumed_parent", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			st := store.NewInMemoryStore()
			ctx = compose_v2.WithThreadIDCtx(store.WithContext(ctx, st), "thread")
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithHandoffTransfer(),
				agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc", "billing")})))
			billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单已处理")})))
			handoff := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial("front")).
				AddAgent("front", front).AddAgent("billing", billing)
			stop := errors.New("host rejected the execution")
			b := compose_v2.NewBuilder().WithName("commit-host")
			b.AddNode("delegate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				events, err := agent_v2.RunSubAgent(ctx, "support", handoff, schema.UserMessage("查账单"))
				if err != nil {
					return nil, err
				}
				if got := agent_v2.FinalMessage(events).GetContent(); got != "账单已处理" {
					t.Errorf("转交应实际运行 billing，实际 %q", got)
				}
				if !tc.resume {
					return nil, stop
				}
				return nil, nil
			})
			b.AddNode("approve", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "接受处理结果吗？"})
				return nil, err
			})
			b.AddEdge(compose_v2.StartNode, "delegate")
			b.AddEdge("delegate", "approve")
			b.AddEdge("approve", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			const sid = "task"
			run := func(opts ...compose_v2.RunOption) error {
				base := []compose_v2.RunOption{compose_v2.WithSessionID(sid), compose_v2.WithCheckpointer(cp)}
				return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(base, opts...)...)
			}
			err = run()
			if !tc.resume && !errors.Is(err, stop) {
				t.Fatalf("测试应在外层否决这次转交，实际 %v", err)
			}
			ns := store.NS("__handoff_active__", "support", "thread")
			if item, err := st.Get(ctx, ns, "active"); err != nil || item != nil {
				t.Fatalf("外层提交前不得写入 active，实际 %+v，错误 %v", item, err)
			}
			if !tc.resume {
				return
			}
			interrupt, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("外层应挂起，实际 %v", err)
			}
			answers := map[string]*schema.Message{interrupt.Query.InterruptID: schema.UserMessage("接受")}
			saved, err := cp.LoadNS(ctx, sid, "")
			if err != nil || saved == nil || len(saved.Signals) == 0 {
				t.Fatalf("挂起存档应带有待提交的转交，实际 %+v，错误 %v", saved, err)
			}
			fork := compose_v2.WithThreadIDCtx(ctx, "fork-thread")
			if err := compose_v2.Run(fork, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID("fork-task"),
				compose_v2.WithResumeFrom(saved), compose_v2.WithResumeAnswers(answers)); err != nil {
				t.Fatalf("分叉任务应正常运行，实际 %v", err)
			}
			if item, err := st.Get(ctx, ns, "active"); err != nil || item != nil {
				t.Fatalf("分叉任务不得发布来源任务的转交，实际 %+v，错误 %v", item, err)
			}
			if err := run(compose_v2.WithResumeAnswers(answers)); err != nil {
				t.Fatal(err)
			}
			if item, err := st.Get(ctx, ns, "active"); err != nil || item == nil || item.Value != "billing" {
				t.Fatalf("提交后应写入恢复的转交目标，实际 %+v，错误 %v", item, err)
			}
		})
	}
}

// TestHandoffAgent_ModelB_LLMTransfer 验证模型 B 核心：LLM 在 sub-agent 内调
// transfer_to_agent 工具自主转交 → return-direct 短路 frontdesk → stepFn 读 tool
// 信号 → 转交 billing 接管。这是模型 A（annotation-only，纯 LLM 不可用）补不了的。
func TestHandoffAgent_ModelB_LLMTransfer(t *testing.T) {
	// frontdesk：LLM 调 transfer_to_agent("billing") 自主转交（配 WithHandoffTransfer）
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			transferToolCall("tc_1", "billing"),
		})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	// billing：接管后回复（无转交 → done）
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("账单已处理"),
		})),
		agent_v2.WithMaxRounds(5),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("我要账单"))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	// 核心断言：转交到 billing —— billing 接管并产出 final。
	final := agent_v2.FinalMessageByAgent(events, "billing")
	if final == nil {
		t.Fatal("LLM 自主转交失败：billing 未接管（FinalMessageByAgent(billing)=nil）")
	}
	if !strings.Contains(final.Content, "账单已处理") {
		t.Errorf("billing final 内容错：%q", final.Content)
	}
}

// TestHandoffAgent_ModelB_PersistActiveAcrossRuns 验证模型 B 跨轮持久 active：
// Run1 transfer 到 billing → 持久 store → Run2（同 ThreadID）直接进 billing（不回 frontdesk）。
func TestHandoffAgent_ModelB_PersistActiveAcrossRuns(t *testing.T) {
	// frontdesk 只在 Run1 用（Run1 transfer billing 后 return-direct 终止）
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			transferToolCall("tc_1", "billing"),
		})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	// billing：Run1 + Run2 各回复一次（同实例 stub idx 跨 Run 累积）
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("Run1-billing"),
			schema.AssistantMessage("Run2-billing"),
		})),
		agent_v2.WithMaxRounds(5),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	// 同 ThreadID 跨两次 Run（模型 B 持久 active 绑 ThreadID）
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-T1")

	// Run1：frontdesk → transfer billing → 持久 active=billing
	ev1, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我要账单"))
	if err != nil {
		t.Fatalf("Run1 error: %v", err)
	}
	if f := agent_v2.FinalMessageByAgent(ev1, "billing"); f == nil || !strings.Contains(f.Content, "Run1-billing") {
		t.Fatalf("Run1 未转交到 billing：%v", f)
	}

	// Run2（同 ThreadID，新 session）：应直接进 billing（持久 active），不回 frontdesk。
	ev2, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("再查一笔"))
	if err != nil {
		t.Fatalf("Run2 error: %v", err)
	}
	// 进了 billing → final=Run2-billing；若退化回 frontdesk（持久失败）→ frontdesk stub
	// 已耗尽返 "[stub exhausted]"，billing 不被调 → 本断言 fail 指向持久 active 未生效。
	f2 := agent_v2.FinalMessageByAgent(ev2, "billing")
	if f2 == nil {
		t.Fatal("跨 Run 持久 active 失效：Run2 未进 billing（退化回 frontdesk）")
	}
	if !strings.Contains(f2.Content, "Run2-billing") {
		t.Errorf("Run2 billing final 内容错：%q", f2.Content)
	}
}

// TestHandoffAgent_ModelB_InvalidTargetSoftFail 验证非法转交目标软失败回灌：
// LLM 调 transfer_to_agent 给不存在的 agent → tool.Failure 软失败（不 return-direct、
// 不崩、不转交）→ frontdesk 看 failure 回灌继续 ReAct 自己处理。
func TestHandoffAgent_ModelB_InvalidTargetSoftFail(t *testing.T) {
	// frontdesk：先调非法 transfer，再（看到失败回灌后）自己回复
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			transferToolCall("tc_1", "nonexistent"), // 非法目标
			schema.AssistantMessage("好的我自己处理"),      // 软失败回灌后第 2 轮
		})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("billing 不该被调")})),
		agent_v2.WithMaxRounds(5),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("帮忙"))
	if err != nil {
		t.Fatalf("Run error: %v（非法目标应软失败，不该 fail-fast 整个 Run）", err)
	}

	// 非法转交不生效 → frontdesk 自己处理（done），billing 不被调。
	if f := agent_v2.FinalMessageByAgent(events, "billing"); f != nil {
		t.Errorf("非法目标不该转交到 billing，但 billing 被调：%q", f.Content)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "好的我自己处理") {
		t.Errorf("非法软失败后 frontdesk 应回灌重试自己处理，final=%v", final)
	}
}

// TestHandoffAgent_ModelB_ReuseSessionNoStalePending：跨 Run **复用同一 session**
// （多轮对话支持模式）时，上一个 Run 的转交不得劫持本轮路由（phantom transfer）。
//
// 注意：PersistActiveAcrossRuns 用 fresh NewRunSession() 每 Run，空 values map 掩盖了
// 这个缺陷（假绿）；本用例**复用同一 session 对象**才测得到。
//
// 信号改成跟着提交走的上行信号之后这条是结构性成立的——信号根本不落 session。
// 但**留着它**：它盯的是「跨 Run 会不会串」这个行为，跟信号存哪儿无关；
// 载体哪天再变，它照样是那道闸。更狠的一版在
// TestHandoffTransfer_NoResidueAcrossRuns（那条把执行体在写完信号之后掐断，
// 任何写在错误路径上的清理都指望不上）。
func TestHandoffAgent_ModelB_ReuseSessionNoStalePending(t *testing.T) {
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			transferToolCall("tc_1", "billing"), // Run1 转交 billing
		})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("Run1-billing"),
			schema.AssistantMessage("Run2-billing"),
		})),
		agent_v2.WithMaxRounds(5),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reuse")
	sess := agent_v2.NewRunSession() // **复用同一 session 对象跨 Run**（关键：复现 HIGH）

	// Run1: frontdesk(hop0)→transfer billing→billing(hop1) done。
	// 信号若活得比这一次 sub-agent 调用长，它就会在 Run2 里再被读一次。
	if _, err := h.Run(ctx, sess, schema.UserMessage("我要账单")); err != nil {
		t.Fatalf("Run1 error: %v", err)
	}

	// Run2（复用同 sess，持久 active=billing）：billing 在 hop0 直接 done（不转交）。
	// 曾经的 bug：billing 在 hop0 读到 Run1 留下的那条 → 幽灵自转交 → 多一跳 → Hops=2。
	// 现在信号活不过那一次调用 → billing 直接 done → Hops=1。
	ev2, err := h.Run(ctx, sess, schema.UserMessage("再查一笔"))
	if err != nil {
		t.Fatalf("Run2 error: %v", err)
	}

	if st := h.State(sess); st.Hops != 1 {
		t.Errorf("跨 Run 残留 pending 劫持路由（audit HIGH 回归）：Run2 Hops=%d 期望 1"+
			"（billing 应直接 done，不被上轮残留信号 phantom 转交）", st.Hops)
	}
	if f := agent_v2.FinalMessageByAgent(ev2, "billing"); f == nil || !strings.Contains(f.Content, "Run2-billing") {
		t.Errorf("Run2 billing 输出异常（phantom transfer 痕迹）：%v", f)
	}
}

// captureModel 记录每次 Generate 收到的 message 内容——验证 handoff 历史可见性（§9）。
type captureModel struct {
	mu   sync.Mutex
	seen []string
	resp *schema.Message
}

func (m *captureModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	for _, msg := range msgs {
		if msg != nil {
			m.seen = append(m.seen, msg.Content)
		}
	}
	m.mu.Unlock()
	return m.resp, nil
}

func (m *captureModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *captureModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *captureModel) sawContains(s string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.seen {
		if strings.Contains(c, s) {
			return true
		}
	}
	return false
}

var _ model.ChatModel = (*captureModel)(nil)

// runHandoffHistoryVisibility 跑 frontdesk(带 mark 的 transfer)→billing(capture) 转交，
// 返回 billing 的 captureModel 供断言它是否看到前接 frontdesk 的 mark。
func runHandoffHistoryVisibility(t *testing.T, modeOpt agent_v2.HandoffOption) *captureModel {
	t.Helper()
	transferMsg := transferToolCall("tc_1", "billing")
	transferMsg.Content = "FRONTDESK_MARK_订单12345" // frontdesk 转交时带可识别 content
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferMsg})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billingCap := &captureModel{resp: schema.AssistantMessage("billing done")}
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(billingCap),
		agent_v2.WithMaxRounds(5),
	)
	opts := []agent_v2.HandoffOption{
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffInitial("frontdesk"),
	}
	if modeOpt != nil {
		opts = append(opts, modeOpt)
	}
	h := agent_v2.NewHandoffAgent(opts...).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("我要账单")); err != nil {
		t.Fatalf("Run error: %v", err)
	}
	return billingCap
}

// TestHandoffAgent_ModelB_HistoryDefault_SeesPredecessor 验证 §9 默认（Rewrite 改写第三
// 人称）：后接 billing 看到前接 frontdesk 内容（对齐业内"后接看前接"接力语义）。默认 Rewrite
// 而非 Raw——Rewrite 保留为接力写的转交叙述（"For context: [X] called tool..."），Raw 下悬空
// transfer 的 tool_call 被剥掉、只留一条中性记账文本（详 NewHandoffAgent godoc + §9）。
func TestHandoffAgent_ModelB_HistoryDefault_SeesPredecessor(t *testing.T) {
	cm := runHandoffHistoryVisibility(t, nil) // 默认 = Rewrite
	t.Logf("billing seen: %v", cm.seen)
	if !cm.sawContains("FRONTDESK_MARK") {
		t.Error("默认（Rewrite）模式 billing 应看到前接 frontdesk 内容（接力语义），实际未看到")
	}
}

// TestHandoffAgent_ModelB_HistorySelfOnly_Isolated 验证 §9 SelfOnly opt-in：后接 billing
// 看不到前接 frontdesk（隔离）。与上一测试互为 sanity——证明区别真在 mode 切换、非偶然。
func TestHandoffAgent_ModelB_HistorySelfOnly_Isolated(t *testing.T) {
	cm := runHandoffHistoryVisibility(t, agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly))
	t.Logf("billing seen: %v", cm.seen)
	if cm.sawContains("FRONTDESK_MARK") {
		t.Error("SelfOnly 模式 billing 不应看到前接 frontdesk（隔离），实际看到了")
	}
}

// TestHistoryFor_SkipsEmptyCrossAgentMessage 验证 audit 视角3 LOW 修复：HistoryFor 对
// "空 content 且无 tool_call 的 cross-agent assistant"不注入 junk user("For context:")
// 消息（rewriteCrossAgentMessage 返 nil → 调用方跳过）。sanity-revert：去掉 session.go
// 的 nil 守卫则空消息退化成裸 "For context:" junk 进输出，本测试 FAIL。
func TestHistoryFor_SkipsEmptyCrossAgentMessage(t *testing.T) {
	sess := agent_v2.NewRunSession()
	// 前接 frontdesk（非 owner billing）：一条空 content 无 tool_call 的 assistant（应被跳过）
	sess.AddEvent(agent_v2.NewOutputEvent("frontdesk", schema.AssistantMessage("")))
	// 前接 frontdesk：一条有内容的 assistant（应改写为 "For context: [frontdesk] said: ..."）
	sess.AddEvent(agent_v2.NewOutputEvent("frontdesk", schema.AssistantMessage("REAL_CONTENT")))
	// owner billing 自己的消息（应原样保留）
	sess.AddEvent(agent_v2.NewOutputEvent("billing", schema.AssistantMessage("BILLING_OWN")))

	got := sess.HistoryFor("billing")
	for _, m := range got {
		if m.Content == "For context:" {
			t.Fatalf("空 content cross-agent 消息产生了 junk user 消息 %q（应跳过）", m.Content)
		}
	}
	sawReal := false
	for _, m := range got {
		if strings.Contains(m.Content, "REAL_CONTENT") {
			sawReal = true
		}
	}
	if !sawReal {
		t.Error("有内容的 cross-agent 消息应被改写保留，实际丢失")
	}
}

func TestHandoffHistoryModeScopedToDirectMember(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		mode   agent_v2.HandoffHistoryMode
		nested bool
		store  bool
	}{
		{name: "direct_rewrite", mode: agent_v2.HandoffHistoryRewrite},
		{name: "nested_rewrite", mode: agent_v2.HandoffHistoryRewrite, nested: true},
		{name: "direct_raw_store", mode: agent_v2.HandoffHistoryRaw, store: true},
		{name: "nested_raw_store", mode: agent_v2.HandoffHistoryRaw, nested: true, store: true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			const marker = "前接私有历史"
			const request = "请处理当前请求"
			const reply = "处理完成"
			msg := transferToolCall("transfer", "target")
			msg.Content = marker
			hs := agent_v2.NewInMemoryHistoryStore()
			const owner, thread = "tenant", "history-mode-scope"
			frontOpts := []agent_v2.Option{agent_v2.WithName("front"), agent_v2.WithHandoffTransfer(),
				agent_v2.WithModel(newStubModel([]*schema.Message{msg}))}
			var prompts [][]*schema.Message
			targetOpts := []agent_v2.Option{agent_v2.WithName("receiver"),
				agent_v2.WithModel(&dupProbeModel{reply: reply, captured: &prompts})}
			if fixture.store {
				frontOpts = append(frontOpts, agent_v2.WithHistory(hs, owner))
				targetOpts = append(targetOpts, agent_v2.WithHistory(hs, owner))
			}
			front := agent_v2.NewBasicAgent(frontOpts...)
			var target agent_v2.Agent = agent_v2.NewBasicAgent(targetOpts...)
			if fixture.nested {
				target = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("handler")).AddStep("member", target)
			}
			workflow := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"),
				agent_v2.WithHandoffInitial("front"), agent_v2.WithHandoffHistoryMode(fixture.mode)).
				AddAgent("front", front).AddAgent("target", target)
			ctx := compose_v2.WithThreadIDCtx(t.Context(), thread)
			events, err := workflow.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(request))
			if err != nil || agent_v2.FinalMessageByAgent(events, "receiver").GetContent() != reply || len(prompts) != 1 {
				t.Fatalf("应由后接模型完成任务，err=%v 模型调用=%d", err, len(prompts))
			}

			var inputCount, markerCount int
			for _, item := range prompts[0] {
				if item.Role == schema.RoleUser && item.GetContent() == request {
					inputCount++
				}
				if strings.Contains(item.GetContent(), marker) {
					markerCount++
				}
			}
			if inputCount != 1 || (markerCount > 0) == fixture.nested {
				t.Fatalf("模型应恰好收到一次当前输入，只有直接接手才看前接标记：输入=%d 前接=%d", inputCount, markerCount)
			}

			if fixture.store {
				stored, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: owner, ConvID: thread})
				if err != nil {
					t.Fatal(err)
				}
				inputs := map[string]int{}
				for _, item := range stored {
					if item.Role == schema.RoleUser && item.GetContent() == request {
						slot, _ := item.Annotations[agent_v2.HistoryAgentAnnotation].(string)
						inputs[slot]++
					}
				}
				wantSlots := 1
				if fixture.nested {
					wantSlots = 2 // 前接共享视图与内层成员自有视图各保留一次输入。
				}
				if len(inputs) != wantSlots {
					t.Fatalf("输入应落入 %d 个历史归属，实际 %v", wantSlots, inputs)
				}
				for slot, count := range inputs {
					if count != 1 {
						t.Fatalf("归属 %q 重复写入 %d 份输入", slot, count)
					}
				}
			}
		})
	}
}

func TestHandoffHistoryProjectsSharedInputOnceAcrossThreeHops(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "events"
		if persistent {
			name = "store"
		}
		t.Run(name, func(t *testing.T) {
			const userText, firstMark, secondMark = "用户原话", "前接标记", "内层产出"
			transfer := transferToolCall("transfer", "middle")
			transfer.Content = firstMark
			front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithHandoffTransfer(),
				agent_v2.WithModel(newStubModel([]*schema.Message{transfer})))
			var innerPrompts, finalPrompts [][]*schema.Message
			leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"),
				agent_v2.WithModel(&dupProbeModel{reply: secondMark, captured: &innerPrompts}))
			last := agent_v2.NewBasicAgent(agent_v2.WithName("last"),
				agent_v2.WithModel(&dupProbeModel{reply: "完成", captured: &finalPrompts}))

			inputCh := compose_v2.LastValue[*schema.Message]("input")
			outCh := compose_v2.LastValue[*schema.Message]("output")
			var middle *agent_v2.GraphAgent
			builder := compose_v2.NewBuilder().WithName("middle")
			compose_v2.WithChannel(builder, inputCh)
			compose_v2.WithChannel(builder, outCh)
			g, err := builder.AddNode("delegate", func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				in, _ := compose_v2.Get(ch, inputCh)
				events, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in)
				if err != nil {
					return nil, err
				}
				if err := agent_v2.HandoffTo(ctx, agent_v2.RegrantFromNode(ctx, middle), middle, "last"); err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(events))}, nil
			}).AddEdge(compose_v2.StartNode, "delegate").AddEdge("delegate", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			middle = agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("middle"),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, m)}
				}),
				agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(ch, outCh)
					return out
				}))
			workflow := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("three-hops"),
				agent_v2.WithHandoffInitial("front"), agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistoryRaw)).
				AddAgent("front", front).AddAgent("middle", middle).AddAgent("last", last)
			ctx := t.Context()
			hs := agent_v2.NewInMemoryHistoryStore()
			if persistent {
				ctx = agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(ctx, "thread"), hs, "owner")
			}
			events, err := workflow.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(userText))
			if err != nil || agent_v2.FinalMessageByAgent(events, "last").GetContent() != "完成" || len(innerPrompts) != 1 || len(finalPrompts) != 1 {
				t.Fatalf("应执行内层模型并由末跳完成：err=%v 内层调用=%d 末跳调用=%d", err, len(innerPrompts), len(finalPrompts))
			}
			if strings.Contains(inputText(innerPrompts[0]), firstMark) {
				t.Fatal("内层模型读到了前接消息")
			}
			prompt := inputText(finalPrompts[0])
			if strings.Count(prompt, userText) != 1 || !strings.Contains(prompt, firstMark) || !strings.Contains(prompt, secondMark) {
				t.Fatalf("末跳模型应收到两跳产出和一份用户原话：%q", prompt)
			}
			if persistent {
				stored, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "owner", ConvID: "thread"})
				if err != nil {
					t.Fatal(err)
				}
				if count := strings.Count(inputText(stored), userText); count != 2 {
					t.Fatalf("审计记录应保留两份独立归属，实际 %d 份：%s", count, inputText(stored))
				}
			}
		})
	}
}

func TestHandoffHistoryKeepsCurrentInputOnFreshSessionResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer()), 5*time.Second)
	defer cancel()
	var prompts [][]*schema.Message
	receiver := agent_v2.NewBasicAgent(agent_v2.WithName("receiver"), agent_v2.WithHooks(branchStartQuestion{}),
		agent_v2.WithModel(&dupProbeModel{reply: "完成", captured: &prompts}))
	handoff := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("resume-input"), agent_v2.WithHandoffInitial("receiver")).
		AddAgent("receiver", receiver)
	session := agent_v2.NewRunSession()
	const question = "用户本轮问题"
	_, err := handoff.Run(ctx, session, schema.UserMessage(question))
	id := compactionQuestionID(t, err, "批准 B 开始")
	if len(prompts) != 0 {
		t.Fatal("启动钩子挂起时还不能调用模型")
	}
	events, err := handoff.Resume(ctx, session.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("继续")}))
	if err != nil || agent_v2.FinalMessageByAgent(events, "receiver").GetContent() != "完成" || len(prompts) != 1 {
		t.Fatalf("续跑应该由同一个候选完成：err=%v 模型调用=%d", err, len(prompts))
	}
	if got := strings.Count(inputText(prompts[0]), question); got != 1 {
		t.Fatalf("使用新 session 续跑时应恢复一次当前输入，实际 %d 次", got)
	}
}
