package agent_v2_test

// 会话历史层的端到端行为——真跑 BasicAgent，验落进历史的到底是什么。
//
// # 为什么必须端到端
//
// 这一层的价值主张只有一句：**模型下一轮能看到上一轮的全过程**。而「全过程」是由
// 三个节点各自写（preamble 记起始输入 / model 记每轮产出 / tools 记每批结果），任何一处没接上，
// 单独看每个写入点的单测都是绿的，合起来就缺一块。
//
// 尤其是工具那一环：不开历史时，模型下一轮只看得到「A 商品还有 3 件」这个结论，看不
// 到它是查出来的还是编的，也就无法判断该不该重查。这正是要修的东西，必须验到。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// histToolModel：第一轮调工具，第二轮给结论。
type histToolModel struct {
	calls int
	seen  [][]*schema.Message // 每次调用看到的输入，供跨轮断言
}

func (m *histToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.seen = append(m.seen, msgs)
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc-1", Type: "function",
			Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`},
		}}), nil
	}
	return schema.AssistantMessage("A 商品还有 3 件"), nil
}

func (m *histToolModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *histToolModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func newStockToolkit() *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("查库存", "查询库存",
		map[string]any{"type": "object", "properties": map[string]any{"sku": map[string]any{"type": "string"}}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("库存只剩 3 件"), nil
		}))
	return tk
}

// TestHistory_E2E_CapturesToolRound：一轮带工具的对话，历史里四条都在。
//
// 不开历史时，这轮结束后留下的（session events）只有首尾两条——工具调用和工具返回
// 全丢。那正是「结论在、来源没了」。
func TestHistory_E2E_CapturesToolRound(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(&histToolModel{}),
		agent_v2.WithToolkit(newStockToolkit()),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A 还有货吗")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := []struct {
		role schema.MessageRole
		mark string
		why  string
	}{
		{schema.RoleUser, "A 还有货吗", "用户这轮说了什么"},
		{schema.RoleAssistant, "", "模型决定调工具（tool_call，Content 为空）"},
		{schema.RoleTool, "库存只剩 3 件", "**工具返回了什么** —— 不开历史时这条就丢了"},
		{schema.RoleAssistant, "A 商品还有 3 件", "最终答复"},
	}
	if len(got) != len(want) {
		t.Fatalf("历史 %d 条, want %d —— 三个节点少接一处就是这个症状\n%s",
			len(got), len(want), dumpHist(got))
	}
	for i, w := range want {
		if got[i].Role != w.role {
			t.Errorf("[%d] role = %s, want %s（%s）\n%s", i, got[i].Role, w.role, w.why, dumpHist(got))
			continue
		}
		if w.mark != "" && !strings.Contains(got[i].Content, w.mark) {
			t.Errorf("[%d] content = %q, want 含 %q（%s）", i, got[i].Content, w.mark, w.why)
		}
	}

	// tool_call ↔ tool_result 配对必须完整过一遍历史层，否则回灌给模型时 API 直接 422
	tcs := schema.GetToolCalls(got[1])
	if len(tcs) != 1 || tcs[0].ID != "tc-1" {
		t.Errorf("assistant 的 tool_call 没保住: %+v", tcs)
	}
	if id := schema.GetToolResultID(got[2]); id != "tc-1" {
		t.Errorf("tool 结果的 tool_call_id = %q, want tc-1 —— 配不上就是孤儿，回灌时 422", id)
	}

	// 归属：每条都盖了 agent 名。出了 channel 就没有「谁的 channel」这个上下文了。
	for i, m := range got {
		if m.Annotations[agent_v2.HistoryAgentAnnotation] != "clerk" {
			t.Errorf("[%d] 归属 = %v, want %q", i, m.Annotations[agent_v2.HistoryAgentAnnotation], "clerk")
		}
	}
}

// TestHistory_E2E_AcrossRuns：两个 Run（同一 conv），历史首尾相接且不重复。
//
// 第二轮的 preamble 会把上一轮的历史拼进模型输入，**那部分绝不能再写一遍**——否则
// 每轮把全部历史抄一份，轮次一多直接爆炸。
func TestHistory_E2E_AcrossRuns(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("答复一"),
			schema.AssistantMessage("答复二"),
		})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("问题一")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := ag.Run(ctx, sess, schema.UserMessage("问题二")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	want := []string{"问题一", "答复一", "问题二", "答复二"}
	if len(got) != len(want) {
		t.Fatalf("两轮后历史 %d 条, want %d —— 多出来的多半是 preamble 把回灌的 prior "+
			"也写了一遍（每轮翻倍）\n%s", len(got), len(want), dumpHist(got))
	}
	for i, w := range want {
		if !strings.Contains(got[i].Content, w) {
			t.Errorf("[%d] = %q, want 含 %q\n%s", i, got[i].Content, w, dumpHist(got))
		}
	}
}

// TestHistory_E2E_SeparateConversations：不同 ThreadID 各写各的。
func TestHistory_E2E_SeparateConversations(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("答复"),
			schema.AssistantMessage("答复"),
		})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	for _, conv := range []string{"conv-1", "conv-2"} {
		ctx := compose_v2.WithThreadIDCtx(context.Background(), conv)
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问 "+conv)); err != nil {
			t.Fatalf("Run(%s): %v", conv, err)
		}
	}

	for _, conv := range []string{"conv-1", "conv-2"} {
		got, _ := hs.Load(context.Background(),
			agent_v2.HistoryKey{Owner: "tenant-a", ConvID: conv})
		if len(got) != 2 {
			t.Fatalf("%s 历史 %d 条, want 2\n%s", conv, len(got), dumpHist(got))
		}
		if !strings.Contains(got[0].Content, conv) {
			t.Errorf("%s 的首条 = %q —— 两场会话串了", conv, got[0].Content)
		}
	}
}

// TestHistory_E2E_NoThreadID_NoWrite：没声明会话身份就不写，且不影响主链路。
//
// 写下去也无从取回。但更重要的是**不能因此让 Run 失败**——历史是旁路。
func TestHistory_E2E_NoThreadID_NoWrite(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("答复")})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	// 不 push ThreadID
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("问题"))
	if err != nil {
		t.Fatalf("没会话身份不该让 Run 失败——历史是旁路: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "答复") {
		t.Errorf("主链路产出不对: %v", final)
	}
	if got, _ := hs.Load(context.Background(),
		agent_v2.HistoryKey{Owner: "tenant-a", ConvID: ""}); len(got) != 0 {
		t.Errorf("没会话身份却写了 %d 条", len(got))
	}
}

// TestHistory_E2E_OptOut_ZeroCost：不配 WithHistory 时一行不写。
func TestHistory_E2E_OptOut_ZeroCost(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("答复")})),
		// 故意不配 WithHistory
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}); len(got) != 0 {
		t.Errorf("没开历史却写了 %d 条", len(got))
	}
}

// TestHistory_E2E_RoutingClassifierExcluded：分类器的内部决策不进会话历史。
//
// 分类器产出的是「走哪个分支」这个控制信号。让它落进历史，业务回看会读到一段自己
// 从没说过、也不该看见的内容；喂回模型更是纯噪声。
func TestHistory_E2E_RoutingClassifierExcluded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()

	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "r1", Type: "function",
				Function: schema.FunctionCall{
					Name: agent_v2.RouteTool, Arguments: `{"target":"sales"}`,
				},
			}}),
		})),
		agent_v2.WithRouteTool(),
		agent_v2.WithHistory(hs, "tenant-a"), // 就算业务给分类器也配了，也不该写
	)
	sales := agent_v2.NewBasicAgent(
		agent_v2.WithName("sales"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("销售答复")})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(classifier),
	).AddBranch("sales", sales)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我要买东西")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	for i, m := range got {
		if m.Annotations[agent_v2.HistoryAgentAnnotation] == "classifier" {
			t.Errorf("[%d] 分类器的内部决策漏进了会话历史: %s: %q\n%s",
				i, m.Role, m.Content, dumpHist(got))
		}
	}
	if len(got) == 0 {
		t.Fatal("被选中分支的对话应当进历史，实际一条都没有")
	}
}

func dumpHist(msgs []*schema.Message) string {
	var b strings.Builder
	b.WriteString("实际历史：\n")
	for i, m := range msgs {
		if m == nil {
			b.WriteString("  <nil>\n")
			continue
		}
		owner, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		b.WriteString("  [")
		b.WriteString(itoa(i)) // 不能用 string(rune('0'+i))——历史过 10 条就打出 ':' ';'
		b.WriteString("] ")
		b.WriteString(owner)
		b.WriteString(" | ")
		b.WriteString(string(m.Role))
		b.WriteString(": ")
		b.WriteString(m.Content)
		if tcs := schema.GetToolCalls(m); len(tcs) > 0 {
			b.WriteString(" <tool_call:")
			b.WriteString(tcs[0].Function.Name)
			b.WriteString(">")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// TestHistory_E2E_PriorSeesToolRound：**这是整件事的靶心**。
//
// 第二轮对话时，模型看到的「上一轮」里必须有工具往返。不开历史时它看到的是：
//
//	user      "A 还有货吗"
//	assistant "A 商品还有 3 件"     ← 结论在，来源没了
//	user      "那给我留一件"
//
// 上一轮调过 `查库存`、工具返回「库存只剩 3 件」——这两条模型完全看不到。后果不只是
// 信息少：它无法判断「3 件」是刚查的还是编的，也就无法判断该不该重查。
func TestHistory_E2E_PriorSeesToolRound(t *testing.T) {
	for _, tc := range []struct {
		name       string
		withHist   bool
		wantToolIn bool
		why        string
	}{
		{"开了历史", true, true, "逐字完整流，工具往返都在"},
		{"没开历史（有意的降级）", false, false, "events 只记首尾，工具往返丢失"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &histToolModel{}
			opts := []agent_v2.Option{
				agent_v2.WithName("clerk"),
				agent_v2.WithModel(m),
				agent_v2.WithToolkit(newStockToolkit()),
			}
			if tc.withHist {
				opts = append(opts, agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-a"))
			}
			ag := agent_v2.NewBasicAgent(opts...)

			ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
			sess := agent_v2.NewRunSession()
			if _, err := ag.Run(ctx, sess, schema.UserMessage("A 还有货吗")); err != nil {
				t.Fatalf("Run 1: %v", err)
			}
			before := len(m.seen)
			if _, err := ag.Run(ctx, sess, schema.UserMessage("那给我留一件")); err != nil {
				t.Fatalf("Run 2: %v", err)
			}
			if len(m.seen) <= before {
				t.Fatal("第二轮没调到模型")
			}

			// 第二轮第一次 LLM 调用看到的消息流
			round2 := m.seen[before]
			sawToolResult, sawToolCall := false, false
			for _, msg := range round2 {
				if msg.Role == schema.RoleTool && strings.Contains(msg.Content, "库存只剩 3 件") {
					sawToolResult = true
				}
				if len(schema.GetToolCalls(msg)) > 0 {
					sawToolCall = true
				}
			}

			if sawToolResult != tc.wantToolIn || sawToolCall != tc.wantToolIn {
				t.Errorf("第二轮模型看到 tool_call=%v tool_result=%v, want 都是 %v（%s）\n%s",
					sawToolCall, sawToolResult, tc.wantToolIn, tc.why, dumpHist(round2))
			}

			// 两档共有：上一轮的最终答复必须在，且当前 input 只出现一次
			foundAnswer, inputCount := false, 0
			for _, msg := range round2 {
				if strings.Contains(msg.Content, "A 商品还有 3 件") {
					foundAnswer = true
				}
				if msg.Content == "那给我留一件" {
					inputCount++
				}
			}
			if !foundAnswer {
				t.Errorf("第二轮看不到上一轮的答复\n%s", dumpHist(round2))
			}
			if inputCount != 1 {
				t.Errorf("本轮 input 出现 %d 次, want 1 —— 换源后 prior 里混进了当前输入\n%s",
					inputCount, dumpHist(round2))
			}
		})
	}
}

// TestHistory_E2E_PriorNoDuplication：换源不能让历史越滚越多。
//
// prior 从历史层读、读回来的内容又不能再被写进历史——否则每轮把全部历史抄一遍。
// 三轮下来消息数应当是线性的。
func TestHistory_E2E_PriorNoDuplication(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("答一"),
			schema.AssistantMessage("答二"),
			schema.AssistantMessage("答三"),
		})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	for i, q := range []string{"问一", "问二", "问三"} {
		if _, err := ag.Run(ctx, sess, schema.UserMessage(q)); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
		got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
		if want := (i + 1) * 2; len(got) != want {
			t.Fatalf("第 %d 轮后历史 %d 条, want %d —— 每轮该只增「问 + 答」两条；"+
				"涨得更快说明读回来的 prior 又被写了一遍\n%s", i+1, len(got), want, dumpHist(got))
		}
	}
}

// flakyLoadStore：写正常，读按开关报错——模拟后端抖动（Redis 超时 / 连接断）。
type flakyLoadStore struct {
	inner    *agent_v2.InMemoryHistoryStore
	failLoad bool
}

func (s *flakyLoadStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	return s.inner.Append(ctx, key, msgs)
}

func (s *flakyLoadStore) Load(ctx context.Context, key agent_v2.HistoryKey) ([]*schema.Message, error) {
	if s.failLoad {
		return nil, errors.New("后端读超时")
	}
	return s.inner.Load(ctx, key)
}

// TestHistory_E2E_LoadFailure_FallsBackToEvents：后端读失败时必须真的回落到 events。
//
// # 为什么这条最容易写错
//
// 「读成功但没内容」和「读失败」都返回空列表——不带信号地混在一起，调用方就只能一律
// 当「历史本来就是空的」处理。于是后端一次超时，模型看到的不是「残缺的历史」而是
// **完全没有历史**：多轮对话中途突然失忆，而日志里那条 warn 还在说「本轮回落到
// session events」——承诺了一件没发生的事。
//
// 回落拿到的 events 确实残缺（只有首尾，没有工具往返），但那正是不开历史时的常态，
// 远好过一片空白。
func TestHistory_E2E_LoadFailure_FallsBackToEvents(t *testing.T) {
	store := &flakyLoadStore{inner: agent_v2.NewInMemoryHistoryStore()}
	m := &histCaptureModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(m),
		agent_v2.WithHistory(store, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()

	// 第一轮正常：历史层写入成功，events 也留了首尾
	if _, err := ag.Run(ctx, sess, schema.UserMessage("第一个问题")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// 第二轮：后端读挂了
	store.failLoad = true
	before := len(m.seen)
	if _, err := ag.Run(ctx, sess, schema.UserMessage("第二个问题")); err != nil {
		t.Fatalf("读失败不该让 Run 挂——历史是旁路: %v", err)
	}
	if len(m.seen) <= before {
		t.Fatal("第二轮没调到模型")
	}

	round2 := m.seen[before]
	sawPrevQuestion, sawPrevAnswer := false, false
	for _, msg := range round2 {
		if strings.Contains(msg.Content, "第一个问题") {
			sawPrevQuestion = true
		}
		if strings.Contains(msg.Content, "回答-1") {
			sawPrevAnswer = true
		}
	}
	if !sawPrevQuestion || !sawPrevAnswer {
		t.Errorf("后端读失败后模型看不到上一轮（问题=%v 答复=%v）—— "+
			"应回落到 session events 拿残缺历史，而不是给一片空白让模型失忆\n%s",
			sawPrevQuestion, sawPrevAnswer, dumpHist(round2))
	}
}

// histCaptureModel 每轮记下看到的输入，产出带轮次编号。
type histCaptureModel struct {
	calls int
	seen  [][]*schema.Message
}

func (m *histCaptureModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.seen = append(m.seen, msgs)
	m.calls++
	return schema.AssistantMessage(fmt.Sprintf("回答-%d", m.calls)), nil
}

func (m *histCaptureModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *histCaptureModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
