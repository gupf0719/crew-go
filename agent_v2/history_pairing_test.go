package agent_v2_test

// prior 换源到历史层之后，喂给模型的消息流必须仍然是 **API 合法**的。
//
// # 为什么这组测试是换源的必要配套
//
// events 源的 prior 里根本没有 `role=tool`（events 只记首尾），所以「按条数截断」「把带
// marker 的消息挪到前面」这些处理**怎么切都断不了配对**——`dropDanglingToolCalls` 的原注释
// 就写着「paired 恒空、本分支不可达」。
//
// 换源到历史层之后这个前提没了：历史是逐字完整流，tool_call 和 tool_result 都在里面。于是
// 同样的截断和前置动作会把一对拆开，留下**孤儿 tool_result**——真实 API 直接 400。而且这不是
// 「少看几条历史」那种可降级的问题：400 不是 prompt-too-long，第 3 层 ReactiveCompact 的 retry
// 根本不进入，历史又只增不减、下一轮切点更靠后 → 会话永久卡死。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// assertPairingValid 按 OpenAI/Claude 的硬约束校验消息流：每条 role=tool 之前必须有声明
// 对应 tool_call 的 assistant。返回违规描述（空串表示合法）。
// **两个方向都要验**：上一版只查孤儿 tool_result，放过了反方向的悬空 tool_call——
// 而那正是「两遍判定宽严错配」的症状：tool_result 被当孤儿丢了，它的 assistant 却因为
// 另一套宽判据认为「找得到配对」而整条留下，于是悬空 tool_call 上 wire → 400。
func assertPairingValid(msgs []*schema.Message) string {
	declared := map[string]bool{}  // 已声明待应答
	answeredAt := map[string]int{} // id → 应答它的下标
	for i, m := range msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool {
			id := schema.GetToolResultID(m)
			if !declared[id] {
				return "第 " + itoa(i) + " 条是孤儿 tool_result(id=" + id +
					")——它前面没有声明这个 tool_call 的 assistant"
			}
			answeredAt[id] = i
			delete(declared, id)
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			declared[tc.ID] = true
		}
	}
	// declared 里剩下的 = 声明了却没人应答 = 悬空 tool_call
	for id := range declared {
		return "有悬空 tool_call(id=" + id + ")——assistant 声明了它，" +
			"但流里没有对应的 tool_result 跟在后面。真实 API 对此直接 400"
	}
	_ = answeredAt
	return assertContiguous(msgs)
}

// assertContiguous 验**紧跟**：assistant(tool_calls) 之后必须立刻是回应它全部 id 的 tool
// 消息，中间不能夹任何东西。
//
// **这条比「声明在前」严一档，而它才是真实约束**：实测严格 provider 对「结果都在但中间
// 夹了一条 user/assistant」报的是跟孤儿同一条错——
// "An assistant message with 'tool_calls' must be followed by tool messages responding to
// each 'tool_call_id'"。宽松的 provider 会放行，所以只拿宽松的那家测，这类 bug 测不出来。
//
// 上一版断言只查「声明在前」，于是分离态永远绿——marker 提升把 assistant 搬到队首、把它
// 的兄弟结果留在原地，就是这个形状。
func assertContiguous(msgs []*schema.Message) string {
	for i, m := range msgs {
		if m == nil {
			continue
		}
		tcs := schema.GetToolCalls(m)
		if len(tcs) == 0 {
			continue
		}
		for j, tc := range tcs {
			k := i + 1 + j
			if k >= len(msgs) {
				return "第 " + itoa(i) + " 条 assistant 声明了 " + itoa(len(tcs)) +
					" 个 tool_call，但后面没有足够的 tool 消息紧跟"
			}
			nxt := msgs[k]
			if nxt == nil || nxt.Role != schema.RoleTool {
				return "第 " + itoa(i) + " 条 assistant 的 tool_call 块被打断——第 " +
					itoa(k) + " 条本该是 tool_result，实际是 " + string(nxt.Role) +
					"。真实 API 要求紧跟，中间夹任何消息都 400"
			}
			if got := schema.GetToolResultID(nxt); got != tc.ID {
				return "第 " + itoa(k) + " 条 tool_result 的 id=" + got +
					"，但按声明顺序这里应该是 " + tc.ID
			}
		}
	}
	return ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestHistory_Pairing_SurvivesTruncation：第 0 层按条数截断不能留下孤儿 tool_result。
//
// 默认配置就会踩：`MaxHistoryPairs` 默认 20 且**默认开启**，业务什么都不配、只要开了历史 +
// 用工具、多聊几轮，切点就会落进某一对 tool_call/tool_result 中间。
func TestHistory_Pairing_SurvivesTruncation(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	m := &pairingProbeModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(newStockToolkit()),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxHistoryPairs(3), // 收紧窗口，让切点必然落在配对中间
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	for i := 0; i < 5; i++ {
		if _, err := ag.Run(ctx, sess, schema.UserMessage("问题")); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
	}

	if len(m.seen) == 0 {
		t.Fatal("模型一次都没被调用——循环体不执行的话本测试是空转全绿（兄弟用例都有这道守卫）")
	}
	for i, seen := range m.seen {
		if bad := assertPairingValid(seen); bad != "" {
			t.Errorf("第 %d 次 LLM 调用的消息流违规：%s\n"+
				"  换源后 prior 含 tool_result，按条数截断会切断配对；孤儿会让真实 API 返 400，\n"+
				"  而 400 不是 prompt-too-long → ReactiveCompact 兜不住 → 会话永久卡死\n%s",
				i, bad, dumpHist(seen))
		}
	}
}

// TestHistory_Pairing_SurvivesMarkerHoisting：marker 前置不能把 tool_result 搬到它的
// assistant 前面。
//
// `SkillBodyAnnotation` 恰恰打在 `activate_skill` 的 **tool_result** 上，而
// `truncatePriorPreservingMarkers` 会把带 marker 的消息抽出来拼到最前——一抽就搬到了声明它的
// assistant 前面。这时孤儿和悬空同时存在，且**不需要真的删掉任何消息**就会发生。
func TestHistory_Pairing_SurvivesMarkerHoisting(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// 直接构造带 marker 的 tool_result 塞进历史，模拟 skill 激活留下的痕迹
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "skill-1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"x"}`},
	}})
	tc.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
	body := schema.ToolResultMessage("skill-1", "skill body 正文")
	body.Annotations = map[string]any{
		agent_v2.HistoryAgentAnnotation: "clerk",
		agent_v2.SkillBodyAnnotation:    true,
	}
	old := []*schema.Message{}
	for i := 0; i < 4; i++ {
		u := schema.UserMessage("旧问题")
		u.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		a := schema.AssistantMessage("旧答复")
		a.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		old = append(old, u, a)
	}
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}
	if err := hs.Append(context.Background(), key, append([]*schema.Message{tc, body}, old...)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	m := &pairingProbeModel{noTools: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(m),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxHistoryPairs(3),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.seen) == 0 {
		t.Fatal("没调到模型")
	}
	if bad := assertPairingValid(m.seen[0]); bad != "" {
		t.Errorf("marker 前置把配对打散了：%s\n"+
			"  SkillBodyAnnotation 打在 tool_result 上，被抽到队首就跑到了它的 assistant 前面\n%s",
			bad, dumpHist(m.seen[0]))
	}
}

// TestHistory_LoopInput_NotRecordedTwice：编排把上一轮产出原样当下一轮 input 时不重复记。
//
// LoopAgent 的 `actualInput = lastOut` 是一条 **assistant** 消息。preamble 若不挑角色就会把
// 它当"这一轮用户说的话"记进历史，于是同一条产出在历史里出现两遍——下一轮 prior 读回来，
// 模型看到自己说过的话被复述。
func TestHistory_LoopInput_NotRecordedTwice(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("body"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("第1轮结论"),
			schema.AssistantMessage("第2轮结论"),
		})),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	// 第 1 轮：真实用户输入
	if _, err := ag.Run(ctx, sess, schema.UserMessage("开始干活")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	// 第 2 轮：模拟 LoopAgent 把上轮产出原样转手
	if _, err := ag.Run(ctx, sess, schema.AssistantMessage("第1轮结论")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	got, _ := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	n := 0
	for _, msg := range got {
		if strings.Contains(msg.Content, "第1轮结论") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("「第1轮结论」在历史里出现 %d 次, want 1 —— "+
			"编排转手的 assistant 消息不该被当成新的用户输入再记一遍\n%s", n, dumpHist(got))
	}
}

// pairingProbeModel 记下每次看到的消息流，第一轮调工具制造配对。
type pairingProbeModel struct {
	calls   int
	seen    [][]*schema.Message
	noTools bool // true = 只产纯文本，用于只验 prior 形状的用例
}

func (m *pairingProbeModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.seen = append(m.seen, msgs)
	m.calls++
	if !m.noTools && m.calls%2 == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc-" + itoa(m.calls), Type: "function",
			Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`},
		}}), nil
	}
	return schema.AssistantMessage("答复-" + itoa(m.calls)), nil
}

func (m *pairingProbeModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *pairingProbeModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHistory_Pairing_MarkerMessageSurvives：带 marker 的 tool_result 不能被「保护」掉。
//
// `SkillBodyAnnotation` 打在 activate_skill 的 tool_result 上。截断层为了不让 skill body
// 被截掉，会把带 marker 的消息抽出来拼到最前——可那是一条 role=tool，挪到声明它的
// assistant 前面就成了孤儿，出口清洗再把它整条丢掉。**保护变成了删除，而且完全静默。**
//
// 这条同时验两件事：body 还在（保护没反转）、配对合法（没制造孤儿或悬空）。
func TestHistory_Pairing_MarkerMessageSurvives(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "skill-1", Type: "function",
		Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"x"}`},
	}})
	tc.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
	body := schema.ToolResultMessage("skill-1", "SKILL_BODY_MARK")
	body.Annotations = map[string]any{
		agent_v2.HistoryAgentAnnotation: "clerk",
		agent_v2.SkillBodyAnnotation:    true,
	}
	seed := []*schema.Message{tc, body}
	for i := 0; i < 4; i++ {
		u := schema.UserMessage("旧问题")
		u.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		a := schema.AssistantMessage("旧答复")
		a.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		seed = append(seed, u, a)
	}
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}
	if err := hs.Append(context.Background(), key, seed); err != nil {
		t.Fatalf("Append: %v", err)
	}

	m := &pairingProbeModel{noTools: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(m),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxHistoryPairs(3),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.seen) == 0 {
		t.Fatal("没调到模型")
	}
	seen := m.seen[0]

	if bad := assertPairingValid(seen); bad != "" {
		t.Errorf("配对被破坏：%s\n%s", bad, dumpHist(seen))
	}
	found := false
	for _, msg := range seen {
		if strings.Contains(msg.Content, "SKILL_BODY_MARK") {
			found = true
		}
	}
	if !found {
		t.Errorf("skill body 没了 —— marker 的作用是「不被截掉」，"+
			"若它被抽到队首成孤儿再被清洗丢掉，这条保护就反转成了删除\n%s", dumpHist(seen))
	}
}

// TestHistory_Pairing_MarkerSiblingNotSeparated：marker 提升不能把兄弟结果甩掉或隔开。
//
// 同一轮里 LLM 既调 activate_skill 又调业务工具（ISSUE-81 起 fanout 支持 N>=2），只有前者
// 的结果带 marker。截断层把「marker 结果 + 它的 assistant」提到队首时，**那条 assistant
// 的另一个结果不带 marker、留在原地**——于是要么被截断切走（悬空），要么活着但被隔开好几条。
// 后者是更隐蔽的一种：配对齐全、声明也在前，但真实 API 照样 400，因为它要的是紧跟。
func TestHistory_Pairing_MarkerSiblingNotSeparated(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	tc := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "skill-1", Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"x"}`}},
		{ID: "biz-2", Type: "function",
			Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`}},
	})
	tc.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
	body := schema.ToolResultMessage("skill-1", "SKILL_BODY_MARK")
	body.Annotations = map[string]any{
		agent_v2.HistoryAgentAnnotation: "clerk",
		agent_v2.SkillBodyAnnotation:    true,
	}
	biz := schema.ToolResultMessage("biz-2", "库存 3 件")
	biz.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}

	seed := []*schema.Message{}
	for i := 0; i < 3; i++ {
		u := schema.UserMessage("更旧问题")
		u.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		a := schema.AssistantMessage("更旧答复")
		a.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		seed = append(seed, u, a)
	}
	seed = append(seed, tc, body, biz)
	for i := 0; i < 2; i++ {
		u := schema.UserMessage("较新问题")
		u.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		a := schema.AssistantMessage("较新答复")
		a.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
		seed = append(seed, u, a)
	}
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}
	if err := hs.Append(context.Background(), key, seed); err != nil {
		t.Fatalf("Append: %v", err)
	}

	m := &pairingProbeModel{noTools: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(m),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxHistoryPairs(4),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.seen) == 0 {
		t.Fatal("没调到模型")
	}
	if bad := assertPairingValid(m.seen[0]); bad != "" {
		t.Errorf("marker 提升破坏了兄弟配对：%s\n%s", bad, dumpHist(m.seen[0]))
	}
}
