package history_conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

var convA = agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}

// testAppendLoadRoundTrip：写进去的能原样读出来，多批之间首尾相接。
func testAppendLoadRoundTrip(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	mustAppend(t, hs, convA,
		schema.UserMessage("第一个问题"),
		schema.AssistantMessage("第一个回答"))
	mustAppend(t, hs, convA, schema.UserMessage("第二个问题"))

	got := mustLoad(t, hs, convA)
	want := []struct {
		role    schema.MessageRole
		content string
	}{
		{schema.RoleUser, "第一个问题"},
		{schema.RoleAssistant, "第一个回答"},
		{schema.RoleUser, "第二个问题"},
	}
	if len(got) != len(want) {
		t.Fatalf("Load 返 %d 条, want %d —— 多批 Append 应首尾相接成一条流\n%s",
			len(got), len(want), dump(got))
	}
	for i, w := range want {
		if got[i].Role != w.role || got[i].Content != w.content {
			t.Errorf("[%d] = (%s, %q), want (%s, %q)",
				i, got[i].Role, got[i].Content, w.role, w.content)
		}
	}
}

// testAppendOnly：逐字重复的内容必须都留着，不能去重 / 覆盖 / 截断。
//
// 模型真的会连说两句一模一样的话（重试、循环、复述确认），去重会把这些抹平。更要命
// 的是覆盖式实现——按「会话」当 key 直接 Set，后一批把前一批冲掉，Load 只剩最后几条，
// 而这一切不报任何错。
func testAppendOnly(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	same := "一模一样的一句话"
	mustAppend(t, hs, convA, schema.UserMessage(same))
	mustAppend(t, hs, convA, schema.UserMessage(same))
	mustAppend(t, hs, convA, schema.UserMessage(same))

	got := mustLoad(t, hs, convA)
	if len(got) != 3 {
		t.Fatalf("三批逐字相同的内容后 Load 返 %d 条, want 3 —— "+
			"少了说明做了去重或覆盖，历史就不是逐字回放了\n%s", len(got), dump(got))
	}

	// 追加不同内容，前面的仍在原位
	mustAppend(t, hs, convA, schema.AssistantMessage("新的一条"))
	got = mustLoad(t, hs, convA)
	if len(got) != 4 {
		t.Fatalf("追加后 Load 返 %d 条, want 4\n%s", len(got), dump(got))
	}
	for i := 0; i < 3; i++ {
		if got[i].Content != same {
			t.Errorf("[%d] = %q, want %q —— 追加不该动到已有条目", i, got[i].Content, same)
		}
	}
}

// testOrder：写入顺序必须原样保住，跨批也一样。
//
// 按 map 存、按 hash 分片、并发写不加锁都可能悄悄打乱顺序。乱序的历史喂给模型，
// 它读到的是一段因果颠倒的对话。
func testOrder(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	const n = 30
	for i := 0; i < n; i++ {
		// 一条一批，最大化暴露批间顺序问题
		mustAppend(t, hs, convA, schema.UserMessage(fmt.Sprintf("seq-%02d", i)))
	}

	got := mustLoad(t, hs, convA)
	if len(got) != n {
		t.Fatalf("Load 返 %d 条, want %d\n%s", len(got), n, dump(got))
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("seq-%02d", i)
		if got[i].Content != want {
			t.Fatalf("[%d] = %q, want %q —— 顺序错了；历史是时序回放，乱序即失真\n%s",
				i, got[i].Content, want, dump(got))
		}
	}
}

// testKeyIsolation：Owner 和 ConvID **两个维度都要区分**。
//
// 只按 ConvID 分而忽略 Owner，是把别人的对话拉给当前用户看——ConvID 通常来自 wire，
// 猜中就能拿到。只按 Owner 分，是把一个用户的所有会话搅成一锅。
func testKeyIsolation(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	sameConvOtherOwner := agent_v2.HistoryKey{Owner: "tenant-b", ConvID: convA.ConvID}
	sameOwnerOtherConv := agent_v2.HistoryKey{Owner: convA.Owner, ConvID: "conv-2"}

	mustAppend(t, hs, convA, schema.UserMessage("A 的内容"))
	mustAppend(t, hs, sameConvOtherOwner, schema.UserMessage("B 的内容"))
	mustAppend(t, hs, sameOwnerOtherConv, schema.UserMessage("另一场对话"))

	for _, tc := range []struct {
		key  agent_v2.HistoryKey
		want string
		why  string
	}{
		{convA, "A 的内容", "同 ConvID 不同 Owner 混在一起 = 数据泄露"},
		{sameConvOtherOwner, "B 的内容", "同 ConvID 不同 Owner 混在一起 = 数据泄露"},
		{sameOwnerOtherConv, "另一场对话", "同 Owner 不同 ConvID 混在一起 = 会话串台"},
	} {
		got := mustLoad(t, hs, tc.key)
		if len(got) != 1 {
			t.Errorf("Load(%s) 返 %d 条, want 1 —— %s\n%s", tc.key, len(got), tc.why, dump(got))
			continue
		}
		if got[0].Content != tc.want {
			t.Errorf("Load(%s)[0] = %q, want %q —— %s", tc.key, got[0].Content, tc.want, tc.why)
		}
	}

	// 分隔符歧义——拼串式主键的经典塌陷。
	//
	// 把两个字段拼成一个字符串是最自然的实现（Redis 的 `hist:{owner}:{conv}`、PG 的
	// 一列文本主键），而只要分隔符能出现在字段值里，两个**不同**的会话就会拼成同一个
	// key。上面那三组都是干净的短串，拼错了也撞不出来；这组专门撞。
	//
	// 值里为什么会有分隔符：Owner 常是带层级的租户标识（`acme/eu`、`org:123`），
	// ConvID 常直接来自 wire，业务控制不住它长什么样。
	//
	// 过不了就两条路：主键用复合列 / 复合 key（不拼），或拼之前对两段各自转义。
	for _, sep := range []string{"/", ":"} {
		left := agent_v2.HistoryKey{Owner: "acme" + sep + "eu", ConvID: "c-1"}
		right := agent_v2.HistoryKey{Owner: "acme", ConvID: "eu" + sep + "c-1"}

		hs := factory()
		mustAppend(t, hs, left, schema.UserMessage("左边租户的对话"))
		mustAppend(t, hs, right, schema.UserMessage("右边租户的对话"))

		for _, tc := range []struct {
			key  agent_v2.HistoryKey
			want string
		}{{left, "左边租户的对话"}, {right, "右边租户的对话"}} {
			got := mustLoad(t, hs, tc.key)
			if len(got) != 1 || got[0].Content != tc.want {
				t.Errorf("分隔符 %q：Load(owner=%q conv=%q) 拿到 %s, want 恰好 [%q]——"+
					"两个不同租户的会话拼成了同一个存储 key，一方读到另一方的对话",
					sep, tc.key.Owner, tc.key.ConvID, dump(got), tc.want)
			}
		}
	}
}

// testMessageFields：Message 上的结构化字段必须完整过一遍存储。
//
// 只存 Role + Content 的实现能通过前面所有用例，却在真实对话上崩：工具调用链断了，
// 模型看到「我调用了工具」但看不到调了什么、返回了什么，于是重复调用；Annotations
// 丢了归属就没了——框架正是靠 HistoryAgentAnnotation 标「这条是谁说的」。
func testMessageFields(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	toolCall := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "call-1",
		Type:     "function",
		Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`},
	}})
	toolCall.Annotations = map[string]any{
		agent_v2.HistoryAgentAnnotation:        "br1",
		agent_v2.HistoryAgentDisplayAnnotation: "researcher",
		"crew_custom":                          "业务自己塞的",
	}
	// thinking 类模型的 wire 协议要求把上一轮 assistant 的 reasoning 原样回传，
	// 不回传第 2 轮直接 400——所以它跟 Content 一样是必须落库的字段。
	toolCall.ReasoningContent = "先查库存再决定是否补货"
	toolResult := schema.ToolResultMessage("call-1", "查库存", "库存只剩 3 件")

	mustAppend(t, hs, convA, toolCall, toolResult)

	got := mustLoad(t, hs, convA)
	if len(got) != 2 {
		t.Fatalf("Load 返 %d 条, want 2\n%s", len(got), dump(got))
	}

	tcs := schema.GetToolCalls(got[0])
	if len(tcs) != 1 {
		t.Fatalf("assistant 消息的 ToolCalls 丢了（返 %d 个）—— "+
			"模型会看到「我调了工具」却不知道调了什么，于是重复调用", len(tcs))
	}
	if tcs[0].ID != "call-1" || tcs[0].Function.Name != "查库存" ||
		!strings.Contains(tcs[0].Function.Arguments, "A") {
		t.Errorf("ToolCall 字段不完整: id=%q name=%q args=%q",
			tcs[0].ID, tcs[0].Function.Name, tcs[0].Function.Arguments)
	}

	if got[0].Annotations == nil {
		t.Fatal("Annotations 整个丢了 —— 归属（HistoryAgentAnnotation）就是存在这里的")
	}
	if got[0].Annotations[agent_v2.HistoryAgentAnnotation] != "br1" {
		t.Errorf("HistoryAgentAnnotation = %v, want %q —— 丢了就不知道每条是谁说的",
			got[0].Annotations[agent_v2.HistoryAgentAnnotation], "br1")
	}
	// 框架的键不止一个：归属（位置，用于比较）和显示名（agent 名，进模型 prompt）是两个。
	// 只映射自己认识的键的 backend 会在这里露馅——丢了显示名不影响读回正确性，但跨归属
	// 改写会退化成把位置名塞给模型（`For context: [br1] said:`），模型看不懂那是谁。
	if got[0].Annotations[agent_v2.HistoryAgentDisplayAnnotation] != "researcher" {
		t.Errorf("HistoryAgentDisplayAnnotation = %v, want %q —— 显示名丢了，"+
			"模型和业务看到的是编排内部的位置名",
			got[0].Annotations[agent_v2.HistoryAgentDisplayAnnotation], "researcher")
	}
	if got[0].Annotations["crew_custom"] != "业务自己塞的" {
		t.Errorf("业务自定义 annotation 丢了: %v", got[0].Annotations["crew_custom"])
	}

	if got[1].Role != schema.RoleTool {
		t.Errorf("tool 结果的 Role = %s, want %s —— 角色错了 API 会 422",
			got[1].Role, schema.RoleTool)
	}
	if got[0].ReasoningContent != "先查库存再决定是否补货" {
		t.Errorf("ReasoningContent = %q, want %q —— thinking 模型要求上一轮的 reasoning "+
			"原样回传，丢了第 2 轮请求直接 400（后端只映射 Role/Content/ToolCalls 是最容易"+
			"踩的写法）", got[0].ReasoningContent, "先查库存再决定是否补货")
	}

	if id := schema.GetToolResultID(got[1]); id != "call-1" {
		t.Errorf("tool 结果的 tool_call_id = %q, want %q —— 配不上就是孤儿 tool_result",
			id, "call-1")
	}
	if got[1].Content != "库存只剩 3 件" {
		t.Errorf("tool 结果的正文 = %q, want %q —— 「工具返回了什么」正是这一层存在的理由，"+
			"配对信息全对但正文丢了，模型下一轮照样不知道查到的是什么",
			got[1].Content, "库存只剩 3 件")
	}
}

// testEmptyConversation：没聊过的会话返 (nil, nil)，不是错误。
//
// 「没聊过」和「出错了」必须能区分——第一轮对话时历史必然是空的，那时报错整个功能
// 就用不了。
func testEmptyConversation(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	got, err := hs.Load(context.Background(),
		agent_v2.HistoryKey{Owner: "tenant-x", ConvID: "从没存在过"})
	if err != nil {
		t.Fatalf("Load 不存在的会话返错误 %v —— 应返 (nil, nil)；"+
			"第一轮对话时历史必然为空，报错则整个功能用不了", err)
	}
	if len(got) != 0 {
		t.Errorf("Load 不存在的会话返 %d 条, want 0\n%s", len(got), dump(got))
	}
}

// testEmptyAppend：写空批是 no-op，不报错也不留痕。
func testEmptyAppend(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	if err := hs.Append(context.Background(), convA, nil); err != nil {
		t.Errorf("Append(nil) 返错误 %v —— 应是 no-op", err)
	}
	if err := hs.Append(context.Background(), convA, []*schema.Message{}); err != nil {
		t.Errorf("Append(空 slice) 返错误 %v —— 应是 no-op", err)
	}
	if got := mustLoad(t, hs, convA); len(got) != 0 {
		t.Errorf("写空批后 Load 返 %d 条, want 0\n%s", len(got), dump(got))
	}
}

// testLoadSnapshotIsolation：改 Load 返回的 slice 不该影响存储。
//
// 直接返内部 slice 的实现，调用方一个 append 就可能踩到存储的底层数组——下次 Load
// 读到被截断或被覆盖的内容，且没有任何错误。
func testLoadSnapshotIsolation(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	mustAppend(t, hs, convA,
		schema.UserMessage("原始-1"),
		schema.UserMessage("原始-2"))

	got := mustLoad(t, hs, convA)
	if len(got) != 2 {
		t.Fatalf("Load 返 %d 条, want 2", len(got))
	}
	got[0] = schema.UserMessage("被调用方改掉了")
	got = append(got, schema.UserMessage("被调用方追加的")) //nolint:staticcheck // 故意污染
	_ = got

	again := mustLoad(t, hs, convA)
	if len(again) != 2 {
		t.Fatalf("调用方改动返回值后再 Load 返 %d 条, want 2 —— "+
			"Load 应返快照而非内部引用\n%s", len(again), dump(again))
	}
	if again[0].Content != "原始-1" {
		t.Errorf("再次 Load [0] = %q, want %q —— 调用方的改动漏进了存储",
			again[0].Content, "原始-1")
	}
}

// testConcurrent：并发写不同会话 + 并发读，不 race 不丢。
//
// 框架的写入走后台 worker，读走主流程；多个 agent 同时跑时是真并发。跑 `-race`。
func testConcurrent(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	const convs, perConv = 8, 20
	var wg sync.WaitGroup
	for c := 0; c < convs; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: fmt.Sprintf("conv-%d", c)}
			for i := 0; i < perConv; i++ {
				if err := hs.Append(context.Background(), key,
					[]*schema.Message{schema.UserMessage(fmt.Sprintf("c%d-%02d", c, i))}); err != nil {
					t.Errorf("并发 Append(%s): %v", key, err)
					return
				}
			}
		}()
		// 并发读同一批会话（读到中间态是正常的，这里只验不崩不 race）
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: fmt.Sprintf("conv-%d", c)}
			for i := 0; i < perConv; i++ {
				if _, err := hs.Load(context.Background(), key); err != nil {
					t.Errorf("并发 Load(%s): %v", key, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for c := 0; c < convs; c++ {
		key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: fmt.Sprintf("conv-%d", c)}
		got := mustLoad(t, hs, key)
		if len(got) != perConv {
			t.Errorf("并发写完后 Load(%s) 返 %d 条, want %d —— 并发下丢了写入",
				key, len(got), perConv)
			continue
		}
		// 同一会话内的写是单 goroutine 串行发的，顺序必须保住
		for i, m := range got {
			want := fmt.Sprintf("c%d-%02d", c, i)
			if m.Content != want {
				t.Errorf("Load(%s)[%d] = %q, want %q —— 同一会话内顺序在并发下被打乱",
					key, i, m.Content, want)
				break
			}
		}
	}
}

func dump(msgs []*schema.Message) string {
	var b strings.Builder
	b.WriteString("实际内容：\n")
	for i, m := range msgs {
		if m == nil {
			fmt.Fprintf(&b, "  [%d] <nil>\n", i)
			continue
		}
		fmt.Fprintf(&b, "  [%d] %s: %s\n", i, m.Role, m.Content)
	}
	return b.String()
}

// testAppendNoMutation：Append 不得原地改写 caller 交进来的 slice 或它的元素。
//
// 接口 godoc 白纸黑字写了这一条，但整套用例没有一条验它——给一个合规的后端加上「往入参
// 消息的 Annotations 里塞个自己的键」，九条子测试照样全绿。
//
// 为什么必须机械验：框架交给 Append 的 `*schema.Message` 是一份**浅拷贝**——顶层字段和
// Annotations 是新的，但 ContentBlocks / ToolCalls 仍与**正在喂给模型的那条消息**共享底层
// 数据。后端顺手改一下（打个自己的 marker、规范化 Content、把工具参数重新序列化），污染的
// 是当轮的模型输入，而症状会出现在离现场极远的地方。
func testAppendNoMutation(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()

	call := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call-1", Type: "function",
		Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`},
	}})
	call.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "clerk"}
	result := schema.ToolResultMessage("call-1", "查库存", "库存只剩 3 件")

	batch := []*schema.Message{call, result}

	// 拍一份「交出去之前长什么样」的快照。比逐字段列举稳的地方在于：将来 Message 加了新
	// 字段，只要它影响这几项之一就仍然会被抓到。
	type shape struct {
		role      schema.MessageRole
		content   string
		anns      string // 键**和值**都进快照，见下
		reasoning string
		callIDs   string
		resultID  string
	}
	snap := func(m *schema.Message) shape {
		// annotation 只数个数是不够的：后端做**等数量的原地改值**（把归属规范化、小写化、
		// 加个租户前缀）计数不变、照样 PASS，而归属一旦被改写，读回时按 agent 名一过滤
		// 就一条不剩——开了历史反而比不开更失忆，且全程无报错。所以键和值都要进快照。
		keys := make([]string, 0, len(m.Annotations))
		for k := range m.Annotations {
			keys = append(keys, k)
		}
		sort.Strings(keys) // map 迭代无序，排完再拼
		var ab strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&ab, "%s=%v|", k, m.Annotations[k])
		}
		s := shape{role: m.Role, content: m.Content, anns: ab.String(),
			reasoning: m.ReasoningContent, resultID: schema.GetToolResultID(m)}
		for _, c := range schema.GetToolCalls(m) {
			s.callIDs += c.ID + ";" + c.Function.Name + ";" + c.Function.Arguments + "|"
		}
		return s
	}
	before := make([]shape, len(batch))
	for i, m := range batch {
		before[i] = snap(m)
	}
	beforeLen := len(batch)

	mustAppend(t, hs, convA, batch...)

	if len(batch) != beforeLen {
		t.Errorf("Append 改了 caller 那个 slice 的长度：%d → %d", beforeLen, len(batch))
	}
	for i, m := range batch {
		if got := snap(m); got != before[i] {
			t.Errorf("Append 原地改写了 caller 的第 %d 条消息\n  交出去时：%+v\n  返回后：  %+v\n"+
				"  这条消息跟正在喂给模型的那条共享底层数据，改它污染的是模型的当轮输入，"+
				"而报错会出现在离这里很远的地方。要留存请自己（深）拷贝",
				i, before[i], got)
		}
	}
}
