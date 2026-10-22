package agent_v2_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// bigOutput 是 n 行「第 i 行」的内容。
func bigOutput(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "第 %05d 行：订单 A%05d 已发货\n", i, i)
	}
	return b.String()
}

var spillIDRe = regexp.MustCompile(`id="(sp_[0-9a-f]+)"`)

// spillReaderModel：第 1 次调大输出工具；第 2 次从工具结果里抠出 id，按 readArgs 调 read_tool_output；
// 第 3 次收尾。记下每次看到的输入。
type spillReaderModel struct {
	mu       sync.Mutex
	calls    int
	inputs   [][]*schema.Message
	readArgs string // 不含 id 的其余参数，如 `"offset":1000,"limit":3`
	toolName string
}

func (m *spillReaderModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	switch m.calls {
	case 1:
		name := m.toolName
		if name == "" {
			name = "dump"
		}
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: name, Arguments: `{}`}}}), nil
	case 2:
		last := msgs[len(msgs)-1].Content
		id := ""
		if mm := spillIDRe.FindStringSubmatch(last); mm != nil {
			id = mm[1]
		}
		args := fmt.Sprintf(`{"id":%q`, id)
		if m.readArgs != "" {
			args += "," + m.readArgs
		}
		args += "}"
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.ReadToolOutputToolName, Arguments: args}}}), nil
	default:
		return schema.AssistantMessage("完成"), nil
	}
}
func (m *spillReaderModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *spillReaderModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *spillReaderModel) toolResult(call int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	in := m.inputs[call-1]
	return in[len(in)-1].Content
}

func dumpToolkit(output string, returnDirect bool) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			r := &tool.Result{Success: true, Output: output}
			if returnDirect {
				r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
			}
			return r, nil
		}))
	return tk
}

func spillAgent(m model.ChatModel, st store.Store, tk *tool.Toolkit, extra ...agent_v2.Option) *agent_v2.BasicAgent {
	opts := []agent_v2.Option{agent_v2.WithName("spiller"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(6),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300})}
	if st != nil {
		opts = append(opts, agent_v2.WithStore(st))
	}
	return agent_v2.NewBasicAgent(append(opts, extra...)...)
}

// TestToolResultSpill_LargeOutputSpilledAndReadable：超过阈值的工具结果在产生时就转存——模型看到的是
// 有界的说明 + 预览 + 取回 id，用 read_tool_output 按行读得回原文里任意一段。
//
// 断言选在模型实际看到的东西上：工具结果的长度（没转存时是整段 9 万字节）；读回的那几行是不是原文里
// 那几行（取回 id 失效、命名空间对不上、按行切错，这里都会变）。
func TestToolResultSpill_LargeOutputSpilledAndReadable(t *testing.T) {
	out := bigOutput(3000)
	m := &spillReaderModel{readArgs: `"offset":1500,"limit":3`}
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(out, false))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-spill")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出订单")); err != nil {
		t.Fatal(err)
	}
	seen := m.toolResult(2)
	if len(seen) > 2000 {
		t.Fatalf("模型看到的工具结果 %d 字节——超大输出没被换成预览（原文 %d 字节）", len(seen), len(out))
	}
	if !spillIDRe.MatchString(seen) || !strings.Contains(seen, "第 00001 行") || !strings.Contains(seen, "第 03000 行") {
		t.Fatalf("说明里应有取回 id、预览应带开头与结尾：\n%s", seen)
	}
	read := m.toolResult(3)
	for _, want := range []string{"1500| 第 01500 行", "1501| 第 01501 行", "1502| 第 01502 行"} {
		if !strings.Contains(read, want) {
			t.Fatalf("read_tool_output 没读回原文第 1500~1502 行（缺 %q）：\n%s", want, read)
		}
	}
	if strings.Contains(read, "第 01503 行") {
		t.Fatalf("limit=3 却读回了第 1503 行：\n%s", read)
	}
}

// TestToolResultSpill_PatternSearch：按正则搜，只返回匹配的行，带原文行号。
func TestToolResultSpill_PatternSearch(t *testing.T) {
	m := &spillReaderModel{readArgs: `"pattern":"A0(1234|2345)"`}
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(bigOutput(3000), false))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-grep")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("找两单")); err != nil {
		t.Fatal(err)
	}
	read := m.toolResult(3)
	if !strings.Contains(read, "1234| 第 01234 行") || !strings.Contains(read, "2345| 第 02345 行") {
		t.Fatalf("pattern 搜索没找到那两行：\n%s", read)
	}
	if strings.Count(read, "| 第 ") != 2 {
		t.Fatalf("pattern 搜索应只返回 2 行匹配：\n%s", read)
	}
}

// TestToolResultSpill_NoStoreFallsBackToTruncation：没有 store 时不转存，截断到阈值、说明取不回全文——
// 上限照样成立，也不留一个取不回来的 id。
func TestToolResultSpill_NoStoreFallsBackToTruncation(t *testing.T) {
	m := &spillReaderModel{}
	ag := spillAgent(m, nil, dumpToolkit(bigOutput(3000), false))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	seen := m.toolResult(2)
	if len(seen) > 1000 {
		t.Fatalf("没有 store 时应截断到阈值 1000 字节，实际 %d", len(seen))
	}
	if spillIDRe.MatchString(seen) || !strings.Contains(seen, "无法取回全文") {
		t.Fatalf("退化成截断时不该给取回 id、要说明取不回：\n%s", seen)
	}
}

// TestToolResultSpill_OtherConversationCannotRead：取回只认当前会话里转存的——别的会话拿着 id 也读不到。
// id 会出现在模型可见的文本里、会话历史里，不能靠它保密。
func TestToolResultSpill_OtherConversationCannotRead(t *testing.T) {
	st := store.NewInMemoryStore()
	m := &spillReaderModel{}
	ag := spillAgent(m, st, dumpToolkit(bigOutput(3000), false))
	if _, err := ag.Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-A"), agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	id := spillIDRe.FindStringSubmatch(m.toolResult(2))[1]

	// 用一个只调 read_tool_output 的模型在会话 B 里读同一个 id
	reader := &fixedCallModel{name: agent_v2.ReadToolOutputToolName, args: fmt.Sprintf(`{"id":%q}`, id)}
	agB := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(reader), agent_v2.WithStore(st),
		agent_v2.WithToolResultSpill(nil), agent_v2.WithMaxRounds(3))
	if _, err := agB.Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-B"), agent_v2.NewRunSession(), schema.UserMessage("读")); err != nil {
		t.Fatal(err)
	}
	if got := reader.lastToolResult(); !strings.Contains(got, "not_found") && !strings.Contains(got, "没找到") {
		t.Fatalf("会话 B 读到了会话 A 转存的内容：\n%.300s", got)
	}
}

// fixedCallModel 第一次调指定工具，之后收尾；记下工具结果。
type fixedCallModel struct {
	mu         sync.Mutex
	name, args string
	calls      int
	last       string
}

func (m *fixedCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "r1", Type: "function",
			Function: schema.FunctionCall{Name: m.name, Arguments: m.args}}}), nil
	}
	m.last = msgs[len(msgs)-1].Content
	return schema.AssistantMessage("好"), nil
}
func (m *fixedCallModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *fixedCallModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m *fixedCallModel) lastToolResult() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

// TestToolResultSpill_SmallOutputUntouched：没超阈值的结果原样进消息流。
func TestToolResultSpill_SmallOutputUntouched(t *testing.T) {
	m := &spillReaderModel{}
	small := bigOutput(10)
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(small, false))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	if got := m.toolResult(2); got != small {
		t.Fatalf("没超阈值的结果被改了：\n%s", got)
	}
}

// TestToolResultSpill_HistoryKeepsPreviewOnly：会话历史里记的是预览，不是全文——长会话的历史不再被
// 一条大输出撑大，下一次 Run 读回来的也只是预览。
func TestToolResultSpill_HistoryKeepsPreviewOnly(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	out := bigOutput(3000)
	m := &spillReaderModel{}
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(out, false), agent_v2.WithHistory(hs, "t"))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-hist")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	msgs, _ := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-hist"})
	total := 0
	spilled := 0
	for _, msg := range msgs {
		total += len(msg.Content)
		if id, _ := msg.Annotations[agent_v2.ToolResultSpillAnnotation].(string); id != "" {
			spilled++
		}
	}
	if total > len(out)/10 {
		t.Fatalf("历史里存了 %d 字节——大输出的全文进了历史（原文 %d 字节）", total, len(out))
	}
	if spilled != 1 {
		t.Fatalf("历史里那条工具结果应带取回 id 标注，找到 %d 条", spilled)
	}
}

// TestToolResultSpill_ReturnDirectExempt：return-direct 的结果产出即答复、不进消息流，不转存——转存了
// 等于把交给调用方的答案换成一段说明。
func TestToolResultSpill_ReturnDirectExempt(t *testing.T) {
	m := &spillReaderModel{}
	out := bigOutput(500)
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(out, true))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("导出"))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != out {
		got := ""
		if final != nil {
			got = final.Content
		}
		t.Fatalf("return-direct 的答复被改了（%d 字节，原文 %d 字节）", len(got), len(out))
	}
}

// TestToolResultSpill_ResumeReplaysSameNotice：同一批里别的工具中断、续跑时这次调用命中工具账——函数体
// 不再跑、也不再转存一份，模型看到的还是同一个 id，按它照样读得回。
func TestToolResultSpill_ResumeReplaysSameNotice(t *testing.T) {
	st := store.NewInMemoryStore()
	runs := 0
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			runs++
			return &tool.Result{Success: true, Output: bigOutput(3000)}, nil
		}))
	m := &batchAskModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("spiller"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithAskUser(), agent_v2.WithStore(st),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300}))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "conv-resume"), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("导出并确认"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应在 ask_user 中断，got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("续跑时大输出工具又跑了一次（共 %d 次）——工具账没命中", runs)
	}
	ids := map[string]bool{}
	for _, in := range m.inputs {
		for _, msg := range in {
			if mm := spillIDRe.FindStringSubmatch(msg.Content); mm != nil {
				ids[mm[1]] = true
			}
		}
	}
	if len(ids) != 1 {
		t.Fatalf("续跑前后模型看到的取回 id 应是同一个，看到 %d 个：%v", len(ids), ids)
	}
}

// batchAskModel 第一次同一批调 dump 与 ask_user，之后收尾；记下每次输入。
type batchAskModel struct {
	mu     sync.Mutex
	calls  int
	inputs [][]*schema.Message
}

func (m *batchAskModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "d1", Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}},
			{ID: "a1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"确认吗"}`}},
		}), nil
	}
	return schema.AssistantMessage("完成"), nil
}
func (m *batchAskModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *batchAskModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// twiceDumpModel 同一批调两次 dump、再收尾。
type twiceDumpModel struct {
	mu     sync.Mutex
	calls  int
	inputs [][]*schema.Message
}

func (m *twiceDumpModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "d1", Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}},
			{ID: "d2", Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{"again":true}`}},
		}), nil
	}
	return schema.AssistantMessage("完成"), nil
}
func (m *twiceDumpModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *twiceDumpModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestToolResultSpill_SameContentStoredOnce：同一份大输出在同一个会话里只存一份、两处给的是同一个取回 id（id 按内容
// 定）——续跑时这一批重做、内容没变也落回同一条，不留孤儿。
func TestToolResultSpill_SameContentStoredOnce(t *testing.T) {
	st := store.NewInMemoryStore()
	m := &twiceDumpModel{}
	ag := spillAgent(m, st, dumpToolkit(bigOutput(3000), false))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-same")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出两次")); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, msg := range m.inputs[1] {
		if id, _ := msg.Annotations[agent_v2.ToolResultSpillAnnotation].(string); id != "" {
			ids[id] = true
		}
	}
	if len(ids) != 1 {
		t.Fatalf("同一份内容应得到同一个取回 id，得到 %d 个：%v", len(ids), ids)
	}
	items, err := st.Search(ctx, store.NS("_tool_spill"), store.WithLimit(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("store 里应只有 1 份转存内容，有 %d 份", len(items))
	}
}

// TestToolResultSpill_OwnerFromHistoryWhenCtxHasNone：ctx 里没有调用主体、会话历史按请求解析归属
// （WithHistoryOwnerFunc）时，转存按那个归属分——两个租户用了同一个会话 ID，也读不到对方转存的内容。
func TestToolResultSpill_OwnerFromHistoryWhenCtxHasNone(t *testing.T) {
	st := store.NewInMemoryStore()
	hs := agent_v2.NewInMemoryHistoryStore()
	type tenantKey struct{}
	ownerFn := agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string { s, _ := ctx.Value(tenantKey{}).(string); return s })
	m := &spillReaderModel{}
	ag := spillAgent(m, st, dumpToolkit(bigOutput(3000), false), agent_v2.WithHistory(hs, "", ownerFn))
	ctxA := context.WithValue(compose_v2.WithThreadIDCtx(context.Background(), "shared-conv"), tenantKey{}, "tenant-A")
	if _, err := ag.Run(ctxA, agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	id := spillIDRe.FindStringSubmatch(m.toolResult(2))[1]

	reader := &fixedCallModel{name: agent_v2.ReadToolOutputToolName, args: fmt.Sprintf(`{"id":%q}`, id)}
	agB := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(reader), agent_v2.WithStore(st),
		agent_v2.WithToolResultSpill(nil), agent_v2.WithMaxRounds(3), agent_v2.WithHistory(hs, "", ownerFn))
	ctxB := context.WithValue(compose_v2.WithThreadIDCtx(context.Background(), "shared-conv"), tenantKey{}, "tenant-B")
	if _, err := agB.Run(ctxB, agent_v2.NewRunSession(), schema.UserMessage("读")); err != nil {
		t.Fatal(err)
	}
	if got := reader.lastToolResult(); !strings.Contains(got, "not_found") {
		t.Fatalf("租户 B 读到了租户 A 转存的内容（同一个会话 ID）：\n%.300s", got)
	}
}
