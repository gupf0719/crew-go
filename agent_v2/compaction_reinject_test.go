package agent_v2_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// crModel 前 rounds 次调用各发一个 tool 调用（pathArg 时带 path=fNN），之后收尾；记下每次输入里找不到声明者的
// tool_result。
type crModel struct {
	mu      sync.Mutex
	rounds  int
	calls   int
	tool    string
	pathArg bool
	inputs  [][]*schema.Message
	orphans []string
}

func (m *crModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	declared := map[string]bool{}
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		for _, tc := range schema.GetToolCalls(msg) {
			declared[tc.ID] = true
		}
		if id := schema.GetToolResultID(msg); msg.Role == schema.RoleTool && !declared[id] {
			m.orphans = append(m.orphans, fmt.Sprintf("call%d:%s", m.calls, id))
		}
	}
	if m.calls > m.rounds {
		return schema.AssistantMessage("完成"), nil
	}
	args := `{}`
	if m.pathArg {
		args = fmt.Sprintf(`{"path":"f%02d"}`, m.calls)
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.calls), Type: "function",
		Function: schema.FunctionCall{Name: m.tool, Arguments: args}}}), nil
}
func (m *crModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *crModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// crNilHook 每轮在消息列表末尾追加一个 nil。
type crNilHook struct{ hook.NoOpHooks }

func (crNilHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	cp := make([]*schema.Message, 0, len(in.Messages)+1)
	in.Messages = append(append(cp, in.Messages...), nil)
	return ctx, nil
}

// TestAutoCompactMemo_HookLeavingNilNoOrphans：OnModelStart 钩子在消息列表里留下 nil 时，套用摘要结果之后的模型输入里
// 也没有找不到声明者的 tool_result。nil 留在列表里的话，压缩分组跳过它、按条数算位置的地方就差一位——备忘的 Upto
// 多算一条，Head 后面直接接上一条 tool_result、声明它的 assistant 丢了（provider 报 400）。
func TestAutoCompactMemo_HookLeavingNilNoOrphans(t *testing.T) {
	m := &crModel{rounds: 40, tool: "grep"}
	s := &countingSummarizer{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(45), agent_v2.WithAutoCompact(smallWindow(s)),
		agent_v2.WithHooks(crNilHook{}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	if s.count() < 2 {
		t.Fatalf("fixture 没搭对：要摘要至少两次才走到套用备忘的路（摘要了 %d 次）", s.count())
	}
	if len(m.orphans) > 0 {
		t.Fatalf("模型输入里出现 %d 条找不到声明者的 tool_result，前几条 %v", len(m.orphans), m.orphans[:min(4, len(m.orphans))])
	}
}

var crPathRe = regexp.MustCompile(`## (f\d\d)\n`)

func readToolkit(output func(path string) string) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("Read", "read", map[string]any{"type": "object"},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			p, _ := args["path"].(string)
			return &tool.Result{Success: true, Output: output(p)}, nil
		}))
	return tk
}

// TestAutoCompact_ReinjectSkipsWhatMicroCompactClears：文件重注入从原始消息流挑文件，但不把 MicroCompact 会清掉的那些
// 注回原文——跟没有备忘时、从 MicroCompact 之后的输入挑文件是同一批：只在每个工具最近 KeepRecent 次的结果里挑。
func TestAutoCompact_ReinjectSkipsWhatMicroCompactClears(t *testing.T) {
	const keep = 4
	m := &crModel{rounds: 40, tool: "Read", pathArg: true}
	s := &countingSummarizer{}
	cfg := smallWindow(s)
	cfg.ReinjectRecentFiles = true
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(readToolkit(func(p string) string { return "CONTENT-OF-" + p + "\n" + strings.Repeat("x", 12000) })),
		agent_v2.WithMaxRounds(45), agent_v2.WithAutoCompact(cfg),
		agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true, KeepRecent: keep, CompactableTools: []string{"Read"}}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for i, in := range m.inputs {
		call := i + 1 // 第 call 次调用之前已经读过 f01..f(call-1)
		for _, msg := range in {
			if msg == nil || !strings.HasPrefix(msg.Content, "# Recently Read Files") {
				continue
			}
			for _, mm := range crPathRe.FindAllStringSubmatch(msg.Content, -1) {
				checked++
				var n int
				fmt.Sscanf(mm[1], "f%02d", &n)
				if n < call-keep {
					t.Fatalf("第 %d 次调用重注入了 %s——它不在最近 %d 次 Read 里，MicroCompact 已经清掉了它的结果", call, mm[1], keep)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("fixture 没搭对：一次重注入都没发生")
	}
}

// TestAutoCompact_ReinjectSkipsSpilledResults：产生时就转存过的 Read 结果不被重注入——它的正文是转存说明 + 预览，
// 不是文件内容。
func TestAutoCompact_ReinjectSkipsSpilledResults(t *testing.T) {
	m := &crModel{rounds: 20, tool: "Read", pathArg: true}
	s := &countingSummarizer{}
	cfg := smallWindow(s)
	cfg.ReinjectRecentFiles = true
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(readToolkit(func(p string) string { return "CONTENT-OF-" + p + "\n" + strings.Repeat("y", 12000) })),
		agent_v2.WithMaxRounds(25), agent_v2.WithAutoCompact(cfg), agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 4000, PreviewBytes: 2000}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	if s.count() == 0 {
		t.Fatal("fixture 没搭对：一次摘要都没发生")
	}
	for i, in := range m.inputs {
		for _, msg := range in {
			if msg != nil && strings.HasPrefix(msg.Content, "# Recently Read Files") && strings.Contains(msg.Content, "已转存") {
				t.Fatalf("第 %d 次调用把转存说明当成文件内容重注入了：\n%.400s", i+1, msg.Content)
			}
		}
	}
}
