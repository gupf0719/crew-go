package agent_v2_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// bigTool 吐一条巨大的结果——业务工具最常见的形态（读文件 / 跑命令）。
type bigTool struct{ size int }

func (b bigTool) Name() string                { return "bulk_read" }
func (b bigTool) Description() string         { return "吐一大坨" }
func (b bigTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (b bigTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	return tool.Success("HEADMARK" + strings.Repeat("z", b.size) + "TAILMARK"), nil
}

// callThenAnswer 第一轮调工具、之后收尾，并记录每次实收的消息。
var toolNameForProbe = "bulk_read"

type callThenAnswer struct {
	mu   sync.Mutex
	last []*schema.Message
}

func (m *callThenAnswer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.last = append([]*schema.Message(nil), msgs...)
	seen := false
	for _, x := range msgs {
		if x != nil && x.Role == schema.RoleTool {
			seen = true
		}
	}
	m.mu.Unlock()
	if seen {
		return schema.AssistantMessage("看完了"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function",
		Function: schema.FunctionCall{Name: toolNameForProbe, Arguments: `{}`},
	}}), nil
}

func (m *callThenAnswer) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := m.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *callThenAnswer) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*callThenAnswer)(nil)

func runWithLimit(t *testing.T, limit int) []*schema.Message {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(bigTool{size: 40000})
	llm := &callThenAnswer{}
	opts := []agent_v2.Option{
		agent_v2.WithName("probe"), agent_v2.WithModel(llm),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
	}
	if limit > 0 {
		opts = append(opts, agent_v2.WithToolResultMaxBytes(limit))
	}
	ag := agent_v2.NewBasicAgent(opts...)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("读一下")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return llm.last
}

func toolResultOf(msgs []*schema.Message) *schema.Message {
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			return m
		}
	}
	return nil
}

// TestToolResultMaxBytes 钉住「单条业务工具结果在**进消息流之前**就被压到上限内」。
//
// **why 在源头拦**：压缩层按「组」丢，一条巨型工具结果会把它所在的整组变得非丢不可；
// 它若又落在受保护的组里，压缩层就只剩「整组丢」「整组留」两个都不能选的动作。
//
// 断言选在**模型实收的那条 tool 消息**上，不是「工具返回了什么」——后者跟这个改动无关，
// 它在两种配置下都一样大。
func TestToolResultMaxBytes(t *testing.T) {
	t.Run("不配时原样进流（默认关闭）", func(t *testing.T) {
		tr := toolResultOf(runWithLimit(t, 0))
		if tr == nil {
			t.Fatal("前提没搭对：模型没收到 tool 消息，工具那轮没跑到")
		}
		if len(tr.Content) < 40000 {
			t.Errorf("默认应当不截断，实际只有 %d 字节", len(tr.Content))
		}
	})

	t.Run("配了就压到上限内且头尾都在", func(t *testing.T) {
		const limit = 2000
		tr := toolResultOf(runWithLimit(t, limit))
		if tr == nil {
			t.Fatal("前提没搭对：模型没收到 tool 消息")
		}
		if len(tr.Content) > limit {
			t.Errorf("超上限：%d > %d", len(tr.Content), limit)
		}
		// 头尾都要在——只留头的实现同样会让长度达标，断言长度抓不到它。
		if !strings.Contains(tr.Content, "HEADMARK") {
			t.Errorf("头没保住：%.60q", tr.Content)
		}
		if !strings.Contains(tr.Content, "TAILMARK") {
			t.Errorf("尾没保住（退化成只留头？）：%.60q", tr.Content)
		}
		if !strings.Contains(tr.Content, "中间省略") {
			t.Errorf("没留痕——模型会把截短的结果当完整的用：%.60q", tr.Content)
		}
	})
}

// TestToolResultMaxBytes_FrameworkToolsExempt 钉住「框架工具的结果永不被这条上限截断」。
//
// 承重的是 `final_output`：它装的**就是最终答案本身**（框架从这条 tool_result 的校验后
// Content 合成对外的最终消息）。截它不是「省了点 token」，是**把答案改坏**——而且是
// 静默的：业务拿到的 JSON 缺一截，`json.Unmarshal` 大概率还报不出来（截在字符串中间时
// 才报，截在末尾对象闭合处就成了另一个合法但残缺的值）。
//
// 上限设得比答案小得多，正常业务工具在这个上限下必被截——所以这条红了就说明豁免没生效。
func TestToolResultMaxBytes_FrameworkToolsExempt(t *testing.T) {
	// 造一个远超上限的合法答案。
	longName := strings.Repeat("龙", 3000) // 3 字节/字 ⟹ ~9000 字节
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"` + longName + `","age":30}`),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("extract"),
		agent_v2.WithModel(m),
		agent_v2.WithOutputSchema("person", personSchema()),
		agent_v2.WithToolResultMaxBytes(500), // 远小于答案
		agent_v2.WithMaxRounds(5),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("extract"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("前提没搭对：没拿到最终消息")
	}
	var got struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if jerr := json.Unmarshal([]byte(final.Content), &got); jerr != nil {
		t.Fatalf("最终答案被截坏了，已经不是合法 JSON（%v）：%.80q", jerr, final.Content)
	}
	if got.Name != longName {
		t.Errorf("最终答案被截短了：期望 %d 字节的 name，实际 %d 字节", len(longName), len(got.Name))
	}
	if strings.Contains(final.Content, "中间省略") {
		t.Errorf("最终答案里出现了截断标记——框架工具的豁免没生效：%.80q", final.Content)
	}
}

// failBigTool 失败且错误正文巨大——失败路径跟成功路径走的是两套 content 组装。
type failBigTool struct{}

func (failBigTool) Name() string                { return "bulk_read" }
func (failBigTool) Description() string         { return "失败且错误正文巨大" }
func (failBigTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (failBigTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	return tool.Failure("io_error", "HEADMARK"+strings.Repeat("z", 40000)+"TAILMARK"), nil
}

// TestToolResultMaxBytes_FailurePathAlsoTruncated 钉住「**失败**的工具结果也真的被截住了」。
//
// **bug 历史**：闸原先站在「`content` 已拼好」那个点上，而失败路径的 `toolErr` 抓的是
// `res.Error` 的**引用**——下游 `basic_builder.go` 按 `(ErrorCode, Error)` **无条件重算**
// content 覆盖回去 ⟹ 截断被完全撤销。实测模型实收 40027 字节**一个字节没少**，
// **而框架已经打了一条 `kept_bytes=2000` 的 WARN**。
//
// 比静默更糟一档：框架出了一句**假的声**，业务照那条日志建监控会以为保护生效。
//
// 承重的量是**模型实收的那条 tool 消息的长度**——不是日志说了什么，也不是截断函数返回了什么。
func TestToolResultMaxBytes_FailurePathAlsoTruncated(t *testing.T) {
	const limit = 2000
	tk := tool.NewToolkit()
	tk.Register(failBigTool{})
	llm := &callThenAnswer{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"), agent_v2.WithModel(llm), agent_v2.WithToolkit(tk),
		agent_v2.WithToolResultMaxBytes(limit), agent_v2.WithMaxRounds(4))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("读一下")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr := toolResultOf(llm.last)
	if tr == nil {
		t.Fatal("前提没搭对：模型没收到 tool 消息")
	}
	// 失败正文经 toolErrorContent 包了个 "[code] " 前缀，给一点余量。
	if len(tr.Content) > limit+64 {
		t.Errorf("失败路径的截断被下游撤销了：模型实收 %d 字节（上限 %d）——"+
			"而框架大概率已经打过一条声称截断成功的 WARN", len(tr.Content), limit)
	}
	if !strings.Contains(tr.Content, "中间省略") {
		t.Errorf("没留痕：%.80q", tr.Content)
	}
}

// rdAnswerTool 是**业务自己的** return-direct 工具：产出即答复，不进消息流。
type rdAnswerTool struct{}

func (rdAnswerTool) Name() string                { return "exit_with_answer" }
func (rdAnswerTool) Description() string         { return "业务 return-direct：产出即答复" }
func (rdAnswerTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (rdAnswerTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	b, _ := json.Marshal(map[string]any{"answer": strings.Repeat("龙", 3000), "code": 200})
	return &tool.Result{Success: true, Output: string(b),
		Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
}

// TestToolResultMaxBytes_ReturnDirectExempt 钉住「业务 return-direct 工具的产出不被截」。
//
// `tool.ReturnDirectMetaKey` 的 godoc 写明「短路之后没有任何人再说话，**产出即答复**」
// ——它**不进消息流、不经过任何一层压缩**。而这条上限的立论是「进消息流前拦下、让压缩层
// 的死局消失」⟹ **在这条路上收益恒为零**，代价是把对外答案改坏。不是取舍失衡，是那个
// 取舍根本不存在。给 `final_output` 豁免的理由在这里一字不改地成立。
//
// ⚠️ 判据必须按 `res.Metadata` 不能按工具名——return-direct 是**运行时**声明的，
// 同一个工具这次是、下次可以不是。
func TestToolResultMaxBytes_ReturnDirectExempt(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(rdAnswerTool{})
	llm := &callThenAnswer{}
	toolNameForProbe = "exit_with_answer"
	defer func() { toolNameForProbe = "bulk_read" }()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"), agent_v2.WithModel(llm), agent_v2.WithToolkit(tk),
		agent_v2.WithToolResultMaxBytes(3000), agent_v2.WithMaxRounds(3))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	fin := agent_v2.FinalMessage(events)
	if fin == nil {
		t.Fatal("前提没搭对：没拿到最终答复")
	}
	var v struct {
		Answer string `json:"answer"`
		Code   int    `json:"code"`
	}
	if jerr := json.Unmarshal([]byte(fin.Content), &v); jerr != nil {
		t.Fatalf("业务 return-direct 的答复被截成了非法 JSON（%v）：%.80q", jerr, fin.Content)
	}
	if len(v.Answer) != len(strings.Repeat("龙", 3000)) {
		t.Errorf("答复被截短了：期望 %d 字节，实际 %d", len(strings.Repeat("龙", 3000)), len(v.Answer))
	}
}
