package agent_v2

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type hugeTool struct{}

func (hugeTool) Name() string                { return "bulk_read" }
func (hugeTool) Description() string         { return "吐一坨超大内容" }
func (hugeTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (hugeTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	return tool.Success("HEADMARK" + strings.Repeat("z", 400000) + "TAILMARK"), nil
}

type callOnceThenAnswer struct{ last []*schema.Message }

func (m *callOnceThenAnswer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.last = append([]*schema.Message(nil), msgs...)
	for _, x := range msgs {
		if x != nil && x.Role == schema.RoleTool {
			return schema.AssistantMessage("看完了"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "bulk_read", Arguments: `{}`},
	}}), nil
}

func (m *callOnceThenAnswer) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := m.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}
func (m *callOnceThenAnswer) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var bigNumRe = regexp.MustCompile(`(\d{4,})`)

func biggestNumberIn(s string) int {
	best := 0
	for _, x := range bigNumRe.FindAllString(s, -1) {
		if n, err := strconv.Atoi(x); err == nil && n > best {
			best = n
		}
	}
	return best
}

// TestNestedTruncation_ReportsTrueOriginal 钉住「同一条正文被截两次时，模型看到的量
// 仍然对得上真原始，而不是第二刀自己删的那点」。
//
// **bug 形态**：源头闸把 400008 字节截到 50000（中缝留一条标记），紧急压缩兜底再截到 2000
// ——第一刀的标记正好落在第二刀挖走的**中段**里被整个吞掉，于是模型只看到第二刀报的量。
// 实测偏小 100 倍，**而且方向正相反**：标记的用途是让模型判断「值不值得去把原文读回来」，
// 报小了它就以为内容基本完整、不去读。这比没有标记更坏——跟「打了 kept_bytes 的 WARN
// 比静默更糟」是同一种病。
//
// ⚠️ **光改措辞治不了**：第二刀手上的 `len` 是第一刀之后的残长，把中缝措辞从「删了 N」
// 改成「原始 M」，M 仍然是 50000——偏差只从 100 倍变成 6 倍，方向不变。免疫力来自
// **上游把真原始结构化地带过来**（[TruncatedOriginalBytesAnnotation]）。
//
// 断言选在**数量级**上：正文里报出来的最大数字必须落在真原始那一档（40 万），
// 而不是残长档（5 万）。只断言「有标记」在 bug 存在时同样为真。
func TestNestedTruncation_ReportsTrueOriginal(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(hugeTool{})
	llm := &callOnceThenAnswer{}
	ag := NewBasicAgent(WithName("probe"), WithModel(llm), WithToolkit(tk),
		WithToolResultMaxBytes(50000), WithMaxRounds(4))
	if _, err := ag.Run(context.Background(), NewRunSession(), schema.UserMessage("读")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var first *schema.Message
	for _, m := range llm.last {
		if m != nil && m.Role == schema.RoleTool {
			first = m
		}
	}
	if first == nil {
		t.Fatal("前提没搭对：模型没收到 tool 消息")
	}
	if len(first.Content) > 50000 || !strings.Contains(first.Content, "中间省略") {
		t.Fatalf("前提没搭对：源头闸没截或没留痕（%d 字节）", len(first.Content))
	}
	orig, ok := first.Annotations[TruncatedOriginalBytesAnnotation].(int)
	if !ok || orig < 400000 {
		t.Fatalf("前提没搭对：源头闸没把真原始记进 annotation（got %v）",
			first.Annotations[TruncatedOriginalBytesAnnotation])
	}

	// 第二刀。
	second := shrinkOneToolResult(first)

	if got := biggestNumberIn(second.Content); got < 300000 {
		t.Errorf("第二刀之后报的量是 %d，而真原始约 %d——模型会以为内容基本完整。正文开头：%.120q",
			got, orig, second.Content)
	}

	// 续跑之后的第二刀：消息经过存档往返（JSON），标注里的数字成了 float64。只按 `.(int)`
	// 读的话这里读不到真原始、回退成残长，报出来的量又落回残长档。
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored schema.Message
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, isInt := restored.Annotations[TruncatedOriginalBytesAnnotation].(int); isInt {
		t.Fatal("前提没搭对：往返之后标注仍是 int，测不到续跑的形态")
	}
	afterResume := shrinkOneToolResult(&restored)
	if got := biggestNumberIn(afterResume.Content); got < 300000 {
		t.Errorf("续跑后（标注经存档往返）第二刀报的量是 %d，而真原始约 %d——读不到往返后的数字标注", got, orig)
	}
}
