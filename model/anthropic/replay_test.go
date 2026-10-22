package anthropic_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"crew-go/model/anthropic"
	"crew-go/schema"
)

// firstTurn 是一条带两个 thinking 块（第二个是工具调用前的进度说明）的响应：回放必须保住块的顺序与
// 签名——把 thinking 归拢到前面、或把文本挪到 thinking 之后，都会改掉第二个 thinking 块的前缀。
var firstTurn = []any{
	map[string]any{"type": "thinking", "thinking": "", "signature": "sig-1"},
	map[string]any{"type": "text", "text": "先看看"},
	map[string]any{"type": "thinking", "thinking": "", "signature": "sig-2"},
	map[string]any{"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": map[string]any{"q": "x"}},
}

// replayServer 第一次请求回 firstTurn，之后都回一段文本。
func replayServer(t *testing.T) (*anthropic.Client, *recorder) {
	t.Helper()
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			replyJSON(w, http.StatusOK, message(firstTurn, "tool_use", nil))
			return
		}
		textReply(w, r, n)
	})
	return newClient(srv), rec
}

// sentAssistant 是第 i 次请求里 assistant 消息（messages[1]）的 content。
func sentAssistant(t *testing.T, rec *recorder, i int) string {
	t.Helper()
	return canon(t, at(rec.at(t, i).body, "messages", 1, "content"))
}

// nextTurn 拿 msg 作历史再调一次。
func nextTurn(t *testing.T, c *anthropic.Client, msg *schema.Message) {
	t.Helper()
	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("查 x"), msg, toolResult("toolu_1", "结果", false),
	}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
}

// TestReplay_Verbatim：返回的 assistant 消息下一轮原样回放（块的顺序、文本边界、签名、参数）；
// 经过存档那样的 JSON 往返之后照样原样回放。
func TestReplay_Verbatim(t *testing.T) {
	c, rec := replayServer(t)
	msg, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("查 x")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := canon(t, firstTurn)

	nextTurn(t, c, msg)
	if got := sentAssistant(t, rec, 1); got != want {
		t.Errorf("回放 =\n%s\n期望\n%s", got, want)
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("编码消息: %v", err)
	}
	var restored schema.Message
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("解码消息: %v", err)
	}
	nextTurn(t, c, &restored)
	if got := sentAssistant(t, rec, 2); got != want {
		t.Errorf("JSON 往返之后回放 =\n%s\n期望\n%s", got, want)
	}
}

// TestReplay_EditedMessageRebuilt：文本或工具调用被改过（钩子改写、业务替换）时，发出去的必须是改过的
// 内容，且按快照原来的块序合并——只改了工具参数时文本块与两个 thinking 块原样原位，所有 thinking 的前缀
// 都没变；把 thinking 归拢到最前的话，排在文本之后的 sig-2 前缀就变了。
func TestReplay_EditedMessageRebuilt(t *testing.T) {
	sig1 := `{"signature":"sig-1","thinking":"","type":"thinking"}`
	sig2 := `{"signature":"sig-2","thinking":"","type":"thinking"}`
	text := `{"text":"先看看","type":"text"}`
	callX := `{"id":"toolu_1","input":{"q":"x"},"name":"lookup","type":"tool_use"}`
	cases := []struct {
		name string
		edit func(m *schema.Message)
		want string
	}{
		{"改了文本：新文本放在原文本的位置", func(m *schema.Message) { m.Content = "改过的" },
			`[` + sig1 + `,{"text":"改过的","type":"text"},` + sig2 + `,` + callX + `]`},
		{"改了工具参数：原位替换", func(m *schema.Message) { m.ContentBlocks[0].Toolcall.Arguments = `{"q":"y"}` },
			`[` + sig1 + `,` + text + `,` + sig2 + `,{"id":"toolu_1","input":{"q":"y"},"name":"lookup","type":"tool_use"}]`},
		{"去掉了工具调用", func(m *schema.Message) { m.ContentBlocks = nil },
			`[` + sig1 + `,` + text + `,` + sig2 + `]`},
		{"加了工具调用：放最后", func(m *schema.Message) {
			m.ContentBlocks = append(m.ContentBlocks, toolCall("toolu_2", "lookup", `{"q":"z"}`))
		}, `[` + sig1 + `,` + text + `,` + sig2 + `,` + callX + `,{"id":"toolu_2","input":{"q":"z"},"name":"lookup","type":"tool_use"}]`},
		{"删了快照标注", func(m *schema.Message) { delete(m.Annotations, anthropic.AssistantContentAnnotation) },
			`[` + text + `,` + callX + `]`},
		{"快照版本不认识", func(m *schema.Message) {
			m.Annotations[anthropic.AssistantContentAnnotation] = `{"v":99,"digest":"x","content":[]}`
		}, `[` + text + `,` + callX + `]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, rec := replayServer(t)
			msg, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("查 x")})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			tc.edit(msg)
			nextTurn(t, c, msg)
			if got := sentAssistant(t, rec, 1); got != tc.want {
				t.Errorf("发出 =\n%s\n期望\n%s", got, tc.want)
			}
		})
	}
}

// TestReplay_EditedToolKeepsSplitText：快照里两段文本被工具调用隔开（Content 拼成「先看看。\n\n再看看。」）时，
// 只改工具参数，两个文本块原样原位保留；按改过的文本处理的话会并成一个文本块、挪动位置。
func TestReplay_EditedToolKeepsSplitText(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			replyJSON(w, http.StatusOK, message([]any{
				map[string]any{"type": "text", "text": "先看看。"},
				map[string]any{"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": map[string]any{"q": "x"}},
				map[string]any{"type": "text", "text": "再看看。"},
				map[string]any{"type": "tool_use", "id": "toolu_2", "name": "lookup", "input": map[string]any{"q": "w"}},
			}, "tool_use", nil))
			return
		}
		textReply(w, r, n)
	})
	c := newClient(srv)
	msg, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("查")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if msg.Content != "先看看。\n\n再看看。" {
		t.Fatalf("Content = %q", msg.Content)
	}
	msg.ContentBlocks[0].Toolcall.Arguments = `{"q":"y"}`
	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("查"), msg, toolResult("toolu_1", "a", false), toolResult("toolu_2", "b", false),
	}); err != nil {
		t.Fatalf("第二轮: %v", err)
	}
	want := `[{"text":"先看看。","type":"text"},{"id":"toolu_1","input":{"q":"y"},"name":"lookup","type":"tool_use"},` +
		`{"text":"再看看。","type":"text"},{"id":"toolu_2","input":{"q":"w"},"name":"lookup","type":"tool_use"}]`
	if got := sentAssistant(t, rec, 1); got != want {
		t.Errorf("发出 =\n%s\n期望\n%s", got, want)
	}
}

// TestReplay_ToolInputValuesExact：回放的 tool_use.input 跟 API 返回的值逐个相同——超过 2^53 的整数
// （雪花 ID、纳秒时间戳）与高精度小数不能经 float64 变形。值变了，模型看到的就不是它调用过的参数，
// 这条消息之后的 thinking 块的前缀也跟着变了。
func TestReplay_ToolInputValuesExact(t *testing.T) {
	const input = `{"id":12345678901234567891,"ts_ns":1729500000000000123,"pi":3.14159265358979323846}`
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			body := message([]any{
				map[string]any{"type": "tool_use", "id": "toolu_big", "name": "lookup", "input": json.RawMessage(input)},
				map[string]any{"type": "thinking", "thinking": "", "signature": "sig-after"},
			}, "tool_use", nil)
			replyJSON(w, http.StatusOK, body)
			return
		}
		textReply(w, r, n)
	})
	c := newClient(srv)
	msg, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("查")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if got := schema.GetToolCalls(msg)[0].Function.Arguments; got != input {
		t.Fatalf("Arguments = %s，期望原样 %s", got, input)
	}
	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("查"), msg, toolResult("toolu_big", "ok", false),
	}); err != nil {
		t.Fatalf("第二轮: %v", err)
	}
	// canon 用 json.Number 解码，数字原文不变；两边都排好键再比
	if got, want := canon(t, at(rec.at(t, 1).body, "messages", 1, "content", 0, "input")), canon(t, jsonOf(t, input)); got != want {
		t.Errorf("回放的 input = %s，期望 %s", got, want)
	}
}

// TestInputTransformations：响应里的 input_transformations 按（种类, 原因）归并计数记进标注，经过 JSON
// 往返照样读得出。逐块记的话，历史改过之后每轮都把同一批块再记一遍，会话标注按 N² 增长。
func TestInputTransformations(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		body := message([]any{map[string]any{"type": "text", "text": "ok"}}, "end_turn", nil)
		body["input_transformations"] = []any{
			map[string]any{"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"},
			map[string]any{"type": "thinking_dropped", "path": "messages.3.content.0", "reason": "prefix_binding_mismatch"},
			map[string]any{"type": "thinking_dropped", "path": "messages.5.content.1", "reason": "model_binding_mismatch"},
		}
		replyJSON(w, http.StatusOK, body)
	})
	msg, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := []anthropic.InputTransformation{
		{Type: "thinking_dropped", Reason: "prefix_binding_mismatch", Count: 2, FirstPath: "messages.1.content.0"},
		{Type: "thinking_dropped", Reason: "model_binding_mismatch", Count: 1, FirstPath: "messages.5.content.1"},
	}
	check := func(label string, m *schema.Message) {
		t.Helper()
		got := anthropic.InputTransformations(m)
		if len(got) != len(want) {
			t.Fatalf("%s：InputTransformations = %+v", label, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s：第 %d 组 = %+v，期望 %+v", label, i, got[i], want[i])
			}
		}
	}
	check("返回时", msg)
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("编码: %v", err)
	}
	var restored schema.Message
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("解码: %v", err)
	}
	check("JSON 往返之后", &restored)
	if got := anthropic.InputTransformations(schema.UserMessage("x")); got != nil {
		t.Errorf("没有标注时应返回 nil：%+v", got)
	}
}
