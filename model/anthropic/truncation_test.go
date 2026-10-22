package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// truncatedStream 是一条被 max_tokens 截在工具调用参数中间的流：先一段文本，再一个只写了一半参数的
// tool_use。
func truncatedStream(w http.ResponseWriter) {
	replySSE(w,
		evMessageStart(nil),
		evBlockStart(0, map[string]any{"type": "text", "text": ""}),
		evDelta(0, map[string]any{"type": "text_delta", "text": "我来部署。"}),
		evBlockStop(0),
		evBlockStart(1, map[string]any{"type": "tool_use", "id": "toolu_t", "name": "deploy", "input": map[string]any{}}),
		evDelta(1, map[string]any{"type": "input_json_delta", "partial_json": `{"env":"pro`}),
		evBlockStop(1),
		evMessageDelta("max_tokens", map[string]any{"output_tokens": 100}),
		evMessageStop(),
	)
}

// TestStream_MaxTokensCutsToolCall：max_tokens 截在工具调用中间时，这个调用带着解析不了的原始参数交出
// （框架据此拒掉、把错误交回模型），回放时 input 是合法的 {}（API 只收 JSON 对象）。SDK 累积时已把截断的
// 参数换成 {}，照它交出就会让工具拿空参数执行。
func TestStream_MaxTokensCutsToolCall(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			truncatedStream(w)
			return
		}
		textReply(w, r, n)
	})
	c := newClient(srv)
	reader, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("部署")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v", err)
	}
	msg := aggregate(chunks)
	calls := schema.GetToolCalls(msg)
	if len(calls) != 1 || calls[0].Function.Arguments != `{"env":"pro` {
		t.Fatalf("工具调用 = %+v，期望带截断的原始参数", calls)
	}
	var args map[string]any
	if json.Unmarshal([]byte(calls[0].Function.Arguments), &args) == nil {
		t.Fatalf("截断的参数必须解析不了（框架只拦解析失败的参数）")
	}
	if msg.Metadata == nil || msg.Metadata.FinishReason != "length" {
		t.Errorf("Metadata = %+v，期望 FinishReason=length", msg.Metadata)
	}

	if _, err := c.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("部署"), msg, toolResult("toolu_t", "[invalid_arguments] 参数解析失败", true),
	}); err != nil {
		t.Fatalf("第二轮: %v", err)
	}
	want := `[{"text":"我来部署。","type":"text"},{"id":"toolu_t","input":{},"name":"deploy","type":"tool_use"}]`
	if got := canon(t, at(rec.at(t, 1).body, "messages", 1, "content")); got != want {
		t.Errorf("回放 =\n%s\n期望\n%s", got, want)
	}
}

// TestGenerate_MaxTokensCutsToolCall：非流式拿不到原文时，把最后那个调用的参数去掉末尾的 } 交出——
// 照样解析不了；同一条响应里更早的、完整的工具调用参数不动。
func TestGenerate_MaxTokensCutsToolCall(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusOK, message([]any{
			map[string]any{"type": "tool_use", "id": "toolu_a", "name": "f", "input": map[string]any{"x": 1}},
			map[string]any{"type": "tool_use", "id": "toolu_b", "name": "f", "input": map[string]any{"x": 2}},
		}, "max_tokens", nil))
	})
	msg, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	calls := schema.GetToolCalls(msg)
	if len(calls) != 2 || calls[0].Function.Arguments != `{"x":1}` || calls[1].Function.Arguments != `{"x":2` {
		t.Fatalf("工具调用 = %+v，期望第一个完整、最后一个截断", calls)
	}
}

// TestBasicAgent_TruncatedToolCallNotRun 端到端：BasicAgent 用本适配器时，被 max_tokens 截断的工具调用
// 不执行（框架拒成参数非法），失败结果以 is_error 交回模型。断言落在「工具有没有真执行」上——这正是
// 截断参数被换成 {} 时会出的事。
func TestBasicAgent_TruncatedToolCallNotRun(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			truncatedStream(w)
			return
		}
		replySSE(w, sseFor([]any{map[string]any{"type": "text", "text": "参数被截断了，我换个方式。"}}, "end_turn")...)
	})
	var runs atomic.Int32
	deploy := tool.NewFuncTool("deploy", "部署到指定环境",
		map[string]any{"type": "object", "properties": map[string]any{"env": map[string]any{"type": "string"}}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			runs.Add(1)
			return tool.Success("已部署"), nil
		})
	tk := tool.NewToolkit()
	tk.Register(deploy)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("deployer"),
		agent_v2.WithModel(newClient(srv)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("部署到生产")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("截断的工具调用被执行了 %d 次", n)
	}
	msgs, _ := at(rec.at(t, 1).body, "messages").([]any)
	last, _ := msgs[len(msgs)-1].(map[string]any)
	if str(last, "content", 0, "type") != "tool_result" || str(last, "content", 0, "tool_use_id") != "toolu_t" ||
		str(last, "content", 0, "is_error") != "true" {
		t.Errorf("第二轮最后一条 = %s，期望 toolu_t 的 is_error tool_result", canon(t, last))
	}
}
