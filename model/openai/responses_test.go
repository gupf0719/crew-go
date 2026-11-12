package openai_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
	"crew-go/tool"
)

type responsesTransport func(*http.Request) (*http.Response, error)

func (f responsesTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func responsesMock(f responsesTransport) *openai.ResponsesClient {
	return openai.NewResponsesClient("test-token", "gpt-responses",
		openai.WithBaseURL("https://responses.invalid/v1"), openai.WithHTTPClient(&http.Client{Transport: f}))
}

func responsesBody(r *http.Request) (map[string]any, error) {
	defer r.Body.Close() // 模拟 HTTP Transport 的请求体生命周期。
	if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer test-token" {
		return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	include, _ := body["include"].([]any)
	if body["store"] != false || len(include) == 0 || include[0] != "reasoning.encrypted_content" || body["messages"] != nil {
		return nil, fmt.Errorf("unexpected Responses input options: %v", body)
	}
	return body, nil
}

func responsesOK(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

const responsesToolTurn = `{"status":"completed","model":"gpt-responses-snapshot","output":[
	{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"verify"}],"encrypted_content":"opaque-native-value"},
	{"type":"message","id":"msg_1","role":"assistant","phase":"commentary","status":"completed","content":[{"type":"output_text","text":"Checking"}]},
	{"type":"function_call","id":"fc_distinct","call_id":"call_checked","name":"lookup","arguments":"{\"key\":\"oak\"}","status":"completed"}
],"usage":{"input_tokens":15,"input_tokens_details":{"cached_tokens":7},"output_tokens":9,"output_tokens_details":{"reasoning_tokens":4},"total_tokens":24}}`
const responsesFinalTurn = `{"status":"completed","model":"gpt-responses-snapshot","output":[
	{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer oak=123"}]}
]}`

func verifyResponsesToolReplay(body map[string]any) error {
	input, _ := json.Marshal(body["input"])
	wire := string(input)
	last := -1
	for _, value := range []string{"opaque-native-value", "commentary", "fc_distinct", "function_call_output"} {
		idx := strings.Index(wire, value)
		if idx <= last {
			return fmt.Errorf("missing or unordered replay value %q: %s", value, input)
		}
		last = idx
	}
	items, _ := body["input"].([]any)
	final, _ := items[len(items)-1].(map[string]any)
	if final["type"] != "function_call_output" || final["call_id"] != "call_checked" || !strings.Contains(fmt.Sprint(final["output"]), "123") {
		return fmt.Errorf("tool output used the wrong call identity or content: %v", final)
	}
	return nil
}

func TestResponsesGenerate_StatelessReplayAndWireOptions(t *testing.T) {
	var calls int
	client := responsesMock(func(r *http.Request) (*http.Response, error) {
		body, err := responsesBody(r)
		if err != nil {
			return nil, err
		}
		calls++
		switch calls {
		case 1:
			tools, _ := body["tools"].([]any)
			input, _ := json.Marshal(body["input"])
			format, _ := json.Marshal(body["text"])
			if body["stream"] != false || body["max_output_tokens"] != float64(400) || body["tool_choice"] != "required" ||
				len(tools) != 1 || !strings.Contains(string(input), `"file_id":"file-1"`) || !strings.Contains(string(format), `"name":"shape"`) {
				return nil, fmt.Errorf("incorrect function or request options: %v", body)
			}
			bound := tools[0].(map[string]any)
			if bound["name"] != "lookup" || bound["strict"] != false || bound["function"] != nil || bound["type"] != "function" {
				return nil, fmt.Errorf("tool must be flat and preserve optional fields: %v", bound)
			}
			return responsesOK(responsesToolTurn), nil
		case 2:
			if err := verifyResponsesToolReplay(body); err != nil {
				return nil, err
			}
		case 3:
			input, _ := json.Marshal(body["input"])
			if strings.Contains(string(input), "opaque-native-value") || strings.Contains(string(input), "Checking") ||
				!strings.Contains(string(input), "rewritten by hook") {
				return nil, fmt.Errorf("rewritten history replayed the stale model output: %s", input)
			}
		default:
			return nil, errors.New("unexpected model request")
		}
		return responsesOK(responsesFinalTurn), nil
	})
	bound, err := client.WithTools([]*schema.ToolInfo{{Name: "lookup"}, {Name: "unused"}})
	if err != nil {
		t.Fatal(err)
	}
	user := schema.UserMessage("lookup oak")
	user.ContentBlocks = []*schema.ContentBlock{{Type: "file", MediaType: "application/pdf", File: &schema.FileContentBlock{
		File: &schema.FileData{ID: "file-1", Provider: "openai"},
	}}}
	out, err := bound.Generate(t.Context(), []*schema.Message{user}, model.WithAllowedTools("lookup"),
		model.WithToolChoice(schema.ToolChoiceRequired), model.WithMaxTokens(400),
		model.WithJSONSchema("shape", map[string]any{"type": "object"}, true))
	if err != nil {
		t.Fatal(err)
	}
	found := schema.GetToolCalls(out)
	if len(found) != 1 || found[0].ID != "call_checked" || out.ReasoningContent != "verify" ||
		out.Metadata.Usage.CachedInputTokens != 7 || out.Metadata.Usage.ThinkTokens != 4 {
		t.Fatalf("native response mapping failed: %+v / %+v", out, found)
	}
	serialized, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var restored schema.Message
	if err := json.Unmarshal(serialized, &restored); err != nil {
		t.Fatal(err)
	}
	in := []*schema.Message{user, &restored, schema.ToolResultMessage("call_checked", "123")}
	if final, err := bound.Generate(t.Context(), in); err != nil || final.Content != "answer oak=123" {
		t.Fatalf("replayed result: %v %v", final, err)
	}
	restored.Content = "rewritten by hook"
	if _, err := bound.Generate(t.Context(), in); err != nil || calls != 3 {
		t.Fatalf("rewritten result: %v, requests=%d", err, calls)
	}
}

func responsesSSE(typ, value string) string {
	return "event: " + typ + "\ndata: " + value + "\n\n"
}

func TestResponsesStream_ReplaysNativeItemsAcrossApproval(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	var requests, toolCalls atomic.Int32
	client := responsesMock(func(r *http.Request) (*http.Response, error) {
		body, err := responsesBody(r)
		if err != nil {
			return nil, err
		}
		if body["stream"] != true {
			return nil, errors.New("expected streaming wire request")
		}
		turn := responsesToolTurn
		prefix := responsesSSE("response.reasoning_summary_text.delta", `{"type":"response.reasoning_summary_text.delta","delta":"verify"}`) +
			responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","delta":"Check"}`)
		switch requests.Add(1) {
		case 1:
			input, _ := json.Marshal(body["input"])
			if !strings.Contains(string(input), "lookup oak") || body["tools"] == nil {
				return nil, fmt.Errorf("lost initial tool request: %v", body)
			}
		case 2:
			if err := verifyResponsesToolReplay(body); err != nil {
				return nil, err
			}
			turn = responsesFinalTurn
			prefix = responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","delta":"answer oak=123"}`)
		default:
			return nil, errors.New("approval resume reran the model call")
		}
		return responsesOK(prefix + responsesSSE("response.completed", `{"type":"response.completed","response":`+strings.Join(strings.Fields(turn), " ")+`}`)), nil
	})
	build := func() agent_v2.Agent {
		kit := tool.NewToolkit()
		kit.Register(tool.NewFuncTool("lookup", "Look up a key", nil,
			func(_ context.Context, args map[string]any) (*tool.Result, error) {
				toolCalls.Add(1)
				if args["key"] != "oak" {
					return nil, fmt.Errorf("unexpected tool arguments %v", args)
				}
				return tool.Success("123"), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("responses-approval"), agent_v2.WithModel(client),
			agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	}
	sess := agent_v2.NewRunSession()
	_, err := build().Run(ctx, sess, schema.UserMessage("lookup oak"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || requests.Load() != 1 || toolCalls.Load() != 0 {
		t.Fatalf("expected one persisted approval before executing: %v, model=%d tool=%d", err, requests.Load(), toolCalls.Load())
	}
	id := interrupt.AllQueries()[0].InterruptID
	events, err := build().Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		id: schema.UserMessage("approve"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "answer oak=123" || requests.Load() != 2 || toolCalls.Load() != 1 {
		t.Fatalf("approval did not carry the tool result to the model: %v (model=%d tool=%d)", final, requests.Load(), toolCalls.Load())
	}
}

func TestResponsesStream_RequiresCompletedTerminal(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		want         error
	}{
		{"eof", "", nil},
		{"filtered", responsesSSE("response.incomplete", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"}}}`), model.ErrContentFiltered},
		{"context_window", responsesSSE("error", `{"type":"error","code":"context_length_exceeded","message":"input too long"}`), model.ErrContextWindowExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := responsesMock(func(r *http.Request) (*http.Response, error) {
				if _, err := responsesBody(r); err != nil {
					return nil, err
				}
				return responsesOK(responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","delta":"partial"}`) + tc.suffix), nil
			})
			stream, err := client.Stream(t.Context(), []*schema.Message{schema.UserMessage("hi")})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			if part, err := stream.Recv(t.Context()); err != nil || part.Content != "partial" {
				t.Fatalf("missing partial: %v %v", part, err)
			}
			_, err = stream.Recv(t.Context())
			if err == nil || errors.Is(err, io.EOF) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("expected incomplete stream error (%v), got %v", tc.want, err)
			}
		})
	}
}

func TestResponsesStream_RebuildsMissingOutputAndPreservesIndexOrder(t *testing.T) {
	client := responsesMock(func(r *http.Request) (*http.Response, error) {
		if _, err := responsesBody(r); err != nil {
			return nil, err
		}
		return responsesOK(
			responsesSSE("response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_0"}}`) +
				responsesSSE("response.output_item.added", `{"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"msg_1"}}`) +
				responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"B"}`) +
				responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"A"}`) +
				responsesSSE("response.output_text.delta", `{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"C"}`) +
				responsesSSE("response.output_item.done", `{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"B"}]}}`) +
				responsesSSE("response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_0","type":"message","role":"assistant","content":[{"type":"output_text","text":"AC"}]}}`) +
				responsesSSE("response.output_item.done", `{"type":"response.output_item.done","output_index":2,"item":{"type":"function_call","id":"fc_0","call_id":"call_checked","name":"lookup","arguments":"{}"}}`) +
				responsesSSE("response.completed", `{"type":"response.completed","response":{"status":"completed","model":"snapshot"}}`)), nil
	})
	stream, err := client.Stream(t.Context(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for i, want := range []string{"A", "C", "B"} {
		msg, err := stream.Recv(t.Context())
		if err != nil || msg == nil || msg.Content != want {
			t.Fatalf("chunk %d should be %q, got %v %v", i, want, msg, err)
		}
		calls := schema.GetToolCalls(msg)
		if i < 2 && len(calls) != 0 || i == 2 && (len(calls) != 1 || calls[0].ID != "call_checked") {
			t.Fatalf("function call must be reconstructed only from the completed turn: %+v", calls)
		}
	}
	if _, err := stream.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("completed stream should reach EOF: %v", err)
	}
}

func TestResponsesGenerate_PreservesRefusalWithoutReplayingAnotherCredentialsItems(t *testing.T) {
	client := responsesMock(func(r *http.Request) (*http.Response, error) {
		if _, err := responsesBody(r); err != nil {
			return nil, err
		}
		return responsesOK(`{"status":"completed","output":[
			{"type":"reasoning","id":"rs_owner","summary":[],"encrypted_content":"opaque-owner-token"},
			{"type":"message","id":"msg_owner","role":"assistant","content":[{"type":"refusal","refusal":"cannot do that"}]}
		]}`), nil
	})
	first, err := client.Generate(t.Context(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil || first.Refusal != "cannot do that" {
		t.Fatalf("refusal: %v %v", first, err)
	}
	rotated := openai.NewResponsesClient("rotated-token", "gpt-responses",
		openai.WithBaseURL("https://responses.invalid/v1"), openai.WithHTTPClient(&http.Client{Transport: responsesTransport(
			func(r *http.Request) (*http.Response, error) {
				defer r.Body.Close() // 模拟 HTTP Transport 释放请求体。
				data, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				wire := string(data)
				if r.Header.Get("Authorization") != "Bearer rotated-token" ||
					strings.Contains(wire, "opaque-owner-token") || strings.Contains(wire, "msg_owner") ||
					!strings.Contains(wire, `"content":"cannot do that","role":"assistant"`) {
					return nil, fmt.Errorf("unexpected history after credential change: %s", wire)
				}
				return responsesOK(responsesFinalTurn), nil
			})}))
	if _, err := rotated.Generate(t.Context(), []*schema.Message{schema.UserMessage("hi"), first, schema.UserMessage("next")}); err != nil {
		t.Fatal(err)
	}
}
