package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/mcp"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/model/openai"
	"crew-go/model/qwen"
	"crew-go/schema"
)

const (
	mcpMediaName = "mcp_media_capture"
	mediaPNG     = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNoYGD4DwADBAGAC4PIFAAAAABJRU5ErkJggg=="
	mediaWAV     = "UklGRigAAABXQVZFZm10IBAAAAABAAEAQB8AAIA+AAACABAAZGF0YQQAAAAAAAAA"
)

func newMediaHub(t *testing.T, ctx context.Context, result func(rpcReq) map[string]any) *mcp.Hub {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-06-18"})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{
				"name": "capture", "inputSchema": map[string]any{"type": "object"},
			}}})
		case "tools/call":
			if req.Params["name"] != "capture" {
				t.Errorf("远端收到错误的工具名 %v", req.Params["name"])
			}
			writeResult(w, req.ID, result(req))
		default:
			t.Errorf("意外的 RPC %q", req.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	c, err := mcp.NewStreamableHTTPClient("media", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() }) // 测试断言消费真实 Hub 的结果。
	return h
}

func mixedMediaResult() map[string]any {
	return map[string]any{"content": []map[string]any{
		{"type": "text", "text": "before image"},
		{"type": "image", "data": mediaPNG, "mimeType": "image/png"},
		{"type": "audio", "data": mediaWAV, "mimeType": "audio/wav"},
		{"type": "text", "text": "after audio"},
	}}
}

// mediaGenerateOnly 让 BasicAgent 通过生产适配器走 JSON 请求，省掉与本回归无关的模型输出 SSE 桩。
type mediaGenerateOnly struct{ model.ChatModel }

func (m mediaGenerateOnly) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m mediaGenerateOnly) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	bound, err := m.ChatModel.WithTools(tools)
	if err != nil {
		return nil, err
	}
	return mediaGenerateOnly{bound}, nil
}

func TestMCPToolMedia_AnthropicToolResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	h := newMediaHub(t, ctx, func(rpcReq) map[string]any {
		calls.Add(1)
		return mixedMediaResult()
	})
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content []struct {
					Type      string `json:"type"`
					ToolUseID string `json:"tool_use_id"`
					Content   []struct {
						Type   string `json:"type"`
						Text   string `json:"text"`
						Source struct {
							Data      string `json:"data"`
							MediaType string `json:"media_type"`
						} `json:"source"`
					} `json:"content"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) == 1 {
			_, _ = fmt.Fprintf(w, `{"type":"message","id":"msg_call","role":"assistant","model":"fake","stop_reason":"tool_use","content":[{"type":"tool_use","id":"toolu_media","name":%q,"input":{}}],"usage":{"input_tokens":20,"output_tokens":6}}`, mcpMediaName)
			return
		}
		if len(req.Messages) == 0 {
			t.Error("送模请求缺少消息历史")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		last := req.Messages[len(req.Messages)-1]
		if last.Role != "user" || len(last.Content) != 1 || last.Content[0].ToolUseID != "toolu_media" {
			t.Errorf("媒体必须属于原工具结果，实际 %+v", last)
		} else {
			parts := last.Content[0].Content
			if len(parts) != 4 || parts[0].Text != "before image" || parts[1].Type != "image" || parts[1].Source.Data != mediaPNG ||
				parts[1].Source.MediaType != "image/png" || !strings.Contains(parts[2].Text, "audio/wav") || parts[3].Text != "after audio" {
				t.Errorf("图片、音频降级和相邻文本应按原顺序送模，实际 %+v", parts)
			}
		}
		_, _ = fmt.Fprint(w, `{"type":"message","id":"msg_done","role":"assistant","model":"fake","stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":40,"output_tokens":3}}`)
	}))
	t.Cleanup(srv.Close)
	llm := mediaGenerateOnly{anthropic.NewClient("fake", "fake", anthropic.WithBaseURL(srv.URL))}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mcp_media"), agent_v2.WithToolkit(h.Toolkit()), agent_v2.WithModel(llm), agent_v2.WithMaxRounds(3))
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("capture"))
	if err != nil || calls.Load() != 1 || requests.Load() != 2 || agent_v2.FinalMessage(events).GetContent() != "done" {
		t.Fatalf("跨层调用应执行一次并收尾，实际 mcp=%d model=%d err=%v", calls.Load(), requests.Load(), err)
	}
}

func TestMCPToolMedia_ResponsesToolResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var calls, requests atomic.Int32
	h := newMediaHub(t, ctx, func(rpcReq) map[string]any {
		calls.Add(1)
		return mixedMediaResult()
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []struct {
				Type   string          `json:"type"`
				CallID string          `json:"call_id"`
				Output json.RawMessage `json:"output"`
			} `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) == 1 {
			_, _ = fmt.Fprintf(w, `{"status":"completed","output":[{"type":"function_call","id":"fc_native","call_id":"call_media","name":%q,"arguments":"{}"}]}`, mcpMediaName)
			return
		}
		if len(req.Input) >= 2 {
			call, result := req.Input[len(req.Input)-2], req.Input[len(req.Input)-1]
			var parts []struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Image string `json:"image_url"`
			}
			if err := json.Unmarshal(result.Output, &parts); err != nil {
				t.Errorf("function_call_output 应带有序内容块，实际 %s：%v", result.Output, err)
			}
			if call.Type != "function_call" || call.CallID != "call_media" || result.Type != "function_call_output" || result.CallID != call.CallID ||
				len(parts) != 4 || parts[0].Text != "before image" || parts[1].Type != "input_image" || parts[1].Image != "data:image/png;base64,"+mediaPNG ||
				!strings.Contains(parts[2].Text, "audio/wav") || parts[3].Text != "after audio" {
				t.Errorf("Responses 应把图像和文字绑在原 call_id 上，实际 %+v / %+v", result, parts)
			}
		} else {
			t.Error("Responses 缺少工具调用与结果")
		}
		_, _ = fmt.Fprint(w, `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}`)
	}))
	t.Cleanup(srv.Close)
	ag := agent_v2.NewBasicAgent(agent_v2.WithToolkit(h.Toolkit()), agent_v2.WithName("mcp_media"), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(mediaGenerateOnly{openai.NewResponsesClient("fake", "fake", openai.WithBaseURL(srv.URL))}))
	assertMediaRun(t, ctx, ag, &requests, &calls, 1)
}

type mediaChatMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id"`
}

type mediaChatPart struct {
	Type       string            `json:"type"`
	Text       string            `json:"text"`
	Image      schema.ImageUrl   `json:"image_url"`
	InputAudio schema.InputAudio `json:"input_audio"`
}

func mediaChatServer(t *testing.T, toolCalls string, verify func([]mediaChatMessage)) (string, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []mediaChatMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) == 1 {
			_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","tool_calls":%s},"finish_reason":"tool_calls"}]}`, toolCalls)
			return
		}
		verify(req.Messages)
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &requests
}

func mediaToolCall(id, source string) string {
	return fmt.Sprintf(`{"id":%q,"type":"function","function":{"name":%q,"arguments":%q}}`,
		id, mcpMediaName, fmt.Sprintf(`{"source":%q}`, source))
}

func structuredAudioResult() map[string]any {
	return map[string]any{"structuredContent": json.RawMessage(`{"record":9007199254740993}`),
		"content": []map[string]any{
			{"type": "text", "text": "旧版结构化正文副本"},
			{"type": "audio", "data": mediaWAV, "mimeType": "audio/wav", "annotations": map[string]any{"audience": []string{"assistant"}}},
		}}
}

func assertMediaRun(t *testing.T, ctx context.Context, ag *agent_v2.BasicAgent, requests, calls *atomic.Int32, wantCalls int32) {
	t.Helper()
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("capture"))
	if err != nil || calls.Load() != wantCalls || requests.Load() != 2 || agent_v2.FinalMessage(events).GetContent() != "done" {
		t.Fatalf("跨层调用应按轮执行并收尾，实际 mcp=%d model=%d err=%v", calls.Load(), requests.Load(), err)
	}
}

func TestMCPToolMedia_ChatAttachmentsFollowWholeBatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		audio string
	}{
		{name: "text-only"},
		{name: "openai", audio: mediaWAV},
		{name: "dashscope", audio: "data:audio/wav;base64," + mediaWAV},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var calls atomic.Int32
			h := newMediaHub(t, ctx, func(req rpcReq) map[string]any {
				calls.Add(1)
				args, _ := req.Params["arguments"].(map[string]any)
				switch args["source"] {
				case "image":
					return map[string]any{"content": []map[string]any{{"type": "image", "data": mediaPNG, "mimeType": "image/png"}}}
				case "audio":
					return structuredAudioResult()
				default:
					t.Errorf("跨层工具收到错误实参 %+v", args)
					return nil
				}
			})
			toolCalls := "[" + mediaToolCall("capture-image", "image") + "," + mediaToolCall("capture-audio", "audio") + "]"
			url, requests := mediaChatServer(t, toolCalls, func(msgs []mediaChatMessage) {
				end := len(msgs)
				if tc.audio != "" {
					end--
				}
				if end < 3 || msgs[end-3].Role != "assistant" || msgs[end-2].Role != "tool" || msgs[end-2].ToolCallID != "capture-image" ||
					msgs[end-1].Role != "tool" || msgs[end-1].ToolCallID != "capture-audio" {
					t.Errorf("用户媒体只能在两条配对工具结果之后，实际 %+v", msgs)
					return
				}
				var body string
				if err := json.Unmarshal(msgs[end-1].Content, &body); err != nil || body != `{"record":9007199254740993}` {
					t.Errorf("结构化文本与音频的 call_id 应独立完整，实际 %q %v", body, err)
				}
				if tc.audio == "" {
					if !strings.Contains(string(msgs[end-2].Content), "image/png") || strings.Contains(string(msgs[end-2].Content), mediaPNG) {
						t.Errorf("默认文本模型应收到附件说明，实际 %s", msgs[end-2].Content)
					}
					return
				}
				var parts []mediaChatPart
				if err := json.Unmarshal(msgs[end].Content, &parts); err != nil || msgs[end].Role != "user" || len(parts) != 4 ||
					parts[0].Type != "text" || !strings.Contains(parts[0].Text, `"capture-image"`) || parts[1].Image.Url != "data:image/png;base64,"+mediaPNG ||
					!strings.Contains(parts[2].Text, `"capture-audio"`) || parts[3].Type != "input_audio" || parts[3].InputAudio.Data != tc.audio || parts[3].InputAudio.Format != "wav" {
					t.Errorf("附件未按原调用顺序和媒体类型抵达，实际 %+v，错误 %v", parts, err)
				}
			})
			opts := []openai.ClientOption{openai.WithBaseURL(url)}
			if tc.audio != "" {
				opts = append(opts, openai.WithChatToolResultMedia(openai.ChatToolResultMedia{Images: true, Audio: true}))
			}
			var client model.ChatModel = openai.NewClient("fake", "fake-multimodal", opts...)
			if tc.name == "dashscope" {
				client = qwen.NewClient("fake", "qwen3.8-omni-flash", opts...)
			}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("mcp_media"), agent_v2.WithModel(mediaGenerateOnly{client}),
				agent_v2.WithToolkit(h.Toolkit()), agent_v2.WithMaxRounds(3))
			assertMediaRun(t, ctx, ag, requests, &calls, 2)
		})
	}
}
