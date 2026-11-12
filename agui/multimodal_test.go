package agui_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model/openai"
)

func TestHandlerMultimodalReachesChatCompletions(t *testing.T) {
	type wireMessage struct {
		Role    string
		Content json.RawMessage
	}
	inputs := make(chan []wireMessage, 2)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct{ Messages []wireMessage }
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "invalid wire request", http.StatusBadRequest)
			return
		}
		inputs <- body.Messages
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer remote.Close()
	provider := openai.NewClient("mock-key", "vision", openai.WithBaseURL(remote.URL))
	handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("vision"), agent_v2.WithHooks(hooks), agent_v2.WithModel(provider))
	})
	part := func(kind, sourceType, value, mediaType string) map[string]any {
		return map[string]any{"type": kind, "source": map[string]any{"type": sourceType, "value": value, "mimeType": mediaType}}
	}
	image := part("image", "url", "https://example.com/pic.png", "image/png")
	audio := part("audio", "data", "UklGRg==", "audio/wav")
	pdf := part("document", "data", "JVBERg==", "application/pdf")
	events := parseSSE(postAGUI(handler, runBody(t, "media", "r1",
		msg("user", []any{image}), msg("assistant", "收到图片"),
		msg("user", []any{map[string]any{"type": "text", "text": "比较媒体"}, audio, pdf}))).Body.String())
	if code := findErrorCode(events); code != "" {
		t.Fatalf("AG-UI 请求失败: %s", code)
	}
	var wire []wireMessage
	select {
	case wire = <-inputs:
	default:
		t.Fatal("没有调用模型 HTTP 入口")
	}
	var users [][]map[string]any
	for _, message := range wire {
		if message.Role != "user" {
			continue
		}
		var parts []map[string]any
		if err := json.Unmarshal(message.Content, &parts); err != nil {
			t.Fatalf("用户消息应为内容数组: %s, %v", message.Content, err)
		}
		users = append(users, parts)
	}
	if len(users) != 2 || len(users[0]) != 1 || users[0][0]["type"] != "image_url" {
		t.Fatalf("前文的纯媒体消息没有送到模型: %+v", users)
	}
	if got := users[0][0]["image_url"]; got.(map[string]any)["url"] != "https://example.com/pic.png" {
		t.Fatalf("图片 URL 错误: %v", got)
	}
	var types []any
	for _, p := range users[1] {
		types = append(types, p["type"])
	}
	if !slices.Equal(types, []any{"text", "input_audio", "file"}) ||
		users[1][0]["text"] != "比较媒体" ||
		users[1][1]["input_audio"].(map[string]any)["data"] != "UklGRg==" ||
		users[1][2]["file"].(map[string]any)["file_data"] != "data:application/pdf;base64,JVBERg==" {
		t.Fatalf("媒体 wire 字段丢失: %+v", users[1])
	}
	events = parseSSE(postAGUI(handler, runBody(t, "invalid-media", "r2",
		msg("user", []any{part("audio", "data", "!!!", "audio/wav")}))).Body.String())
	if code := findErrorCode(events); code == "" || len(inputs) != 0 {
		t.Fatalf("非法媒体进入了模型，code=%q", code)
	}
}
