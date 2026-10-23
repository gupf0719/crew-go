package openai_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"crew-go/model/openai"
	"crew-go/schema"
)

// 用量里的推理 token（completion_tokens_details.reasoning_tokens）进 MessageUsage.ThinkTokens，Generate 与流式两条路
// 都解。原来没有任何 client 给 ThinkTokens 赋值，observability 的 gen_ai.usage.reasoning.output_tokens 永远不发。

const usageJSON = `{"prompt_tokens":10,"completion_tokens":30,"total_tokens":40,` +
	`"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":12}}`

func checkUsage(t *testing.T, u *schema.MessageUsage) {
	t.Helper()
	if u == nil {
		t.Fatal("没有解出用量")
	}
	if u.InputTokens != 10 || u.OutputTokens != 30 || u.TotalTokens != 40 || u.CachedInputTokens != 4 || u.ThinkTokens != 12 {
		t.Errorf("用量 = %+v，want input 10 / output 30（含推理）/ total 40 / cached 4 / think 12", *u)
	}
}

func TestUsage_ReasoningTokens_Generate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"答案"},"finish_reason":"stop"}],"usage":` + usageJSON + `}`))
	}))
	defer srv.Close()
	m := openai.NewClient("k", "deepseek-reasoner", openai.WithBaseURL(srv.URL))
	resp, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if resp.Metadata == nil {
		t.Fatal("没有 Metadata")
	}
	checkUsage(t, resp.Metadata.Usage)
}

func TestUsage_ReasoningTokens_Stream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"role":"assistant","content":"答案"},"finish_reason":"stop"}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"choices":[],"usage":`+usageJSON+`}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	m := openai.NewClient("k", "deepseek-reasoner", openai.WithBaseURL(srv.URL))
	sr, err := m.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer sr.Close()
	var got *schema.MessageUsage
	for {
		msg, err := sr.Recv(context.Background())
		if err != nil {
			break
		}
		if msg.Metadata != nil && msg.Metadata.Usage != nil {
			got = msg.Metadata.Usage
		}
	}
	checkUsage(t, got)
}
