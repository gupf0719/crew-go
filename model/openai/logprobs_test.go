package openai_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

func TestChatCompletionsGeneratePreservesLogprobs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ABC"},
			"finish_reason":"stop","logprobs":{"content":[
				{"token":"A","bytes":[65],"logprob":-0.125,"top_logprobs":[{"token":"A","logprob":-0.125},{"token":"B","logprob":-2.125}]},
				null,
				{"token":"B","logprob":null},
				{"token":"C","logprob":0}
			],"refusal":[{"token":"ignored","logprob":0}]}}]}`)
	}))
	defer server.Close()
	client := openai.NewClient("k", "m", openai.WithBaseURL(server.URL))
	reply, err := client.Generate(t.Context(), []*schema.Message{schema.UserMessage("内容")},
		model.WithExtraBody(map[string]any{"logprobs": true, "top_logprobs": 2}))
	if err != nil {
		t.Fatal(err)
	}
	if reply.Metadata == nil || len(reply.Metadata.Logprobs) != 4 {
		t.Fatalf("应保留可见词元及缺失位置，得到 %+v", reply.Metadata)
	}
	probs := reply.Metadata.Logprobs
	a := probs[0]
	if a == nil || a.TokenText != "A" || a.Logprob == nil || *a.Logprob != -0.125 || !slices.Equal(a.Bytes, []int32{65}) {
		t.Fatalf("第一个词元的原始类型或值不完整，得到 %+v", a)
	}
	if len(a.TopLogprobs) != 2 || a.TopLogprobs[1] == nil || a.TopLogprobs[1].TokenText != "B" ||
		a.TopLogprobs[1].Logprob == nil || *a.TopLogprobs[1].Logprob != -2.125 {
		t.Fatalf("应保留候选词元的分数，得到 %+v", a.TopLogprobs)
	}
	if probs[1] != nil || probs[2] == nil || probs[2].Logprob != nil ||
		probs[3] == nil || probs[3].Logprob == nil || *probs[3].Logprob != 0 {
		t.Fatalf("缺失概率与有效的零值应有所区分，得到 %+v", probs)
	}
}

func TestChatCompletionsGenerateIgnoresUnsupportedLogprobShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"完整答复"},
			"finish_reason":"stop","logprobs":{"content":[{"token":"A","logprob":"invalid"}]}}]}`)
	}))
	defer server.Close()
	client := openai.NewClient("k", "m", openai.WithBaseURL(server.URL))
	reply, err := client.Generate(context.Background(), []*schema.Message{schema.UserMessage("内容")})
	if err != nil || reply == nil || reply.Content != "完整答复" || reply.Metadata == nil || reply.Metadata.Logprobs != nil {
		t.Fatalf("异常的可选字段不应破坏普通生成，得到 %+v，err=%v", reply, err)
	}
}
