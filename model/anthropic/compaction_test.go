package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"testing"

	"crew-go/model/anthropic"
	"crew-go/schema"
)

// TestGenerate_CompactionRequestShape 钉住服务端压缩的请求形状：context_management 的 compact_20260112
// 与 beta 头；不开时两样都没有。
func TestGenerate_CompactionRequestShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []anthropic.ClientOption
		want string // context_management 的规范 JSON；空 = 不该有
	}{
		{"off", nil, ""},
		{"defaults", []anthropic.ClientOption{anthropic.WithCompaction(anthropic.Compaction{})},
			`{"edits":[{"type":"compact_20260112"}]}`},
		{"trigger and instructions", []anthropic.ClientOption{anthropic.WithCompaction(anthropic.Compaction{
			TriggerTokens: 50000, Instructions: "保留用户的偏好与未完成的事"})},
			`{"edits":[{"instructions":"保留用户的偏好与未完成的事","trigger":{"type":"input_tokens","value":50000},"type":"compact_20260112"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, rec := newServer(t, textReply)
			if _, err := newClient(srv, tc.opts...).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")}); err != nil {
				t.Fatalf("Generate: %v", err)
			}
			req := rec.at(t, 0)
			hasBeta := slices.Contains(betas(req.header), "compact-2026-01-12")
			cm, ok := req.body["context_management"]
			if tc.want == "" {
				if ok || hasBeta {
					t.Fatalf("没开压缩却发了 context_management=%v、beta=%v", cm, betas(req.header))
				}
				return
			}
			if !hasBeta {
				t.Errorf("缺 beta 头 compact-2026-01-12：%v", betas(req.header))
			}
			if got := canon(t, cm); got != tc.want {
				t.Errorf("context_management =\n%s\n期望\n%s", got, tc.want)
			}
		})
	}
}

// compactionUsage 是带一次压缩的用量：顶层只算模型答的那一轮，压缩那一步在 iterations 里。
func compactionUsage() map[string]any {
	return map[string]any{"input_tokens": 3000, "output_tokens": 5, "iterations": []any{
		map[string]any{"type": "compaction", "input_tokens": 180000, "output_tokens": 900,
			"cache_read_input_tokens": 150000, "cache_creation_input_tokens": 0},
		map[string]any{"type": "message", "model": "claude-opus-5-5", "input_tokens": 3000, "output_tokens": 5,
			"cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
	}}
}

// TestGenerate_CompactionBlockReplayed：compaction 块原样回放（下一轮 API 据它换掉之前的历史），压缩那一步的
// 用量记在标注里、不混进 Usage（Usage.InputTokens 被当成上下文大小用）。
func TestGenerate_CompactionBlockReplayed(t *testing.T) {
	block := map[string]any{"type": "compaction", "content": "之前聊了部署", "encrypted_content": "enc-1", "signature": "sig-c"}
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			replyJSON(w, http.StatusOK, message([]any{block, map[string]any{"type": "text", "text": "好的"}}, "end_turn", compactionUsage()))
			return
		}
		textReply(w, r, n)
	})
	c := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}))
	msg, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if msg.Content != "好的" {
		t.Errorf("Content = %q，摘要不该混进答复", msg.Content)
	}
	if u := msg.Metadata.Usage; u.InputTokens != 3000 || u.OutputTokens != 5 {
		t.Errorf("Usage = %+v，应只算模型答的那一轮", u)
	}
	want := []anthropic.CompactionUsage{{InputTokens: 180000, OutputTokens: 900, CacheReadInputTokens: 150000}}
	if got := anthropic.Compactions(msg); !slices.Equal(got, want) {
		t.Errorf("Compactions = %+v，期望 %+v", got, want)
	}
	// 标注要经得起存档的 JSON 往返
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var back schema.Message
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if got := anthropic.Compactions(&back); !slices.Equal(got, want) {
		t.Errorf("往返后 Compactions = %+v", got)
	}
	if _, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("q"), &back, schema.UserMessage("再说")}); err != nil {
		t.Fatalf("第二轮 Generate: %v", err)
	}
	wantContent := `[{"content":"之前聊了部署","encrypted_content":"enc-1","signature":"sig-c","type":"compaction"},{"text":"好的","type":"text"}]`
	if got := canon(t, at(rec.at(t, 1).body, "messages", 1, "content")); got != wantContent {
		t.Errorf("回放的 assistant content =\n%s\n期望\n%s", got, wantContent)
	}
}

// TestStream_CompactionBlockReplayed：流式时 compaction 块经 compaction_delta 填满，同样原样回放；排在服务端
// 回退切换点之前也保留（压缩的是这次的输入，跟哪个模型答无关）。
func TestStream_CompactionBlockReplayed(t *testing.T) {
	events := []sse{evMessageStart(nil),
		evBlockStart(0, map[string]any{"type": "compaction", "content": "", "encrypted_content": ""}),
		evDelta(0, map[string]any{"type": "compaction_delta", "content": "之前聊了部署", "encrypted_content": "enc-1"}),
		evBlockStop(0),
		evBlockStart(1, map[string]any{"type": "fallback", "from": map[string]any{"model": "claude-opus-5-5"}, "to": map[string]any{"model": "claude-opus-5"}}),
		evBlockStop(1),
		evBlockStart(2, map[string]any{"type": "text", "text": ""}),
		evDelta(2, map[string]any{"type": "text_delta", "text": "好的"}),
		evBlockStop(2),
		evMessageDelta("end_turn", compactionUsage()),
		evMessageStop(),
	}
	srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			replySSE(w, events...)
			return
		}
		textReply(w, r, n)
	})
	c := newClient(srv, anthropic.WithCompaction(anthropic.Compaction{}))
	reader, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	chunks, err := collect(t, reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("流结束于 %v", err)
	}
	msg := aggregate(chunks)
	if msg.Content != "好的" {
		t.Errorf("Content = %q", msg.Content)
	}
	if got := anthropic.Compactions(msg); len(got) != 1 || got[0].InputTokens != 180000 {
		t.Errorf("Compactions = %+v", got)
	}
	if _, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("q"), msg, schema.UserMessage("再说")}); err != nil {
		t.Fatalf("第二轮 Generate: %v", err)
	}
	want := `[{"content":"之前聊了部署","encrypted_content":"enc-1","type":"compaction"},{"text":"好的","type":"text"}]`
	if got := canon(t, at(rec.at(t, 1).body, "messages", 1, "content")); got != want {
		t.Errorf("回放的 assistant content =\n%s\n期望\n%s", got, want)
	}
}

// TestGenerate_CompactionBlockWithoutUsageStillRecorded：iterations 没报压缩项时，按 compaction 块补一项
// 用量为 0 的记录——至少知道压缩发生过。
func TestGenerate_CompactionBlockWithoutUsageStillRecorded(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusOK, message([]any{
			map[string]any{"type": "compaction", "content": "摘要", "encrypted_content": "enc"},
			map[string]any{"type": "text", "text": "好"},
		}, "end_turn", nil))
	})
	msg, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if got := anthropic.Compactions(msg); !slices.Equal(got, []anthropic.CompactionUsage{{}}) {
		t.Errorf("Compactions = %+v，期望一项用量为 0 的记录", got)
	}
}
