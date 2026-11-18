package mcp_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model/openai"
	"crew-go/schema"
	"crew-go/tool"
)

// mediaCheckpoint 持久化走生产编解码，隔开 Resume 和首轮的 Go 对象。
type mediaCheckpoint struct {
	*compose_v2.InMemoryCheckpointer
	saves atomic.Int32
}

func (p *mediaCheckpoint) SaveNS(ctx context.Context, session, ns string, cp *compose_v2.Checkpoint) error {
	raw, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		return err
	}
	decoded, err := compose_v2.UnmarshalCheckpoint(raw)
	if err != nil {
		return err
	}
	p.saves.Add(1)
	return p.InMemoryCheckpointer.SaveNS(ctx, session, ns, decoded)
}

func TestMCPToolMedia_ResumeReplaysStructuredAudioFromCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	var calls atomic.Int32
	h := newMediaHub(t, ctx, func(rpcReq) map[string]any {
		calls.Add(1)
		return structuredAudioResult()
	})
	url, requests := mediaChatServer(t, "["+mediaToolCall("recording", "audio")+"]", func(msgs []mediaChatMessage) {
		if len(msgs) < 2 {
			t.Error("续跑后缺少工具结果和附件")
			return
		}
		result, media := msgs[len(msgs)-2], msgs[len(msgs)-1]
		var text string
		var parts []mediaChatPart
		textErr := json.Unmarshal(result.Content, &text)
		partErr := json.Unmarshal(media.Content, &parts)
		if result.ToolCallID != "recording" || textErr != nil || text != `{"record":9007199254740993}` ||
			partErr != nil || media.Role != "user" || len(parts) != 2 || !strings.Contains(parts[0].Text, `"recording"`) ||
			parts[1].InputAudio.Data != mediaWAV {
			t.Errorf("存档续跑应保留 JSON 精度、原调用 ID 和完整音频，实际 %q %+v %v %v", text, parts, textErr, partErr)
		}
	})
	llm := openai.NewClient("fake", "gpt-audio", openai.WithBaseURL(url), openai.WithChatToolResultMedia(openai.ChatToolResultMedia{Audio: true}))
	middleware := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
			result, err := next(ctx, name, args)
			if err != nil {
				return nil, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认使用录音"}); err != nil {
				return nil, err
			}
			return result, nil
		}
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mcp_media_resume"), agent_v2.WithToolkit(h.Toolkit()),
		agent_v2.WithModel(mediaGenerateOnly{llm}), agent_v2.WithToolMiddleware(middleware), agent_v2.WithMaxRounds(3))
	cp := &mediaCheckpoint{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("capture"))
	paused, ok := compose_v2.IsInterrupt(err)
	if !ok || calls.Load() != 1 || requests.Load() != 1 || cp.saves.Load() == 0 {
		t.Fatalf("首跑应在一次远端调用之后持久挂起，实际 mcp=%d model=%d saves=%d err=%v", calls.Load(), requests.Load(), cp.saves.Load(), err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{paused.Query.InterruptID: schema.UserMessage("确认")}))
	if err != nil || calls.Load() != 1 || requests.Load() != 2 || agent_v2.FinalMessage(events).GetContent() != "done" {
		t.Fatalf("续跑应取回原媒体，工具只执行一次，实际 mcp=%d model=%d err=%v", calls.Load(), requests.Load(), err)
	}
}

type mediaRedactionHook struct {
	hook.NoOpHooks
	replaceResult bool
}

func (h mediaRedactionHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if h.replaceResult {
		// 即使说明未变，替换整个结果也应收走原附件。
		out.Result = &schema.ToolCallResult{Success: true, Output: out.Result.Output}
	}
	return ctx, nil
}

func (h mediaRedactionHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if !h.replaceResult {
		for _, msg := range in.Messages {
			if msg.Role == schema.RoleTool {
				msg.ContentBlocks[0].Toolresult.Content = "redacted"
			}
		}
	}
	return ctx, nil
}

func TestMCPToolMedia_FallbackRespectsRewritesAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []agent_v2.Option
		result func() map[string]any
		want   string
	}{
		{name: "replace whole result", opts: []agent_v2.Option{agent_v2.WithHooks(mediaRedactionHook{replaceResult: true})},
			result: structuredAudioResult, want: `{"record":9007199254740993}`},
		{name: "rewrite nested text", opts: []agent_v2.Option{agent_v2.WithHooks(mediaRedactionHook{})},
			result: structuredAudioResult, want: "redacted"},
		{name: "original byte budget", opts: []agent_v2.Option{agent_v2.WithToolResultMaxBytes(40)},
			result: structuredAudioResult, want: `{"record":9007199254740993}`},
		{name: "mislabeled and malformed media", result: func() map[string]any {
			return map[string]any{"content": []map[string]any{
				{"type": "audio", "data": mediaPNG, "mimeType": "audio/wav"},
				{"type": "image", "data": "???", "mimeType": "image/png"},
				{"type": "text", "text": "available"},
			}}
		}, want: "available"},
		{name: "user-only media", result: func() map[string]any {
			return map[string]any{"content": []map[string]any{{
				"type": "audio", "data": mediaWAV, "mimeType": "audio/wav",
				"annotations": map[string]any{"audience": []string{"user"}},
			}}}
		}, want: "audio/wav"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var calls atomic.Int32
			h := newMediaHub(t, ctx, func(rpcReq) map[string]any {
				calls.Add(1)
				return tc.result()
			})
			url, requests := mediaChatServer(t, "["+mediaToolCall("recording", "audio")+"]", func(msgs []mediaChatMessage) {
				if len(msgs) == 0 {
					t.Error("送模请求缺少消息")
					return
				}
				last := msgs[len(msgs)-1]
				var body string
				if err := json.Unmarshal(last.Content, &body); err != nil || last.Role != "tool" || last.ToolCallID != "recording" ||
					!strings.Contains(body, tc.want) || strings.Contains(body, mediaWAV) || strings.Contains(body, mediaPNG) {
					t.Errorf("降级结果应只保留正确正文与原调用配对，实际 %+v %v", last, err)
				}
			})
			llm := openai.NewClient("fake", "gpt-audio", openai.WithBaseURL(url),
				openai.WithChatToolResultMedia(openai.ChatToolResultMedia{Images: true, Audio: true}))
			opts := []agent_v2.Option{agent_v2.WithName("mcp_media_fallback"), agent_v2.WithToolkit(h.Toolkit()),
				agent_v2.WithModel(mediaGenerateOnly{llm}), agent_v2.WithMaxRounds(3)}
			ag := agent_v2.NewBasicAgent(append(opts, tc.opts...)...)
			assertMediaRun(t, ctx, ag, requests, &calls, 1)
		})
	}
}
