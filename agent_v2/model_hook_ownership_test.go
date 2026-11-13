package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type inPlaceModelRedaction struct{ hook.NoOpHooks }

func (inPlaceModelRedaction) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message.Content = "[redacted]"
	delete(out.Message.Annotations, compose_v2.RunIDAnnotationKey)
	return ctx, nil
}

func TestOnModelEnd_InPlaceRewriteKeepsStreamIdentity(t *testing.T) {
	capture := &captureEventsHook{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("redact"),
		agent_v2.WithModel(&chunkStreamModel{contents: []string{"secret", " value"}}),
		agent_v2.WithHooks(inPlaceModelRedaction{}))
	events, err := ag.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("hello"), agent_v2.WithOrchHooks(capture))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "[redacted]" {
		t.Fatalf("最终答复 = %+v", final)
	}
	runID, _ := final.Annotations[compose_v2.RunIDAnnotationKey].(string)
	var raw strings.Builder
	var resets int
	for _, ev := range capture.events {
		switch ev.Type {
		case "chunk":
			raw.WriteString(ev.Content)
		case "reset_stream":
			if ev.Metadata["replacement"] != "[redacted]" || ev.Metadata["run_id"] != runID {
				t.Errorf("替换事件未指向原调用，事件=%+v run_id=%q", ev, runID)
			}
			resets++
		}
	}
	if runID == "" || raw.String() != "secret value" || resets != 1 {
		t.Fatalf("未用原调用身份替换已发片段，run_id=%q chunks=%q resets=%d", runID, raw.String(), resets)
	}
}

type editModelMessages struct {
	hook.NoOpHooks
	replaceElement bool
}

func (h editModelMessages) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if len(in.Messages) == 0 {
		return ctx, nil
	}
	i := len(in.Messages) - 1
	if h.replaceElement {
		cp := *in.Messages[i]
		in.Messages[i] = &cp
	}
	in.Messages[i].Content += " TRANSIENT-HOOK"
	return ctx, nil
}

func TestOnModelStart_EditedViewStaysOutOfCompactionHistory(t *testing.T) {
	for _, tc := range []struct {
		name           string
		replaceElement bool
	}{{"replace_element", true}, {"edit_message", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := compose_v2.WithThreadIDCtx(t.Context(), "conv")
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv"}
			hs := agent_v2.NewInMemoryHistoryStore()
			summary := &countingSummarizer{}
			model := &compactLoopModel{rounds: 60}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(model),
				agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(100),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0),
				agent_v2.WithHooks(editModelMessages{replaceElement: tc.replaceElement}),
				agent_v2.WithAutoCompact(smallWindow(summary)))
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL-INPUT")); err != nil {
				t.Fatal(err)
			}
			if summary.count() == 0 || !strings.Contains(inputText(model.inputs[0]), "TRANSIENT-HOOK") {
				t.Fatal("测试未同时触发本轮改写和压缩")
			}
			all, err := agent_v2.LoadAllHistory(ctx, hs, key)
			if err != nil {
				t.Fatal(err)
			}
			if n := countBoundaries(all); n != 0 {
				t.Fatalf("钩子改写的视图产生了 %d 份跨 Run 边界", n)
			}
			nextModel := &compactLoopModel{}
			next := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(nextModel),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
			if _, err := next.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("NEXT-INPUT")); err != nil {
				t.Fatal(err)
			}
			got := inputText(nextModel.inputs[0])
			if !strings.Contains(got, "ORIGINAL-INPUT") || strings.Contains(got, "TRANSIENT-HOOK") {
				t.Fatal("后续 Run 未恢复原始历史，或看到上一轮的瞬态改写")
			}
		})
	}
}
