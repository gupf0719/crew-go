package agui_test

import (
	"context"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
)

//go:noinline
func aguiFactoryExplodes() agent_v2.Agent { panic("factory boom") }

// TestHandler_PanicStackOnlyInServerLog：handler 兜住的 panic，栈只进服务端日志，发给前端的 RUN_ERROR 不带栈。
func TestHandler_PanicStackOnlyInServerLog(t *testing.T) {
	logs := &aguiLogRecorder{}
	oldDefault := slog.Default()
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() {
		// log 包的输出要一起还原：换回原来的默认 logger 时 slog 不会把它改回来
		slog.SetDefault(oldDefault)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	h := ownedHandler(func(hook.Hooks) agent_v2.Agent { return aguiFactoryExplodes() })
	events := parseSSE(postAGUI(h, `{"threadId":"c","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String())

	found := false
	for _, ev := range events {
		if ev["type"] != "RUN_ERROR" || ev["code"] != "panic" {
			continue
		}
		found = true
		if msg, _ := ev["message"].(string); strings.Contains(msg, "goroutine ") || strings.Contains(msg, "aguiFactoryExplodes") {
			t.Errorf("发给前端的 RUN_ERROR 不该带栈：%q", msg)
		}
	}
	if !found {
		t.Fatalf("该补发 RUN_ERROR code=panic，got %v", events)
	}
	if n := logs.countStack("agui_test.aguiFactoryExplodes"); n != 1 {
		t.Errorf("服务端该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

type aguiLogRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *aguiLogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *aguiLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec.Clone())
	r.mu.Unlock()
	return nil
}
func (r *aguiLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *aguiLogRecorder) WithGroup(string) slog.Handler      { return r }

func (r *aguiLogRecorder) countStack(site string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "stack" && strings.Contains(a.Value.String(), site) {
				n++
			}
			return true
		})
	}
	return n
}
