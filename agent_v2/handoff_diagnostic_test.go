package agent_v2_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestHandoffExpectedTransferDiagnosesOnlyMissingOptIn(t *testing.T) {
	var logs bytes.Buffer
	var level slog.LevelVar
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: &level})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	missing := &grantPeekModel{reply: schema.AssistantMessage("本次直接作答")}
	shared := &idEmbedShell{BasicAgent: agent_v2.NewBasicAgent(agent_v2.WithName("shared-model"), agent_v2.WithModel(missing))}
	enabled := &grantPeekModel{reply: schema.AssistantMessage("本次直接作答")}
	configured := agent_v2.NewBasicAgent(agent_v2.WithName("configured-model"),
		agent_v2.WithModel(enabled), agent_v2.WithHandoffTransfer())
	build := func(initial string) *agent_v2.HandoffAgent {
		return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial(initial),
			agent_v2.WithExpectedHandoffTransfer("dispatcher", "configured")).
			AddAgent("dispatcher", shared).AddAgent("endpoint", shared).AddAgent("configured", configured)
	}
	run := func(h *agent_v2.HandoffAgent) error {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		session := agent_v2.NewRunSession()
		events, err := h.Run(ctx, session, schema.UserMessage("你好"))
		if err != nil {
			return err
		}
		if final := agent_v2.FinalMessage(events); final == nil || final.Content != "本次直接作答" || !h.State(session).Done {
			return fmt.Errorf("final=%v state=%+v", final, h.State(session))
		}
		return nil
	}

	for _, initial := range []string{"endpoint", "configured"} {
		if err := run(build(initial)); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(logs.String(), "WithHandoffTransfer") {
		t.Fatalf("合法终点和已装工具的候选直接作答时应保持安静: %s", logs.String())
	}
	h := build("dispatcher")
	level.Set(slog.LevelError)
	if err := run(h); err != nil {
		t.Fatal(err)
	}
	level.Set(slog.LevelWarn)
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- run(h)
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if got := logs.String(); strings.Count(got, "WithHandoffTransfer") != 1 || !strings.Contains(got, "level=WARN") ||
		!strings.Contains(got, "agent=support candidate=dispatcher tool=transfer_to_agent") {
		t.Fatalf("应仅按注册名报告一次漏配: %s", got)
	}
	if slices.Contains(missing.seenTools(), agent_v2.HandoffTransferToolName) ||
		!slices.Contains(enabled.seenTools(), agent_v2.HandoffTransferToolName) {
		t.Fatalf("送模工具列表与配置不一致: shared=%v configured=%v", missing.seenTools(), enabled.seenTools())
	}
}
