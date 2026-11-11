package agent_v2_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

func TestHandoffActive_FailedGraphOutputDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st := store.NewInMemoryStore()
	ctx = compose_v2.WithThreadIDCtx(store.WithContext(ctx, st), "thread")
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithHandoffTransfer(),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc", "billing")})))
	billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("done")})))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)
	g := hoHostGraph(t, "handoff-outer-output", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := agent_v2.RunSubAgent(ctx, "support", h, schema.UserMessage("查账单"))
		return nil, err
	})
	host := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("outer"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { panic("failed to construct outer output") }))
	_, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("start"))
	if err == nil {
		t.Fatal("外层读取器异常应使 Run 失败")
	}
	active, lookupErr := st.Get(ctx, store.NS("__handoff_active__", "support", "thread"), "active")
	if lookupErr != nil || active != nil {
		t.Fatalf("外层 Run 失败时写入了 active: item=%+v err=%v runErr=%v", active, lookupErr, err)
	}
}

func TestHandoffActive_DirectChildResumeDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	st := store.NewInMemoryStore()
	ctx = compose_v2.WithThreadIDCtx(store.WithContext(ctx, st), "thread")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithHandoffTransfer(),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc", "billing")})))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("support"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", doAsker(t, "billing", "确认吗？"))
	g := hoHostGraph(t, "handoff-child-direct-resume", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := agent_v2.RunSubAgent(ctx, "support", h, schema.UserMessage("查账单"))
		return nil, err
	})
	const sid = "parent-task"
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("子图应触发挂起: %v", err)
	}
	entries, err := cp.ListPending(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	var childNS string
	for _, entry := range entries {
		if strings.HasPrefix(entry.NS, "support#") && !strings.Contains(entry.NS, "/") && len(entry.Checkpoint.Signals) != 0 {
			childNS = entry.NS
		}
	}
	if childNS == "" {
		t.Fatalf("子图存档应包含转交信号: %+v", entries)
	}
	if active, err := st.Get(ctx, store.NS("__handoff_active__", "support", "thread"), "active"); err != nil || active != nil {
		t.Fatalf("子图恢复前写入了 active: %+v %v", active, err)
	}
	if _, err := h.Resume(compose_v2.WithGraphNS(ctx, childNS), sid,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatalf("单独恢复子图应成功: %v", err)
	}
	if parent, err := cp.LoadNS(ctx, sid, ""); err != nil || parent == nil || !parent.HasResumableWork() {
		t.Fatalf("外层图应仍然挂起: %+v %v", parent, err)
	}
	if active, err := st.Get(ctx, store.NS("__handoff_active__", "support", "thread"), "active"); err != nil || active != nil {
		t.Fatalf("单独恢复子图时写入了外层任务的 active: %+v %v", active, err)
	}
}
