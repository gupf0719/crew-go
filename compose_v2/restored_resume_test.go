package compose_v2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type restoredResumeHooks struct {
	compose_v2.NoOpOrchestrationHooks
	childStarts []int
}

func (h *restoredResumeHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) == "child" {
		h.childStarts = append(h.childStarts, compose_v2.RestoredCheckpointVersion(ctx))
	}
	return ctx, nil
}

func TestRestoredPhaseQuestion_UsesOnlyCurrentGraph(t *testing.T) {
	child := compose_v2.NewBuilder()
	var childVersions []int
	var accepted bool
	child.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		childVersions = append(childVersions, compose_v2.RestoredCheckpointVersion(ctx))
		if id, _ := compose_v2.RestoredPhaseQuestion(compose_v2.EnterPhase(ctx, "other")); id != "" {
			return nil, errors.New("another phase can see the restored question")
		}
		phase := compose_v2.WithAddressedResumeOnly(compose_v2.EnterPhase(ctx, "summary"))
		id, answered := compose_v2.RestoredPhaseQuestion(phase)
		if answered != nil && answered() {
			return nil, errors.New("querying a restored question consumed its answer")
		}
		answer, err := compose_v2.Ask(phase, &compose_v2.AskQuery{Question: "confirm child?"})
		if err != nil {
			return nil, err
		}
		accepted = answer.Content == "yes" && id != "" && answered != nil && answered()
		return nil, nil
	})
	child.AddEdge(compose_v2.StartNode, "ask")
	child.AddEdge("ask", compose_v2.EndNode)
	sub, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := compose_v2.NewBuilder()
	parent.AddSubgraphNode("child", sub)
	parent.AddEdge(compose_v2.StartNode, "child")
	parent.AddEdge("child", compose_v2.EndNode)
	graph, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const sid = "restore-child"
	hooks := &restoredResumeHooks{}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithOrchHooks(ctx, hooks)
	run := func(answers map[string]*schema.Message) error {
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
			compose_v2.WithSessionID(sid), compose_v2.WithResumeAnswers(answers))
	}
	err = run(nil)
	interrupted, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupted.AllQueries()) != 1 {
		t.Fatalf("预期子图挂起，实际 %v", err)
	}
	saved, err := cp.LoadNS(ctx, sid, "child")
	if err != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
		t.Fatalf("子图存档 %+v，错误 %v", saved, err)
	}
	id := interrupted.AllQueries()[0].InterruptID
	if id != saved.PendingInterrupts[0].InterruptID {
		t.Fatalf("父图收到的问题 %q 与子图存档不符", id)
	}
	if err := run(map[string]*schema.Message{id: schema.UserMessage("yes")}); err != nil || !accepted {
		t.Fatalf("子图未通过本图存档恢复答复，accepted=%v err=%v", accepted, err)
	}
	if len(hooks.childStarts) != 2 || hooks.childStarts[0] != 0 || hooks.childStarts[1] != 0 {
		t.Fatalf("子图开始时继承了父图的恢复元数据，实际 %v", hooks.childStarts)
	}
	if len(childVersions) != 2 || childVersions[0] != 0 || childVersions[1] != saved.SchemaVer {
		t.Fatalf("子图节点没有使用自己存档的版本，实际 %v，存档版本 %d", childVersions, saved.SchemaVer)
	}
}
