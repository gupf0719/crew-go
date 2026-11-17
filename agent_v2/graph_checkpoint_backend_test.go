package agent_v2_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type selectResumeSubgraphBackend struct {
	compose_v2.NoOpOrchestrationHooks
	cp compose_v2.Checkpointer
}

func (h selectResumeSubgraphBackend) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) == "" {
		ctx = compose_v2.WithCheckpointerCtx(ctx, h.cp)
	}
	return ctx, nil
}

func TestGraphCheckpointResumePreservesForeignTaskAfterBackendSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	originalStore := compose_v2.NewInMemoryCheckpointer()
	newStore := compose_v2.NewInMemoryCheckpointer()
	sess := agent_v2.NewRunSession()
	build := func(body compose_v2.NodeFunc) *agent_v2.GraphAgent {
		t.Helper()
		sub, err := compose_v2.NewBuilder().WithName("child").
			AddNode("perform", body).
			AddEdge(compose_v2.StartNode, "perform").AddEdge("perform", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		parent, err := compose_v2.NewBuilder().WithName("parent").
			AddSubgraphNode("child", sub).
			AddEdge(compose_v2.StartNode, "child").AddEdge("child", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		return agent_v2.NewGraphAgent(parent, agent_v2.WithGraphAgentName("workflow"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
				return schema.AssistantMessage("完成")
			}))
	}
	original := build(func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续原任务？"})
		return nil, err
	})
	originalCtx := compose_v2.WithCheckpointerCtx(ctx, originalStore)
	if _, err := original.Run(originalCtx, sess, schema.UserMessage("原任务")); err == nil {
		t.Fatal("原任务应挂起")
	}
	before, err := originalStore.LoadNS(ctx, sess.RunID, "child")
	if err != nil || !before.HasResumableWork() || before.TaskID == "" {
		t.Fatalf("原任务应有独立子档，实际 %v，%v", before, err)
	}
	oldPayload, err := compose_v2.MarshalCheckpoint(before)
	if err != nil {
		t.Fatal(err)
	}

	var calls int
	next := build(func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls++
		return nil, nil
	})
	runCtx := compose_v2.WithCheckpointerCtx(ctx, newStore)
	foreignCtx := compose_v2.WithOrchHooks(runCtx, selectResumeSubgraphBackend{cp: originalStore})
	if _, err := next.Run(foreignCtx, sess, schema.UserMessage("独立任务")); !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) {
		t.Fatalf("首跑应报告目标后端已经有待答档，实际 %v", err)
	}
	failed, err := newStore.LoadNS(ctx, sess.RunID, "")
	if err != nil || !failed.HasResumableWork() || failed.TaskID == "" || failed.TaskID == before.TaskID {
		t.Fatalf("新任务应保留自己的可续父档，实际 %v，%v", failed, err)
	}
	if _, err := next.Resume(foreignCtx, sess.RunID); !errors.Is(err, compose_v2.ErrGraphIDConflict) ||
		errors.Is(err, compose_v2.ErrRunAborted) || calls != 0 {
		t.Fatalf("续跑也应保留冲突子档，模型节点调用 %d 次，错误 %v", calls, err)
	}
	events, err := next.Resume(runCtx, sess.RunID)
	if err != nil || calls != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
		t.Fatalf("回到新任务使用的后端后应能继续运行，实际节点 %d 次，结果 %v，错误 %v", calls, events, err)
	}
	after, err := originalStore.LoadNS(ctx, sess.RunID, "child")
	if err != nil || after == nil {
		t.Fatalf("原任务的子档应保留，实际 %v，%v", after, err)
	}
	newPayload, err := compose_v2.MarshalCheckpoint(after)
	if err != nil || !bytes.Equal(oldPayload, newPayload) {
		t.Fatalf("原任务的子档应完整保留，编码错误 %v", err)
	}
}

type changeResumeOwner struct {
	hook.NoOpHooks
	to string
}

func (h *changeResumeOwner) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return compose_v2.WithOwner(ctx, h.to), nil
}

func TestGraphCheckpointResumeRejectsOwnerChangedAtAgentStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithOwner(compose_v2.WithCheckpointerCtx(ctx, cp), "original")
	var completed int
	m := &fnModel{fn: func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return nil, err
		}
		completed++
		return schema.AssistantMessage("完成"), nil
	}}
	h := &changeResumeOwner{}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("versioned"), agent_v2.WithModel(m), agent_v2.WithHooks(h))
	sess := agent_v2.NewRunSession()
	_, err := a.Run(ctx, sess, schema.UserMessage("任务"))
	id := compactionQuestionID(t, err, "继续？")
	before := checkpointIdentityBytes(t, ctx, cp, sess.RunID, "")
	answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("是")})
	h.to = "other"
	if _, err = a.Resume(ctx, sess.RunID, answers); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("改变恢复主体应产生冲突，实际 %v", err)
	}
	if !bytes.Equal(before, checkpointIdentityBytes(t, ctx, cp, sess.RunID, "")) || completed != 0 {
		t.Fatal("冲突应保留原档和原问题")
	}
	h.to = ""
	events, err := a.Resume(ctx, sess.RunID, answers)
	if err != nil || completed != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
		t.Fatalf("原主体应继续执行，实际次数 %d，事件 %v，错误 %v", completed, events, err)
	}
}

type checkpointOwnerSelector struct {
	compose_v2.Checkpointer
	other *compose_v2.InMemoryCheckpointer
}

func (c checkpointOwnerSelector) LoadAllInSession(ctx context.Context, sid string) ([]compose_v2.NSCheckpoint, error) {
	if compose_v2.GetOwner(ctx) == "other" {
		return c.other.LoadAllInSession(ctx, sid)
	}
	return c.Checkpointer.LoadAllInSession(ctx, sid)
}

func (c checkpointOwnerSelector) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if compose_v2.GetOwner(ctx) == "other" {
		return c.other.DeleteSubtree(ctx, sid, ns, condition...)
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns, condition...)
}

func TestFreshRunCheckpoint_RechecksOwnerChangedAtAgentStart(t *testing.T) {
	original := compose_v2.NewInMemoryCheckpointer()
	other := compose_v2.NewInMemoryCheckpointer()
	cp := checkpointOwnerSelector{Checkpointer: original, other: other}
	ctx := compose_v2.WithOwner(compose_v2.WithCheckpointerCtx(t.Context(), cp), "original")
	sess := agent_v2.NewRunSession()
	saved := &compose_v2.Checkpoint{CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "old"}}, Epoch: "other-task"}
	if err := other.SaveNS(ctx, sess.RunID, "", saved); err != nil {
		t.Fatal(err)
	}
	model := newStubModel([]*schema.Message{schema.AssistantMessage("不应执行")})
	worker := agent_v2.NewBasicAgent(agent_v2.WithModel(model), agent_v2.WithHooks(&changeResumeOwner{to: "other"}))
	if _, err := worker.Run(ctx, sess, schema.UserMessage("新任务")); !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) {
		t.Fatalf("换分区后应阻止覆盖旧任务，实际 %v", err)
	}
	after, err := other.LoadNS(ctx, sess.RunID, "")
	if err != nil || after == nil || after.Epoch != saved.Epoch || model.idx != 0 {
		t.Fatalf("分区里的旧档应原样保留，实际 %v，错误 %v，模型调用 %d 次", after, err, model.idx)
	}
}
