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

type checkpointPartitionKey struct{}

type routedGraphCheckpointer struct {
	compose_v2.Checkpointer
	other    compose_v2.Checkpointer
	useOther func(context.Context) bool
}

func (c *routedGraphCheckpointer) partition(ctx context.Context) compose_v2.Checkpointer {
	if c.useOther(ctx) {
		return c.other
	}
	return c.Checkpointer
}

func (c *routedGraphCheckpointer) ResolveCheckpointPartition(ctx context.Context) (compose_v2.Checkpointer, error) {
	return compose_v2.ResolveCheckpointPartition(ctx, c.partition(ctx))
}

func (c *routedGraphCheckpointer) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	return c.partition(ctx).LoadNS(ctx, sid, ns)
}

func (c *routedGraphCheckpointer) LoadAllInSession(ctx context.Context, sid string) ([]compose_v2.NSCheckpoint, error) {
	return c.partition(ctx).LoadAllInSession(ctx, sid)
}

func (c *routedGraphCheckpointer) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	return c.partition(ctx).SaveNS(ctx, sid, ns, cp)
}

func (c *routedGraphCheckpointer) ClaimResume(ctx context.Context, sid, ns string, expected, source *compose_v2.Checkpoint) (*compose_v2.Checkpoint, error) {
	return c.partition(ctx).ClaimResume(ctx, sid, ns, expected, source)
}

func (c *routedGraphCheckpointer) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	return c.partition(ctx).DeleteSubtree(ctx, sid, ns, condition...)
}

type chooseGraphCheckpointPartition struct {
	compose_v2.NoOpOrchestrationHooks
	nodeAt      string
	graphAt     *string
	selectNode  func(context.Context) context.Context
	selectGraph func(context.Context) context.Context
	graphErr    error
}

func (h chooseGraphCheckpointPartition) OnNodeStart(ctx context.Context, node compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if h.nodeAt != "" && string(node) == h.nodeAt && compose_v2.GetGraphNS(ctx) == "" {
		ctx = h.selectNode(ctx)
	}
	return ctx, nil
}

func (h chooseGraphCheckpointPartition) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if h.graphAt != nil && compose_v2.GetGraphNS(ctx) == *h.graphAt {
		if h.selectGraph != nil {
			ctx = h.selectGraph(ctx)
		}
		return ctx, h.graphErr
	}
	return ctx, nil
}

func compilePartitionTestGraph(t *testing.T, b *compose_v2.GraphBuilder) *compose_v2.Graph {
	t.Helper()
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func partitionTestGraph(t *testing.T, name string, fn compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	return compilePartitionTestGraph(t, compose_v2.NewBuilder().WithName(name).AddNode("work", fn).
		AddEdge(compose_v2.StartNode, "work").AddEdge("work", compose_v2.EndNode))
}

func partitionTestAgent(g *compose_v2.Graph) *agent_v2.GraphAgent {
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("partition-workflow"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("完成") }))
}

func partitionQuestionID(t *testing.T, err error) string {
	t.Helper()
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("测试应生成一条真实待答问题，实际 %v", err)
	}
	return interrupt.AllQueries()[0].InterruptID
}

func partitionCheckpointBytes(t *testing.T, ctx context.Context, cp compose_v2.Checkpointer, sid, ns string) []byte {
	t.Helper()
	saved, err := cp.LoadNS(ctx, sid, ns)
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil {
		return nil
	}
	raw, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func partitionAsk(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
	return nil, err
}

func TestGraphCheckpointFreshRechecksLatePartition(t *testing.T) {
	for _, ownerRoute := range []bool{true, false} {
		name := "context_value"
		if ownerRoute {
			name = "owner"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			change := func(ctx context.Context) context.Context {
				return context.WithValue(ctx, checkpointPartitionKey{}, true)
			}
			useOther := func(ctx context.Context) bool {
				selected, _ := ctx.Value(checkpointPartitionKey{}).(bool)
				return selected
			}
			if ownerRoute {
				change = func(ctx context.Context) context.Context { return compose_v2.WithOwner(ctx, "other") }
				useOther = func(ctx context.Context) bool { return compose_v2.GetOwner(ctx) == "other" }
			}
			cp := &routedGraphCheckpointer{
				Checkpointer: compose_v2.NewInMemoryCheckpointer(),
				other:        compose_v2.NewInMemoryCheckpointer(), useOther: useOther,
			}
			ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
			otherCtx := change(ctx)
			sess := agent_v2.NewRunSession()
			old := partitionTestAgent(partitionTestGraph(t, "old", partitionAsk))
			_, err := old.Run(otherCtx, sess, schema.UserMessage("原任务"))
			question := partitionQuestionID(t, err)
			before := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, "")

			var calls int
			next := partitionTestAgent(partitionTestGraph(t, "next", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				calls++
				return nil, nil
			}))
			root := ""
			switcher := chooseGraphCheckpointPartition{graphAt: &root, selectGraph: change}
			_, err = next.Run(compose_v2.WithOrchHooks(ctx, switcher), sess, schema.UserMessage("新任务"))
			after := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, "")
			if err == nil || calls != 0 || len(before) == 0 || !bytes.Equal(before, after) {
				t.Fatalf("首跑晚选分区应在节点执行前拒绝并保留原档，实际 err=%v calls=%d before=%d after=%d",
					err, calls, len(before), len(after))
			}

			events, err := old.Resume(otherCtx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				question: schema.UserMessage("批准"),
			}))
			if err != nil || agent_v2.FinalMessage(events).GetContent() != "完成" {
				t.Fatalf("原分区应能用原问题续跑，实际 err=%v events=%v", err, events)
			}
			events, err = next.Run(compose_v2.WithOrchHooks(ctx, switcher), agent_v2.NewRunSession(), schema.UserMessage("无冲突新任务"))
			if err != nil || calls != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
				t.Fatalf("晚选空分区应继续首次执行，实际 err=%v calls=%d events=%v", err, calls, events)
			}
		})
	}
}

func TestGraphCheckpointResumePreservesLatePartitionDescendant(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "foreign"
		if unknown {
			name = "unknown_task"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			change := func(ctx context.Context) context.Context { return compose_v2.WithOwner(ctx, "other") }
			cp := &routedGraphCheckpointer{
				Checkpointer: compose_v2.NewInMemoryCheckpointer(),
				other:        compose_v2.NewInMemoryCheckpointer(),
				useOther:     func(ctx context.Context) bool { return compose_v2.GetOwner(ctx) == "other" },
			}
			ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
			otherCtx := change(ctx)
			sess := agent_v2.NewRunSession()
			foreign := partitionTestGraph(t, "old", partitionAsk)
			const ns = "child/grandchild"
			foreignRun := func(opts ...compose_v2.RunOption) error {
				base := []compose_v2.RunOption{compose_v2.WithSessionID(sess.RunID), compose_v2.WithRunNS(ns)}
				return compose_v2.Run(otherCtx, foreign, compose_v2.NewChannelManager(), append(base, opts...)...)
			}
			foreignQuestion := partitionQuestionID(t, foreignRun())
			before := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, ns)
			old, err := compose_v2.UnmarshalCheckpoint(before)
			if err != nil {
				t.Fatal(err)
			}
			if unknown {
				old.TaskID = ""
				if err := cp.SaveNS(otherCtx, sess.RunID, ns, old); err != nil {
					t.Fatal(err)
				}
				before = partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, ns)
			}
			var calls int
			child := partitionTestGraph(t, "child", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				calls++
				return nil, nil
			})
			const childID = "child"
			g := compilePartitionTestGraph(t, compose_v2.NewBuilder().WithName("parent").
				AddNode("gate", partitionAsk).
				AddSubgraphNode(childID, child).
				AddEdge(compose_v2.StartNode, "gate").
				AddEdge("gate", childID).
				AddEdge(childID, compose_v2.EndNode))
			parent := partitionTestAgent(g)
			_, err = parent.Run(ctx, sess, schema.UserMessage("父任务"))
			parentQuestion := partitionQuestionID(t, err)
			answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				parentQuestion: schema.UserMessage("批准"),
			})
			switchAt := childID
			resumeCtx := compose_v2.WithOrchHooks(ctx, chooseGraphCheckpointPartition{graphAt: &switchAt, selectGraph: change})
			_, err = parent.Resume(resumeCtx, sess.RunID, answers)
			after := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, ns)
			if !errors.Is(err, compose_v2.ErrGraphIDConflict) || calls != 0 || !bytes.Equal(before, after) {
				t.Fatalf("晚选分区存在异源或未知后代档时应拒绝，必须保留子档，实际 err=%v calls=%d before=%d after=%d",
					err, calls, len(before), len(after))
			}
			events, err := parent.Resume(ctx, sess.RunID, answers)
			if err != nil || calls != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
				t.Fatalf("父任务回到原分区应可续跑，实际 err=%v calls=%d events=%v", err, calls, events)
			}
			if !bytes.Equal(before, partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, ns)) {
				t.Fatal("父任务恢复后不能删除另一分区的档")
			}
			if !unknown {
				if err := foreignRun(compose_v2.WithResumeAnswers(map[string]*schema.Message{
					foreignQuestion: schema.UserMessage("批准"),
				})); err != nil {
					t.Fatalf("外部后代仍应可以回答原问题，实际 %v", err)
				}
			}
		})
	}
}

func TestGraphCheckpointResumeAcceptsOwnChildInLatePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	change := func(ctx context.Context) context.Context {
		return context.WithValue(ctx, checkpointPartitionKey{}, true)
	}
	cp := &routedGraphCheckpointer{
		Checkpointer: compose_v2.NewInMemoryCheckpointer(),
		other:        compose_v2.NewInMemoryCheckpointer(),
		useOther: func(ctx context.Context) bool {
			selected, _ := ctx.Value(checkpointPartitionKey{}).(bool)
			return selected
		},
	}
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
	otherCtx := change(ctx)
	sess := agent_v2.NewRunSession()
	var calls int
	child := partitionTestGraph(t, "child", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := partitionAsk(ctx, chs); err != nil {
			return nil, err
		}
		calls++
		return nil, nil
	})
	const childID = "child"
	g := compilePartitionTestGraph(t, compose_v2.NewBuilder().WithName("parent").
		AddSubgraphNode(childID, child).
		AddEdge(compose_v2.StartNode, childID).
		AddEdge(childID, compose_v2.EndNode))
	parent := partitionTestAgent(g)
	initialCtx := compose_v2.WithOrchHooks(ctx, chooseGraphCheckpointPartition{nodeAt: childID, selectNode: change})
	_, err := parent.Run(initialCtx, sess, schema.UserMessage("父任务"))
	question := partitionQuestionID(t, err)
	parentCP, err := cp.LoadNS(ctx, sess.RunID, "")
	if err != nil || parentCP == nil {
		t.Fatalf("父图应挂起在任务自己的后端，实际 %+v %v", parentCP, err)
	}
	childCP, err := cp.LoadNS(otherCtx, sess.RunID, childID)
	if err != nil || childCP == nil || childCP.TaskID == "" || childCP.TaskID != parentCP.TaskID {
		t.Fatalf("子图应在另一分区保存同任务档，实际 %+v %v", childCP, err)
	}

	const siblingNS = "child#0"
	sibling := partitionTestGraph(t, "sibling", partitionAsk)
	err = compose_v2.Run(otherCtx, sibling, compose_v2.NewChannelManager(),
		compose_v2.WithSessionID(sess.RunID), compose_v2.WithRunNS(siblingNS))
	partitionQuestionID(t, err)
	siblingBefore := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, siblingNS)

	switchAt := childID
	resumeCtx := compose_v2.WithOrchHooks(ctx, chooseGraphCheckpointPartition{graphAt: &switchAt, selectGraph: change})
	events, err := parent.Resume(resumeCtx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		question: schema.UserMessage("批准"),
	}))
	if err != nil || calls != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
		t.Fatalf("应从晚选分区恢复同任务的子档，实际 err=%v calls=%d events=%v", err, calls, events)
	}
	if saved := partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, childID); len(saved) != 0 {
		t.Fatal("成功后应清理本次子图的存档")
	}
	if !bytes.Equal(siblingBefore, partitionCheckpointBytes(t, otherCtx, cp, sess.RunID, siblingNS)) {
		t.Fatal("清理子图不能删除兄弟实例的存档")
	}
}

func TestGraphCheckpointSubgraphCleansOnlySelectedLatePartition(t *testing.T) {
	for _, tt := range []struct {
		name          string
		late          bool
		terminal      bool
		graphStartErr error
	}{
		{name: "graph_start_selects_clear_partition", late: true},
		{name: "node_start_selects_foreign_partition"},
		{name: "node_start_selects_foreign_terminal_marker", terminal: true},
		{name: "graph_start_aborts_in_foreign_partition", graphStartErr: compose_v2.ErrRunAborted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testSubgraphCheckpointPartitions(t, tt.late, tt.terminal, tt.graphStartErr)
		})
	}
}

func testSubgraphCheckpointPartitions(t *testing.T, late, terminal bool, graphStartErr error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	route := func(ctx context.Context) string {
		name, _ := ctx.Value(checkpointPartitionKey{}).(string)
		return name
	}
	selectPartition := func(name string) func(context.Context) context.Context {
		return func(ctx context.Context) context.Context {
			return context.WithValue(ctx, checkpointPartitionKey{}, name)
		}
	}
	cp := &routedGraphCheckpointer{
		Checkpointer: compose_v2.NewInMemoryCheckpointer(),
		other: &routedGraphCheckpointer{
			Checkpointer: compose_v2.NewInMemoryCheckpointer(),
			other:        compose_v2.NewInMemoryCheckpointer(),
			useOther:     func(ctx context.Context) bool { return route(ctx) == "selected" },
		},
		useOther: func(ctx context.Context) bool { return route(ctx) != "" },
	}
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
	foreignCtx := selectPartition("foreign")(ctx)
	sess := agent_v2.NewRunSession()
	const childID, foreignNS = "child", "child/grandchild"
	foreign := partitionTestGraph(t, "foreign", partitionAsk)
	err := compose_v2.Run(foreignCtx, foreign, compose_v2.NewChannelManager(),
		compose_v2.WithSessionID(sess.RunID), compose_v2.WithRunNS(foreignNS))
	partitionQuestionID(t, err)
	foreignBefore := partitionCheckpointBytes(t, foreignCtx, cp, sess.RunID, foreignNS)
	if terminal {
		old, err := compose_v2.UnmarshalCheckpoint(foreignBefore)
		if err != nil {
			t.Fatal(err)
		}
		marker := &compose_v2.Checkpoint{
			Status: compose_v2.CPStatusAborted, GraphID: old.GraphID, TaskID: old.TaskID,
		}
		if err := cp.SaveNS(foreignCtx, sess.RunID, foreignNS, marker); err != nil {
			t.Fatal(err)
		}
		foreignBefore = partitionCheckpointBytes(t, foreignCtx, cp, sess.RunID, foreignNS)
	}

	var calls int
	child := partitionTestGraph(t, "child", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls++
		if late && route(ctx) != "selected" {
			t.Errorf("子图应在 GraphStart 选定的最终分区执行，实际 %q", route(ctx))
		}
		return nil, nil
	})
	g := compilePartitionTestGraph(t, compose_v2.NewBuilder().WithName("parent").AddNode("gate", partitionAsk).
		AddSubgraphNode(childID, child).
		AddEdge(compose_v2.StartNode, "gate").
		AddEdge("gate", childID).
		AddEdge(childID, compose_v2.EndNode))
	parent := partitionTestAgent(g)
	_, err = parent.Run(ctx, sess, schema.UserMessage("父任务"))
	answerID := partitionQuestionID(t, err)

	switchAt := childID
	hook := chooseGraphCheckpointPartition{nodeAt: childID, selectNode: selectPartition("foreign")}
	if late {
		hook.graphAt = &switchAt
		hook.selectGraph = selectPartition("selected")
	}
	if graphStartErr != nil {
		hook.graphAt = &switchAt
		hook.graphErr = graphStartErr
	}
	events, err := parent.Resume(compose_v2.WithOrchHooks(ctx, hook), sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		answerID: schema.UserMessage("批准"),
	}))
	after := partitionCheckpointBytes(t, foreignCtx, cp, sess.RunID, foreignNS)
	if !late {
		if !errors.Is(err, compose_v2.ErrGraphIDConflict) || calls != 0 || !bytes.Equal(foreignBefore, after) {
			t.Fatalf("节点钩子选到异源分区时不能执行子图或删档，实际 err=%v calls=%d before=%d after=%d",
				err, calls, len(foreignBefore), len(after))
		}
		if graphStartErr != nil && !errors.Is(err, graphStartErr) {
			t.Fatalf("存档保护必须保留钩子的终局语义，实际 %v", err)
		}
		if terminal {
			_, err = parent.Run(foreignCtx, sess, schema.UserMessage("复用已有终局标记的 RunID"))
			if !errors.Is(err, compose_v2.ErrGraphIDConflict) || !bytes.Equal(foreignBefore, partitionCheckpointBytes(t, foreignCtx, cp, sess.RunID, foreignNS)) {
				t.Fatalf("首跑也应保留终局标记，实际 %v", err)
			}
		}
		return
	}
	if err != nil || calls != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" ||
		len(foreignBefore) == 0 || !bytes.Equal(foreignBefore, after) {
		t.Fatalf("仅应清理最终选定分区，实际 err=%v calls=%d before=%d after=%d",
			err, calls, len(foreignBefore), len(after))
	}
}

func TestGraphCheckpointBareRestartPreservesOtherPartition(t *testing.T) {
	t.Run("node_start_selects_other_partition", func(t *testing.T) {
		testGraphCheckpointBareRestartPreservesOtherPartition(t, false)
	})
	t.Run("root_graph_start_selects_other_partition", func(t *testing.T) {
		testGraphCheckpointBareRestartPreservesOtherPartition(t, true)
	})
}

func testGraphCheckpointBareRestartPreservesOtherPartition(t *testing.T, atRoot bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	selectOther := func(ctx context.Context) context.Context {
		return context.WithValue(ctx, checkpointPartitionKey{}, true)
	}
	cp := &routedGraphCheckpointer{
		Checkpointer: compose_v2.NewInMemoryCheckpointer(),
		other:        compose_v2.NewInMemoryCheckpointer(),
		useOther: func(ctx context.Context) bool {
			selected, _ := ctx.Value(checkpointPartitionKey{}).(bool)
			return selected
		},
	}
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
	otherCtx := selectOther(ctx)
	const sid, childID, leaf = "bare-restart", "child", "child/grandchild"
	old := partitionTestGraph(t, "old", partitionAsk)
	for _, pctx := range []context.Context{ctx, otherCtx} {
		err := compose_v2.Run(pctx, old, compose_v2.NewChannelManager(),
			compose_v2.WithSessionID(sid), compose_v2.WithRunNS(leaf))
		partitionQuestionID(t, err)
	}
	primaryBefore := partitionCheckpointBytes(t, ctx, cp, sid, leaf)
	foreignBefore := partitionCheckpointBytes(t, otherCtx, cp, sid, leaf)
	if len(primaryBefore) == 0 || len(foreignBefore) == 0 || bytes.Equal(primaryBefore, foreignBefore) {
		t.Fatal("测试需要同一 namespace 的两份独立子档")
	}
	if root := partitionCheckpointBytes(t, ctx, cp, sid, ""); len(root) != 0 {
		t.Fatal("低阶隐式重开必须从空根槽开始")
	}

	var childCalls int
	child := partitionTestGraph(t, "child", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		childCalls++
		return nil, nil
	})
	g := compilePartitionTestGraph(t, compose_v2.NewBuilder().WithName("restarted-root").
		AddSubgraphNode(childID, child).
		AddEdge(compose_v2.StartNode, childID).
		AddEdge(childID, compose_v2.EndNode))
	route := chooseGraphCheckpointPartition{nodeAt: childID, selectNode: selectOther}
	if atRoot {
		root := ""
		route = chooseGraphCheckpointPartition{graphAt: &root, selectGraph: selectOther}
	}
	err := compose_v2.Run(compose_v2.WithOrchHooks(ctx, route), g, compose_v2.NewChannelManager(), compose_v2.WithSessionID(sid))
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || childCalls != 0 {
		t.Fatalf("低阶重开应拒绝选到另一分区的子档，实际 err=%v child_calls=%d", err, childCalls)
	}
	if !bytes.Equal(primaryBefore, partitionCheckpointBytes(t, ctx, cp, sid, leaf)) ||
		!bytes.Equal(foreignBefore, partitionCheckpointBytes(t, otherCtx, cp, sid, leaf)) {
		t.Fatal("拒绝接管时两份旧档都应保留")
	}
}

type chooseAgentCheckpointPartition struct {
	hook.NoOpHooks
	selectPartition func(context.Context) context.Context
}

func (h chooseAgentCheckpointPartition) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return h.selectPartition(ctx), nil
}

func TestGraphCheckpointResumeRejectsAnotherPartitionWithIdenticalSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	primary, other := compose_v2.NewInMemoryCheckpointer(), compose_v2.NewInMemoryCheckpointer()
	change := func(ctx context.Context) context.Context {
		return context.WithValue(ctx, checkpointPartitionKey{}, true)
	}
	cp := &routedGraphCheckpointer{Checkpointer: primary, other: other, useOther: func(ctx context.Context) bool {
		selected, _ := ctx.Value(checkpointPartitionKey{}).(bool)
		return selected
	}}
	ctx = compose_v2.WithBoundCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()
	var writes int
	g := partitionTestGraph(t, "same-image", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := partitionAsk(ctx, chs); err != nil {
			return nil, err
		}
		writes++
		return nil, nil
	})
	ga := partitionTestAgent(g)
	_, err := ga.Run(ctx, sess, schema.UserMessage("等待批准"))
	question := partitionQuestionID(t, err)
	before := partitionCheckpointBytes(t, ctx, primary, sess.RunID, "")
	clone, err := compose_v2.UnmarshalCheckpoint(before)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.SaveNS(ctx, sess.RunID, "", clone); err != nil {
		t.Fatal(err)
	}
	root := ""
	graphSwitch := chooseGraphCheckpointPartition{graphAt: &root, selectGraph: change}
	agentSwitch := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("partition-workflow"),
		agent_v2.WithGraphAgentHooks(chooseAgentCheckpointPartition{selectPartition: change}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("完成") }))
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{question: schema.UserMessage("批准")})
	_, err = ga.Resume(compose_v2.WithOrchHooks(ctx, graphSwitch), sess.RunID, answer)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || writes != 0 {
		t.Fatalf("OnGraphStart 切分区应拒绝续跑且不执行写入：%v, writes=%d", err, writes)
	}
	_, err = agentSwitch.Resume(ctx, sess.RunID, answer)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || writes != 0 {
		t.Fatalf("OnAgentStart 切分区应拒绝续跑且不执行写入：%v, writes=%d", err, writes)
	}
	if !bytes.Equal(before, partitionCheckpointBytes(t, ctx, primary, sess.RunID, "")) ||
		!bytes.Equal(before, partitionCheckpointBytes(t, ctx, other, sess.RunID, "")) {
		t.Fatal("两个物理分区的原始存档都须保持不变")
	}
	events, err := ga.Resume(ctx, sess.RunID, answer)
	if err != nil || writes != 1 || agent_v2.FinalMessage(events).GetContent() != "完成" {
		t.Fatalf("固定在原分区才可续跑一次：%v, writes=%d", err, writes)
	}
	if len(partitionCheckpointBytes(t, ctx, primary, sess.RunID, "")) != 0 ||
		!bytes.Equal(before, partitionCheckpointBytes(t, ctx, other, sess.RunID, "")) {
		t.Fatal("应只清理实际获准续跑的分区")
	}
}
