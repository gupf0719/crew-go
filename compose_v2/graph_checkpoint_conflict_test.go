package compose_v2_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type graphConflictStore struct {
	*compose_v2.InMemoryCheckpointer
	saves   atomic.Int32
	deletes atomic.Int32
}

func (s *graphConflictStore) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	s.saves.Add(1)
	return s.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (s *graphConflictStore) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	s.deletes.Add(1)
	return s.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns, condition...)
}

type graphConflictHooks struct {
	compose_v2.NoOpOrchestrationHooks
	starts int
	ends   int
}

func (h *graphConflictHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.starts++
	return ctx, nil
}

func (h *graphConflictHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	h.ends++
	return ctx
}

type graphConflictCalls struct {
	question string
	answer   string
	ask      int
	safe     int
	wrong    int
}

func buildGraphConflictFixture(t *testing.T, calls *graphConflictCalls, nodes []compose_v2.NodeID, next compose_v2.NodeID, options func(*compose_v2.GraphBuilder)) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder()
	options(b)
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls.ask++
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: calls.question})
		if err != nil {
			return nil, err
		}
		calls.answer = answer.Content
		return nil, nil
	})
	for _, node := range nodes {
		b.AddNode(node, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			switch node {
			case "safe":
				calls.safe++
			case "wrong":
				calls.wrong++
			}
			return nil, nil
		}).AddEdge(node, compose_v2.EndNode)
	}
	b.AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", next)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func graphConflictCheckpoint(t *testing.T, ctx context.Context, cp compose_v2.Checkpointer, sid, ns string) (*compose_v2.Checkpoint, []byte) {
	t.Helper()
	saved, err := cp.LoadNS(ctx, sid, ns)
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil {
		return nil, nil
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	return saved, wire
}

func graphConflictAnswer(t *testing.T, err error, question string) (string, compose_v2.RunOption) {
	t.Helper()
	interrupted, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupted.AllQueries()) != 1 {
		t.Fatalf("应留下一条可答问题，实际 %v", err)
	}
	query := interrupted.AllQueries()[0]
	if query.Question != question || query.InterruptID == "" {
		t.Fatalf("问题和 ID 应来自原图，实际 %+v", query)
	}
	return query.InterruptID, compose_v2.WithResumeAnswers(map[string]*schema.Message{
		query.InterruptID: schema.UserMessage("approve-original"),
	})
}

func requireGraphConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) ||
		errors.Is(err, compose_v2.ErrRunAborted) || errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Errorf("冲突应返回可识别的身份错误并保留旧档，实际 %v", err)
	}
}

func TestT503GraphCheckpointRejectsChangedStructure(t *testing.T) {
	for _, tt := range []struct {
		name     string
		options  func(*compose_v2.GraphBuilder)
		oldNodes []compose_v2.NodeID
		newNodes []compose_v2.NodeID
	}{
		{
			"named_changed_nodes", func(b *compose_v2.GraphBuilder) { b.WithName("workflow") },
			[]compose_v2.NodeID{"safe"}, []compose_v2.NodeID{"wrong"},
		},
		{
			"anonymous_changed_edge", func(*compose_v2.GraphBuilder) {},
			[]compose_v2.NodeID{"safe", "wrong"}, []compose_v2.NodeID{"safe", "wrong"},
		},
		{
			"prefixed_changed_edge", func(b *compose_v2.GraphBuilder) { b.WithIdentityPrefix("workflow") },
			[]compose_v2.NodeID{"safe", "wrong"}, []compose_v2.NodeID{"safe", "wrong"},
		},
	} {
		for _, mode := range []struct {
			name     string
			explicit bool
		}{{"auto_load", false}, {"explicit_snapshot", true}} {
			t.Run(tt.name+"/"+mode.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				cp := &graphConflictStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
				const sid = "graph-structure"
				opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
				original := &graphConflictCalls{question: "批准原流程？"}
				other := &graphConflictCalls{question: "批准新流程？"}
				oldGraph := buildGraphConflictFixture(t, original, tt.oldNodes, "safe", tt.options)
				newGraph := buildGraphConflictFixture(t, other, tt.newNodes, "wrong", tt.options)
				if oldGraph.ID() != newGraph.ID() {
					t.Fatalf("两张图的公开 Graph.ID 应相同，实际 %q / %q", oldGraph.ID(), newGraph.ID())
				}
				err := compose_v2.Run(ctx, oldGraph, compose_v2.NewChannelManager(), opts...)
				id, answer := graphConflictAnswer(t, err, original.question)
				saved, before := graphConflictCheckpoint(t, ctx, cp, sid, "")
				if saved == nil || len(saved.PendingInterrupts) != 1 || saved.PendingInterrupts[0].InterruptID != id {
					t.Fatal("原流程的待答现场未写入存档")
				}
				saves, deletes := cp.saves.Load(), cp.deletes.Load()
				hooks := &graphConflictHooks{}
				try := append(opts, answer, compose_v2.WithHooks(hooks))
				if mode.explicit {
					try = append(try, compose_v2.WithResumeFrom(saved))
				}
				requireGraphConflict(t, compose_v2.Run(ctx, newGraph, compose_v2.NewChannelManager(), try...))
				if other.ask != 0 || other.wrong != 0 || other.answer != "" || hooks.starts != 0 || hooks.ends != 0 {
					t.Errorf("新图应在钩子与答案消费之前拒绝，节点=%+v 钩子=%+v", other, hooks)
				}
				_, after := graphConflictCheckpoint(t, ctx, cp, sid, "")
				if cp.saves.Load() != saves || cp.deletes.Load() != deletes || !bytes.Equal(before, after) {
					t.Fatalf("冲突应零写删并原样保存旧档，写入=%d 删除=%d 同档=%t",
						cp.saves.Load()-saves, cp.deletes.Load()-deletes, bytes.Equal(before, after))
				}
				// 模拟进程重建图后，用原答案继续。
				rebuilt := buildGraphConflictFixture(t, original, tt.oldNodes, "safe", tt.options)
				if err := compose_v2.Run(ctx, rebuilt, compose_v2.NewChannelManager(), append(opts, answer)...); err != nil {
					t.Fatalf("重建原图应该可以继续，实际 %v", err)
				}
				if original.answer != "approve-original" || original.safe != 1 || original.wrong != 0 {
					t.Fatalf("原图应只跑原分支一次并消费原答案，实际 %+v", original)
				}
				if found, _ := graphConflictCheckpoint(t, ctx, cp, sid, ""); found != nil {
					t.Fatal("原图成功后应清理旧档")
				}
			})
		}
	}
}

func TestT503GraphCheckpointConflictWithPreRestoredManager(t *testing.T) {
	for _, mode := range []struct {
		name     string
		explicit bool
	}{{"auto_load", false}, {"explicit_foreign_checkpoint", true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := &graphConflictStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
			original := &graphConflictCalls{question: "批准来源流程？"}
			other := &graphConflictCalls{question: "批准目标流程？"}
			source := buildGraphConflictFixture(t, original, []compose_v2.NodeID{"safe"}, "safe",
				func(b *compose_v2.GraphBuilder) { b.WithName("source-graph") })
			target := buildGraphConflictFixture(t, other, []compose_v2.NodeID{"wrong"}, "wrong",
				func(b *compose_v2.GraphBuilder) { b.WithName("target-graph") })
			const sourceID, targetID = "source-task", "target-task"
			_, answer := graphConflictAnswer(t, compose_v2.Run(ctx, source, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sourceID)), original.question)
			_, targetAnswer := graphConflictAnswer(t, compose_v2.Run(ctx, target, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(targetID)), other.question)
			sourceCp, sourceBefore := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
			targetCp, targetBefore := graphConflictCheckpoint(t, ctx, cp, targetID, "")
			if sourceCp == nil || targetCp == nil {
				t.Fatal("应同时存在源和目标档")
			}
			cm := compose_v2.NewChannelManager()
			if err := cm.Restore(sourceCp); err != nil {
				t.Fatal(err)
			}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp)}
			if mode.explicit {
				// 显式传入来源档，但目标槽属于传入的目标图。后端同名守卫拦不住错写。
				opts = append(opts, compose_v2.WithSessionID(targetID), compose_v2.WithResumeFrom(sourceCp))
			} else {
				opts = append(opts, compose_v2.WithSessionID(sourceID))
			}
			saves, deletes := cp.saves.Load(), cp.deletes.Load()
			hooks := &graphConflictHooks{}
			requireGraphConflict(t, compose_v2.Run(ctx, target, cm, append(opts, answer, compose_v2.WithHooks(hooks))...))
			_, sourceAfter := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
			_, targetAfter := graphConflictCheckpoint(t, ctx, cp, targetID, "")
			if cp.saves.Load() != saves || cp.deletes.Load() != deletes ||
				!bytes.Equal(sourceBefore, sourceAfter) || !bytes.Equal(targetBefore, targetAfter) {
				t.Errorf("身份冲突后预恢复的 cm 不得修改源或目标，写入=%d 删除=%d 源同档=%t 目标同档=%t",
					cp.saves.Load()-saves, cp.deletes.Load()-deletes,
					bytes.Equal(sourceBefore, sourceAfter), bytes.Equal(targetBefore, targetAfter))
			}
			if hooks.starts != 0 || hooks.ends != 0 || other.ask != 1 || other.answer != "" || other.wrong != 0 {
				t.Errorf("冲突前不应触发新执行，节点=%+v 钩子=%+v", other, hooks)
			}
			if t.Failed() {
				return
			}
			if err := compose_v2.Run(ctx, target, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
				compose_v2.WithSessionID(targetID), targetAnswer); err != nil || other.wrong != 1 || other.answer != "approve-original" {
				t.Fatalf("原目标仍必须用自己的答案位正常执行，实际 %+v err=%v", other, err)
			}
			if found, _ := graphConflictCheckpoint(t, ctx, cp, sourceID, ""); found == nil {
				t.Fatal("成功清理目标任务不得删除同一后端中的来源任务")
			}
			if err := compose_v2.Run(ctx, source, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
				compose_v2.WithSessionID(sourceID), answer); err != nil || original.safe != 1 || original.answer != "approve-original" {
				t.Fatalf("原来源仍必须用自己的答案位正常执行，实际 %+v err=%v", original, err)
			}
		})
	}
}

func TestT503GraphCheckpointCompatibleSourceRejectsForeignManager(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sameGraph bool
	}{{"different_graph", false}, {"same_graph_other_task", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := &graphConflictStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
			state := compose_v2.LastValue[string]("proposal")
			const sourceID, foreignID = "verified-task", "foreign-manager"
			const question = "批准当前任务？"
			var accepted string
			var hasProposal bool
			build := func(name string) *compose_v2.Graph {
				b := compose_v2.NewBuilder().WithName(name)
				compose_v2.WithChannel(b, state)
				b.AddNode("ask", func(ctx context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
					if err != nil {
						return nil, err
					}
					accepted = answer.Content
					_, hasProposal = compose_v2.Get(channels, state)
					return nil, nil
				})
				graph, err := b.AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
				if err != nil {
					t.Fatal(err)
				}
				return graph
			}
			target := build("verified")
			foreign := target
			if !tc.sameGraph {
				foreign = build("foreign")
			}
			load := func(sid string) []compose_v2.RunOption {
				return []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
			}
			_, answer := graphConflictAnswer(t, compose_v2.Run(ctx, target, compose_v2.NewChannelManager(), load(sourceID)...), question)
			seed := compose_v2.WithInitialWrites(compose_v2.Write(state, "alien-proposal"))
			graphConflictAnswer(t, compose_v2.Run(ctx, foreign, compose_v2.NewChannelManager(), append(load(foreignID), seed)...), question)
			source, sourceBefore := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
			alien, alienBefore := graphConflictCheckpoint(t, ctx, cp, foreignID, "")
			if source == nil || alien == nil || len(source.Channels) != 0 || len(alien.Channels) != 1 ||
				source.TaskID == alien.TaskID || (source.GraphID == alien.GraphID) != tc.sameGraph {
				t.Fatal("正确档不写提案，外来档应留下另一项提案")
			}

			restore := func(from *compose_v2.Checkpoint) *compose_v2.ChannelManager {
				cm := compose_v2.NewChannelManager()
				if err := compose_v2.RegisterChannel(cm, state); err != nil {
					t.Fatal(err)
				}
				if err := cm.Restore(from); err != nil {
					t.Fatal(err)
				}
				return cm
			}
			manager := restore(alien)
			hooks := &graphConflictHooks{}
			saves, deletes := cp.saves.Load(), cp.deletes.Load()
			opts := append(load(sourceID), compose_v2.WithResumeFrom(source), answer)
			requireGraphConflict(t, compose_v2.Run(ctx, target, manager, append(opts, compose_v2.WithHooks(hooks))...))
			_, sourceAfter := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
			_, alienAfter := graphConflictCheckpoint(t, ctx, cp, foreignID, "")
			if cp.saves.Load() != saves || cp.deletes.Load() != deletes ||
				!bytes.Equal(sourceBefore, sourceAfter) || !bytes.Equal(alienBefore, alienAfter) ||
				hooks.starts != 0 || hooks.ends != 0 || accepted != "" {
				t.Fatalf("须在钩子和应用任何档之前拒绝外来 cm，写入=%d 删除=%d 源同档=%t 外来同档=%t 钩子=%+v 答复=%q 错用提案=%t",
					cp.saves.Load()-saves, cp.deletes.Load()-deletes, bytes.Equal(sourceBefore, sourceAfter),
					bytes.Equal(alienBefore, alienAfter), hooks, accepted, hasProposal)
			}
			if got := compose_v2.MustGet(manager.Channels(), state); got != "alien-proposal" {
				t.Fatalf("拒绝时不得修改调用方已有的 cm，实际 %q", got)
			}
			// 对照：以同一来源预恢复一次是既有调用方式，仍应按原图完成。
			if err := compose_v2.Run(ctx, target, restore(source), opts...); err != nil ||
				accepted != "approve-original" || hasProposal {
				t.Fatalf("正确的已恢复 cm 应继续完成，实际 err=%v answer=%q hasProposal=%t", err, accepted, hasProposal)
			}
			if left, _ := graphConflictCheckpoint(t, ctx, cp, sourceID, ""); left != nil {
				t.Fatal("原任务完成后应清理自己的档")
			}
		})
	}
}

func TestT503GraphCheckpointExplicitSourceCannotReplaceForeignTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := &graphConflictStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	original := &graphConflictCalls{question: "批准来源流程？"}
	occupied := &graphConflictCalls{question: "批准驻留流程？"}
	source := buildGraphConflictFixture(t, original, []compose_v2.NodeID{"safe"}, "safe",
		func(b *compose_v2.GraphBuilder) { b.WithName("source-graph") })
	resident := buildGraphConflictFixture(t, occupied, []compose_v2.NodeID{"wrong"}, "wrong",
		func(b *compose_v2.GraphBuilder) { b.WithName("resident-graph") })
	const sourceID, targetID, forkID = "source-task", "target-task", "empty-task"
	load := func(sid string) []compose_v2.RunOption {
		return []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
	}
	_, answer := graphConflictAnswer(t, compose_v2.Run(ctx, source, compose_v2.NewChannelManager(), load(sourceID)...),
		original.question)
	_, residentAnswer := graphConflictAnswer(t, compose_v2.Run(ctx, resident, compose_v2.NewChannelManager(), load(targetID)...),
		occupied.question)
	sourceCp, sourceBefore := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
	residentCp, residentBefore := graphConflictCheckpoint(t, ctx, cp, targetID, "")
	if sourceCp == nil || residentCp == nil {
		t.Fatal("试验应同时存在来源档与不兼容的目标档")
	}
	hooks := &graphConflictHooks{}
	saves, deletes := cp.saves.Load(), cp.deletes.Load()
	requireGraphConflict(t, compose_v2.Run(ctx, source, compose_v2.NewChannelManager(),
		append(load(targetID), compose_v2.WithResumeFrom(sourceCp), answer, compose_v2.WithHooks(hooks))...))
	_, sourceAfter := graphConflictCheckpoint(t, ctx, cp, sourceID, "")
	_, residentAfter := graphConflictCheckpoint(t, ctx, cp, targetID, "")
	if cp.saves.Load() != saves || cp.deletes.Load() != deletes ||
		!bytes.Equal(sourceBefore, sourceAfter) || !bytes.Equal(residentBefore, residentAfter) ||
		hooks.starts != 0 || hooks.ends != 0 || original.answer != "" || original.safe != 0 {
		t.Fatalf("不兼容的目标应零执行、零写删，来源和目标必须保持原样，写入=%d 删除=%d 来源同档=%t 目标同档=%t 来源=%+v 钩子=%+v",
			cp.saves.Load()-saves, cp.deletes.Load()-deletes, bytes.Equal(sourceBefore, sourceAfter),
			bytes.Equal(residentBefore, residentAfter), original, hooks)
	}
	// 合法的显式恢复可以把同一份来源档放进一个尚未占用的任务槽。
	if err := compose_v2.Run(ctx, source, compose_v2.NewChannelManager(),
		append(load(forkID), compose_v2.WithResumeFrom(sourceCp), answer)...); err != nil ||
		original.answer != "approve-original" || original.safe != 1 {
		t.Fatalf("显式恢复到空槽应运行原分支一次，实际 %+v err=%v", original, err)
	}
	if found, _ := graphConflictCheckpoint(t, ctx, cp, forkID, ""); found != nil {
		t.Fatal("空槽恢复成功后应清理自己的任务")
	}
	if _, after := graphConflictCheckpoint(t, ctx, cp, sourceID, ""); !bytes.Equal(sourceBefore, after) {
		t.Fatal("恢复并清理空槽不得修改来源档")
	}
	if err := compose_v2.Run(ctx, resident, compose_v2.NewChannelManager(), append(load(targetID), residentAnswer)...); err != nil ||
		occupied.answer != "approve-original" || occupied.wrong != 1 {
		t.Fatalf("原目标应继续用自己的答案位完成，实际 %+v err=%v", occupied, err)
	}
}

func TestT503GraphCheckpointUnverifiedLegacyIdentityPreserved(t *testing.T) {
	for _, mode := range []struct {
		name     string
		explicit bool
	}{{"same_legacy_graph_name", false}, {"missing_identity", true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const sid = "legacy-task"
			calls := &graphConflictCalls{question: "批准旧流程？"}
			graph := buildGraphConflictFixture(t, calls, []compose_v2.NodeID{"safe"}, "safe",
				func(b *compose_v2.GraphBuilder) { b.WithName("legacy-graph") })
			original := compose_v2.NewInMemoryCheckpointer()
			_, answer := graphConflictAnswer(t, compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(original), compose_v2.WithSessionID(sid)), calls.question)
			saved, _ := graphConflictCheckpoint(t, ctx, original, sid, "")
			if saved == nil {
				t.Fatal("原任务应在真实待答位置写档")
			}
			legacy := *saved
			legacy.GraphID = graph.ID()
			if mode.explicit {
				legacy.GraphID = ""
			}
			cp := &graphConflictStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
			if err := cp.SaveNS(ctx, sid, "", &legacy); err != nil {
				t.Fatal(err)
			}
			_, before := graphConflictCheckpoint(t, ctx, cp, sid, "")
			hooks := &graphConflictHooks{}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid), answer, compose_v2.WithHooks(hooks)}
			if mode.explicit {
				opts = append(opts, compose_v2.WithResumeFrom(&legacy))
			}
			requireGraphConflict(t, compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), opts...))
			_, after := graphConflictCheckpoint(t, ctx, cp, sid, "")
			if cp.saves.Load() != 1 || cp.deletes.Load() != 0 || !bytes.Equal(before, after) ||
				hooks.starts != 0 || hooks.ends != 0 || calls.answer != "" || calls.safe != 0 {
				t.Errorf("不可验证的旧身份应原样保留，写入含原档=%d 删除=%d 同档=%t 钩子=%+v 节点=%+v",
					cp.saves.Load(), cp.deletes.Load(), bytes.Equal(before, after), hooks, calls)
			}
		})
	}
}

func TestT503GraphCheckpointNestedConflictPreservesQuestionsAndSiblingProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const sid, childNS = "parent-task", "dynamic-child"
	cp := compose_v2.NewInMemoryCheckpointer()
	original := &graphConflictCalls{question: "批准原子图？"}
	other := &graphConflictCalls{question: "批准新版子图？"}
	oldChild := buildGraphConflictFixture(t, original, []compose_v2.NodeID{"safe"}, "safe",
		func(b *compose_v2.GraphBuilder) { b.WithName("child-v1") })
	wrongChild := buildGraphConflictFixture(t, other, []compose_v2.NodeID{"wrong"}, "wrong",
		func(b *compose_v2.GraphBuilder) { b.WithName("child-v2") })
	selectedChild := oldChild
	var completedCalls, siblingAnswers atomic.Int32
	completedResult := compose_v2.LastValue[string]("completed-result")
	siblingResult := compose_v2.LastValue[string]("sibling-result")
	const siblingQuestion = "批准兄弟分支？"
	b := compose_v2.NewBuilder().WithName("parent")
	compose_v2.WithChannel(b, completedResult)
	compose_v2.WithChannel(b, siblingResult)
	b.AddNode("completed", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		completedCalls.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(completedResult, "already-completed")}, nil
	})
	b.AddNode("sibling", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: siblingQuestion})
		if err != nil {
			return nil, err
		}
		siblingAnswers.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(siblingResult, answer.Content)}, nil
	})
	b.AddNode("dispatch", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 真实调用方动态选子图，父编译时尚不知道它将载入哪个版本。
		return nil, compose_v2.Run(ctx, selectedChild, compose_v2.NewChannelManager(),
			compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), childNS)))
	})
	for _, node := range []compose_v2.NodeID{"completed", "sibling", "dispatch"} {
		b.AddEdge(compose_v2.StartNode, node).AddEdge(node, compose_v2.EndNode)
	}
	parent, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
	err = compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(), opts...)
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 2 || completedCalls.Load() != 1 || siblingAnswers.Load() != 0 {
		t.Fatalf("父任务应完成一个分支并露出两个待答问题，实际 err=%v completed=%d sibling answers=%d",
			err, completedCalls.Load(), siblingAnswers.Load())
	}
	ids := make(map[string]string)
	for _, q := range interrupt.AllQueries() {
		ids[q.Question] = q.InterruptID
	}
	childID, siblingID := ids[original.question], ids[siblingQuestion]
	if childID == "" || siblingID == "" || childID == siblingID {
		t.Fatalf("子问题和兄弟问题应各有稳定的答案位，实际 %v", ids)
	}
	oldParent, _ := graphConflictCheckpoint(t, ctx, cp, sid, "")
	oldSub, oldSubWire := graphConflictCheckpoint(t, ctx, cp, sid, childNS)
	if oldParent == nil || len(oldParent.PendingInterrupts) != 2 || oldSub == nil ||
		len(oldSub.PendingInterrupts) != 1 || oldSub.PendingInterrupts[0].InterruptID != childID {
		t.Fatal("父子应持久化同一条子问题和各自的进度")
	}

	selectedChild = wrongChild
	answer := compose_v2.WithResumeAnswers(map[string]*schema.Message{
		childID: schema.UserMessage("approve-original"), siblingID: schema.UserMessage("approve-sibling"),
	})
	requireGraphConflict(t, compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(), append(opts, answer)...))
	root, _ := graphConflictCheckpoint(t, ctx, cp, sid, "")
	if root == nil || len(root.PendingInterrupts) != 1 || root.PendingInterrupts[0].InterruptID != childID {
		t.Errorf("父档应保留未执行子图的原问题，已经兑付的兄弟问题退出待答，实际 checkpoint=%+v", root)
	}
	_, newSubWire := graphConflictCheckpoint(t, ctx, cp, sid, childNS)
	if !bytes.Equal(oldSubWire, newSubWire) || other.ask != 0 || original.answer != "" {
		t.Errorf("冲突不得修改子档或消费子问题，子档一致=%t 新子图=%+v 原子图=%+v",
			bytes.Equal(oldSubWire, newSubWire), other, original)
	}
	if completedCalls.Load() != 1 || siblingAnswers.Load() != 1 {
		t.Errorf("原成功分支保持一次，仍合法的兄弟分支成功兑付一次，实际 %d/%d",
			completedCalls.Load(), siblingAnswers.Load())
	}
	if root == nil || !slices.Contains(root.ConsumedAnswers, siblingID) {
		t.Error("父档应保留兄弟分支的已兑付账本")
	}

	selectedChild = oldChild
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, parent, cm, append(opts, compose_v2.WithResumeAnswers(map[string]*schema.Message{
		childID: schema.UserMessage("approve-original"),
	}))...); err != nil {
		t.Fatalf("原版本应接着使用保留的答复位续跑，实际 %v", err)
	}
	if original.answer != "approve-original" || original.safe != 1 || other.wrong != 0 ||
		completedCalls.Load() != 1 || siblingAnswers.Load() != 1 {
		t.Fatalf("应只补原子图，保持两份兄弟进度，原子图=%+v 新子图=%+v completed=%d sibling=%d",
			original, other, completedCalls.Load(), siblingAnswers.Load())
	}
	if got := compose_v2.MustGet(cm.Channels(), completedResult); got != "already-completed" {
		t.Errorf("原兄弟输出应保留，实际 %q", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), siblingResult); got != "approve-sibling" {
		t.Errorf("冲突批次成功兄弟的输出应保留，实际 %q", got)
	}
	if left, err := cp.LoadAllInSession(ctx, sid); err != nil || len(left) != 0 {
		t.Errorf("原图全部完成后才能清理该任务，剩余=%d err=%v", len(left), err)
	}
}

func TestT503GraphCheckpointConflictKeepsAllPendingAcrossAncestors(t *testing.T) {
	for _, scenario := range []string{"nested_graph", "nested_tool_batch", "nested_commit_error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const sid = "ancestor-task"
			cp := compose_v2.NewInMemoryCheckpointer()
			calls := &graphConflictCalls{question: "批准深层流程？"}
			original := buildGraphConflictFixture(t, calls, []compose_v2.NodeID{"safe"}, "safe",
				func(b *compose_v2.GraphBuilder) { b.WithName("original") })
			wrong := buildGraphConflictFixture(t, &graphConflictCalls{question: "批准新流程？"}, []compose_v2.NodeID{"wrong"}, "wrong",
				func(b *compose_v2.GraphBuilder) { b.WithName("wrong") })
			selected := original
			const siblingQuestion = "批准并行流程？"
			var siblingAnswer string
			callChild := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ns := compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "child")
				return nil, compose_v2.Run(ctx, selected, compose_v2.NewChannelManager(), compose_v2.WithRunNS(ns))
			}
			callSibling := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: siblingQuestion})
				if err != nil {
					return nil, err
				}
				siblingAnswer = answer.Content
				return nil, nil
			}
			builder := compose_v2.NewBuilder().WithName("inner")
			if scenario == "nested_tool_batch" {
				builder.AddNode("tools", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					_, childErr := callChild(compose_v2.WithCurrentToolCallContext(ctx, "dynamic", 0), chs)
					_, siblingErr := callSibling(compose_v2.WithCurrentToolCallContext(ctx, "sibling", 1), chs)
					return nil, compose_v2.PickFanoutError([]error{childErr, siblingErr})
				}).AddEdge(compose_v2.StartNode, "tools").AddEdge("tools", compose_v2.EndNode)
			} else {
				builder.AddNode("dynamic", callChild).AddNode("sibling", callSibling).
					AddEdge(compose_v2.StartNode, "dynamic").AddEdge(compose_v2.StartNode, "sibling").
					AddEdge("dynamic", compose_v2.EndNode).AddEdge("sibling", compose_v2.EndNode)
			}
			if scenario == "nested_commit_error" {
				waitFirst := true
				ch := compose_v2.Custom("commit-test", func(_, next string) string {
					if next == "fail" {
						panic("test reducer rejects this input")
					}
					return next
				}, "")
				compose_v2.WithChannel(builder, ch).AddNode("commit", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if waitFirst {
						waitFirst = false
						return nil, errors.New("commit will retry on the next run")
					}
					if selected == wrong {
						return []compose_v2.ChannelWrite{compose_v2.Write(ch, "fail")}, nil
					}
					return []compose_v2.ChannelWrite{compose_v2.Write(ch, "completed")}, nil
				}).AddEdge(compose_v2.StartNode, "commit").AddEdge("commit", compose_v2.EndNode)
			}
			inner, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			root, err := compose_v2.NewBuilder().WithName("root").AddSubgraphNode("inner", inner).
				AddEdge(compose_v2.StartNode, "inner").AddEdge("inner", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
			pause, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, root, compose_v2.NewChannelManager(), opts...))
			if !ok || len(pause.AllQueries()) != 2 {
				t.Fatalf("根图应露出两条深层待答问题，实际 %v", pause)
			}
			wantIDs := fanoutQuestions(pause.AllQueries())
			slices.Sort(wantIDs)

			// 下一次从原图续跑时，两条原答案都必须找得到。
			// reducer 失败的这轮虽然读到了兄弟分支的答案，但未提交兑付。
			const rolledBackAnswer = "这次答复随提交一起回滚"
			conflictingOpts := slices.Clone(opts)
			if scenario == "nested_commit_error" {
				for _, q := range pause.AllQueries() {
					if q.Question == siblingQuestion {
						conflictingOpts = append(conflictingOpts, compose_v2.WithResumeAnswers(map[string]*schema.Message{
							q.InterruptID: schema.UserMessage(rolledBackAnswer),
						}))
					}
				}
			}
			selected = wrong
			requireGraphConflict(t, compose_v2.Run(ctx, root, compose_v2.NewChannelManager(), conflictingOpts...))
			if scenario == "nested_commit_error" && siblingAnswer != rolledBackAnswer {
				t.Errorf("提交回滚前兄弟分支应拿到本轮答复，实际 %q", siblingAnswer)
			}
			for _, ns := range []string{"", "inner"} {
				saved, _ := graphConflictCheckpoint(t, ctx, cp, sid, ns)
				if saved == nil {
					t.Errorf("原祖先档应保留，ns=%q", ns)
					continue
				}
				pending := fanoutQuestions(saved.PendingInterrupts)
				slices.Sort(pending)
				if !slices.Equal(pending, wantIDs) {
					t.Errorf("祖先 %q 应保留所有未答中断 ID，实际 %v，期望 %v", ns, pending, wantIDs)
				}
			}
			answers := make(map[string]*schema.Message)
			for _, q := range pause.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage(q.Question + "允许")
			}
			selected = original
			err = compose_v2.Run(ctx, root, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
			if scenario == "nested_commit_error" {
				if !errors.Is(err, compose_v2.ErrCheckpointResumeUnconfirmed) || calls.safe != 0 || siblingAnswer != rolledBackAnswer {
					t.Fatalf("未提交的子图续跑必须保留原档并拒绝重放，实际 err=%v child=%+v answer=%q", err, calls, siblingAnswer)
				}
				return
			}
			if err != nil {
				t.Fatalf("恢复原子图后，两条原答案应能完成执行，实际 %v", err)
			}
			if calls.safe != 1 || calls.answer != calls.question+"允许" || siblingAnswer != siblingQuestion+"允许" {
				t.Errorf("两条原答案应交到各自节点，实际子图=%+v 兄弟答案=%q", calls, siblingAnswer)
			}
			if left, err := cp.LoadAllInSession(ctx, sid); err != nil || len(left) != 0 {
				t.Errorf("原根图完成后应清理全树，剩余=%d err=%v", len(left), err)
			}
		})
	}
}

func TestCheckpointIdentity_RuntimeChannelMerge(t *testing.T) {
	for _, declared := range []bool{true, false} {
		t.Run(fmt.Sprintf("declared=%t", declared), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			sum := compose_v2.Sum("total")
			b := compose_v2.NewBuilder().WithName("sum-contract")
			if declared {
				compose_v2.WithChannel(b, sum)
			}
			b.AddNode("seed", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return []compose_v2.ChannelWrite{compose_v2.Write(sum, 4)}, nil
			})
			effects := 0
			b.AddNode("add", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "加二？"}); err != nil {
					return nil, err
				}
				effects++
				return []compose_v2.ChannelWrite{compose_v2.Write(sum, 2)}, nil
			})
			g, err := b.AddEdge(compose_v2.StartNode, "seed").AddEdge("seed", "add").AddEdge("add", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			const sid = "runtime-reducer"
			run := func(cm *compose_v2.ChannelManager, extra ...compose_v2.RunOption) error {
				opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
				return compose_v2.Run(ctx, g, cm, append(opts, extra...)...)
			}
			cm := compose_v2.NewChannelManager()
			compose_v2.RegisterChannel(cm, sum)
			_, answer := graphConflictAnswer(t, run(cm), "加二？")
			_, before := graphConflictCheckpoint(t, ctx, cp, sid, "")
			replaced := compose_v2.NewChannelManager()
			compose_v2.RegisterChannel(replaced, compose_v2.LastValue[int]("total"))
			requireGraphConflict(t, run(replaced, answer))
			_, after := graphConflictCheckpoint(t, ctx, cp, sid, "")
			if effects != 0 || !bytes.Equal(before, after) {
				t.Fatal("错误 reducer 不应执行待答节点或改变原档")
			}
			valid := compose_v2.NewChannelManager()
			compose_v2.RegisterChannel(valid, sum)
			err = run(valid, answer)
			total, _ := compose_v2.Get(valid.Channels(), sum)
			if err != nil || total != 6 || effects != 1 {
				t.Fatalf("原 reducer 应累加得到 6，实际 %d、执行 %d 次、错误 %v", total, effects, err)
			}
		})
	}
}

func TestLateIdentityConflictPreservesChild(t *testing.T) {
	t.Run("保留祖先原问题", func(t *testing.T) {
		testLateIdentityConflict(t, false)
	})
	t.Run("兄弟终局失败", func(t *testing.T) {
		testLateIdentityConflict(t, true)
	})
}

func testLateIdentityConflict(t *testing.T, terminalSibling bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	mk := func(name string) *compose_v2.Graph {
		g, err := compose_v2.NewBuilder().WithName(name).
			AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续子图？"})
				return nil, err
			}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	child, replacement := mk("original-child"), mk("replacement")
	changed := false
	var terminalCalls atomic.Int32
	terminalCause := errors.New("兄弟分支写入后失败")
	g, err := compose_v2.NewBuilder().WithName("parent").
		AddNode("child", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			opts := []compose_v2.RunOption{compose_v2.WithRunNS("child")}
			selected := child
			if changed {
				selected = replacement
				opts = append(opts, compose_v2.WithFreshRun())
			}
			return nil, compose_v2.Run(ctx, selected, compose_v2.NewChannelManager(), opts...)
		}).
		AddNode("sibling", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if changed && terminalSibling {
				terminalCalls.Add(1)
				return nil, fmt.Errorf("%w: %w", compose_v2.ErrRunAborted, terminalCause)
			}
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续兄弟？"})
			return nil, err
		}).AddEdge(compose_v2.StartNode, "child").AddEdge(compose_v2.StartNode, "sibling").
		AddEdge("child", compose_v2.EndNode).AddEdge("sibling", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	const sid = "late-write-conflict"
	run := func(opts ...compose_v2.RunOption) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(
			[]compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}, opts...)...)
	}
	initial := run()
	ie, ok := compose_v2.IsInterrupt(initial)
	if !ok || len(ie.AllQueries()) != 2 {
		t.Fatalf("首次应留下两个问题，实际 %v", initial)
	}
	oldParent, _ := graphConflictCheckpoint(t, ctx, cp, sid, "")
	_, before := graphConflictCheckpoint(t, ctx, cp, sid, "child")
	changed = true
	err = run()
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Errorf("错误链应保留晚到写冲突，实际 %v", err)
	}
	parent, _ := graphConflictCheckpoint(t, ctx, cp, sid, "")
	_, after := graphConflictCheckpoint(t, ctx, cp, sid, "child")
	if parent == nil {
		t.Fatal("晚到写冲突应保留本层存档")
	}
	if !bytes.Equal(before, after) {
		t.Error("晚到写冲突不得修改原子档")
	}
	if !terminalSibling {
		want, got := fanoutQuestions(ie.AllQueries()), fanoutQuestions(parent.PendingInterrupts)
		slices.Sort(want)
		slices.Sort(got)
		if errors.Is(err, compose_v2.ErrRunAborted) || !parent.HasResumableWork() || !slices.Equal(want, got) {
			t.Errorf("仅有身份冲突时应保留祖先原待答项，实际 err=%v IDs=%v 期望 %v", err, got, want)
		}
		return
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) || !errors.Is(err, terminalCause) {
		t.Errorf("错误链应保留兄弟终局失败，实际 %v", err)
	}
	if parent.Status != compose_v2.CPStatusAborted || parent.HasResumableWork() {
		t.Errorf("终局失败后父档应封存，实际 status=%s 可续=%t", parent.Status, parent.HasResumableWork())
	}
	answers := make(map[string]*schema.Message)
	for _, q := range ie.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage("同意")
	}
	if err := run(compose_v2.WithResumeAnswers(answers)); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("自动载入不应从终局档续跑，实际 %v", err)
	}
	if err := run(); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("默认 Run 也不应误从终局档重开同一任务，实际 %v", err)
	}
	if err := run(compose_v2.WithResumeFrom(oldParent)); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("显式旧快照不应覆盖终局档，实际 %v", err)
	}
	if calls := terminalCalls.Load(); calls != 1 {
		t.Errorf("终局分支的副作用只允许一次，实际 %d", calls)
	}
}
