package agent_v2_test

// 同一个节点先后两次执行（失败、中断、被条件边否决之后的续跑，可能跨请求）、或一组重试尝试里，被取代的那次交出、取代它的
// 那次没再登记的流撤回；取代它的那次拿回了说不清对应哪些流的东西（从存档续跑、命中节点私有态）时一个都不撤。有存档时不再
// 一律不撤（曾经按「配了 checkpointer」猜下一次是从存档续，节点体直接发、下一次重新生成的片段留在前端）。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// vetoGraph 是一个单节点 GraphAgent：节点发片段 v-<n>（n 是第几次执行），出边的条件函数第一次判为不合格（按
// CondFailRerunFrom 否决、要求重新产出），之后放行。
func vetoGraph(t *testing.T, runs, evals *atomic.Int64) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("veto")
	out := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, out)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		id := "v-" + string(rune('0'+runs.Add(1)))
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: id, Metadata: map[string]any{"run_id": id}})
		return []compose_v2.ChannelWrite{compose_v2.Write(out, id)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		if evals.Add(1) == 1 {
			return "", errors.New("产出不合格")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("veto"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
}

func cpCtx(log *eventLog) context.Context {
	return agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), log)
}

// 条件边否决了节点的产出，成员重试从存档续跑、重跑这个节点（同一个请求里）：被否决那次的片段撤回一次，重做那次的不撤。
func TestNodeStreams_VetoedRetractedOnMemberRetry(t *testing.T) {
	var runs, evals atomic.Int64
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
		AddStep("s1", vetoGraph(t, &runs, &evals))
	log := &eventLog{}
	if _, err := seq.Run(cpCtx(log), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runs.Load() != 2 {
		t.Fatalf("fixture 没搭对：节点应执行 2 次，实际 %d", runs.Load())
	}
	_, resets := resetsOf(log, "v-1")
	if resets["v-1"] != 1 || resets["v-2"] != 0 {
		t.Fatalf("被否决那次（v-1）应撤回一次、重做那次（v-2）不撤，实际 %v", resets)
	}
}

// 否决之后由调用方续跑（另一个请求）：续跑那次重跑节点时撤回被否决那次的片段——它记在节点执行的私有态里、随存档走。
func TestNodeStreams_VetoedRetractedOnLaterResume(t *testing.T) {
	var runs, evals atomic.Int64
	ga := vetoGraph(t, &runs, &evals)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	first := &eventLog{}
	if _, err := ga.Run(agent_v2.WithCtxOrchHooks(ctx, first), sess, schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：第一次应被否决、以错误结束")
	}
	second := &eventLog{}
	if _, err := ga.Resume(agent_v2.WithCtxOrchHooks(ctx, second), sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	_, resets := resetsOf(second, "v-2")
	if resets["v-1"] != 1 || resets["v-2"] != 0 {
		t.Fatalf("续跑应撤回被否决那次（v-1）、不撤重做那次（v-2），实际 %v", resets)
	}
}

// 配了存档的节点重试：节点体直接发的片段每次尝试都重新生成，失败那次的撤回（曾经有存档就一律不撤）。
func TestNodeStreams_NodeRetryWithCheckpointerRetracted(t *testing.T) {
	var runs atomic.Int64
	g := retryNodeGraph(t, func(ctx context.Context, _ string) (string, error) {
		n := runs.Add(1)
		id := "n-" + string(rune('0'+n))
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: id, Metadata: map[string]any{"run_id": id}})
		if n == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	log := &eventLog{}
	if _, err := g.Run(cpCtx(log), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, resets := resetsOf(log, "n-1")
	if resets["n-1"] != 1 || resets["n-2"] != 0 {
		t.Fatalf("失败那次（n-1）应撤回一次、重来那次（n-2）不撤，实际 %v", resets)
	}
}

// 配了存档的自研成员重试：它不存档、每次从头跑，失败那次的片段撤回（曾经有存档就一律不撤）。
func TestNodeStreams_CustomMemberWithCheckpointerRetracted(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", &chunkThenFailAgent{})
	log := &eventLog{}
	if _, err := seq.Run(cpCtx(log), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, resets := resetsOf(log, "")
	if resets["custom-1"] != 1 || resets["custom-2"] != 0 {
		t.Fatalf("失败那次（custom-1）应撤回一次、重来那次（custom-2）不撤，实际 %v", resets)
	}
}

// OnModelEnd 里问人、续跑沿用那份答复：续跑那次没再调模型、也没再流出，答复的流不能撤（模型答复的记录声明了自己的流）。
func TestNodeStreams_ReusedAnswerKeptOnResume(t *testing.T) {
	m := &scriptedStreamModel{script: func(int) ([]string, error) { return []string{"draft"}, nil }}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("r"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithHooks(hook.NewChainHooks(askOnModelEnd{})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	first := &eventLog{}
	_, err := ag.Run(agent_v2.WithCtxOrchHooks(ctx, first), sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：应停在问人上，实际 %v", err)
	}
	second := &eventLog{}
	events, err := ag.Resume(agent_v2.WithCtxOrchHooks(ctx, second), sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}))
	if err != nil || agent_v2.FinalMessage(events).Content != "draft" || m.calls.Load() != 1 {
		t.Fatalf("续跑应沿用答复、不再调模型，实际 err=%v 模型被调 %d 次", err, m.calls.Load())
	}
	draft, _ := resetsOf(first, "draft")
	_, resets := resetsOf(second, "")
	if draft == "" || resets[draft] != 0 {
		t.Fatalf("沿用的答复（run_id %q）不该撤回，实际 %v", draft, resets)
	}
}

// 业务节点流出内容、用没声明流的步骤记录记下它，再问人；续跑时拿回记录、不再流出：框架说不清它对应哪些流，一个都不撤。
func TestNodeStreams_UndeclaredStepRecallKeeps(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("ask")
	out := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, out)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		text, ok := compose_v2.GetStepResult[string](ctx, "text")
		if !ok {
			text = "x"
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: text, Metadata: map[string]any{"run_id": "X"}})
			if err := compose_v2.SaveStepResult(ctx, "text", text); err != nil {
				return nil, err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发出去吗"}); err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, text)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("ask"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = ga.Run(agent_v2.WithCtxOrchHooks(ctx, &eventLog{}), sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：应停在问人上，实际 %v", err)
	}
	second := &eventLog{}
	if _, err := ga.Resume(agent_v2.WithCtxOrchHooks(ctx, second), sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if _, resets := resetsOf(second, ""); resets["X"] != 0 {
		t.Fatalf("拿回的步骤对应的流（X）不该撤回，实际 %v", resets)
	}
}
