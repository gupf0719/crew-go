package agent_v2_test

// 暂停（问人）之后续跑：已经流出、续跑最终也采用的内容不能撤——续跑是接着办，进度可能存在框架看不见的地方。

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// pausedGraph 把几个节点串成一条线的 GraphAgent。
func pausedGraph(t *testing.T, name string, nodes []string, fns map[string]compose_v2.NodeFunc, out *compose_v2.Channel[string]) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, out)
	prev := compose_v2.StartNode
	for _, n := range nodes {
		b.AddNode(compose_v2.NodeID(n), fns[n])
		b.AddEdge(prev, compose_v2.NodeID(n))
		prev = compose_v2.NodeID(n)
	}
	b.AddEdge(prev, compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
}

// runToEnd 跑到收敛（每次停下对所有提问答「是」、答案累积传），返回每次请求各自的事件。
func runToEnd(t *testing.T, ag agent_v2.Agent, n int) []*eventLog {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	logs := make([]*eventLog, n)
	for i := range logs {
		logs[i] = &eventLog{}
	}
	_, err := ag.Run(agent_v2.WithCtxOrchHooks(ctx, logs[0]), sess, schema.UserMessage("go"))
	for i := 1; i < n; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("第 %d 次应停在问人上，实际 %v", i, err)
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		_, err = ag.Resume(agent_v2.WithCtxOrchHooks(ctx, logs[i]), sess.RunID, agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(answers))
	}
	if err != nil {
		t.Fatalf("最后一次: %v", err)
	}
	return logs
}

// 外层节点 X：RunSubAgent(sub) 之后再问人。sub：节点 a 流出 SUBA 并提交，节点 b 问人。第 1 次：sub 的 b 停；第 2 次：sub
// 从存档续完（a 不重跑、SUBA 不重新登记）、X 自己停；第 3 次：X 拿回完成记录、完成。SUBA 不能撤——曾经完成记录只声明了
// 最后那次执行登记到的流（空），第 3 次把 SUBA 撤了（审计实测）。
func TestPausedStreams_NestedRecordAfterResumeKeeps(t *testing.T) {
	subOut := compose_v2.LastValue[string]("sout")
	sub := pausedGraph(t, "sub", []string{"a", "b"}, map[string]compose_v2.NodeFunc{
		"a": func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: "SUBA", Metadata: map[string]any{"run_id": "SUBA"}})
			return []compose_v2.ChannelWrite{compose_v2.Write(subOut, "SUBA")}, nil
		},
		"b": func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "sub 确认"}); err != nil {
				return nil, err
			}
			v, _ := compose_v2.Get(chs, subOut)
			return []compose_v2.ChannelWrite{compose_v2.Write(subOut, v+"+b")}, nil
		},
	}, subOut)
	out := compose_v2.LastValue[string]("out")
	outer := pausedGraph(t, "outer", []string{"x"}, map[string]compose_v2.NodeFunc{
		"x": func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			evs, err := agent_v2.RunSubAgent(ctx, "x", sub, schema.UserMessage("in"))
			if err != nil {
				return nil, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "outer 确认"}); err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(evs).Content)}, nil
		},
	}, out)
	logs := runToEnd(t, outer, 3)
	_, r3 := resetsOf(logs[2], "")
	if r3["SUBA"] != 0 {
		t.Errorf("SUBA 是 sub 已提交、被最终结果采用的产出，第 3 次请求撤回了它 %d 次", r3["SUBA"])
	}
}

// ownStateAsker 是自研成员：第一次流出草稿 C1、记在自己身上（不经 compose_v2 的私有态 / 存档）、问人；续跑时从自己记的
// 草稿接着办、不再流出，最终答复就是那份草稿。
type ownStateAsker struct{ draft string }

func (a *ownStateAsker) Name() string        { return "own" }
func (a *ownStateAsker) Description() string { return "" }
func (a *ownStateAsker) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if a.draft == "" {
		a.draft = "草稿C1"
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: a.draft, Metadata: map[string]any{"run_id": "C1"}})
	}
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "草稿可以吗"}); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("own", schema.AssistantMessage(a.draft))}, nil
}
func (a *ownStateAsker) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// 续跑沿用自研成员自己记下的草稿：C1 不能撤（曾经框架看不到拿回就撤，审计实测）。
func TestPausedStreams_CustomMemberOwnStateKeeps(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s1", &ownStateAsker{})
	logs := runToEnd(t, seq, 2)
	_, r := resetsOf(logs[1], "")
	if r["C1"] != 0 {
		t.Errorf("C1 是最终被采用的草稿，续跑那次撤回了它 %d 次", r["C1"])
	}
}

// 完成记录声明的流必须是这次调用最终采用的全部：节点 X 配了重试，第 1 次尝试里 sub 流出 SUBA 后失败；第 2 次尝试 sub 从存档
// 续完（SUBA 不再登记）、X 随后失败、重试用完。续跑时 X 拿回完成记录收尾——记录要是声明了「第 2 次登记到的」（空），SUBA
// 落在上一次执行自己交出的那份里、被当成没采用撤掉。被调方这次是从存档续的，完成记录不声明流，拿回时按说不清处理。
func TestPausedStreams_NestedRecordFromResumedCalleeKeeps(t *testing.T) {
	subOut := compose_v2.LastValue[string]("sout")
	var bRuns atomic.Int64
	sub := pausedGraph(t, "sub", []string{"a", "b"}, map[string]compose_v2.NodeFunc{
		"a": func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: "SUBA", Metadata: map[string]any{"run_id": "SUBA"}})
			return []compose_v2.ChannelWrite{compose_v2.Write(subOut, "SUBA")}, nil
		},
		"b": func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if bRuns.Add(1) == 1 {
				return nil, errors.New("sub 暂时失败")
			}
			v, _ := compose_v2.Get(chs, subOut)
			return []compose_v2.ChannelWrite{compose_v2.Write(subOut, v+"+b")}, nil
		},
	}, subOut)
	out := compose_v2.LastValue[string]("out")
	var xRuns atomic.Int64
	b := compose_v2.NewBuilder().WithName("outer")
	compose_v2.WithChannel(b, out)
	b.AddNode("x", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		evs, err := agent_v2.RunSubAgent(ctx, "x", sub, schema.UserMessage("in"))
		if err != nil {
			return nil, err
		}
		if xRuns.Add(1) == 1 {
			return nil, errors.New("下游暂时不可用")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(evs).Content)}, nil
	}, compose_v2.WithNodeRetry(compose_v2.RetryConfig{MaxRetries: 1, ShouldRetry: func(error) bool { return true }}))
	b.AddEdge(compose_v2.StartNode, "x")
	b.AddEdge("x", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	outer := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("outer"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := outer.Run(agent_v2.WithCtxOrchHooks(ctx, &eventLog{}), sess, schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：第 1 次请求应在 X 重试用完后失败")
	}
	if bRuns.Load() != 2 || xRuns.Load() != 1 {
		t.Fatalf("fixture 没搭对：sub 的 b 应执行 2 次、X 走到失败点 1 次，实际 %d / %d", bRuns.Load(), xRuns.Load())
	}
	second := &eventLog{}
	events, err := outer.Resume(agent_v2.WithCtxOrchHooks(ctx, second), sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil || agent_v2.FinalMessage(events).Content != "SUBA+b" {
		t.Fatalf("续跑应拿回完成记录收尾，实际 err=%v", err)
	}
	if _, r := resetsOf(second, ""); r["SUBA"] != 0 {
		t.Errorf("SUBA 是 sub 已提交、被最终结果采用的产出，续跑撤回了它 %d 次", r["SUBA"])
	}
}

// timeoutThenOKGraph：节点第一次发 t-1 后以 failErr 失败，之后发 t-2 成功。
func timeoutThenOKGraph(t *testing.T, failErr error, runs *atomic.Int64) *agent_v2.GraphAgent {
	t.Helper()
	out := compose_v2.LastValue[string]("out")
	return pausedGraph(t, "pg", []string{"n"}, map[string]compose_v2.NodeFunc{
		"n": func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			n := runs.Add(1)
			id := "t-" + string(rune('0'+n))
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: id, Metadata: map[string]any{"run_id": id}})
			if n == 1 {
				return nil, failErr
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(out, id)}, nil
		},
	}, out)
}

// 后端自己的超时（错误链上有 DeadlineExceeded、调用方 ctx 还活着）是一次普通的失败、不是暂停：续跑与成员重试重新生成时，
// 失败那次的片段照撤（曾经按「错误链上有 ctx 错」判成暂停、不撤，前端两份都留着，审计实测）。
func TestPausedStreams_BackendTimeoutIsFailure(t *testing.T) {
	backend := fmt.Errorf("db query: %w", context.DeadlineExceeded)
	t.Run("续跑", func(t *testing.T) {
		var runs atomic.Int64
		ga := timeoutThenOKGraph(t, backend, &runs)
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		sess := agent_v2.NewRunSession()
		if _, err := ga.Run(agent_v2.WithCtxOrchHooks(ctx, &eventLog{}), sess, schema.UserMessage("go")); err == nil {
			t.Fatal("fixture 没搭对：第一次应失败")
		}
		second := &eventLog{}
		if _, err := ga.Resume(agent_v2.WithCtxOrchHooks(ctx, second), sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if _, r := resetsOf(second, ""); r["t-1"] != 1 || r["t-2"] != 0 {
			t.Fatalf("失败那次（t-1）应撤回一次，实际 %v", r)
		}
	})
	t.Run("成员重试", func(t *testing.T) {
		var runs atomic.Int64
		seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
			AddStep("s1", timeoutThenOKGraph(t, backend, &runs))
		log := &eventLog{}
		if _, err := seq.Run(cpCtx(log), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, r := resetsOf(log, ""); runs.Load() != 2 || r["t-1"] != 1 || r["t-2"] != 0 {
			t.Fatalf("失败那次（t-1）应撤回一次，实际节点执行 %d 次、%v", runs.Load(), r)
		}
	})
}

// 节点体（进度存在自己身上）流出 C1 成功之后，出边的条件函数求值时调用方被叫停：这次执行算暂停结束，续跑时节点体沿用自己的
// 草稿、不再流出，C1 不能撤（曾经只看节点体的结局、按失败撤了，审计实测）。
func TestPausedStreams_CondStoppedAfterBodySuccessKeeps(t *testing.T) {
	out := compose_v2.LastValue[string]("out")
	draft := ""
	var evals atomic.Int64
	var cancel context.CancelFunc
	b := compose_v2.NewBuilder().WithName("cs")
	compose_v2.WithChannel(b, out)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if draft == "" {
			draft = "C1"
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "chunk", Content: "C1", Metadata: map[string]any{"run_id": "C1"}})
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, draft)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		if evals.Add(1) == 1 {
			cancel()
			return "", ctx.Err()
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("cs"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
	base := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	c1, cf := context.WithCancel(base)
	cancel = cf
	if _, err := ga.Run(agent_v2.WithCtxOrchHooks(c1, &eventLog{}), sess, schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：第一次应在出边上被叫停")
	}
	second := &eventLog{}
	events, err := ga.Resume(agent_v2.WithCtxOrchHooks(base, second), sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil || agent_v2.FinalMessage(events).Content != "C1" {
		t.Fatalf("续跑应沿用 C1 收尾，实际 err=%v", err)
	}
	if _, r := resetsOf(second, ""); r["C1"] != 0 {
		t.Fatalf("C1 是最终采用的内容，续跑撤回了它 %d 次", r["C1"])
	}
}
