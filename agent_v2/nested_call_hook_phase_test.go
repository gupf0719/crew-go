package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 钩子相位（OnNodeStart / OnGraphStart 等）与节点体里发起的嵌套调用：
//
//   - 身份含调用方所在的节点与相位（节点、它的第几次执行、钩子种类）——两个节点、同一个节点
//     循环的两次执行、并行的两个节点，用同一份输入调同一个子 agent 是不同的调用。缺这一维时它们
//     撞成一次：答案账本判「已兑付」、终局 ErrRunAborted 并清档（实测，改动前就有）。
//   - 钩子里完成的调用被记住：节点中断之后重跑、钩子再触发时拿回，不再执行、不再问人。不记的话，
//     钩子里问过的问题在每次续跑时以同一个身份重问，只答当前一问的前端（agui / a2a 都是）两个
//     问题轮流出现、永远走不完。

// hpGuardHook 在指定节点的 OnNodeStart 里经 RunSubAgent 调一个会问人的 guard，记下每次拿到的裁决。
type hpGuardHook struct {
	agent_v2.NoOpOrchestrationHooks
	guard   agent_v2.Agent
	targets map[string]bool
	input   func(nodeID string) string
	mu      sync.Mutex
	verdict []string
}

func (h *hpGuardHook) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if !h.targets[nodeID] {
		return ctx, nil
	}
	ev, err := agent_v2.RunSubAgent(ctx, "guard", h.guard, schema.UserMessage(h.input(nodeID)))
	if err != nil {
		return ctx, err
	}
	h.mu.Lock()
	h.verdict = append(h.verdict, nodeID+"="+nestedFinal(ev))
	h.mu.Unlock()
	return ctx, nil
}

func (h *hpGuardHook) verdicts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.verdict...)
}

// hpGuard 是一个问人的子 agent：asked 记下它**真正执行到提问**的输入（拿回记录时不执行，不记）。
func hpGuard(t *testing.T, asked *inputLog) agent_v2.Agent {
	return newSingleNodeAgent(t, "guard", "g", nil, func(ctx context.Context, c string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准：" + c})
		if err != nil {
			if _, ok := compose_v2.IsInterrupt(err); ok {
				asked.add(c)
			}
			return "", err
		}
		return "批准人答:" + ans.Content, nil
	})
}

// hpDrive 跑到收敛（最多 8 次续跑）。每次只答**当前挂着的**问题（不累积传旧答案——那是
// agui / a2a 前端的做法，钩子里的调用若被重跑重问，这种前端会走不完）。
func hpDrive(t *testing.T, host agent_v2.Agent, hooks agent_v2.OrchestrationHooks, reply func(i int, q *compose_v2.AskQuery) string) (string, error, []string) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	events, err := host.Run(ctx, sess, schema.UserMessage("go"), agent_v2.WithOrchHooks(hooks))
	var asked []string
	for round := 0; round < 8; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		answers := map[string]*schema.Message{}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage(reply(len(asked), q))
		}
		events, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers),
			agent_v2.WithResumeOrchHooks(hooks), agent_v2.WithResumeSession(sess))
	}
	return nestedFinal(events), err, asked
}

// hpLoopHost：节点 X 循环两次执行；bodyAsks 时节点体每轮也问一句。
func hpLoopHost(t *testing.T, bodyAsks bool) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	roundCh := compose_v2.Sum("round")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, roundCh)
	b.AddNode("X", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if bodyAsks {
			r, _ := compose_v2.Get(chs, roundCh)
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("第 %d 轮要记日志吗", r)}); err != nil {
				return nil, err
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(roundCh, 1)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "X")
	b.AddConditionalEdge("X", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if r, _ := compose_v2.Get(chs, roundCh); r < 2 {
			return "X", nil
		}
		return compose_v2.EndNode, nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("end") }),
	)
}

// hpTwoNodeHost：X、Y 两个节点，parallel 时并行、否则 X → Y。
func hpTwoNodeHost(t *testing.T, parallel bool) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	compose_v2.WithChannel(b, inCh)
	noop := func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }
	b.AddNode("X", noop)
	b.AddNode("Y", noop)
	b.AddEdge(compose_v2.StartNode, "X")
	if parallel {
		b.AddEdge(compose_v2.StartNode, "Y")
		b.AddEdge("X", compose_v2.EndNode)
	} else {
		b.AddEdge("X", "Y")
	}
	b.AddEdge("Y", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("end") }),
	)
}

// TestNestedCall_HookPhaseCallsDistinctPerNodeExecution：同一个节点循环两次执行、每次在 OnNodeStart
// 里用同一份输入问批准，节点体每轮也问一句。每轮的批准是一次独立的调用：第 2 轮要真的问人、用
// 第 2 轮的回答；每轮的批准在节点体提问之后的重跑里拿回、不重问。
func TestNestedCall_HookPhaseCallsDistinctPerNodeExecution(t *testing.T) {
	asked := &inputLog{}
	hooks := &hpGuardHook{guard: hpGuard(t, asked), targets: map[string]bool{"X": true},
		input: func(id string) string { return "执行节点 " + id }}
	// 第 1 问（第 1 轮批准）答「是」，第 2 问（节点体）答「好」，之后一律「否」
	_, err, qs := hpDrive(t, hpLoopHost(t, true), hooks, func(i int, _ *compose_v2.AskQuery) string {
		switch i {
		case 1:
			return "是"
		case 2:
			return "好"
		}
		return "否"
	})
	if err != nil {
		t.Fatalf("应收敛（两轮各问一次批准、各问一次节点体），got %v；提问序列 %v", err, qs)
	}
	if got := asked.count("执行节点 X"); got != 2 {
		t.Errorf("guard 应恰好真问 2 次（每轮一次；续跑时拿回、不重问），got %d：%v", got, asked.snapshot())
	}
	want := []string{"X=批准人答:是", "X=批准人答:是", "X=批准人答:否", "X=批准人答:否"}
	if got := hooks.verdicts(); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("每轮的钩子应在节点体提问前、续跑时各拿一次本轮自己的裁决：got %v want %v"+
			"（第 2 轮若拿到「是」，就是第 1 轮的答案被套到了第 2 轮）", got, want)
	}
}

// TestNestedCall_HookPhaseCallsDistinctAcrossNodes：两个节点（顺序 / 并行）各自在 OnNodeStart 里用
// 同一份输入问批准——是两次调用，各问各的、各拿各的回答。
func TestNestedCall_HookPhaseCallsDistinctAcrossNodes(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			asked := &inputLog{}
			hooks := &hpGuardHook{guard: hpGuard(t, asked), targets: map[string]bool{"X": true, "Y": true},
				input: func(string) string { return "继续执行下一步吗" }}
			_, err, qs := hpDrive(t, hpTwoNodeHost(t, parallel), hooks, func(i int, _ *compose_v2.AskQuery) string {
				return fmt.Sprintf("答%d", i)
			})
			if err != nil {
				t.Fatalf("两个节点各问一次、各自作答后应收敛，got %v（提问 %v）", err, qs)
			}
			if got := asked.count("继续执行下一步吗"); got != 2 {
				t.Errorf("guard 应恰好真问 2 次（两个节点各一次），got %d", got)
			}
			got := map[string]string{}
			for _, v := range hooks.verdicts() {
				node, verdict, _ := strings.Cut(v, "=")
				if prev, ok := got[node]; ok && prev != verdict {
					t.Errorf("节点 %s 两次拿到的裁决不一样：%q vs %q", node, prev, verdict)
				}
				got[node] = verdict
			}
			if got["X"] == "" || got["Y"] == "" || got["X"] == got["Y"] {
				t.Errorf("X、Y 应各自拿到自己那次提问的回答，got %v", got)
			}
		})
	}
}

// hpNodeStartGate 在 host 节点的 OnNodeStart 里经 RunSubAgent 调一个两节点的 gate（第二个节点问人）。
type hpNodeStartGate struct {
	agent_v2.NoOpOrchestrationHooks
	gate agent_v2.Agent
}

func (h *hpNodeStartGate) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if nodeID != "host" {
		return ctx, nil
	}
	_, err := agent_v2.RunSubAgent(ctx, "gate", h.gate, schema.UserMessage("批准执行 host"))
	return ctx, err
}

// TestNestedCall_NodeStartGateRememberedAcrossBodyInterrupt：OnNodeStart 里的把关问过人之后，节点体
// 又问一句（中断）。续跑重跑节点、钩子再触发时把关那次调用拿回——不重跑、不重问。只答当前一问的
// 前端下，重问会让两个问题轮流出现、永远走不完。
func TestNestedCall_NodeStartGateRememberedAcrossBodyInterrupt(t *testing.T) {
	gateFirsts := &countLog{}
	gate := newTwoNodeAgent(t, "gate",
		func(_ context.Context, c string) (string, error) {
			gateFirsts.inc(c)
			return c, nil
		},
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "gate: " + c + "？"})
			if err != nil {
				return "", err
			}
			return "gate 放行(" + ans.Content + ")", nil
		})
	bodyRuns := &countLog{}
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		bodyRuns.inc("host")
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "body: 目标账户？"})
		if err != nil {
			return "", err
		}
		return "body 完成(" + ans.Content + ")", nil
	})
	final, err, qs := hpDrive(t, host, &hpNodeStartGate{gate: gate}, func(i int, _ *compose_v2.AskQuery) string {
		return fmt.Sprintf("ans%d", i)
	})
	if err != nil {
		t.Fatalf("把关与节点体各答一次后应收敛，got %v（提问序列 %v）", err, qs)
	}
	if want := []string{"gate: 批准执行 host？", "body: 目标账户？"}; strings.Join(qs, "|") != strings.Join(want, "|") {
		t.Errorf("提问序列 = %v，want %v（把关若在续跑时被重跑，会再问一次）", qs, want)
	}
	if n := gateFirsts.get("批准执行 host"); n != 1 {
		t.Errorf("把关子 agent 应只执行一次（续跑时拿回），first 节点执行 %d 次", n)
	}
	if final != "body 完成(ans2)" {
		t.Errorf("final = %q", final)
	}
}

// hpGraphStartCaller 在（嵌套）图的 OnGraphStart 里经 RunSubAgent 调一个会问人的 asker。
type hpGraphStartCaller struct {
	agent_v2.NoOpOrchestrationHooks
	sub agent_v2.Agent
}

func (h *hpGraphStartCaller) OnGraphStart(ctx context.Context) (context.Context, error) {
	_, err := agent_v2.RunSubAgent(ctx, "gs", h.sub, schema.UserMessage("固定的活"))
	return ctx, err
}

// TestNestedCall_NestedGraphStartCallRememberedAcrossBodyInterrupt：嵌套图的 OnGraphStart 里问过人，
// 之后嵌套图的节点体又问一句。续跑时外层节点重跑、嵌套图跟着重跑、OnGraphStart 再触发——那次调用
// 记在外层节点执行名下，拿回、不重问。
func TestNestedCall_NestedGraphStartCallRememberedAcrossBodyInterrupt(t *testing.T) {
	askerFirsts := &countLog{}
	asker := newTwoNodeAgent(t, "asker",
		func(_ context.Context, c string) (string, error) {
			askerFirsts.inc(c)
			return c, nil
		},
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认处理「" + c + "」？"})
			if err != nil {
				return "", err
			}
			return "执行了(" + ans.Content + ")", nil
		})
	hooks := &hpGraphStartCaller{sub: asker}
	mid := newTwoNodeAgent(t, "mid",
		func(_ context.Context, c string) (string, error) { return "mid1:" + c, nil },
		func(ctx context.Context, _, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "mid 的第二个问题"})
			if err != nil {
				return "", err
			}
			return "mid 完成(" + ans.Content + ")", nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "mid", mid, schema.UserMessage("x"), agent_v2.WithOrchHooks(hooks))
		if err != nil {
			return "", err
		}
		return nestedFinal(events), nil
	})
	_, err, qs := hpDrive(t, host, agent_v2.NoOpOrchestrationHooks{}, func(i int, _ *compose_v2.AskQuery) string {
		return fmt.Sprintf("批准%d", i)
	})
	if err != nil {
		t.Fatalf("两问各答一次后应收敛，got %v（提问序列 %v）", err, qs)
	}
	if len(qs) != 2 {
		t.Errorf("应恰好问 2 次，got %v", qs)
	}
	if n := askerFirsts.get("固定的活"); n != 1 {
		t.Errorf("OnGraphStart 里那次调用应只执行一次（续跑时拿回），asker.first 执行 %d 次", n)
	}
}

// TestNestedCall_TwoNodesSameInputAreDifferentCalls：两个不同节点的节点体里按「转发 sub.Name()」的约定
// 调同一个子 agent、同一份输入——两次调用，各问各的。身份不含调用方节点时两次首访撞成一次：第二个
// 节点拿到第一个的答案位，终局 ErrRunAborted（实测）。
func TestNestedCall_TwoNodesSameInputAreDifferentCalls(t *testing.T) {
	asked := &inputLog{}
	reviewer := newSingleNodeAgent(t, "reviewer", "r", nil, func(ctx context.Context, c string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "这份稿子能过吗：" + c})
		if err != nil {
			if _, ok := compose_v2.IsInterrupt(err); ok {
				asked.add(c)
			}
			return "", err
		}
		return "审稿意见:" + ans.Content, nil
	})
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outs := compose_v2.Append[string]("outs")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outs)
	review := func(label string) compose_v2.NodeFunc {
		return func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ev, err := agent_v2.RunSubAgent(ctx, reviewer.Name(), reviewer, schema.UserMessage("同一份稿子"))
			if err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(outs, []string{label + "=" + nestedFinal(ev)})}, nil
		}
	}
	b.AddNode("legal", review("法务"))
	b.AddNode("brand", review("品牌"))
	b.AddEdge(compose_v2.StartNode, "legal")
	b.AddEdge("legal", "brand")
	b.AddEdge("brand", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	host := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outs)
			return schema.AssistantMessage(strings.Join(o, ";"))
		}),
	)
	final, err, _ := hpDrive(t, host, agent_v2.NoOpOrchestrationHooks{}, func(i int, _ *compose_v2.AskQuery) string {
		return fmt.Sprintf("第%d次答", i)
	})
	if err != nil {
		t.Fatalf("两个节点各审一次、各问一次后应收敛，got %v", err)
	}
	if n := len(asked.snapshot()); n != 2 {
		t.Errorf("审稿人应被问 2 次（两个节点各一次），got %d", n)
	}
	if final != "法务=审稿意见:第1次答;品牌=审稿意见:第2次答" {
		t.Errorf("两个节点应各拿各的回答，final=%q", final)
	}
}
