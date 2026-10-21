package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// 嵌套调用的身份：同一次节点执行里对同一个名字调多次、模型跨轮或批内复用 tool_call ID 时，
// 每一次调用都要落在自己的存档槽上；父节点重跑时，已完成的调用直接拿回结果、没完成的续自己
// 的存档、输入变了的是另一次调用。
//
// 断言一律落在「子 agent 的节点体实际处理了什么输入、跑了几次」上——入口被调几次不承重
// （续跑时入口照样被调，但节点读的是存档里的输入）。

// inputLog 记下子 agent 节点体每次实际处理的输入。
type inputLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *inputLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, s)
}

func (l *inputLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

func (l *inputLog) count(s string) int {
	n := 0
	for _, v := range l.snapshot() {
		if v == s {
			n++
		}
	}
	return n
}

// newSingleNodeAgent 造一个单节点 GraphAgent：节点读输入消息的文本、交给 handle、把
// handle 的返回写成产出。log 非 nil 时节点先记下读到的输入。handle 返回错误时节点失败
// ——业务错会留下一份可续的失败存档，中断留下挂起存档。
func newSingleNodeAgent(t *testing.T, name, nodeID string, log *inputLog,
	handle func(ctx context.Context, content string) (string, error)) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode(compose_v2.NodeID(nodeID), func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if in == nil {
			if log != nil {
				log.add("<nil>")
			}
			return nil, errors.New("node got nil input")
		}
		if log != nil {
			log.add(in.Content)
		}
		out, err := handle(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, compose_v2.NodeID(nodeID))
	b.AddEdge(compose_v2.NodeID(nodeID), compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
}

func nestedFinal(events []*agent_v2.AgentEvent) string {
	if m := agent_v2.FinalMessage(events); m != nil {
		return m.Content
	}
	return "<no final>"
}

// TestNestedCall_RecallWithNewInputAfterFailure 复现「失败后带补充再调一次」：子 agent 在输入 A 上失败
// → 父节点问用户 → 用同一个名字带着补充再调一次。第二次调用必须用上补充。
//
// 缺「第几次调用」这一维时，第二次调用落在第一次的槽上：续跑段里没有「新开」信号，
// 它把第一次的失败存档捡起来接着跑，读到的仍是 A——补充一次都没进子 agent，Resume
// 以同样的失败收尾。
func TestNestedCall_RecallWithNewInputAfterFailure(t *testing.T) {
	const vague = "帮我处理一下"
	log := &inputLog{}
	sub := newSingleNodeAgent(t, "router", "work", log, func(_ context.Context, c string) (string, error) {
		if c == vague {
			return "", errors.New("判不出该去哪个分支")
		}
		return "处理了:" + c, nil
	})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, c string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "route", sub, schema.UserMessage(c))
		if err == nil {
			return nestedFinal(events), nil
		}
		if compose_v2.MustPropagate(ctx, err) {
			return "", err
		}
		ans, askErr := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "能具体说说吗？"})
		if askErr != nil {
			return "", askErr
		}
		events, err = agent_v2.RunSubAgent(ctx, "route", sub, schema.UserMessage(c+"（补充："+ans.Content+"）"))
		if err != nil {
			return "", err
		}
		return nestedFinal(events), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := host.Run(ctx, sess, schema.UserMessage(vague))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在追问，got %v（子 agent 看到的输入：%v）", err, log.snapshot())
	}
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("系统报错了"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	want := vague + "（补充：系统报错了）"
	if err != nil {
		t.Fatalf("续跑失败：%v\n子 agent 依次处理的输入：%v\n"+
			"——带着补充的第二次调用没用上新输入（它续上了第一次调用的失败存档）", err, log.snapshot())
	}
	if n := log.count(want); n != 1 {
		t.Fatalf("子 agent 应恰好处理一次带补充的输入，实际 %d 次；依次处理的输入：%v", n, log.snapshot())
	}
	if got := nestedFinal(events); got != "处理了:"+want {
		t.Fatalf("最终产出 = %q，want %q", got, "处理了:"+want)
	}
	// 成功收尾之后，第一次调用那份失败存档也跟着清掉，不留给同一个 sid 的下一个任务
	left, err := cper.LoadAllInSession(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("LoadAllInSession: %v", err)
	}
	if len(left) != 0 {
		nss := make([]string, 0, len(left))
		for _, c := range left {
			nss = append(nss, c.NS)
		}
		t.Errorf("成功收尾后仍残留存档：%v", nss)
	}
}

// newTwoNodeAgent 造一个两节点 GraphAgent：first → second，两个节点都读输入文本，first 的
// 返回值交给 second，second 的返回值是产出。
//
// 给「子 agent 走到一半停下」的场景用：续上自己的存档时 first 不会再跑，存档丢了、从头
// 跑时会——断言落在 first 的执行次数上才分得出这两种情况（单节点子 agent 分不出：它提问
// 的 ID 按位置算，从头跑也能被 ctx 里的答案接住，结果看着一样）。
func newTwoNodeAgent(t *testing.T, name string,
	first func(ctx context.Context, content string) (string, error),
	second func(ctx context.Context, content, fromFirst string) (string, error)) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	midCh := compose_v2.LastValue[string]("mid")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, midCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("first", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if in == nil {
			return nil, errors.New("first got nil input")
		}
		mid, err := first(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(midCh, mid)}, nil
	})
	b.AddNode("second", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		mid, _ := compose_v2.Get(chs, midCh)
		if in == nil {
			return nil, errors.New("second got nil input")
		}
		out, err := second(ctx, in.Content, mid)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "first")
	b.AddEdge("first", "second")
	b.AddEdge("second", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
}

// countLog 按 key 数执行次数。
type countLog struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *countLog) inc(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[k]++
}

func (c *countLog) get(k string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[k]
}

// TestNestedCall_ThreeCallsSameNameEachResumesOwnCheckpoint：同一个节点里用同一个名字调
// 三次，中间那次停在自己的第二个节点上等人回答。续跑时三次调用各自接回自己的东西：第一次
// **拿回完成时记下的结果、不再执行**、第二次**续上自己的存档**（第一个节点不再跑）、第三次新开。
//
// 断言落在每次调用的第一个节点跑了几次上：
//   - 身份里缺输入这一维时，第一次调用先把第二次的挂起档捡走、把别人的结果当自己的；
//   - 第一次调用成功清理时若连带删了第二次的存档（兄弟执行被当成自己的子树），第二次
//     只能从头跑——它的第一个节点跑两遍、副作用重放；
//   - 完成记录没进父节点的存档时，第一次调用在续跑里整个重跑一遍。
func TestNestedCall_ThreeCallsSameNameEachResumesOwnCheckpoint(t *testing.T) {
	firsts := &countLog{}
	sub := newTwoNodeAgent(t, "worker",
		func(_ context.Context, c string) (string, error) {
			firsts.inc(c)
			return "prep:" + c, nil
		},
		func(ctx context.Context, c, mid string) (string, error) {
			if c != "two" {
				return "处理了:" + c, nil
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "two 要确认吗？"})
			if err != nil {
				return "", err
			}
			return "处理了:" + mid + "(答:" + ans.Content + ")", nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		outs := make([]string, 0, 3)
		for _, s := range []string{"one", "two", "three"} {
			events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(s))
			if err != nil {
				return "", err
			}
			outs = append(outs, nestedFinal(events))
		}
		return strings.Join(outs, " | "), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在第二次调用的提问，got %v", err)
	}
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	want := "处理了:one | 处理了:prep:two(答:好) | 处理了:three"
	if got := nestedFinal(events); got != want {
		t.Fatalf("三次调用的结果串了：got %q, want %q", got, want)
	}
	if n := firsts.get("two"); n != 1 {
		t.Fatalf("第二次调用的第一个节点应只跑 1 次（续跑接上自己的存档），实际 %d 次——"+
			"它的存档在续跑前没了，只能从头跑、副作用重放", n)
	}
	// 第一次调用已完成：续跑时拿回记下的结果，不再执行；第三次只在续跑里跑
	if n := firsts.get("one"); n != 1 {
		t.Errorf("第一次调用应只跑 1 次（续跑拿回完成时记下的结果），实际 %d——已完成的调用重跑了、副作用重放", n)
	}
	if n := firsts.get("three"); n != 1 {
		t.Errorf("第三次调用应只跑 1 次，实际 %d", n)
	}
}

// TestNestedCall_SecondCallTwoQuestionsConverges：同名第二次调用的子 agent 先后问两个
// 问题，调用方每次只答当前那一问（CLAUDE.md 示例的写法）。两次续跑之内必须收敛。
//
// 第二次调用的存档若在续跑时被第一次调用的清理连带删掉，它每次都从头跑：第一问的答案
// 上一轮已经用过、这一轮没带 ⟹ 重新问第一问 ⟹ 永远问不到第二问（审计实测无限重问）。
func TestNestedCall_SecondCallTwoQuestionsConverges(t *testing.T) {
	sub := newTwoNodeAgent(t, "worker",
		func(ctx context.Context, c string) (string, error) {
			if c != "two" {
				return "", nil
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第一问？"})
			if err != nil {
				return "", err
			}
			return ans.Content, nil
		},
		func(ctx context.Context, c, a1 string) (string, error) {
			if c != "two" {
				return "处理了:" + c, nil
			}
			a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第二问？"})
			if err != nil {
				return "", err
			}
			return "处理了:two(" + a1 + "," + a2.Content + ")", nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		outs := make([]string, 0, 2)
		for _, s := range []string{"one", "two"} {
			events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(s))
			if err != nil {
				return "", err
			}
			outs = append(outs, nestedFinal(events))
		}
		return strings.Join(outs, " | "), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	var asked []string
	for round := 0; round < 3; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		asked = append(asked, ie.Query.Question)
		reply := map[string]string{"第一问？": "甲", "第二问？": "乙"}[ie.Query.Question]
		var events []*agent_v2.AgentEvent
		events, err = host.Resume(ctx, sess.RunID,
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(reply)}),
			agent_v2.WithResumeSession(sess),
		)
		if err == nil {
			if got, want := nestedFinal(events), "处理了:one | 处理了:two(甲,乙)"; got != want {
				t.Fatalf("最终产出 = %q, want %q", got, want)
			}
			if len(asked) != 2 {
				t.Fatalf("应恰好问两次（第一问、第二问），实际依次被问 %v", asked)
			}
			return
		}
	}
	t.Fatalf("三次续跑仍未收敛：依次被问 %v，最后 err=%v——第二次调用的存档在续跑时丢了，"+
		"每次都从第一问重来", asked, err)
}

// TestNestedCall_RetryInsideNodeResumesOwnCheckpoint：节点里用 compose_v2.RunWithRetry 包
// RunSubAgent，子 agent 的第二个节点第一次失败。重试的第二次尝试必须续上第一次尝试的失败
// 存档：第一个节点不再跑。
//
// 重试若被当成「同一个名字的第二次调用」，第二次尝试落到新槽、从头跑，第一个节点连同
// 副作用重放（审计实测：重复扣款）。
func TestNestedCall_RetryInsideNodeResumesOwnCheckpoint(t *testing.T) {
	counts := &countLog{}
	failedOnce := false
	var mu sync.Mutex
	child := newTwoNodeAgent(t, "child",
		func(_ context.Context, c string) (string, error) {
			counts.inc("first")
			return "charged", nil
		},
		func(_ context.Context, c, mid string) (string, error) {
			counts.inc("second")
			mu.Lock()
			defer mu.Unlock()
			if !failedOnce {
				failedOnce = true
				return "", errors.New("下游抖了一下")
			}
			return "done:" + mid, nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, c string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "child", child, schema.UserMessage(c))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	events, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := nestedFinal(events); got != "done:charged" {
		t.Fatalf("最终产出 = %q", got)
	}
	if n := counts.get("first"); n != 1 {
		t.Fatalf("子 agent 的第一个节点应只跑 1 次（重试续上第一次尝试的存档），实际 %d 次——"+
			"重试被当成了一次新调用、从头跑，已成功的步骤连同副作用重放", n)
	}
	if n := counts.get("second"); n != 2 {
		t.Errorf("第二个节点应跑 2 次（失败一次 + 重试一次），实际 %d", n)
	}
}

// newForwardingHost 造一个单节点 GraphAgent：把自己收到的那条输入消息**原样**交给嵌套调用。
// 续跑时那条消息是从存档里解回来的。seen 非 nil 时记下每次执行看到的标注 "meta" 的 Go 类型。
func newForwardingHost(t *testing.T, sub agent_v2.Agent, seen *[]string) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	var mu sync.Mutex
	b.AddNode("host", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if in == nil {
			return nil, errors.New("host got nil input")
		}
		if seen != nil {
			mu.Lock()
			*seen = append(*seen, fmt.Sprintf("%T", in.Annotations["meta"]))
			mu.Unlock()
		}
		events, err := agent_v2.RunSubAgent(ctx, "w", sub, in)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(nestedFinal(events)))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile host: %v", err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
}

// TestNestedCall_IdentityStableAcrossCheckpointRoundTrip：父节点把自己收到的输入原样交给会
// 提问的子 agent；输入的标注里放一个 struct（字段不是字母序）和一个超过 2^53 的整数。续跑时
// 父节点拿到的是从存档里解回来的输入（struct 变成 map、大整数丢了精度）——它仍是同一次调用：
// 续上自己的存档、用上答案，不重问、第一个节点不重跑。
//
// 身份若按内存里的原值算，续跑时这次调用成了一次新调用：从头跑、换个提问身份重问。
func TestNestedCall_IdentityStableAcrossCheckpointRoundTrip(t *testing.T) {
	type meta struct {
		Zeta  string `json:"zeta"`
		Alpha int    `json:"alpha"`
	}
	firsts := &countLog{}
	asker := newTwoNodeAgent(t, "asker",
		func(_ context.Context, c string) (string, error) {
			firsts.inc(c)
			return c, nil
		},
		func(ctx context.Context, _, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return "", err
			}
			return "ok:" + ans.Content, nil
		})
	var seen []string
	host := newForwardingHost(t, asker, &seen)

	in := schema.UserMessage("hi")
	in.Annotations = map[string]any{"meta": meta{Zeta: "z", Alpha: 1}, "big": int64(1<<60 + 1)}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, in)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问，got %v", err)
	}
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("续跑应收敛，got %v——续跑时这次调用被当成了新调用、换了提问身份重问", err)
	}
	if got := nestedFinal(events); got != "ok:好" {
		t.Fatalf("最终产出 = %q", got)
	}
	// 前提：续跑那次父节点拿到的确实是从存档解回来的输入
	if len(seen) != 2 || seen[1] != "map[string]interface {}" {
		t.Fatalf("前提不成立：续跑时父节点应拿到解回来的输入（标注成 map），实际依次 %v", seen)
	}
	if n := firsts.get("hi"); n != 1 {
		t.Errorf("子 agent 的第一个节点应只跑 1 次（续上自己的存档），实际 %d 次", n)
	}
}

// TestNestedCall_VolatileInputNeedsCallKey：父节点每次执行都往输入里放一个不同的值（请求序号）。
//
//   - 不带 key：每次重跑都是一次新调用——子 agent 从头问、换一个提问身份，用户对上一次提问
//     的回答用不到这一次上，永远答不完（这是「输入变了就是另一次调用」的直接后果）；
//   - 给调用一个 key：身份跟着 key 走，续上自己的存档、一次续跑收敛（输入变了告警一次）；
//   - 把那个会变的值用 SaveStepResult 记住：每次执行给出同一份输入，照样收敛。
//
// 钉的是 RunSubAgent godoc 里「输入里带着每次都变的值」那条说明：第一格证明问题真实存在，
// 后两格证明给出的两条出路真的管用。
func TestNestedCall_VolatileInputNeedsCallKey(t *testing.T) {
	capture := installWarnCapture(t)
	asker := func(t *testing.T) agent_v2.Agent {
		return newSingleNodeAgent(t, "asker", "work", nil, func(ctx context.Context, c string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return "", err
			}
			return "ok:" + c + "(" + ans.Content + ")", nil
		})
	}
	// host 每次执行铸一个新的请求序号，mode 决定怎么把它交出去
	host := func(t *testing.T, sub agent_v2.Agent, mode string) agent_v2.Agent {
		var seq atomic.Int32
		return newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
			n := int(seq.Add(1))
			if mode == "remember" {
				if prev, ok := compose_v2.GetStepResult[int](ctx, "request_seq"); ok {
					n = prev
				} else if err := compose_v2.SaveStepResult(ctx, "request_seq", n); err != nil {
					return "", err
				}
			}
			msg := schema.UserMessage("同样的任务")
			msg.Annotations = map[string]any{"request_seq": n}
			var opts []agent_v2.RunOption
			if mode == "key" {
				opts = append(opts, agent_v2.WithCallKey("task"))
			}
			events, err := agent_v2.RunSubAgent(ctx, "w", sub, msg, opts...)
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	}
	start := func(t *testing.T, h agent_v2.Agent) (context.Context, *agent_v2.RunSession, *compose_v2.InterruptError) {
		t.Helper()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		sess := agent_v2.NewRunSession()
		_, err := h.Run(ctx, sess, schema.UserMessage("go"))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("首跑应停在子 agent 的提问，got %v", err)
		}
		return ctx, sess, ie
	}

	t.Run("不带 key：每次重跑都是新调用，重新问", func(t *testing.T) {
		h := host(t, asker(t), "none")
		ctx, sess, ie := start(t, h)
		answers := map[string]*schema.Message{}
		ids := []string{ie.Query.InterruptID}
		for round := 0; round < 2; round++ {
			answers[ie.Query.InterruptID] = schema.UserMessage("好")
			_, err := h.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			var ok bool
			ie, ok = compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("第 %d 次续跑：输入每次都变时应又一次停在提问上（新调用），got %v", round+1, err)
			}
			ids = append(ids, ie.Query.InterruptID)
		}
		if ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
			t.Fatalf("每次重跑的提问应各有自己的身份（上一次的回答不能套到这一次上），got %v", ids)
		}
	})

	t.Run("带 key：续上自己的存档、告警一次", func(t *testing.T) {
		before := len(capture.digestWarnings())
		h := host(t, asker(t), "key")
		ctx, sess, ie := start(t, h)
		events, err := h.Resume(ctx, sess.RunID,
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
			agent_v2.WithResumeSession(sess))
		if err != nil {
			t.Fatalf("带 key 时一次续跑就该收敛，got %v", err)
		}
		if got := nestedFinal(events); got != "ok:同样的任务(好)" {
			t.Fatalf("最终产出 = %q", got)
		}
		if got := capture.digestWarnings()[before:]; len(got) != 1 {
			t.Errorf("请求序号变了应告警一次，got %v", got)
		}
	})

	t.Run("用 SaveStepResult 记住会变的值：收敛、不告警", func(t *testing.T) {
		before := len(capture.digestWarnings())
		h := host(t, asker(t), "remember")
		ctx, sess, ie := start(t, h)
		events, err := h.Resume(ctx, sess.RunID,
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
			agent_v2.WithResumeSession(sess))
		if err != nil {
			t.Fatalf("记住会变的值之后一次续跑就该收敛，got %v", err)
		}
		if got := nestedFinal(events); got != "ok:同样的任务(好)" {
			t.Fatalf("最终产出 = %q", got)
		}
		if got := capture.digestWarnings()[before:]; len(got) != 0 {
			t.Errorf("输入没变，不该告警，got %v", got)
		}
	})
}

// scriptedToolModel 按调用次序返回写好的回复，每次调用都记下它收到的全部 tool_result
// 内容——断言「模型最后拿到的工具结果是什么」用。
type scriptedToolModel struct {
	mu      sync.Mutex
	replies []*schema.Message
	calls   int
	results [][]string
}

func (m *scriptedToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rs []string
	for _, mm := range msgs {
		if mm != nil && mm.Role == schema.RoleTool {
			rs = append(rs, mm.Content)
		}
	}
	m.results = append(m.results, rs)
	if m.calls >= len(m.replies) {
		return nil, fmt.Errorf("scripted model: no reply for call %d", m.calls+1)
	}
	r := m.replies[m.calls]
	m.calls++
	return r, nil
}

func (m *scriptedToolModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *scriptedToolModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// lastResults 返回模型最后一次被调用时看到的 tool_result。
func (m *scriptedToolModel) lastResults() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.results) == 0 {
		return nil
	}
	return m.results[len(m.results)-1]
}

func toolCall(id, name, input string) schema.ToolCall {
	return schema.ToolCall{
		ID:       id,
		Type:     "function",
		Function: schema.FunctionCall{Name: name, Arguments: fmt.Sprintf(`{"input":%q}`, input)},
	}
}

func newToolParent(t *testing.T, m model.ChatModel, sub agent_v2.Agent) agent_v2.Agent {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("查一件事")))
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(6),
	)
}

// TestNestedCall_ToolCallIDReusedAcrossRounds：模型两轮都用 `call_0` 调同一个包成工具的
// 子 agent。第 1 轮的调用在续跑里失败、留下可续的失败存档；第 2 轮带新输入调用时必须
// 用上新输入。
//
// 只按 tool_call ID 定身份时两轮落在同一个槽上，第 2 轮（续跑段、没有「新开」信号）
// 把第 1 轮的失败存档捡起来接着跑：新输入没进子 agent，模型拿到的是旧输入的失败，
// 全程 `err == nil`。
func TestNestedCall_ToolCallIDReusedAcrossRounds(t *testing.T) {
	log := &inputLog{}
	sub := newSingleNodeAgent(t, "lookup", "work", log, func(ctx context.Context, c string) (string, error) {
		if c == "查A" {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "真的要查 A 吗？"}); err != nil {
				return "", err
			}
			return "", errors.New("A 查不到")
		}
		return "处理了:" + c, nil
	})
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("call_0", "lookup", "查A")}),
		schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("call_0", "lookup", "查B")}),
		schema.AssistantMessage("done"),
	}}
	parent := newToolParent(t, m, sub)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("帮我查"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的确认，got %v", err)
	}
	if _, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("续跑失败：%v（子 agent 依次处理的输入：%v）", err, log.snapshot())
	}
	if n := log.count("查B"); n != 1 {
		t.Fatalf("第 2 轮的新输入应被子 agent 处理 1 次，实际 %d 次；依次处理：%v\n"+
			"——第 2 轮续上了第 1 轮的失败存档", n, log.snapshot())
	}
	got := m.lastResults()
	if len(got) == 0 || got[len(got)-1] != "处理了:查B" {
		t.Fatalf("模型最后拿到的工具结果 = %v，want 末条为 %q", got, "处理了:查B")
	}
}

// TestNestedCall_SkillForkToolCallIDReusedAcrossRounds：同上一条，入口换成 skill 的 fork
// 模式（`activate_skill` 把 skill 交给子 agent 跑）。它跟 AgentAsTool 是派生子 ns 的两个
// 入口，各自都得有一格。
func TestNestedCall_SkillForkToolCallIDReusedAcrossRounds(t *testing.T) {
	log := &inputLog{}
	leaf := newSingleNodeAgent(t, "leaf", "work", log, func(ctx context.Context, c string) (string, error) {
		if c == "查A" {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "真的要查 A 吗？"}); err != nil {
				return "", err
			}
			return "", errors.New("A 查不到")
		}
		return "处理了:" + c, nil
	})
	box := &forkBox{
		skills:      []skill.SkillDescriptor{{Name: "lookup", Description: "查一件事", Context: "fork", Agent: "leaf"}},
		activations: map[string]string{"lookup": "你负责查询"},
	}
	activate := func(query string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:   "call_0",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.ActivateSkillToolName,
				Arguments: fmt.Sprintf(`{"skill":"lookup","query":%q}`, query),
			},
		}})
	}
	m := &scriptedToolModel{replies: []*schema.Message{activate("查A"), activate("查B"), schema.AssistantMessage("done")}}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(func(name string) (agent_v2.Agent, error) {
			if name == "leaf" {
				return leaf, nil
			}
			return nil, fmt.Errorf("unknown agent %q", name)
		}),
		agent_v2.WithMaxRounds(6),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("帮我查"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在 fork 子 agent 的确认，got %v", err)
	}
	if _, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("续跑失败：%v（子 agent 依次处理的输入：%v）", err, log.snapshot())
	}
	if n := log.count("查B"); n != 1 {
		t.Fatalf("第 2 轮的新 query 应被 fork 子 agent 处理 1 次，实际 %d 次；依次处理：%v\n"+
			"——第 2 轮续上了第 1 轮的失败存档", n, log.snapshot())
	}
	got := strings.Join(m.lastResults(), " ; ")
	if !strings.Contains(got, "处理了:查B") {
		t.Fatalf("模型最后拿到的工具结果应含 %q：%q", "处理了:查B", got)
	}
}

// TestNestedCall_DuplicateToolCallIDInOneBatch：同一批里两个 tool_call 同 ID，调同一个
// 包成工具的子 agent，两边都要问用户。两次调用必须各有各的存档与提问身份。
//
// 只按 ID 定身份时两次并发调用写同一个槽，两个提问算出同一个 ID——整批被当成「同批
// 重复 InterruptID」拒掉，存档清空。
func TestNestedCall_DuplicateToolCallIDInOneBatch(t *testing.T) {
	log := &inputLog{}
	sub := newSingleNodeAgent(t, "lookup", "work", log, func(ctx context.Context, c string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认" + c + "？"})
		if err != nil {
			return "", err
		}
		return "处理了:" + c + "(答:" + ans.Content + ")", nil
	})
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			toolCall("dup", "lookup", "查A"),
			toolCall("dup", "lookup", "查B"),
		}),
		schema.AssistantMessage("done"),
	}}
	parent := newToolParent(t, m, sub)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("两个都查"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在两个确认上，got %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 2 || qs[0].InterruptID == qs[1].InterruptID {
		ids := make([]string, 0, len(qs))
		for _, q := range qs {
			ids = append(ids, q.InterruptID)
		}
		t.Fatalf("应挂起两个身份不同的提问，got %d 个：%v", len(qs), ids)
	}
	answers := make(map[string]*schema.Message, 2)
	for _, q := range qs {
		switch q.Question {
		case "确认查A？":
			answers[q.InterruptID] = schema.UserMessage("a")
		case "确认查B？":
			answers[q.InterruptID] = schema.UserMessage("b")
		default:
			t.Fatalf("意外的提问 %q", q.Question)
		}
	}
	if _, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(answers),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("续跑失败：%v（子 agent 依次处理的输入：%v）", err, log.snapshot())
	}
	got := strings.Join(m.lastResults(), " ; ")
	for _, want := range []string{"处理了:查A(答:a)", "处理了:查B(答:b)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("模型最后拿到的工具结果缺 %q：%q", want, got)
		}
	}
}

// warnCapture 收集 slog 记录。
type warnCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCapture) WithGroup(string) slog.Handler      { return h }

// digestWarnings 返回「输入与存档建立时不同」那条告警各自的 ns。
func (h *warnCapture) digestWarnings() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var nss []string
	for _, r := range h.recs {
		if !strings.Contains(r.Message, "输入与存档建立时不同") {
			continue
		}
		ns := "<no ns>"
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "ns" {
				ns = a.Value.String()
				return false
			}
			return true
		})
		nss = append(nss, ns)
	}
	return nss
}

// installWarnCapture 把默认 logger 换成捕获器，测试结束时换回。用了它的测试不能 t.Parallel。
func installWarnCapture(t *testing.T) *warnCapture {
	t.Helper()
	capture := &warnCapture{}
	orig := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return capture
}

// TestNestedCall_SameCallChangedInputIsANewCall：父节点重跑时给同一个位置换了输入——那是另一次
// 调用：新输入从头处理、自己问自己的问题，用户对旧输入的回答不会被套到新输入上。对照组：父节点
// 重跑时把**同一句话**重新构造一遍（消息 ID 每次都不同），仍是同一次调用、续上自己的存档。
//
// 子 agent 问两个问题；调用方每次把答过的答案一路累积着传（CLAUDE.md 示例的写法）。
func TestNestedCall_SameCallChangedInputIsANewCall(t *testing.T) {
	capture := installWarnCapture(t)
	replies := map[string]string{"第一问？": "好", "第二问？": "行"}

	// run 跑到收敛，返回子 agent 处理过的输入、依次被问的问题、最终产出与收尾后的残留存档数。
	run := func(t *testing.T, inputFor func(execution int) string) (log *inputLog, asked []string, final string, left int) {
		t.Helper()
		log = &inputLog{}
		sub := newSingleNodeAgent(t, "sub", "work", log, func(ctx context.Context, c string) (string, error) {
			a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第一问？"})
			if err != nil {
				return "", err
			}
			a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第二问？"})
			if err != nil {
				return "", err
			}
			return "处理了:" + c + "(答:" + a1.Content + "," + a2.Content + ")", nil
		})
		var executions atomic.Int32
		host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "sub", sub, schema.UserMessage(inputFor(int(executions.Add(1)))))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
		sess := agent_v2.NewRunSession()
		answers := map[string]*schema.Message{}
		_, err := host.Run(ctx, sess, schema.UserMessage("go"))
		for round := 0; round < 6; round++ {
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				break
			}
			asked = append(asked, ie.Query.Question)
			answers[ie.Query.InterruptID] = schema.UserMessage(replies[ie.Query.Question])
			var events []*agent_v2.AgentEvent
			events, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			if err == nil {
				final = nestedFinal(events)
			}
		}
		if err != nil {
			t.Fatalf("六次续跑内没有收敛：%v（依次被问 %v）", err, asked)
		}
		rest, lerr := cper.LoadAllInSession(ctx, sess.RunID)
		if lerr != nil {
			t.Fatalf("LoadAllInSession: %v", lerr)
		}
		return log, asked, final, len(rest)
	}

	t.Run("换了输入：新输入从头处理、自己问", func(t *testing.T) {
		before := len(capture.digestWarnings())
		log, asked, final, left := run(t, func(n int) string {
			if n == 1 {
				return "原来的输入"
			}
			return "换过的输入"
		})
		if final != "处理了:换过的输入(答:好,行)" {
			t.Fatalf("最终产出 = %q，want 处理换过的输入", final)
		}
		// 承重的是这一条：用户对原来的输入答过的第一问，没有被套到换过的输入上
		if want := []string{"第一问？", "第一问？", "第二问？"}; strings.Join(asked, "|") != strings.Join(want, "|") {
			t.Fatalf("换过的输入应自己再问一遍第一问，依次被问 %v，want %v", asked, want)
		}
		if n := log.count("原来的输入"); n != 1 {
			t.Errorf("原来的输入只在首跑处理过 1 次，实际 %d 次；依次：%v", n, log.snapshot())
		}
		if got := capture.digestWarnings()[before:]; len(got) != 0 {
			t.Errorf("不带 key 时输入变了就是另一次调用，不告警，got %v", got)
		}
		// 原来那次调用留下的挂起档没人再续，任务成功收尾时跟着清掉
		if left != 0 {
			t.Errorf("成功收尾后仍残留 %d 份存档", left)
		}
	})

	t.Run("同一句话重新构造：续上自己的存档", func(t *testing.T) {
		before := len(capture.digestWarnings())
		_, asked, final, _ := run(t, func(int) string { return "同一句话" })
		if final != "处理了:同一句话(答:好,行)" {
			t.Fatalf("最终产出 = %q", final)
		}
		if want := []string{"第一问？", "第二问？"}; strings.Join(asked, "|") != strings.Join(want, "|") {
			t.Fatalf("同一句话重新构造（只有消息 ID 不同）仍是同一次调用，每问只问一次；依次被问 %v", asked)
		}
		if got := capture.digestWarnings()[before:]; len(got) != 0 {
			t.Fatalf("不该告警，got %v", got)
		}
	})
}

// toolThenFinalModel 看对话里有没有工具结果：没有就调一次 name 工具，有就给出最终答复——
// 不按调用次数走，所以同一个实例在「续跑」与「新开」两种路径上都给出对的回复。
type toolThenFinalModel struct{ name string }

func (m *toolThenFinalModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, mm := range msgs {
		if mm != nil && mm.Role == schema.RoleTool {
			return schema.AssistantMessage("好了"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", m.name, "干活")}), nil
}

func (m *toolThenFinalModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *toolThenFinalModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestNestedCall_EveryOrchestrationAsNestedCallee：九个编排各自作为嵌套调用的被调方，父节点
// 重跑时三种情形：
//
//   - 同一份输入：续上自己的存档、用上答案。承重的是「编排交给引擎的身份摘要首跑与续跑一致」
//     ——哪个编排续跑时交了不同的摘要（比如取在钩子改写之后），它这一格会以「撞槽」终局失败；
//   - 不带 key、换了输入：另一次调用，重新问、提问身份不同；
//   - 带 key、换了输入：续上自己的存档并告警。承重的是「编排在 Run 入口把摘要交给了引擎」
//     ——漏交的那个编排续跑时悄无声息，别的格子照样绿。
func TestNestedCall_EveryOrchestrationAsNestedCallee(t *testing.T) {
	capture := installWarnCapture(t)

	asker := func(t *testing.T) agent_v2.Agent {
		return newSingleNodeAgent(t, "asker", "work", nil, func(ctx context.Context, c string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return "", err
			}
			return "ok:" + ans.Content, nil
		})
	}
	cases := []struct {
		name  string
		build func(t *testing.T, p agent_v2.Agent) agent_v2.Agent
	}{
		{"sequential", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).AddStep("w", p)
		}},
		{"parallel", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("p")).AddBranch("w", p)
		}},
		{"loop", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewLoopAgent(agent_v2.WithLoopName("l"), agent_v2.WithLoopMaxIterations(1)).WithBody(p)
		}},
		{"routing", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("r"),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) { return "w", nil }),
			).AddBranch("w", p)
		}},
		{"reflect", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewReflectAgent(
				agent_v2.WithReflectName("rf"),
				agent_v2.WithReflectMaxRounds(1),
				agent_v2.WithEvalFunc(func(context.Context, *schema.Message) (bool, error) { return true, nil }),
			).WithActor(p)
		}},
		{"handoff", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewHandoffAgent(
				agent_v2.WithHandoffName("h"),
				agent_v2.WithHandoffInitial("w"),
				agent_v2.WithHandoffMaxHops(2),
			).AddAgent("w", p)
		}},
		{"basic", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(p, agent_v2.WithAgentToolDescription("确认一件事")))
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("b"),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3),
				agent_v2.WithModel(&toolThenFinalModel{name: p.Name()}),
			)
		}},
		{"graph", func(t *testing.T, p agent_v2.Agent) agent_v2.Agent {
			return newSingleNodeAgent(t, "ga", "n", nil, func(ctx context.Context, _ string) (string, error) {
				events, err := agent_v2.RunSubAgent(ctx, "w", p, schema.UserMessage("干活"))
				if err != nil {
					return "", err
				}
				return nestedFinal(events), nil
			})
		}},
		{"plan", func(_ *testing.T, p agent_v2.Agent) agent_v2.Agent {
			planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				if len(executed) > 0 {
					return nil, "done", nil
				}
				return []agent_v2.PlanStep{{Description: "干活", SubAgentName: "w"}}, "", nil
			}
			pa := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("pl"))
			pa.AddSubAgent("w", p)
			return pa
		}},
	}
	// start 让 host 把编排嵌在自己节点里调一次，停在成员的提问上。inputFor 给第 n 次执行的输入。
	start := func(t *testing.T, orch agent_v2.Agent, inputFor func(n int) string, opts ...agent_v2.RunOption) (
		agent_v2.Agent, context.Context, *agent_v2.RunSession, *compose_v2.InterruptError) {
		t.Helper()
		var executions atomic.Int32
		host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "x", orch, schema.UserMessage(inputFor(int(executions.Add(1)))), opts...)
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		sess := agent_v2.NewRunSession()
		_, err := host.Run(ctx, sess, schema.UserMessage("go"))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("首跑应停在成员的提问，got %v", err)
		}
		return host, ctx, sess, ie
	}
	changing := func(n int) string { return fmt.Sprintf("第%d次的输入", n) }
	for _, c := range cases {
		t.Run(c.name+"/同一份输入续上存档", func(t *testing.T) {
			host, ctx, sess, ie := start(t, c.build(t, asker(t)), func(int) string { return "同样的输入" })
			if _, err := host.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
				agent_v2.WithResumeSession(sess)); err != nil {
				t.Fatalf("同一份输入应续上自己的存档、用上答案，got %v", err)
			}
		})
		t.Run(c.name+"/不带key换了输入是新调用", func(t *testing.T) {
			host, ctx, sess, ie := start(t, c.build(t, asker(t)), changing)
			_, err := host.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
				agent_v2.WithResumeSession(sess))
			ie2, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("换了输入是另一次调用，应重新问，got %v", err)
			}
			if ie2.Query.InterruptID == ie.Query.InterruptID {
				t.Fatalf("新调用的提问应有自己的身份，却与上一次相同 %q——给旧输入的回答会被套上来", ie.Query.InterruptID)
			}
		})
		t.Run(c.name+"/带key换了输入续存档并告警", func(t *testing.T) {
			host, ctx, sess, ie := start(t, c.build(t, asker(t)), changing, agent_v2.WithCallKey("k"))
			before := len(capture.digestWarnings())
			if _, err := host.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
				agent_v2.WithResumeSession(sess)); err != nil {
				t.Fatalf("带 key 的调用应续上自己的存档，got %v", err)
			}
			found := false
			for _, ns := range capture.digestWarnings()[before:] {
				if strings.HasPrefix(ns, "x#") {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s 作为带 key 的被调方、父节点重跑换了输入，它这一层（ns x#…）应告警；本次告警 ns：%v"+
					"——它的 Run 入口没把输入摘要交给引擎", c.name, capture.digestWarnings()[before:])
			}
		})
	}
}

// TestNestedCall_InstanceToolCallStableAcrossMixedFrontier：同一个节点既是静态边目标又是
// SendFrom 目标（混合 frontier），实例那位在自己起的 tool_call 里调子 agent，子 agent 走到
// 第二个节点提问。续跑时它必须续上自己的存档：只问一次、第一个节点不重跑。
//
// 实例维与 tool_call 维一起进身份时若叠了 visit：visit 按节点 ID 共享、只由静态成员推进，
// 静态兄弟成功后它从 0 变 1，中断的实例续跑时身份就变了——换一个提问 ID 重问、子 agent
// 从头跑（副作用重放）。
func TestNestedCall_InstanceToolCallStableAcrossMixedFrontier(t *testing.T) {
	firsts := &countLog{}
	var mu sync.Mutex
	var asked []string
	sub := newTwoNodeAgent(t, "worker",
		func(_ context.Context, c string) (string, error) {
			firsts.inc(c)
			return "prep:" + c, nil
		},
		func(ctx context.Context, _, mid string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				if ie, ok := compose_v2.IsInterrupt(err); ok {
					mu.Lock()
					asked = append(asked, ie.Query.InterruptID)
					mu.Unlock()
				}
				return "", err
			}
			return "处理了:" + mid + "(答:" + ans.Content + ")", nil
		})

	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "X"})
		return nil, nil
	})
	b.AddNode("X", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if compose_v2.GetCurrentInstanceID(ctx) == "" {
			return nil, nil // 静态那位：直接成功，推高共享的 visit
		}
		tcCtx := compose_v2.WithCurrentToolCallContext(ctx, "tc_1", 0)
		events, err := agent_v2.RunSubAgent(tcCtx, "lookup", sub, schema.UserMessage("q"))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(nestedFinal(events)))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "A")
	b.AddEdge("A", "X") // 静态边——跟 SendFrom 的实例并存
	b.AddEdge("X", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err = host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在实例里子 agent 的提问，got %v", err)
	}
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应收敛，got %v（依次被问 %v）——实例的身份在续跑时变了，换了一个提问 ID 重问", err, asked)
	}
	if got, want := nestedFinal(events), "处理了:prep:q(答:是)"; got != want {
		t.Errorf("产出 = %q，want %q", got, want)
	}
	if n := firsts.get("q"); n != 1 {
		t.Errorf("子 agent 的第一个节点应只跑 1 次（续上自己的存档），实际 %d 次——从头跑了、副作用重放", n)
	}
}

// TestNestedCall_CompletedSkillForkNotRerun：同一批里一个 tool_call 激活 fork 模式的 skill（交给
// 子 agent 跑完），另一个业务工具停在提问上。续跑时 tools 节点重跑：fork 那次已完成的调用拿回
// 记下的结果，子 agent 不再执行。
//
// activate_skill 是框架工具，不进 tools 节点的工具记账（见 shouldMemoize），挡住重跑的只有嵌套
// 调用自己的完成记录。没有它时子 agent 在续跑里整个再跑一遍，副作用重放。
func TestNestedCall_CompletedSkillForkNotRerun(t *testing.T) {
	log := &inputLog{}
	leaf := newSingleNodeAgent(t, "leaf", "work", log, func(_ context.Context, c string) (string, error) {
		return "查到了:" + c, nil
	})
	box := &forkBox{
		skills:      []skill.SkillDescriptor{{Name: "lookup", Description: "查一件事", Context: "fork", Agent: "leaf"}},
		activations: map[string]string{"lookup": "你负责查询"},
	}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("confirm", "问用户确认",
		map[string]any{"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string"}}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗？"})
			if err != nil {
				return nil, err
			}
			return tool.Success("用户说:" + ans.Content), nil
		}))
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "call_0", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.ActivateSkillToolName, Arguments: `{"skill":"lookup","query":"查A"}`}},
			toolCall("call_1", "confirm", "x"),
		}),
		schema.AssistantMessage("done"),
	}}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(func(name string) (agent_v2.Agent, error) {
			if name == "leaf" {
				return leaf, nil
			}
			return nil, fmt.Errorf("unknown agent %q", name)
		}),
		agent_v2.WithMaxRounds(4),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("查一下再确认"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在确认工具的提问，got %v", err)
	}
	if n := log.count("查A"); n != 1 {
		t.Fatalf("前提不成立：首跑里 fork 子 agent 应已跑完 1 次，实际 %d", n)
	}
	if _, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if n := log.count("查A"); n != 1 {
		t.Fatalf("fork 子 agent 应只跑 1 次（续跑拿回完成时记下的结果），实际 %d 次——已完成的 fork 在续跑里重跑了", n)
	}
	got := strings.Join(m.lastResults(), " ; ")
	if !strings.Contains(got, "查到了:查A") || !strings.Contains(got, "用户说:好") {
		t.Fatalf("模型最后拿到的工具结果应含两个工具各自的结果：%q", got)
	}
}
