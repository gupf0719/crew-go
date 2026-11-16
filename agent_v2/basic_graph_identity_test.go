package agent_v2_test

// BasicAgent 的图身份：显式给了名字时是「名字 + 结构指纹」，没给时只有结构指纹（自动名每次构造都变）。
//
// 名字进身份是为了让两个结构相同的 BasicAgent 不再共用一个身份：经 EnterRun 进入的自研壳在自己的阶段里拿 session 裸调 x、
// 再把调用透传给 inner（同一个 ns）时，槽里放着 inner 的挂起档却没有执行占着它，x 进入那个槽。两个身份相同时 x 把 inner 的
// 挂起档读进来、用自己的工具接着跑（旁路工具执行一次、inner 那一问的答复没被用上），err == nil；现在 x 在载入时撞
// ErrGraphIDConflict、什么都不执行，inner 的挂起档原样留着。

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// sideCallMsg 是调一次 side 工具的模型产出。
func sideCallMsg(id string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
		Function: schema.FunctionCall{Name: "side", Arguments: `{}`}}})
}

// sideToolkit 是只有一件 side 工具的 toolkit：ask 为真时先问人、答了才计数。
func sideToolkit(ask bool, n *atomic.Int32) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("side", "旁路工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ask {
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "执行旁路吗"}); err != nil {
					return nil, err
				}
			}
			n.Add(1)
			return tool.Success("side done"), nil
		}))
	return tk
}

// bareCallShell 是经 EnterRun 进入的自研壳：bare 为真时先在自己的阶段里拿 session 裸调 x（预检），再把调用透传给 inner。
type bareCallShell struct {
	x, inner agent_v2.Agent
	bare     *atomic.Bool
}

func (s *bareCallShell) Name() string        { return "shell" }
func (s *bareCallShell) Description() string { return "预检后透传" }
func (s *bareCallShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, s.Name())
	if err != nil {
		return nil, err
	}
	if s.bare.Load() {
		if _, err := s.x.Run(ctx, sess, schema.UserMessage("预检")); err != nil {
			return nil, err
		}
	}
	return s.inner.Run(ctx, sess, in, agent_v2.RegrantRouteTo(opts, s, s.inner)...)
}
func (s *bareCallShell) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return s.inner.Resume(ctx, sid, opts...)
}

func TestShellBareCall_DoesNotLoadForwardedAgentsCheckpoint(t *testing.T) {
	var sideX, sideInner atomic.Int32
	var bare atomic.Bool
	bare.Store(true)
	x := agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithToolkit(sideToolkit(false, &sideX)), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x ok"), schema.AssistantMessage("x ok")})))
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"), agent_v2.WithToolkit(sideToolkit(true, &sideInner)), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{sideCallMsg("c1"), schema.AssistantMessage("inner done")})))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("shell", &bareCallShell{x: x, inner: inner, bare: &bare})

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：inner 的 side 应停在问人上，实际 %v", err)
	}
	original := checkpointIdentityBytes(t, ctx, cp, sess.RunID, "shell")
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("执行")}

	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if sideX.Load() != 0 || sideInner.Load() != 0 {
		t.Fatalf("x 不该接着跑 inner 的挂起档（曾经 x 的旁路工具执行一次），实际 x=%d inner=%d err=%v",
			sideX.Load(), sideInner.Load(), err)
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("壳在自己的阶段里裸调的 x 进了 inner 的槽，应在载入时撞 ErrGraphIDConflict，实际 %v", err)
	}

	// inner 的挂起档原样留着：还是 inner 的图、还挂着那一问
	got, err := cp.LoadNS(ctx, sess.RunID, "shell")
	if err != nil || got == nil {
		t.Fatalf("inner 的挂起档应还在槽里，实际 cp=%v err=%v", got, err)
	}
	if !bytes.Equal(original, checkpointIdentityBytes(t, ctx, cp, sess.RunID, "shell")) || len(got.PendingInterrupts) != 1 ||
		got.PendingInterrupts[0].InterruptID != ie.AllQueries()[0].InterruptID {
		t.Fatalf("槽里应是 inner 挂着那一问的档，实际 GraphID=%q 挂起 %d 条", got.GraphID, len(got.PendingInterrupts))
	}
}

// runToInterruptThenResumeWith 让 first 跑到问人，再用 second（另一个实例）续跑，返回续跑的错误与 second 的 side 计数。
func runToInterruptThenResumeWith(t *testing.T, first, second func(*atomic.Int32) agent_v2.Agent) (error, int32) {
	t.Helper()
	var n1, n2 atomic.Int32
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := first(&n1).Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：side 应停在问人上，实际 %v", err)
	}
	_, err = second(&n2).Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("执行")}))
	return err, n2.Load()
}

// sideAskingAgent 构造会在 side 上问人的 agent。resumer 为真时是续跑用的新实例：它的模型只剩收尾那一句（第一次调模型发出
// side 调用的是先前那个实例）。
func sideAskingAgent(resumer bool, opts ...agent_v2.Option) func(*atomic.Int32) agent_v2.Agent {
	return func(n *atomic.Int32) agent_v2.Agent {
		replies := []*schema.Message{sideCallMsg("c1"), schema.AssistantMessage("done")}
		if resumer {
			replies = replies[1:]
		}
		base := []agent_v2.Option{agent_v2.WithToolkit(sideToolkit(true, n)), agent_v2.WithMaxRounds(3),
			agent_v2.WithModel(newStubModel(replies))}
		return agent_v2.NewBasicAgent(append(base, opts...)...)
	}
}

// 每个请求重建 agent（agui 的用法）：同名、同配置的新实例续得上；没给名字的也续得上——自动名每次构造都变，不进身份。
func TestBasicAgentGraphIdentity_RebuiltInstanceResumes(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []agent_v2.Option
	}{
		{"显式名字", []agent_v2.Option{agent_v2.WithName("assistant")}},
		{"没给名字", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err, n := runToInterruptThenResumeWith(t, sideAskingAgent(false, tc.opts...), sideAskingAgent(true, tc.opts...))
			if err != nil || n != 1 {
				t.Fatalf("新实例应续上挂起档、side 执行一次，实际 err=%v side=%d", err, n)
			}
		})
	}
}

// 显式名字不顶替结构：同名的 agent 改了配置、图多了一个节点之后，续跑旧挂起档照样撞冲突（只用名字的话就静默接着跑了）。
func TestBasicAgentGraphIdentity_StructureChangeStillConflicts(t *testing.T) {
	gate := agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) { return "", nil })
	err, n := runToInterruptThenResumeWith(t, sideAskingAgent(false, agent_v2.WithName("assistant")),
		sideAskingAgent(true, agent_v2.WithName("assistant"), gate))
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || n != 0 {
		t.Fatalf("结构变了应撞 ErrGraphIDConflict、不执行，实际 err=%v side=%d", err, n)
	}
}

// 改名之后旧挂起档续不上，明确报冲突（这是名字进身份的代价）。
func TestBasicAgentGraphIdentity_RenamedAgentConflicts(t *testing.T) {
	err, n := runToInterruptThenResumeWith(t, sideAskingAgent(false, agent_v2.WithName("assistant")),
		sideAskingAgent(true, agent_v2.WithName("helper")))
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || n != 0 {
		t.Fatalf("改名后续跑应撞 ErrGraphIDConflict、不执行，实际 err=%v side=%d", err, n)
	}
}

func checkpointIdentityBytes(t *testing.T, ctx context.Context, cp compose_v2.Checkpointer, runID, ns string) []byte {
	t.Helper()
	saved, err := cp.LoadNS(ctx, runID, ns)
	if err != nil || !saved.HasResumableWork() {
		t.Fatalf("原存档应保留在 (%q, %q)，实际 err=%v, cp=%v", runID, ns, err, saved != nil)
	}
	raw, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type checkpointHookCounter struct {
	hook.NoOpHooks
	calls atomic.Int32
}

func (h *checkpointHookCounter) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.calls.Add(1)
	return ctx, nil
}

func TestCheckpointIdentity_GraphAgentWrapperNameProtectsCheckpoint(t *testing.T) {
	out := compose_v2.LastValue[*schema.Message]("out")
	builder := compose_v2.NewBuilder()
	builder = compose_v2.WithChannel(builder, out)
	var effects atomic.Int32
	builder.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准原任务？"})
		if err != nil {
			return nil, err
		}
		effects.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage(answer.Content))}, nil
	})
	graph, err := builder.AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	newAgent := func(name string, hooks hook.Hooks) *agent_v2.GraphAgent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName(name), agent_v2.WithGraphAgentHooks(hooks),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				result, _ := compose_v2.Get(chs, out)
				return result
			}))
	}
	h := &checkpointHookCounter{}
	a, b := newAgent("wrapper-a", hook.NoOpHooks{}), newAgent("wrapper-b", h)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), cp)
	session := agent_v2.NewRunSession()
	_, err = a.Run(ctx, session, schema.UserMessage("原任务"))
	interrupted, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupted.AllQueries()) != 1 {
		t.Fatalf("原任务应挂起一个问题，err=%v", err)
	}
	question := interrupted.AllQueries()[0].InterruptID
	raw := checkpointIdentityBytes(t, ctx, cp, session.RunID, "")
	answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{question: schema.UserMessage("批准原任务")})

	_, err = b.Resume(ctx, session.RunID, answers)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || h.calls.Load() != 0 || effects.Load() != 0 {
		t.Fatalf("包装名不同时应在副作用前拒绝，err=%v hook=%d, effects=%d", err, h.calls.Load(), effects.Load())
	}
	if !bytes.Equal(raw, checkpointIdentityBytes(t, ctx, cp, session.RunID, "")) {
		t.Fatal("旧档应保持不变")
	}
	rebuilt := newAgent("wrapper-a", hook.NoOpHooks{})
	events, err := rebuilt.Resume(ctx, session.RunID, answers)
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "批准原任务" || effects.Load() != 1 {
		t.Fatalf("同名重建应使用原答案，final=%v err=%v effects=%d", final, err, effects.Load())
	}
	saved, err := cp.LoadNS(ctx, session.RunID, "")
	if err != nil || saved != nil {
		t.Fatalf("成功后应清除旧档，err=%v saved=%v", err, saved != nil)
	}
}
