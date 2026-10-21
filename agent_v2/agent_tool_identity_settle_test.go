package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// resumeUntilDone 带累积答案续跑到收敛（最多 n 轮），返回续跑的轮数。
func resumeUntilDone(ctx context.Context, ag agent_v2.Agent, sess *agent_v2.RunSession, err error, n int) (int, error) {
	answers := map[string]*schema.Message{}
	rounds := 0
	for i := 0; i < n; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		rounds++
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	return rounds, err
}

// traceIDMiddleware 给每次工具调用注入一个 trace_id（每次执行都不同）。
func traceIDMiddleware(seq *atomic.Int32) agent_v2.ToolMiddleware {
	return func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
			a := map[string]any{}
			for k, v := range args {
				a[k] = v
			}
			a["trace_id"] = fmt.Sprintf("t-%d", seq.Add(1))
			return next(ctx, name, a)
		}
	}
}

// traceIDOnToolStart 在 OnToolStart 里往参数注入 trace_id（「参数注入」是 OnToolStart [消费] 档的本职）。
type traceIDOnToolStart struct {
	hook.NoOpHooks
	seq atomic.Int32
}

func (h *traceIDOnToolStart) OnToolStart(ctx context.Context, _ *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	ti.ArgumentsJSON = fmt.Sprintf(`{"input":"合同","trace_id":"t-%d"}`, h.seq.Add(1))
	return ctx, nil
}

// TestAgentAsTool_ArgsRewritten_CallKeyConverges：tools 节点直接调起的 AgentAsTool，参数在模型消息与工具体
// 之间被注入每次都不同的值（中间件的 trace_id、OnToolStart 的注入），被调方问人。按内容定身份时每次续跑
// 都是一次新调用：被调方从头跑、换 ID 重问，永远收敛不了（实测续跑 4 轮仍在问）。声明
// WithAgentToolCallKey（取不含注入值的那个参数）之后一次续跑收敛、被调方第一个节点只跑一次。
func TestAgentAsTool_ArgsRewritten_CallKeyConverges(t *testing.T) {
	for _, variant := range []string{"middleware", "onToolStart"} {
		t.Run(variant, func(t *testing.T) {
			firsts := &countLog{}
			callee := newTwoNodeAgent(t, "approver",
				func(_ context.Context, c string) (string, error) { firsts.inc("first"); return c, nil },
				func(ctx context.Context, c, _ string) (string, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗？"})
					if err != nil {
						return "", err
					}
					return "approved:" + ans.Content, nil
				})
			at := agent_v2.AgentAsTool(callee, agent_v2.WithAgentToolDescription("审批"),
				agent_v2.WithAgentToolInputSchema(map[string]any{"type": "object",
					"properties": map[string]any{"input": map[string]any{"type": "string"}}}),
				agent_v2.WithAgentToolCallKey(func(args map[string]any) string {
					s, _ := args["input"].(string)
					return s
				}))
			tk := tool.NewToolkit()
			tk.Register(at)
			opts := []agent_v2.Option{agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "approver", "合同")}),
					schema.AssistantMessage("ok")}))}
			var seq atomic.Int32
			if variant == "middleware" {
				opts = append(opts, agent_v2.WithToolMiddleware(traceIDMiddleware(&seq)))
			} else {
				opts = append(opts, agent_v2.WithHooks(&traceIDOnToolStart{}))
			}
			host := agent_v2.NewBasicAgent(opts...)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := host.Run(ctx, sess, schema.UserMessage("go"))
			rounds, err := resumeUntilDone(ctx, host, sess, err, 4)
			if err != nil || rounds != 1 || firsts.get("first") != 1 {
				t.Errorf("声明 key 之后应一次续跑收敛、被调方第一个节点只跑一次：err=%v 续跑 %d 轮 执行 %d 次",
					err, rounds, firsts.get("first"))
			}
		})
	}
}

// TestAgentAsTool_OnToolStartInjection_ConvergesWithoutCallKey：OnToolStart 往参数里注入每次都变的 trace_id，
// 不声明 WithAgentToolCallKey 也一次续跑收敛——一批调用的内容在第一次交给工具执行时定下、续跑沿用（见
// fixToolCallContent），交给被调方的输入续跑不变。中间件的注入在内容定下之后、每次执行都跑，仍要声明 key（上一条）。
func TestAgentAsTool_OnToolStartInjection_ConvergesWithoutCallKey(t *testing.T) {
	firsts := &countLog{}
	callee := newTwoNodeAgent(t, "approver",
		func(_ context.Context, c string) (string, error) { firsts.inc("first"); return c, nil },
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗？"})
			if err != nil {
				return "", err
			}
			return "approved:" + ans.Content, nil
		})
	at := agent_v2.AgentAsTool(callee, agent_v2.WithAgentToolDescription("审批"),
		agent_v2.WithAgentToolInputSchema(map[string]any{"type": "object",
			"properties": map[string]any{"input": map[string]any{"type": "string"}}}))
	tk := tool.NewToolkit()
	tk.Register(at)
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithHooks(&traceIDOnToolStart{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "approver", "合同")}),
			schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	rounds, err := resumeUntilDone(ctx, host, sess, err, 4)
	if err != nil || rounds != 1 || firsts.get("first") != 1 {
		t.Errorf("OnToolStart 的注入续跑不变，不声明 key 也应一次续跑收敛、被调方第一个节点只跑一次：err=%v 续跑 %d 轮 执行 %d 次",
			err, rounds, firsts.get("first"))
	}
}

// settleProbeHook 在名为 name 的 agent 的 OnAgentStart 里调一次 AgentAsTool。
type settleProbeHook struct {
	hook.NoOpHooks
	name string
	at   tool.Tool
}

func (h *settleProbeHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name != h.name {
		return ctx, nil
	}
	_, err := h.at.Call(ctx, map[string]any{"input": "预检请求"})
	return ctx, err
}

// TestAgentAsTool_OutsideToolCall_SettlesHistory：在不是 tool_call 的地方调 AgentAsTool（GraphAgent 的节点体、
// 被调方的 agent 级钩子），被调方又不是 BasicAgent（自己不记历史）时，它的输入与产出由框架在边界补记。
// 那里没有 tool_result 兜底——曾经一律不补，两种位置下被调方在会话历史里一条不剩（实测 0/0）。
func TestAgentAsTool_OutsideToolCall_SettlesHistory(t *testing.T) {
	t.Run("节点体", func(t *testing.T) {
		hs := agent_v2.NewInMemoryHistoryStore()
		at := agent_v2.AgentAsTool(&externalAgent{name: "helper"})
		inCh := compose_v2.LastValue[*schema.Message]("in")
		outCh := compose_v2.LastValue[*schema.Message]("out")
		bld := compose_v2.NewBuilder().WithName("g_settle")
		compose_v2.WithChannel(bld, inCh)
		compose_v2.WithChannel(bld, outCh)
		bld.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, inCh)
			if _, err := at.Call(ctx, map[string]any{"input": "子任务:" + in.Content}); err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("节点结果"))}, nil
		})
		bld.AddEdge(compose_v2.StartNode, "call")
		bld.AddEdge("call", compose_v2.EndNode)
		g, err := bld.Compile()
		if err != nil {
			t.Fatal(err)
		}
		ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("ga"),
			agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
			}),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				out, _ := compose_v2.Get(chs, outCh)
				return out
			}))
		ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(context.Background(), "conv-settle-node"), hs, "t")
		if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		requireSettled(t, loadHist(t, hs, ctx, "conv-settle-node"), "子任务:原话")
	})
	t.Run("被调方的钩子", func(t *testing.T) {
		hs := agent_v2.NewInMemoryHistoryStore()
		worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithDescription("w"),
			agent_v2.WithHooks(&settleProbeHook{name: "worker", at: agent_v2.AgentAsTool(&externalAgent{name: "checker"})}),
			agent_v2.WithMaxRounds(2),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("worker done")})))
		tk := tool.NewToolkit()
		tk.Register(agent_v2.AgentAsTool(worker))
		parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("w1", "worker", "做事")}),
				schema.AssistantMessage("综合结论"),
			})),
			agent_v2.WithToolkit(tk), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(4))
		ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-settle-hook")
		if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		requireSettled(t, loadHist(t, hs, ctx, "conv-settle-hook"), "预检请求")
	})
}

// requireSettled 断言被调方的输入（原文 in）与产出（externalAgent 的「【外部产出】」）在历史里各一份。
func requireSettled(t *testing.T, msgs []*schema.Message, in string) {
	t.Helper()
	inputs, outputs := 0, 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && strings.TrimSpace(m.Content) == in {
			inputs++
		}
		if strings.Contains(m.Content, "【外部产出】") {
			outputs++
		}
	}
	if inputs != 1 || outputs != 1 {
		t.Errorf("被调方的输入与产出应在历史里各一份，实际输入 %d 份、产出 %d 份（共 %d 条）", inputs, outputs, len(msgs))
	}
}

// TestAgentAsTool_SchemaAndFullHistory_NewArgsReachCallee：WithAgentToolInputSchema 与
// WithAgentToolFullHistoryInput 同时设时 schema 优先（输入是调用方给的 args）。身份、派生标记也要按这个
// 判据：曾经身份按「全历史输入」只取位置，节点体里换参数重试时 3 次尝试都续上第一次的失败存档，新参数
// 永远进不了被调方（实测）。
func TestAgentAsTool_SchemaAndFullHistory_NewArgsReachCallee(t *testing.T) {
	log := &inputLog{}
	sub := newTwoNodeAgent(t, "writer",
		func(_ context.Context, c string) (string, error) { log.add(c); return c, nil },
		func(_ context.Context, c, _ string) (string, error) {
			if strings.Contains(c, "第1版") {
				return "", errors.New("第1版不合格")
			}
			return "成稿(" + c + ")", nil
		})
	at := agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("写"),
		agent_v2.WithAgentToolInputSchema(map[string]any{"type": "object",
			"properties": map[string]any{"q": map[string]any{"type": "string"}}}),
		agent_v2.WithAgentToolFullHistoryInput())
	var final string
	host := newSingleNodeAgent(t, "host", "work", nil, func(ctx context.Context, _ string) (string, error) {
		n := 0
		out, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			n++
			res, err := at.Call(ctx, map[string]any{"q": fmt.Sprintf("第%d版", n)})
			if err != nil {
				return "", err
			}
			if !res.Success {
				return "", errors.New("子 agent 失败")
			}
			s, _ := res.Output.(string)
			return s, nil
		})
		final = out
		return out, err
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v（子 agent 依次看到的输入 %q）", err, log.snapshot())
	}
	if !strings.Contains(final, "第2版") {
		t.Errorf("第二次尝试的新参数应进到被调方：最终 %q，被调方依次看到 %q", final, log.snapshot())
	}
}

// TestSkillFork_EmptyToolCallID_Works：provider 不回 tool_call ID 时，fork 模式的 activate_skill 照样能跑。
// 曾经拿「tool_call ID 非空」判在不在 tool_call 里，这种 provider 下每次 fork 都失败、父模型拿到
// 「invariant violated」（实测）——同一批 tool_call 靠批内下标区分，在不在 tool_call 里看的是章。
func TestSkillFork_EmptyToolCallID_Works(t *testing.T) {
	box := &forkBox{
		skills:      []skill.SkillDescriptor{{Name: "research", Description: "调研", Context: "fork", Agent: "research_agent"}},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	ext := &externalAgent{name: "skill_research"}
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "research_agent" {
			return ext, nil
		}
		return nil, nil
	}
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "", Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"research","query":"调研子任务"}`}}}),
		schema.AssistantMessage("综合结论"),
	}}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(4))
	if _, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Join(m.lastResults(), " | "); !strings.Contains(got, "【外部产出】调研子任务") {
		t.Errorf("fork 应跑通、父模型拿到它的产出，实际工具结果 %q", got)
	}
}
