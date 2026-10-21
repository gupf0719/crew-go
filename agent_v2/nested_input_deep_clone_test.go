package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// redactContactsHook 在被调方的 OnAgentStart 里原地脱敏输入标注里的联系人电话（深一层的容器）。
type redactContactsHook struct{ hook.NoOpHooks }

func (redactContactsHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	for _, m := range in.Messages {
		if m == nil {
			continue
		}
		if cs, ok := m.Annotations["contacts"].([]map[string]any); ok {
			for _, c := range cs {
				c["phone"] = "***" + fmt.Sprint(c["phone"])
			}
		}
	}
	return ctx, nil
}

// askingCalleeWithHook 是一个一节点的被调方：节点先计一次数，再问一个问题。
func askingCalleeWithHook(t *testing.T, name string, firsts *atomic.Int32, h hook.Hooks) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, outCh)
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		firsts.Add(1)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发吗？"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("done:"+ans.Content))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name), agent_v2.WithGraphAgentHooks(h),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// contactsMsg 是一条带 []map[string]any 标注的输入（最常见的 JSON 形业务容器）。
func contactsMsg() *schema.Message {
	m := schema.UserMessage("通知联系人")
	m.Annotations = map[string]any{"contacts": []map[string]any{{"name": "张三", "phone": "13800001111"}}}
	return m
}

// TestNestedCall_CalleeHookEditsDeepContainer_CallerUnchangedAndConverges：调用方把 channel 里那条消息原样
// 交给 RunSubAgent，被调方钩子原地脱敏它标注里 `[]map[string]any` 的元素。交出去的是深拷贝：调用方
// 手里那条不变（它随调用方的存档落盘），续跑时摘要不变、同一次调用一次续跑就收敛。曾经只拷到「元素是
// 标量」那一层，这个形态续跑时换身份重问、被调方从头再跑（实测），父节点重跑时已完成的扣款重放。
func TestNestedCall_CalleeHookEditsDeepContainer_CallerUnchangedAndConverges(t *testing.T) {
	var firsts atomic.Int32
	callee := askingCalleeWithHook(t, "notifier", &firsts, redactContactsHook{})
	var handed *schema.Message
	pb := compose_v2.NewBuilder()
	msgCh := compose_v2.LastValue[*schema.Message]("msg")
	resCh := compose_v2.LastValue[*schema.Message]("res")
	compose_v2.WithChannel(pb, msgCh)
	compose_v2.WithChannel(pb, resCh)
	pb.AddNode("prep", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		handed = contactsMsg()
		return []compose_v2.ChannelWrite{compose_v2.Write(msgCh, handed)}, nil
	})
	pb.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		m, _ := compose_v2.Get(chs, msgCh)
		if m == nil {
			return nil, errors.New("nil msg")
		}
		evs, err := agent_v2.RunSubAgent(ctx, "notify", callee, m)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(resCh, agent_v2.FinalMessage(evs))}, nil
	})
	pb.AddEdge(compose_v2.StartNode, "prep")
	pb.AddEdge("prep", "call")
	pb.AddEdge("call", compose_v2.EndNode)
	pg, err := pb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(pg, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, resCh)
			return out
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = parent.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在被调方的提问上：%v", err)
	}
	if got := handed.Annotations["contacts"].([]map[string]any)[0]["phone"]; got != "13800001111" {
		t.Errorf("被调方钩子的改写写回了调用方：%v", got)
	}
	evs, err := parent.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("发")}))
	if err != nil {
		t.Fatalf("一次续跑应收敛：%v", err)
	}
	if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "done:发" {
		t.Errorf("final=%v", f)
	}
	if n := firsts.Load(); n != 2 {
		// 首跑 1 次（中断）+ 续跑 1 次（拿到答案）；换身份重问时会是 3 次以上
		t.Errorf("被调方节点应执行 2 次（首跑 + 续跑），实际 %d 次", n)
	}
}

// TestNestedCall_FanoutSharedDeepInput_NoRace：同一批 4 个 tool_call 把同一条消息交给 RunSubAgent，被调方
// 钩子原地改它 `[]map[string]any` 的元素。每个被调方拿自己的深拷贝：框架读摘要（遍历原件）与兄弟
// 被调方的钩子（写拷贝）之间没有共享——曾经共享深一层的 map，`-race` 报 DATA RACE、不开 -race 时并发
// map 读写直接 fatal（recover 不住，进程退出，实测 5/5）。本用例要在 -race 下才承重。
func TestNestedCall_FanoutSharedDeepInput_NoRace(t *testing.T) {
	cb := compose_v2.NewBuilder()
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(cb, outCh)
	cb.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("ok"))}, nil
	})
	cb.AddEdge(compose_v2.StartNode, "n")
	cb.AddEdge("n", compose_v2.EndNode)
	cg, err := cb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	callee := agent_v2.NewGraphAgent(cg, agent_v2.WithGraphAgentName("callee"),
		agent_v2.WithGraphAgentHooks(redactContactsHook{}),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	shared := contactsMsg()
	shared.Annotations["contacts"] = []map[string]any{{"phone": "13800001111", "a": 1, "b": 2, "c": 3, "d": 4}}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("delegate", "委派", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, err := agent_v2.RunSubAgent(ctx, "sub", callee, shared); err != nil {
				return nil, err
			}
			return tool.Success("ok"), nil
		}))
	tcs := make([]schema.ToolCall, 0, 4)
	for i := 0; i < 4; i++ {
		tcs = append(tcs, schema.ToolCall{ID: fmt.Sprintf("c%d", i), Type: "function",
			Function: schema.FunctionCall{Name: "delegate", Arguments: "{}"}})
	}
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantToolCallMessage(tcs), schema.AssistantMessage("好了")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := ba.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := shared.Annotations["contacts"].([]map[string]any)[0]["phone"]; got != "13800001111" {
		t.Errorf("被调方钩子的改写写回了共享的那条消息：%v", got)
	}
}
