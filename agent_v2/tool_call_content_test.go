package agent_v2_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// tokenHook 每次都把 confirm 的参数改写成带令牌的那份（确定性改写）。
type tokenHook struct{ hook.NoOpHooks }

func (tokenHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == "confirm" {
		in.ArgumentsJSON = `{"token":"tok-7f3a"}`
	}
	return ctx, nil
}

// 一批调用定下的内容只另存被改写过的：没改写的参数本来就在存档里那条 assistant 消息上，再存一份的话，大参数
// 每份中断档里多一份副本。改写过的必须记下——续跑时按它执行。
func TestToolCall_FixedContent_StoresOnlyRewrittenCalls(t *testing.T) {
	bulk := strings.Repeat("原文", 2000)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	tk.Register(tool.NewFuncTool("confirm", "问人", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	ba := agent_v2.NewBasicAgent(
		agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(tokenHook{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "lookup", bulk), toolCall("c2", "confirm", "y")}),
			schema.AssistantMessage("好了"),
		})),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	if _, err := ba.Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("首跑应停在 confirm 的提问")
	}
	cp, err := cper.LoadNS(ctx, sess.RunID, "")
	if err != nil || cp == nil {
		t.Fatalf("应留下中断档：cp=%v err=%v", cp, err)
	}
	raw, err := json.Marshal(cp.NodeStates)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "原文原文原文") {
		t.Errorf("没被改写的调用的参数另存进了节点私有态（%d 字节）", len(raw))
	}
	if !strings.Contains(string(raw), "tok-7f3a") {
		t.Errorf("被改写的调用应记下改写后的内容，节点私有态里没有")
	}
}

// 自写 toolsFn（低阶 builder）拿到的也是定下的内容：续跑时钩子把 send_sms 改名成 send_email，toolsFn 收到的仍是
// send_sms。只数短信、邮件的次数分不出对错——改名生效时新工具读到旧工具记的「发过了」，同样是短信 1、邮件 0
// （那正是自写 toolsFn 拿不到按工具名分域时的旧缺陷）——所以断言 toolsFn 每次收到的工具名。
func TestBasicAgentBuilder_CustomToolsFn_GetsFixedContent(t *testing.T) {
	round := 0
	llm := func(context.Context, []*schema.Message, []*schema.Message) (*schema.Message, error) {
		round++
		if round == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "send_sms", "x")}), nil
		}
		return schema.AssistantMessage("done"), nil
	}
	var mu sync.Mutex
	var seen []string
	tools := func(_ context.Context, tcs []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error) {
		out := make([]*schema.Message, 0, len(tcs))
		for i, tc := range tcs {
			mu.Lock()
			seen = append(seen, tc.Function.Name)
			mu.Unlock()
			c := compose_v2.WithCurrentToolCallContext(callCtxs[i], tc.ID, i)
			if tc.Function.Name == "send_sms" {
				if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "短信发了，要回执吗？"}); err != nil {
					return nil, err
				}
			}
			out = append(out, schema.ToolResultMessage(tc.ID, tc.Function.Name, tc.Function.Name+" ok"))
		}
		return out, nil
	}
	h := &renameOnResumeHook{}
	g, err := agent_v2.NewBasicAgentBuilder(llm, tools).WithAgentHooks(h).Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s-custom-toolsfn")}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(opts, compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("通知用户"))...))...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在回执的提问, got %v", err)
	}
	h.resumed.Store(true)
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(opts, compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("不用")}))...); err != nil {
		t.Fatalf("续跑: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(seen, ",") != "send_sms,send_sms" {
		t.Errorf("续跑时的改名不该生效，自写 toolsFn 两次都应收到 send_sms，实际 %q", seen)
	}
}
