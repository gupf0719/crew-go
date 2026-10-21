package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestR31_GraphAgent_Resume_NSForward 复现 R31 视角 C HIGH-1：
//
// GraphAgent.Resume 漏 forward 父 graphNS——caller 持子 GraphAgent ref 嵌套使用
// 直接调 .Resume 时 ctx 中含父 graphNS，但 Resume 内 runOpts 未 forward 给框架，
// LoadNS(sid, "") 拿到的是父 cp 而非自己的 cp → GraphID 不一致 → 报
// ErrGraphIDConflict（或裸 ns 无 cp 时 silent fresh run 误用 resume answer）。
//
// 对照 R29 视角4/5 HIGH-1 fix: 其他 6 编排 .Resume 都已修同协议——GraphAgent.Resume
// 漏修（视角 C HIGH-1 揭露）。
//
// **测法**：用真实嵌套场景——预先注入一个不同 GraphID 的父 cp 在 (sid, "")，
// 让 Resume 不带 ns forward 时 LoadNS 拿父 cp 触发 ErrGraphIDConflict。
// fix 后 LoadNS(sid, "parent_x") 拿到对应 cp，跑通；fix 前 LoadNS(sid, "")
// 拿父 cp，GraphID 冲突报错。
func TestR31_GraphAgent_Resume_NSForward(t *testing.T) {
	inputCh := compose_v2.LastValue[*schema.Message]("__r31_in__")
	outputCh := compose_v2.LastValue[*schema.Message]("__r31_out__")

	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)

	bld.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		res, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "go on?"})
		if err != nil {
			return nil, err
		}
		ansText, _ := res.Output.(string)
		out := schema.AssistantMessage("done:" + ansText)
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "ask")
	bld.AddEdge("ask", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ga"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	parentNS := "parent_x"
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithGraphNS(ctx, parentNS)

	sess := agent_v2.NewRunSession()

	// 第一次 Run：ask_user 抛 InterruptError → cp 存到 (sid, "parent_x")
	_, err = ga.Run(ctx, sess, schema.UserMessage("x"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expected interrupt, got %v", err)
	}
	t.Logf("第一次 Run interrupt: ID=%s", ie.Query.InterruptID)

	// 验证 cp 在 ns="parent_x" 下
	cp, lerr := cper.LoadNS(ctx, sess.RunID, parentNS)
	if lerr != nil {
		t.Fatalf("LoadNS(%q): %v", parentNS, lerr)
	}
	if cp == nil {
		t.Fatal("cp 未在 ns=parent_x 存储——前置假设失败")
	}

	// 关键陷阱：在 (sid, "") 注入一个**不同 GraphID** 的"父"cp 模拟嵌套场景。
	// fix 前 GraphAgent.Resume 不 forward ns → 框架 auto-load LoadNS(sid, "")
	// 拿这个父 cp → GraphID != g.ID() → ErrGraphIDConflict 报错。
	// fix 后 forward ns="parent_x" → LoadNS(sid, "parent_x") 拿到正确 cp。
	fakeParentCP := &compose_v2.Checkpoint{
		GraphID: "fake_parent_graph_id",
		Status:  compose_v2.CPStatusPending,
	}
	if err := cper.SaveNS(ctx, sess.RunID, "", fakeParentCP); err != nil {
		t.Fatalf("SaveNS fake parent: %v", err)
	}

	// Resume：ctx 含 GraphNS="parent_x" → fix 后 forward 让 LoadNS 拿到子 cp（"parent_x"），
	// 而不是父 cp（""）。fix 前 fallback LoadNS(sid, "") 拿到 fake_parent_graph_id 触发冲突。
	answers := map[string]*schema.Message{
		ie.Query.InterruptID: schema.UserMessage("yes"),
	}
	events, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil {
		t.Fatalf("Resume: %v (fix 前应报 ErrGraphIDConflict，但若 sanity-revert 仍 pass 说明测试设计未让 fix 真正参与)", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("expected final, got nil")
	}
	if !strings.Contains(final.Content, "done:yes") {
		t.Fatalf("expected 'done:yes' in final, got %q", final.Content)
	}
}
