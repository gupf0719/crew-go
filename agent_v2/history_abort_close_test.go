package agent_v2_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// danglingCount 数历史里**没有配对结果**的 tool_call 有几个。
func danglingCount(msgs []*schema.Message) int {
	answered := map[string]bool{}
	for _, m := range msgs {
		if id := schema.GetToolResultID(m); id != "" {
			answered[id] = true
		}
	}
	n := 0
	for _, m := range msgs {
		for _, tc := range schema.GetToolCalls(m) {
			if !answered[tc.ID] {
				n++
			}
		}
	}
	return n
}

// TestHistory_AbortClosesUnansweredToolCalls 钉住**用户放弃任务时，历史里的 tool_call
// 会拿到配对的结果**。
//
// ⚠️ 不补的话历史（append-only）里**永久**留一条 `assistant tool_calls=1 content=""`
// ——tools 节点原子，审批中断时结果那半根本没写过，而放弃之后也不会再有人写。
// 实测：审批中断 → Abort → 历史停在 `[user, assistant(tc=1)]`，永远配不上。
//
// 框架下游对这种残缺已经有一整套**读侧**修补（`dropDanglingToolCalls` + 合成记账文本），
// 而那条记账文本是 assistant + 零 tool_call、**正好长得像一条最终答复**，凡是「挑一条
// 消息代表某件事」的地方都得记得跳过它。**在洞产生的那一刻填上，那整串防护就不必存在。**
//
// # 两个子测试不是重复
//
// `subs`（历史配置从谁那儿借）是 9 个 Resume 入口**各自**传的，编译器只保证「传了」、
// 不保证「传对」。只钉 BasicAgent 的话，某个编排传成 nil 完全测不出来。
// 历史里那一轮的结果是一条合并消息（两个结果块，业务照 Anthropic 的样子合成的）时，放弃收尾不给第二个调用多补一条——两个
// 调用都有结果。曾经只认第一块，b 被补了一条「被放弃」，同一个调用两条结果。
func TestHistory_AbortCountsEveryResultBlock(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "tenant1", ConvID: "conv-merged"}
	const task = "task-merged"
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), key.ConvID), compose_v2.NewInMemoryCheckpointer())
	stamp := func(m *schema.Message, id string) *schema.Message {
		m.ID = id
		m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "worker", agent_v2.HistoryTaskAnnotation: task}
		return m
	}
	merged := &schema.Message{Role: schema.RoleTool, ContentBlocks: []*schema.ContentBlock{
		{Type: "tool_result", Toolresult: &schema.ToolresultContentBlock{ToolCallId: "a", Content: "ra"}},
		{Type: "tool_result", Toolresult: &schema.ToolresultContentBlock{ToolCallId: "b", Content: "rb"}},
	}}
	seed := []*schema.Message{
		stamp(schema.UserMessage("hi"), "m0"),
		stamp(schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "a", Type: "function", Function: schema.FunctionCall{Name: "f", Arguments: "{}"}},
			{ID: "b", Type: "function", Function: schema.FunctionCall{Name: "g", Arguments: "{}"}},
		}), "m1"),
		stamp(merged, "m2"),
	}
	if err := hs.Append(ctx, key, seed); err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithHistory(hs, "tenant1"),
		agent_v2.WithModel(&identityProbeModel{reply: "x"}))
	if _, err := ag.Resume(ctx, task, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	after, err := agent_v2.LoadAllHistory(ctx, hs, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(seed) {
		t.Errorf("两个调用都有结果，放弃收尾不该再补，实际历史从 %d 条变成 %d 条", len(seed), len(after))
	}
}

func TestHistory_AbortClosesUnansweredToolCalls(t *testing.T) {
	build := func(hs agent_v2.HistoryStore) *agent_v2.BasicAgent {
		rt := &recordTool{name: "do_thing"}
		tk := agent_v2_newToolkit()
		tk.Register(rt)
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
			agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHistory(hs, "tenant1"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				oneToolCall("do_thing"), schema.AssistantMessage("final")})),
		)
	}

	run := func(t *testing.T, conv string, mk func(hs agent_v2.HistoryStore) agent_v2.Agent) {
		t.Helper()
		hs := agent_v2.NewInMemoryHistoryStore()
		ag := mk(hs)
		cp := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(
			compose_v2.WithThreadIDCtx(context.Background(), conv), cp)
		sess := agent_v2.NewRunSession()

		_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
		if _, ok := compose_v2.IsInterrupt(err); !ok {
			t.Fatalf("该抛审批中断，got %v", err)
		}
		key := agent_v2.HistoryKey{Owner: "tenant1", ConvID: conv}
		before, _ := agent_v2.LoadAllHistory(ctx, hs, key)
		if danglingCount(before) == 0 {
			t.Fatalf("fixture 没搭对：中断后本该留下悬空 tool_call，实际 0 个（%d 条消息）"+
				"——测不出「补没补」", len(before))
		}

		// 用户放弃（不是 Resume 继续，是显式 Abort）
		if _, aerr := ag.Resume(ctx, sess.RunID,
			agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); aerr != nil {
			t.Fatalf("Abort: %v", aerr)
		}
		after, _ := agent_v2.LoadAllHistory(ctx, hs, key)
		if n := danglingCount(after); n != 0 {
			t.Errorf("Abort 之后历史里还有 %d 个悬空 tool_call（%d 条消息）——"+
				"append-only，这条残缺会永久留着", n, len(after))
		}
		if len(after) <= len(before) {
			t.Errorf("Abort 没往历史里补任何东西：before=%d after=%d", len(before), len(after))
		}
	}

	t.Run("BasicAgent", func(t *testing.T) {
		run(t, "conv-basic", func(hs agent_v2.HistoryStore) agent_v2.Agent { return build(hs) })
	})

	// 两个编排各跑一遍：`subs`（历史配置从谁那儿借）是 9 个 Resume 入口**各自**传的，
	// 而它们传的东西各不相同（`historySubs()` 的实现每个编排一份）。只覆盖一个编排，
	// 另一个传错完全测不出来。
	t.Run("Sequential（配置从子 agent 借）", func(t *testing.T) {
		run(t, "conv-seq", func(hs agent_v2.HistoryStore) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
				AddStep("s1", build(hs))
		})
	})

	t.Run("Parallel（另一份 historySubs 实现）", func(t *testing.T) {
		run(t, "conv-par", func(hs agent_v2.HistoryStore) agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
				AddBranch("b1", build(hs))
		})
	})
}

// TestResumePrep_EverySiteSuppliesHistorySubs 机械对账：**每个** `commonResumePrep` 调用点
// 都必须给出「历史配置从谁那儿借」，不许传 nil（GraphAgent 是唯一有记录的例外）。
//
// ⚠️ 为什么需要这条：加 `subs` 参数时编译器把 9 个调用点全列了出来——那是**「传了没」**
// 的保证，不是**「传对没」**的。传 nil 能编译，症状是 Abort 时那条编排的历史里悬空
// tool_call 不被补上，而且**无错无告警**。实测把 Sequential 那处改成 nil：
// 只有编排那条子测试红，BasicAgent 那条仍绿。
//
// 真要新增一个传 nil 的调用点，加进 allowlist 并写明为什么它没有历史配置可借。
func TestResumePrep_EverySiteSuppliesHistorySubs(t *testing.T) {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("不在 git 仓库里：%v", err)
	}
	// 唯一例外：GraphAgent 没有静态子列表，历史配置只能来自 ctx（见 WithHistoryCtx）。
	allowed := map[string]bool{"agent_v2/graph_agent.go": true}

	out, _ := exec.Command("git", "-C", strings.TrimSpace(string(root)), "grep", "-n",
		"--full-name", "commonResumePrep(ctx", "--", "agent_v2/").Output()
	seen := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}
		file, body := parts[0], parts[2]
		if strings.Contains(body, "func commonResumePrep") || strings.HasSuffix(file, "_test.go") {
			continue
		}
		seen++
		if allowed[file] {
			continue
		}
		// 末尾实参是 subs：不许是裸 nil
		if strings.Contains(body, ", nil)") || strings.Contains(body, ", nil /*") {
			t.Errorf("%s 给 commonResumePrep 传了 nil subs：\n  %s\n"+
				"Abort 收尾要靠它找历史配置，传 nil 则悬空 tool_call 不被补上且无错无告警。\n"+
				"编排传自己的 historySubs()；确实没有可借配置的加进 allowlist 并写明理由。",
				file, strings.TrimSpace(body))
		}
	}
	if seen < 5 {
		t.Fatalf("只数到 %d 个调用点——grep 写错了，这条闸是哑的", seen)
	}
	t.Logf("%d 个调用点全部给出了 subs（%d 个记录在案的例外）", seen, len(allowed))
}
