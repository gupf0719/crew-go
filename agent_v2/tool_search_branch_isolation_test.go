package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// fnTool 是个只做一件事的工具——用来把「它到底执行没执行」变成一个可数的量。
type fnTool struct {
	n  string
	fn func() (*tool.Result, error)
}

func (t *fnTool) Name() string                { return t.n }
func (t *fnTool) Description() string         { return t.n }
func (t *fnTool) InputSchema() map[string]any { return map[string]any{} }
func (t *fnTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return t.fn()
}

// TestToolSearch_RevealDoesNotLeakAcrossParallelBranches 钉住**渐进披露的解锁不跨调用流**。
//
// 构造：两个**同名**的 BasicAgent 跑在 ParallelAgent 的两个分支里。
// b1 调 tool_search 解锁 query_db；b2 **一次都没检索过**，等 b1 解锁完之后直接调 query_db。
//
// 修复前：揭示集的键只带 agent 名，而 `RunSession` 的 values map 在分支之间共享
// （`ForkLane`）⟹ 两个分支读写同一份揭示集 ⟹ b2 白拿了 b1 的解锁，fail-closed
// （`tool_not_revealed`）那道闸被击穿。
//
// ⚠️ 断言选在「**工具体有没有真的执行**」上，不在「有没有被拒」上——CLAUDE.md 记过这条：
// 验泄漏时断言「有没有被拦」不够，被拦可能来自别的原因，而**工具静默执行才是泄漏的实质**。
//
// ⚠️ 配套的对照组是 [TestToolSearch_NoRevealNoExecute]：b1 不检索时同一个 b2 是 0 次。
// 没有它，这条用例证明不了「0 是因为隔离生效」而不是「query_db 本来就跑不起来」。
func TestToolSearch_RevealDoesNotLeakAcrossParallelBranches(t *testing.T) {
	n := runCrossBranchReveal(t, true)
	if n != 0 {
		t.Errorf("b2 从没调过 tool_search，它的 query_db 却被执行了 %d 次 ⟹ "+
			"隔壁同名分支的解锁泄漏过来了，fail-closed 被击穿", n)
	}
}

// TestToolSearch_NoRevealNoExecute 是上一条的对照组：没人解锁时 query_db 必须跑不起来。
//
// 它同时是上一条的**前置假设检查**——这条若也是 0，说明上一条的 0 什么都不证明。
func TestToolSearch_NoRevealNoExecute(t *testing.T) {
	if n := runCrossBranchReveal(t, false); n != 0 {
		t.Errorf("没有任何分支解锁 query_db，它却被执行了 %d 次 ⟹ fail-closed 本身就没生效", n)
	}
}

// runCrossBranchReveal 跑「b1 解锁 / b2 白拿」的对照实验，返回 b2 的 query_db 执行次数。
//
// b1Searches 控制唯一的变量：b1 到底检不检索。两边其余部分逐字相同。
func runCrossBranchReveal(t *testing.T, b1Searches bool) int64 {
	t.Helper()
	signal := make(chan struct{})
	var b2DBCalls atomic.Int64
	var gateTimedOut atomic.Bool

	mkCall := func(name, args string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_" + name, Type: "function",
			Function: schema.FunctionCall{Name: name, Arguments: args},
		}})
	}
	deferQueryDB := func(tl tool.Tool) bool { return tl.Name() == "query_db" }

	// --- b1：（可选）检索解锁，然后发信号告诉 b2「我解完了」---
	tk1 := agent_v2_newToolkit()
	tk1.Register(&fnTool{n: "query_db", fn: func() (*tool.Result, error) { return tool.Success("b1_db"), nil }})
	tk1.Register(&fnTool{n: "signal", fn: func() (*tool.Result, error) { close(signal); return tool.Success("ok"), nil }})
	b1Resps := []*schema.Message{}
	if b1Searches {
		b1Resps = append(b1Resps, mkCall("tool_search", `{"query":"query_db 数据库"}`))
	}
	b1Resps = append(b1Resps, mkCall("signal", `{}`), schema.AssistantMessage("b1 done"))
	b1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), // ⬅️ 与 b2 同名——这是泄漏的全部触发条件
		agent_v2.WithToolkit(tk1), agent_v2.WithMaxRounds(6),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: deferQueryDB}),
		agent_v2.WithModel(newStubModel(b1Resps)),
	)

	// --- b2：等 b1 解锁完，然后直接调 query_db（自己从没检索过）---
	tk2 := agent_v2_newToolkit()
	tk2.Register(&fnTool{n: "query_db", fn: func() (*tool.Result, error) {
		b2DBCalls.Add(1) // ⬅️ 断言点：工具体真的跑了吗
		return tool.Success("b2_db"), nil
	}})
	tk2.Register(&fnTool{n: "gate", fn: func() (*tool.Result, error) {
		select {
		case <-signal: // 握手：保证 b1 的解锁先落地，泄漏窗口是打开的
		case <-time.After(5 * time.Second):
			gateTimedOut.Store(true)
		}
		return tool.Success("ok"), nil
	}})
	b2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), // ⬅️ 同名
		agent_v2.WithToolkit(tk2), agent_v2.WithMaxRounds(6),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: deferQueryDB}),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			mkCall("gate", `{}`), mkCall("query_db", `{}`), schema.AssistantMessage("b2 done"),
		})),
	)

	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("b1", b1).AddBranch("b2", b2)
	if _, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gateTimedOut.Load() {
		t.Fatal("fixture 没搭对：b2 等 b1 的信号超时 ⟹ 泄漏窗口没打开，这条用例什么都没测")
	}
	return b2DBCalls.Load()
}
