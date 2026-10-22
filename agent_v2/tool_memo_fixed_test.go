package agent_v2_test

// 一次调用的结果在这一批第一次处理完（OnToolEnd、批内裁决、按大小处理）时定下（见 tool_memo.go 的 finalizeToolMemo）：
// 之后这次执行在提交之前作废、续跑时不再处理——工具不执行、OnToolStart / OnToolEnd 不触发，交出定下的那份（内容、结论、
// 钩子做出的决定都不变）；处理完之前作废的，续跑时照常重新处理。这里用「tools 节点成功结束时 OnNodeEnd 钩子 Goexit 一次」
// 造出「节点体已成功、提交前作废」那段窗口。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// toolEndFunc 把一个函数当 OnToolEnd 钩子，并数 OnToolStart 被调的次数。
type toolEndFunc struct {
	hook.NoOpHooks
	starts atomic.Int64
	fn     func(ctx context.Context, out *schema.ToolOutput)
}

func (h *toolEndFunc) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	h.starts.Add(1)
	return ctx, nil
}

func (h *toolEndFunc) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil {
		h.fn(ctx, out)
	}
	return ctx, nil
}

// runThroughPostToolsPanic 跑 ag 一次（tools 节点成功之后的 OnNodeEnd 掐断那次 Run，见 goexitAfterNodeOnce），再续跑到成功；
// 返回续跑那次收到的上行信号。
func runThroughPostToolsPanic(t *testing.T, ag agent_v2.Agent) compose_v2.Signals {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	ctx = agent_v2.WithCtxOrchHooks(ctx, &goexitAfterNodeOnce{node: "tools"})
	sess := agent_v2.NewRunSession()
	runExpectingGoexit(t, func() { _, _ = ag.Run(ctx, sess, schema.UserMessage("go")) })
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		_, e := ag.Resume(c, sess.RunID, agent_v2.WithResumeSession(sess))
		return e
	})
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	return sigs
}

// TestToolMemo_FixedAfterProcessing_HookVerdictDoesNotFlip：开转存，钩子按全文判（不是合法 JSON 就改成失败）。首跑钩子看到
// 3011 字节的 JSON、判成功，结果转存成说明；这次执行在提交前作废。续跑时交给钩子的只能是转存说明（不是 JSON），照它
// 重新处理会把结论翻成失败——定下了就不再处理：模型看到转存说明，工具只执行一次，钩子只在首跑触发一次。
func TestToolMemo_FixedAfterProcessing_HookVerdictDoesNotFlip(t *testing.T) {
	var execs, hookCalls atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("big", "大输出", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		execs.Add(1)
		return tool.Success(`{"data":"` + strings.Repeat("y", 3000) + `"}`), nil
	}))
	h := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
		hookCalls.Add(1)
		if !json.Valid([]byte(out.Result.Output)) {
			out.Result = &schema.ToolCallResult{Success: false, ErrorCode: "bad_json", Error: "工具输出不是合法 JSON"}
		}
	}}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "big")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h), agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300}))
	runThroughPostToolsPanic(t, ag)
	got := lastResultFor(t, seen, "c1")
	if strings.HasPrefix(got.Content, "[bad_json]") {
		t.Fatalf("首跑判成功、结果已定下，续跑重放时钩子在转存说明上重新处理、把结论翻成了失败：模型看到 %q", got.Content)
	}
	if !spillIDRe.MatchString(got.Content) {
		t.Fatalf("模型应看到定下的转存说明，实际 %q", got.Content[:min(80, len(got.Content))])
	}
	if n := execs.Load(); n != 1 {
		t.Fatalf("工具应只执行 1 次（续跑命中工具账），实际 %d 次", n)
	}
	if n := hookCalls.Load(); n != 1 {
		t.Fatalf("定下了的续跑不再处理，OnToolEnd 应只在首跑触发一次，实际 %d 次", n)
	}
}

// TestToolMemo_FixedAfterProcessing_NotReprocessed：没开转存，钩子每次在结果后面追加它被调的序号。首跑处理完是「ok#1」；
// 续跑按原文重新处理的话是「ok#2」、在定下的那份上再处理是「ok#1#2」——定下了就不再处理，跟配没配转存无关。
func TestToolMemo_FixedAfterProcessing_NotReprocessed(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	var n atomic.Int64
	h := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
		out.Result.Output += fmt.Sprintf("#%d", n.Add(1))
	}}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "t")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	runThroughPostToolsPanic(t, ag)
	if n.Load() != 1 {
		t.Fatalf("定下了的续跑不再处理，OnToolEnd 应只触发一次，实际 %d 次", n.Load())
	}
	if s := h.starts.Load(); s != 1 {
		t.Errorf("OnToolStart 应跟 OnToolEnd 一样只触发一次（钩子开的 span 才不落单），实际 %d 次", s)
	}
	if got := lastResultFor(t, seen, "c1").Content; got != "ok#1" {
		t.Fatalf("结果应是首跑处理完定下的 ok#1，实际 %q——续跑按原文重新处理了一遍", got)
	}
}

// TestToolMemo_FixedAfterProcessing_HookDecisionReplayed：钩子在 OnToolEnd 里做决定，第一次定 alpha、之后定 beta（看到的
// 东西变了、或钩子本身不确定）。首跑处理完时这次调用的决定是 alpha；续跑交出的应是定下的 alpha，钩子不再触发。
func TestToolMemo_FixedAfterProcessing_HookDecisionReplayed(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	var n atomic.Int64
	h := &toolEndFunc{fn: func(ctx context.Context, _ *schema.ToolOutput) {
		if n.Add(1) == 1 {
			evSegDecide(ctx, "alpha")
		} else {
			evSegDecide(ctx, "beta")
		}
	}}
	ag := evSegAgent(tk, h, schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "t")}), schema.AssistantMessage("done"))
	sigs := runThroughPostToolsPanic(t, ag)
	if n.Load() != 1 {
		t.Fatalf("定下了的续跑不再处理，OnToolEnd 应只触发一次，实际 %d 次", n.Load())
	}
	if got := evSegDecision(sigs); got != "alpha" {
		t.Fatalf("续跑交出的决定应是定下时的 alpha，实际 %s", got)
	}
}

// TestToolMemo_FixedAfterProcessing_SupersededKeepsFirstReplacement：同一批 [c1 定 alpha、c2 定 beta 并返回 3000 字节]，开转存，
// 钩子给每个成功结果追加它被调的序号。c2 被裁决换成失败结果（带着它原本的结果）。首跑处理完之后作废：出口存档里不该留着
// c2 转存前的原文——定下的是替换后的那份；续跑后生效的仍是 alpha，模型看到的 c2 是首跑那份替换结果（带 #2），不是在原文上
// 重新处理出来的（#4）。
func TestToolMemo_FixedAfterProcessing_SupersededKeepsFirstReplacement(t *testing.T) {
	var execs atomic.Int64
	big := strings.Repeat("Q", 3000)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("d1", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		execs.Add(1)
		evSegDecide(ctx, "alpha")
		return tool.Success("ok1"), nil
	}))
	tk.Register(tool.NewFuncTool("d2", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		execs.Add(1)
		evSegDecide(ctx, "beta")
		return tool.Success(big), nil
	}))
	var n atomic.Int64
	h := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
		if out.Result.Success {
			out.Result.Output += fmt.Sprintf("#%d", n.Add(1))
		}
	}}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "d1"), evSegCall("c2", "d2")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ev_seg"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h), agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300}))
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), cper), &goexitAfterNodeOnce{node: "tools"})
	sess := agent_v2.NewRunSession()
	runExpectingGoexit(t, func() { _, _ = ag.Run(ctx, sess, schema.UserMessage("go")) })
	saved, err := cper.LoadNS(context.Background(), sess.RunID, "")
	if err != nil || saved == nil {
		t.Fatalf("fixture 没搭对：作废之后应留下可续的存档，err=%v", err)
	}
	blob, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), big) {
		t.Errorf("被裁决换掉的 c2 处理完之后，工具账里仍留着它转存前的原文、随出口存档落盘")
	}
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		_, e := ag.Resume(c, sess.RunID, agent_v2.WithResumeSession(sess))
		return e
	})
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := evSegDecision(sigs); got != "alpha" {
		t.Fatalf("续跑后生效的决定应是 alpha，实际 %s", got)
	}
	if n := execs.Load(); n != 2 {
		t.Fatalf("两个工具应各执行 1 次，实际共 %d 次", n)
	}
	c2 := lastResultFor(t, seen, "c2")
	if want := "[" + agent_v2.SupersededDecisionErrCode + "] "; !strings.HasPrefix(c2.Content, want) {
		t.Fatalf("c2 应是被换掉的失败结果，开头 %q", c2.Content[:min(60, len(c2.Content))])
	}
	if !strings.Contains(c2.Content, "#2") || strings.Contains(c2.Content, "#4") {
		t.Fatalf("c2 应是首跑那份替换结果（带 #2），实际尾部 %q——续跑在原文上重新处理了一遍", c2.Content[max(0, len(c2.Content)-80):])
	}
}

// TestToolMemo_InterruptBeforeProcessing_HookRewriteStillApplies：同一批 [t, ask_user]，ask_user 中断——这一批没处理完：
// t 的 OnToolEnd 只按失败路径收过尾（带 ToolBatchFailedExtraKey，改写不被消费），工具账里 t 是工具刚返回的原样。续跑时
// 钩子第一次处理 t，它的改写照常生效（模型看到 ok#1，不是没处理过的 ok）；t 不重跑。
func TestToolMemo_InterruptBeforeProcessing_HookRewriteStillApplies(t *testing.T) {
	var execs atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		execs.Add(1)
		return tool.Success("ok"), nil
	}))
	var n atomic.Int64
	h := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
		if failed, _ := out.Extra[agent_v2.ToolBatchFailedExtraKey].(bool); failed {
			return // 这一批没整体成功时的收尾：结果不进历史，改写也不被消费
		}
		if out.CallId == "c1" {
			out.Result.Output += fmt.Sprintf("#%d", n.Add(1))
		}
	}}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "t"),
			{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"继续吗"}`}}}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h), agent_v2.WithAskUser())
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应在 ask_user 中断，got %v", err)
	}
	if n.Load() != 0 {
		t.Fatalf("fixture 没搭对：中断的那一批只该按失败路径收尾，进历史的那次 OnToolEnd 不该跑，实际跑了 %d 次", n.Load())
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")})); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := lastResultFor(t, seen, "c1").Content; got != "ok#1" {
		t.Fatalf("这一批上次没处理完，续跑时钩子的改写应照常生效（ok#1），实际 %q", got)
	}
	if n := execs.Load(); n != 1 {
		t.Fatalf("t 应只执行 1 次（续跑命中工具账），实际 %d 次", n)
	}
}

// TestToolMemo_FixedReplay_NonIdempotentHookConverges：钩子把工具返回的 JSON 拆成正文（解析不了就报错）。首跑处理完、正文是
// 拆出来的那段；作废再续跑时交给钩子的只能是处理完的正文（不是 JSON），照样交给它的话它报错、续跑永远过不去——定下了
// 就不再处理，续跑收敛，模型看到首跑拆出来的正文。
func TestToolMemo_FixedReplay_NonIdempotentHookConverges(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success(`{"answer":"拆出来的正文"}`), nil
	}))
	var parses atomic.Int64
	h := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
		parses.Add(1)
		var v struct{ Answer string }
		if err := json.Unmarshal([]byte(out.Result.Output), &v); err != nil {
			out.Result = &schema.ToolCallResult{Success: false, ErrorCode: "bad_json", Error: "工具输出不是合法 JSON"}
			return
		}
		out.Result.Output = v.Answer
	}}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "t")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	runThroughPostToolsPanic(t, ag)
	if got := lastResultFor(t, seen, "c1").Content; got != "拆出来的正文" {
		t.Fatalf("模型应看到首跑拆出来的正文，实际 %q", got)
	}
	if n := parses.Load(); n != 1 {
		t.Fatalf("钩子应只在首跑处理一次，实际 %d 次", n)
	}
}
