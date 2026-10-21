package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// phRunInNode 在一张只有一个节点的图里跑 body，节点 ctx 带着 sid "s" 与 checkpointer。
func phRunInNode(t *testing.T, name string, body func(ctx context.Context) error) error {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, body(ctx)
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	return compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(), rsOpts()...)
}

// TestInterruptSite_NodeLevelAndEmptyIDToolCallZero_Distinct：provider 不回 tool_call ID 时，节点自己（不在
// 任何 tool_call 里）的第一问与 tool_call #0 里的第一问要算出不同的 ID。
//
// 曾经只靠 ToolCallID / ToolCallIdx 两维：空 ID + 下标 0 与「不在 tool_call 里」长得一样，两问撞成同一个
// ID——用户答了前一问，后一问被判「答案已兑付」、整个 Run 终局失败（实测）。
func TestInterruptSite_NodeLevelAndEmptyIDToolCallZero_Distinct(t *testing.T) {
	var nodeLevel, toolCall compose_v2.InterruptSite
	if err := phRunInNode(t, "ph_site", func(ctx context.Context) error {
		nodeLevel = compose_v2.CurrentInterruptSite(ctx)
		toolCall = compose_v2.CurrentInterruptSite(compose_v2.WithCurrentToolCallContext(ctx, "", 0))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if nodeLevel.Idx != 0 || toolCall.Idx != 0 || nodeLevel.ToolCallID != "" || toolCall.ToolCallID != "" {
		t.Fatalf("fixture 没搭对：两问都应是各自计数的第 0 问、tool_call ID 都为空：%+v / %+v", nodeLevel, toolCall)
	}
	if nodeLevel.InterruptID() == toolCall.InterruptID() {
		t.Errorf("节点级第一问与空 ID 的 tool_call #0 第一问算出同一个 ID %s", nodeLevel.InterruptID())
	}
}

// TestEnterPhase_OwnCountersPhaseName_AndAdoptRestores：EnterPhase 开的相位有自己的提问计数（从 0
// 数、不占调用方的序号）、自己的相位名（进提问的身份）、不算在 tool_call 里；CallHook 采纳时把调用方那一套
// 取回来，调用方接着自己的计数。
func TestEnterPhase_OwnCountersPhaseName_AndAdoptRestores(t *testing.T) {
	var before, inPhase, after compose_v2.InterruptSite
	var phaseName string
	var phaseInTC, afterInTC bool
	if err := phRunInNode(t, "ph_enter", func(ctx context.Context) error {
		tc := compose_v2.WithCurrentToolCallContext(ctx, "call_1", 0)
		before = compose_v2.CurrentInterruptSite(tc)
		back, _ := compose_v2.CallHook(tc, func(c context.Context) (context.Context, error) {
			p := compose_v2.EnterPhase(c, "hook-a")
			phaseName, phaseInTC = compose_v2.SubCallScope(p), compose_v2.InToolCall(p)
			inPhase = compose_v2.CurrentInterruptSite(p)
			// 相位里再问一次：相位的计数走到 2、调用方的停在 1，两者错开，「铺回来的是不是调用方那个
			// 计数器」才分得出来（都停在 1 的话铺没铺回都是 1）
			_ = compose_v2.CurrentInterruptSite(p)
			return p, nil
		})
		afterInTC = compose_v2.InToolCall(back)
		after = compose_v2.CurrentInterruptSite(back)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if phaseName != "hook-a" || phaseInTC {
		t.Errorf("相位里：相位名应为 hook-a、不在 tool_call 里，实际 %q / %v", phaseName, phaseInTC)
	}
	if inPhase.Idx != 0 || inPhase.Phase != "hook-a" {
		t.Errorf("相位里的第一问应是它自己计数的第 0 问、带相位名：%+v", inPhase)
	}
	if after.Idx != 1 || after.Phase != "" || !afterInTC {
		t.Errorf("铺回之后应接着调用方的计数（第 1 问）、相位名为空、回到 tool_call 里：%+v inTC=%v", after, afterInTC)
	}
	ids := map[string]bool{before.InterruptID(): true, inPhase.InterruptID(): true, after.InterruptID(): true}
	if len(ids) != 3 {
		t.Errorf("三问应算出三个不同的 ID：%v", ids)
	}
}

// phRejectingCallee 是交给被调方跑的图：入口读 draftCh，v1 按 CondFailRerunFrom 否决。
func phRejectingCallee(t *testing.T, name string, draftCh *compose_v2.Channel[string]) *compose_v2.Graph {
	evals := 0
	return erRejectGraph(t, name, draftCh, "p-v1", &evals)
}

// TestHandOver_MainGraphEntryRejection_RedoesCallerProducer：节点体按 Rerunnable 起草，推好被调方的 ns 再
// HandOver，被调方的主图（落在交接 ns 上）入口否决 v1。作废的应是调用方的草稿：续跑一次、起草 v2 收敛。
//
// 曾经推进的是交给被调方的那一格：调用方的草稿原样拿回，续跑永不收敛（经 agent_v2.RunSubAgent 调起的
// 判官图实测续跑 4 次卡在 v1）。
func TestHandOver_MainGraphEntryRejection_RedoesCallerProducer(t *testing.T) {
	draftCh := compose_v2.LastValue[string]("ph_ho_draft")
	callee := phRejectingCallee(t, "ph_ho_callee", draftCh)
	drafts := 0
	b := compose_v2.NewBuilder().WithName("ph_ho_host")
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, err := erDraftOnce(ctx, &drafts, "p")
		if err != nil {
			return nil, err
		}
		sub := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "callee"))
		sub = compose_v2.HandOver(sub, "call-1")
		return nil, compose_v2.Run(sub, callee, compose_v2.NewChannelManager(),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	resumes, err := erLoop(rsCompile(t, b), rsOpts())
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if resumes != 1 || drafts != 2 {
		t.Errorf("应续跑 1 次、调用方起草 2 次，实际续跑 %d 次、起草 %d 次", resumes, drafts)
	}
}

// TestHandOver_GraphOffTheHandoverNS_UsesCalleeScope：同上，但被调方在自己的图之外（交接 ns 之外）另跑
// 一张图——那张图的输入是被调方自己产的，否决只作废被调方那一格：调用方的草稿不重做。
//
// 这条钉的是「交接主图」的判据只认交接 ns：按「凡是交接过来的执行都算调用方产的」判，被调方钩子里的
// 否决会作废调用方的产出（钩子相位的否决作废节点体的草稿，实测过的同类问题）。
func TestHandOver_GraphOffTheHandoverNS_UsesCalleeScope(t *testing.T) {
	draftCh := compose_v2.LastValue[string]("ph_off_draft")
	callee := phRejectingCallee(t, "ph_off_callee", draftCh)
	callerDrafts, calleeDrafts := 0, 0
	b := compose_v2.NewBuilder().WithName("ph_off_host")
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := erDraftOnce(ctx, &callerDrafts, "caller"); err != nil {
			return nil, err
		}
		sub := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "callee"))
		sub = compose_v2.HandOver(sub, "call-1")
		// 被调方在自己图外（比如它的 agent 级钩子里）起草、另跑一张图
		d, err := erDraftOnce(sub, &calleeDrafts, "p")
		if err != nil {
			return nil, err
		}
		return nil, compose_v2.Run(sub, callee, compose_v2.NewChannelManager(), compose_v2.WithRunNS("elsewhere"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	resumes, err := erLoop(rsCompile(t, b), rsOpts())
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if callerDrafts != 1 || calleeDrafts != 2 {
		t.Errorf("只该作废被调方那一格：调用方起草应 1 次、被调方 2 次，实际 %d / %d", callerDrafts, calleeDrafts)
	}
}

// TestDetachExecution_RootInterruptReturnedFromNode_Rejected：节点里用 DetachExecution 开一个独立运行，它
// 挂起了，节点把它的错原样返回。这个任务答不了那个挂起——引擎拒收（终局失败、不可重试），而不是把它
// 当成自己的挂起落档。节点体拿到的错里原中断照样取得到（可以拿去展示、记下它的 sid）。
//
// 曾经收下它：续跑时答案进的是节点的 ctx、被脱离剥掉，独立运行按新任务从头再问、换一个 ID，永不收敛
// （实测每次续跑换一个 ID，孤儿存档逐轮累积，全程没有报错）。
func TestDetachExecution_RootInterruptReturnedFromNode_Rejected(t *testing.T) {
	sb := compose_v2.NewBuilder().WithName("ph_side")
	sb.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "独立运行要人确认"})
		return nil, err
	})
	sb.AddEdge(compose_v2.StartNode, "ask")
	sb.AddEdge("ask", compose_v2.EndNode)
	side := rsCompile(t, sb)
	cper := compose_v2.NewInMemoryCheckpointer()
	var sideErrIsInterrupt bool
	err := phRunInNode(t, "ph_detach_host", func(ctx context.Context) error {
		sideErr := compose_v2.Run(compose_v2.DetachExecution(ctx), side, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("side-task"))
		_, sideErrIsInterrupt = compose_v2.IsInterrupt(sideErr)
		return sideErr
	})
	if !sideErrIsInterrupt {
		t.Fatal("fixture 没搭对：独立运行应当挂起，节点体拿到的错里应取得到中断")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("外层把独立运行的挂起当成了自己的中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) || compose_v2.IsRetryable(err) {
		t.Errorf("应终局拒收（ErrRunAborted、不可重试），实际：%v", err)
	}
	if pending, _ := cper.ListPending(context.Background(), "side-task"); len(pending) != 1 {
		t.Errorf("独立运行自己的挂起档应原样留着（在顶层用它自己的 sid 续），实际 %d 份", len(pending))
	}
}

// TestDetachExecution_TopLevelRootInterrupt_StillResumable：顶层（不在任何节点里）脱离开的运行挂起时，
// 调用方照常拿它的中断、用它的 sid 续——记号只在所在任务的节点把它原样返回时才拒收。
func TestDetachExecution_TopLevelRootInterrupt_StillResumable(t *testing.T) {
	sb := compose_v2.NewBuilder().WithName("ph_top")
	sb.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认吗"})
		if err != nil {
			return nil, err
		}
		if ans.Content != "好" {
			return nil, fmt.Errorf("答案不对：%q", ans.Content)
		}
		return nil, nil
	})
	sb.AddEdge(compose_v2.StartNode, "ask")
	sb.AddEdge("ask", compose_v2.EndNode)
	g := rsCompile(t, sb)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.DetachExecution(context.Background())
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("top"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应挂起：%v", err)
	}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("top"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")}))
	if err != nil {
		t.Errorf("用它自己的 sid 续跑应收敛：%v", err)
	}
}

type phGraphStartSID struct {
	compose_v2.NoOpOrchestrationHooks
}

func (phGraphStartSID) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return compose_v2.WithSessionIDCtx(ctx, "conv-42"), nil
}

// TestGraphStartHook_PushedSID_DoesNotBecomeNodeTask：OnGraphStart 钩子往 ctx 上推了别的 sid（把会话 ID
// 当 sid 推），节点里看到的 sid 与「所在任务」仍是图自己的 sid。
//
// 曾经节点执行范围记的是钩子推的那个：节点里的嵌套调用被当成别的任务的而拒掉、被调方的存档落到
// 别的 sid 下（续跑找不到、留孤儿档），另一个任务成功收尾时把它清掉（均实测）。
func TestGraphStartHook_PushedSID_DoesNotBecomeNodeTask(t *testing.T) {
	var nodeSID, scopeSID string
	b := compose_v2.NewBuilder().WithName("ph_gs_sid")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nodeSID = compose_v2.GetSessionID(ctx)
		scopeSID, _ = compose_v2.NodeScopeSessionID(ctx)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	if err := compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(),
		append(rsOpts(), compose_v2.WithHooks(phGraphStartSID{}))...); err != nil {
		t.Fatal(err)
	}
	if nodeSID != "s" || scopeSID != "s" {
		t.Errorf("节点里的 sid / 所在任务应是图自己的 s，实际 %q / %q", nodeSID, scopeSID)
	}
}

// TestDetachExecution_StripsSoftCancelAndThreadID：软取消信号一个 Run 一个、会话 ID 标识的是当前这场
// 对话——两者都不属于一个独立运行，脱离时剥掉。
//
// 曾经留着：从一个被软取消的 Run 里带出来的 ctx 开独立运行，一个节点都不跑就以上一个 Run 的原因停下、
// 还留一份挂起档；独立运行写进当前会话的历史、排在用户这一轮的提问之前（均实测）。
func TestDetachExecution_StripsSoftCancelAndThreadID(t *testing.T) {
	sig := compose_v2.NewSoftCancelSignal()
	sig.Trigger("上一个 Run 被停下")
	ctx := compose_v2.WithSoftCancelCtx(context.Background(), sig)
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-1")
	ctx = compose_v2.WithOwner(ctx, "tenant-a")
	if compose_v2.GetSoftCancel(ctx) == nil || compose_v2.GetThreadID(ctx) != "conv-1" {
		t.Fatal("fixture 没搭对：ctx 上应带着软取消信号与会话 ID")
	}
	d := compose_v2.DetachExecution(ctx)
	if compose_v2.GetSoftCancel(d) != nil {
		t.Error("软取消信号没剥掉")
	}
	if tid := compose_v2.GetThreadID(d); tid != "" {
		t.Errorf("会话 ID 没剥掉：%q", tid)
	}
	if compose_v2.GetOwner(d) != "tenant-a" {
		t.Error("调用主体是请求的环境，不该剥")
	}
}

type phNodeStartSID struct {
	compose_v2.NoOpOrchestrationHooks
}

func (phNodeStartSID) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	return compose_v2.WithSessionIDCtx(ctx, "hook-sid"), nil
}

// TestNodeStartHook_PushedSID_NodeBodySeesGraphSID：OnNodeStart 钩子往 ctx 上推了别的 sid，节点体读到的
// sid 仍是图自己的——节点里直接跑的嵌套图不显式给 sid 时取 ctx 上的，那就是它存档的主索引，跟节点执行
// 范围记下的「所在任务」必须是同一个。
func TestNodeStartHook_PushedSID_NodeBodySeesGraphSID(t *testing.T) {
	var bodySID, scopeSID string
	b := compose_v2.NewBuilder().WithName("ph_ns_sid")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		bodySID = compose_v2.GetSessionID(ctx)
		scopeSID, _ = compose_v2.NodeScopeSessionID(ctx)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	if err := compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(),
		append(rsOpts(), compose_v2.WithHooks(phNodeStartSID{}))...); err != nil {
		t.Fatal(err)
	}
	if bodySID != "s" || scopeSID != "s" {
		t.Errorf("节点体读到的 sid / 所在任务应是图自己的 s，实际 %q / %q", bodySID, scopeSID)
	}
}
