package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

var errFcpBiz = errors.New("确定性业务错")

// 失败退出留下的存档标 CPStatusFailed：存档保留、`ListPending` 不返它（那是「谁在等答案」
// 的入口）。⚠️ 2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status，所以**失败档
// 照样被捡起来从失败点续跑**——本组钉三件事：档位标对、存档不丢、续跑时上游不重放。
//
// ⚠️ **上游节点是这组用例的承重维度**。这里原先是单节点图，③ 断言「第二次 Run 只跑 1 个
// 节点」——而单节点图上 fresh run（START→n）与 auto-load（frontier=[n]）**恰好都跑 1 个**，
// 那个量在两种行为下都一样，断言恒真。实测：auto-load 确实发生了，而 ③ 一声不吭。加一个
// 上游计数节点才让它承重（「断言选在不承重的维度上，等于没测」）。
//
// **sanity-revert**：把 graph_loop.go 出口那段判据改回无条件 `Status = CPStatusPending`，
// ① 与 ② 变红（档位 + ListPending）；把 auto-load 闸门改回只捡 Pending，③ 变红并报上游被重放。
func TestFailedCp_StatusMarkedAndResumedFromFailurePoint(t *testing.T) {
	var ranUp, ranN atomic.Int64
	b := compose_v2.NewBuilder().WithName("fcp")
	b.AddNode("up", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranUp.Add(1)
		return nil, nil
	})
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranN.Add(1)
		return nil, errFcpBiz
	})
	b.AddEdge(compose_v2.StartNode, "up")
	b.AddEdge("up", "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "fcp_sid"

	// Run 1：上游成功、下游业务错退出
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err == nil {
		t.Fatal("Run1 应失败")
	}
	if ranUp.Load() != 1 || ranN.Load() != 1 {
		t.Fatalf("Run1 应当上游跑 1 次、失败节点跑 1 次，实际 up=%d n=%d", ranUp.Load(), ranN.Load())
	}

	// ① 存档保留，且标成失败态
	cp, err := cper.LoadNS(ctx, sid, "")
	if err != nil || cp == nil {
		t.Fatalf("失败存档应当保留（只是不进 ListPending）, err=%v cp=%v", err, cp)
	}
	if cp.Status != compose_v2.CPStatusFailed {
		t.Errorf("失败退出的存档 Status = %v, want failed——标成 pending 会让它混进 "+
			"ListPending 的「有人等答案」清单", cp.Status)
	}
	if !cp.Status.Resumable() {
		t.Error("失败态应当仍可恢复（Resumable）——存着完整 frontier，续得上")
	}

	// ② ListPending 不返它：「谁在等答案」的清单里没有失败现场
	pend, err := cper.ListPending(ctx, sid)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pend) != 0 {
		t.Errorf("ListPending 返了 %d 条失败存档——它不该被当成「等答案的挂起」", len(pend))
	}

	// ③ 同 sid 再跑一次：闸门按内容判 ⟹ 失败档被捡起、从失败点续跑，**上游不重放**
	_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if got := ranUp.Load(); got != 1 {
		t.Errorf("上游跑了 %d 次，期望仍是 1 次——多出来的是闸门没捡失败档、整条链从头重放的", got)
	}
	if got := ranN.Load(); got != 2 {
		t.Errorf("失败节点跑了 %d 次，期望 2 次（Run1 一次 + 这次续跑一次）", got)
	}

	// ④ 显式指名那份存档：走的是另一条路（WithResumeFrom 根本不看 Status），同样从失败点续
	upBefore, nBefore := ranUp.Load(), ranN.Load()
	_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeFrom(cp))
	if ranN.Load() == nBefore {
		t.Error("显式 WithResumeFrom 失败存档时应当照常恢复执行")
	}
	if got := ranUp.Load(); got != upBefore {
		t.Errorf("WithResumeFrom 后上游跑了 %d 次（此前 %d）——显式指名同样不该重放上游", got, upBefore)
	}
}

// 控制流退出（中断 / 软取消 / 陈旧写回）不算失败——它们的存档仍是 Pending，auto-load 照捡。
func TestFailedCp_InterruptStaysPending(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("fcp_int")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "failed_cp_ask", Question: "要继续吗"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "fcp_int_sid"
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应当中断, got %v", err)
	}

	cp, _ := cper.LoadNS(ctx, sid, "")
	if cp == nil || cp.Status != compose_v2.CPStatusPending {
		t.Fatalf("中断存档 Status 应为 pending（真的有人在等答案）, got %v", cp)
	}
	pend, _ := cper.ListPending(ctx, sid)
	if len(pend) != 1 {
		t.Errorf("ListPending 应返 1 条中断存档, got %d——挡错了会让 HITL 恢复不了", len(pend))
	}
}
