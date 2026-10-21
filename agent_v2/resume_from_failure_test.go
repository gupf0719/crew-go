package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestResume_FromFailureSite_UpstreamNotReplayed 钉住「**从失败现场续跑**」这条能力在
// `agent_v2` 层是通的——不带答案的 Resume = 从失败点重试，上游已成功的成员不重放。
//
// # 为什么单独钉它
//
// 这条曾经**不通**，而且 `Checkpoint.Status` 的 godoc 逐字写着相反的话（「auto-load 认不了
// 失败档，判据是 `Status == CPStatusPending`」「9 编排对 Failed 档一律返
// `ErrNoPendingCheckpoint`」）。撤掉那道档位 gate、三处闸门统一改读
// `Checkpoint.HasResumableWork()`（按内容判）之后它通了，但**注释在很长一段时间里没跟上**
// ——业务照那段写会得出完全相反的结论，而全仓没有任何测试会因此变红。
//
// 所以这里钉的不是「gate 的实现细节」，是**业务能观察到的那个结果**：
//
//   - 失败退出留下的存档 `Status` 是 `failed`，但 `HasResumableWork()` 为真（判据是内容）
//   - 同 sid `Resume`（不带任何答案）返回 nil，不是 `ErrNoPendingCheckpoint`
//   - **上游已成功的节点一次都不重跑**——这条才是「续跑」区别于「重来」的地方
//
// sanity-revert：把三处闸门中的任意一处改回 `Status == CPStatusPending` → 失败档不被捡起
// → 退化成 fresh run → 上游重放计数从 0 变 1，本用例红。
func TestResume_FromFailureSite_UpstreamNotReplayed(t *testing.T) {
	outCh := compose_v2.LastValue[*schema.Message]("__probe_fs_out__")
	var charged, shipped int
	shipOK := false

	mk := func() agent_v2.Agent {
		bld := compose_v2.NewBuilder().WithName("probe_failsite")
		compose_v2.WithChannel(bld, outCh)
		bld.AddNode("charge", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			charged++
			return nil, nil
		})
		bld.AddNode("ship", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			shipped++
			if !shipOK {
				return nil, errors.New("发货网关 503")
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("shipped"))}, nil
		})
		bld.AddEdge(compose_v2.StartNode, "charge")
		bld.AddEdge("charge", "ship")
		bld.AddEdge("ship", compose_v2.EndNode)
		g, err := bld.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return agent_v2.NewGraphAgent(g,
			agent_v2.WithGraphAgentName("ga"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				out, _ := compose_v2.Get(chs, outCh)
				return out
			}),
		)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	if _, err := mk().Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：Run1 该因下游失败而失败")
	}
	if charged != 1 || shipped != 1 {
		t.Fatalf("fixture 没搭对：首跑该是 charge=1 ship=1，实际 charge=%d ship=%d", charged, shipped)
	}
	t.Logf("Run1 后: charge=%d ship=%d", charged, shipped)
	cp, _ := cper.LoadNS(ctx, sess.RunID, "")
	if cp == nil {
		t.Fatal("失败后该留下存档（否则「续跑失败现场」无从谈起）")
	}
	// 档位是 failed，但「捡不捡」按内容判——两者同时成立正是本条要钉的
	if cp.Status != compose_v2.CPStatusFailed {
		t.Errorf("失败退出该留 failed 档（业务据此知道上次是失败不是挂起），实际 %s", cp.Status)
	}
	if !cp.HasResumableWork() {
		t.Fatalf("failed 档也该是「有活可续」（frontier 非空）——按档位判会在这里判成不可续: %+v", cp.CurrentFrontier)
	}

	// **断言的那一步**：不带答案，同 sid Resume
	shipOK = true
	before := charged
	_, err2 := mk().Resume(ctx, sess.RunID)
	t.Logf("Resume err=%v", err2)
	t.Logf("Resume 后: charge=%d(增 %d) ship=%d", charged, charged-before, shipped)

	if err2 != nil {
		t.Errorf("从失败现场续跑该跑通，实测 Resume 返错: %v\n"+
			"  ErrNoPendingCheckpoint = 某处闸门又改回按 Status 档位判了", err2)
	}
	// **承重**：上游不重放才叫「续跑」——只断言 err==nil 的话，退化成 fresh run 也会绿
	if replayed := charged - before; replayed != 0 {
		t.Errorf("上游已成功的节点重放了 %d 次——那是 fresh run 不是续跑（副作用会重做）", replayed)
	}
	if shipped != 2 {
		t.Errorf("失败节点该恰重跑 1 次，实际总执行 %d 次", shipped)
	}
}
