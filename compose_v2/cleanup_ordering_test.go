package compose_v2_test

// `Cleanup(keepLastN)` 在**框架自己产的 cp** 上要按时间保留最新的 N 个。
//
// # 为什么这条不能靠 conformance 覆盖
//
// conformance 的 Cleanup 用例是手工构造 cp、显式填 `CreatedAt` 再 SaveNS——它验的是
// **backend 的排序实现**（还故意让 sid 字典序反着 CreatedAt 防假绿）。但框架路径上
// 没人填这个字段：`Snapshot()` 产的 cp 里 `CreatedAt` 恒 0。
//
// 于是所有 sid 的排序键全等，落到 sid 字典序 tie-break，而 sid 通常是
// `RunSession.RunID`（crypto/rand UUIDv4）——**跟时间毫无关系**。实测修复前
// `Cleanup(2)` 保留的是最先创建的两个、删掉最新三个：业务把它当"保留最近 N 个会话"
// 用，它会删掉正在跑的新会话、留下早就死掉的旧会话。

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// runAndLeaveCheckpoint 跑一个必定中断的图，在 (sid, "") 留下 pending cp。
// 走**真实 Run 路径**——手工 SaveNS 就绕开了"框架产的 cp 不带时间"这个前提。
func runAndLeaveCheckpoint(t *testing.T, cper compose_v2.Checkpointer, sid string) {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("cleanup_probe")
	b.AddNode("ask", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Reason: "probe",
			Query:  &compose_v2.AskQuery{InterruptID: "cleanup_ask", Question: "?"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("sid %q 应中断留下 cp, got %v", sid, err)
	}
}

func TestCleanup_KeepsMostRecentByTime_NotLexicalSID(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// **sid 字典序故意反着时间序**：不这么排的话，即使排序键全等落到字典序，
	// 结果也可能碰巧正确——测试就白做了（conformance 用的是同一个手法）。
	// 创建顺序 e→d→c→b→a，字典序恰好 a<b<c<d<e。
	order := []string{"sid_e", "sid_d", "sid_c", "sid_b", "sid_a"}
	for _, sid := range order {
		runAndLeaveCheckpoint(t, cper, sid)
	}

	if _, err := cper.Cleanup(ctx, 2); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	var kept []string
	for _, sid := range order {
		ok, err := cper.ExistsNS(ctx, sid, "")
		if err != nil {
			t.Fatalf("ExistsNS(%q): %v", sid, err)
		}
		if ok {
			kept = append(kept, sid)
		}
	}
	// 最后创建的两个是 sid_b、sid_a
	want := map[string]bool{"sid_b": true, "sid_a": true}
	if len(kept) != 2 || !want[kept[0]] || !want[kept[1]] {
		t.Errorf("Cleanup(2) 应保留**最后创建**的 [sid_b sid_a]，实际保留 %v\n"+
			"  框架产的 cp 不带 CreatedAt 时所有 sid 排序键全等 → 落到 sid 字典序，\n"+
			"  而 sid 是 UUIDv4 跟时间无关 → 生产上保留的是随机子集：\n"+
			"  业务当'保留最近 N 个会话'用，它会删掉正在跑的新会话、留下早死的旧会话",
			kept)
	}
}
