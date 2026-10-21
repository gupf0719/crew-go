package agent_v2

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestOwnerSourceWarn_OnlyWhenActuallySplit 钉住那条 warn 的**触发条件**：两档都真的
// 被走过才喊，不是「看到调用方身份就喊」。
//
// ⚠️ 第一版就是「看到就喊」——那会在**全认证部署**里也喊，而那种部署什么都没丢
// （协议层强制配 resolver ⟹ 每个请求都带身份 ⟹ 静态档永远走不到 ⟹ 历史一条都没散）。
// 根因是**在信息不全时下判断**：第一个请求到达时无法知道后面会不会有不带身份的，
// 而「散不散」完全取决于后者。
func TestOwnerSourceWarn_OnlyWhenActuallySplit(t *testing.T) {
	capture := func(fn func(hc *historyConfig)) string {
		var sb strings.Builder
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		// 必须给 store——「见过哪几档」记在 store 维度上（见 ownerSourceTracker），
		// 裸构造的 config 没有 store，走不到那条路。
		hc := &historyConfig{owner: "prod", store: NewInMemoryHistoryStore()}
		fn(hc)
		return sb.String()
	}

	t.Run("全认证部署不喊", func(t *testing.T) {
		out := capture(func(hc *historyConfig) {
			// 每个请求都带身份——静态档永远走不到
			for _, who := range []string{"tenant-a", "tenant-b", "tenant-a"} {
				hc.resolveOwner(compose_v2.WithOwner(context.Background(), who))
			}
		})
		if strings.Contains(out, "分散在两个 owner 分区") {
			t.Errorf("全认证部署什么都没丢，不该喊：%s", out)
		}
	})

	t.Run("全匿名部署不喊", func(t *testing.T) {
		out := capture(func(hc *historyConfig) {
			for i := 0; i < 3; i++ {
				hc.resolveOwner(context.Background()) // 没有身份，全落静态档
			}
		})
		if strings.Contains(out, "分散在两个 owner 分区") {
			t.Errorf("单租户部署全落静态档，也没散，不该喊：%s", out)
		}
	})

	t.Run("混合部署喊一次", func(t *testing.T) {
		out := capture(func(hc *historyConfig) {
			hc.resolveOwner(compose_v2.WithOwner(context.Background(), "tenant-a"))
			hc.resolveOwner(context.Background()) // 这一条落静态档 → 真的散了
			hc.resolveOwner(compose_v2.WithOwner(context.Background(), "tenant-b"))
			hc.resolveOwner(context.Background())
		})
		if n := strings.Count(out, "分散在两个 owner 分区"); n != 1 {
			t.Errorf("混合部署应恰好喊 1 次，实际 %d 次：%s", n, out)
		}
		// 消息里不该带具体 owner 值——只喊一次的话那是随机第一个租户，会误导。
		if strings.Contains(out, "tenant-a") || strings.Contains(out, "tenant-b") {
			t.Error("消息里带了具体租户值——只喊一次时那是随机第一个，读日志的人会以为只有它有问题")
		}
	})

	t.Run("没配静态档时永不喊", func(t *testing.T) {
		var sb strings.Builder
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		hc := &historyConfig{ownerFunc: func(context.Context) string { return "" },
			store: NewInMemoryHistoryStore()}
		hc.resolveOwner(compose_v2.WithOwner(context.Background(), "tenant-a"))
		hc.resolveOwner(context.Background())
		if strings.Contains(sb.String(), "分散在两个 owner 分区") {
			t.Errorf("没有静态档就不存在「分散」这回事：%s", sb.String())
		}
	})
}

// TestOwnerSourceWarn_SurvivesPerRequestConfig 钉住**真实生命周期**下 warn 仍然有效。
//
// ⚠️ 上面那四条子用例复用同一个 `historyConfig` 实例，**绕过了生命周期问题**——而
// `WithHistoryCtx` 是运行入口**每次调用新建一个 config**。第一版把「见过哪几档」记在
// config 上，于是那条路上 flag 每次清零、一次请求只走一档 ⟹ 条件永远不成立 ⟹
// **warn 永远不喊，而那四条用例全绿**。
//
// 这条走真实入口：两次独立的 WithHistoryCtx 调用（模拟两个请求），一次带身份一次不带。
func TestOwnerSourceWarn_SurvivesPerRequestConfig(t *testing.T) {
	var sb strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	hs := NewInMemoryHistoryStore() // 跨请求复用的那个对象
	// 请求 1：带调用方身份
	c1 := WithHistoryCtx(compose_v2.WithOwner(context.Background(), "tenant-a"), hs, "prod")
	historyCfgFromCtx(c1).resolveOwner(c1)
	// 请求 2：不带 —— 落进静态档，此刻历史真的散了
	c2 := WithHistoryCtx(context.Background(), hs, "prod")
	historyCfgFromCtx(c2).resolveOwner(c2)

	if n := strings.Count(sb.String(), "分散在两个 owner 分区"); n != 1 {
		t.Errorf("每请求新建 config 的路径上应喊恰好 1 次，实际 %d 次——"+
			"「见过哪几档」记在了活不过一个请求的东西上？\n%s", n, sb.String())
	}

	// 再来两个请求，不该重复喊（warnOnce 挂在 store 维度上）。
	c3 := WithHistoryCtx(compose_v2.WithOwner(context.Background(), "tenant-b"), hs, "prod")
	historyCfgFromCtx(c3).resolveOwner(c3)
	c4 := WithHistoryCtx(context.Background(), hs, "prod")
	historyCfgFromCtx(c4).resolveOwner(c4)
	if n := strings.Count(sb.String(), "分散在两个 owner 分区"); n != 1 {
		t.Errorf("应仍是 1 次（warnOnce 按 store 维度），实际 %d 次", n)
	}

	// 换一个 store 实例 = 另一场部署，各记各的。
	var sb2 strings.Builder
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb2, &slog.HandlerOptions{Level: slog.LevelWarn})))
	hs2 := NewInMemoryHistoryStore()
	d1 := WithHistoryCtx(compose_v2.WithOwner(context.Background(), "x"), hs2, "prod")
	historyCfgFromCtx(d1).resolveOwner(d1)
	d2 := WithHistoryCtx(context.Background(), hs2, "prod")
	historyCfgFromCtx(d2).resolveOwner(d2)
	if n := strings.Count(sb2.String(), "分散在两个 owner 分区"); n != 1 {
		t.Errorf("另一个 store 实例应独立计数，实际 %d 次", n)
	}
}
