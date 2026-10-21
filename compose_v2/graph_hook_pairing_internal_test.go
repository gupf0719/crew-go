package compose_v2

// OnGraphStart / OnGraphEnd 的配对承诺：**end 只在 start 之后**。
//
// 本仓多处 godoc 把它当不变量写（「OnGraphStart 返 error 时 OnGraphEnd 仍会调——hook
// 实现者用 start/end 管 tracing span / metric inc-dec / 资源 acquire-release，缺 end 会
// 泄漏」）。反过来那半原先没人钉：**start 压根没被调到时 end 也不该 fire**，否则同一批
// 写法会 double-release。
//
// 这条曾经真的破过：`setBaseline`（子图 import baseline）挪到 OnGraphStart 之前之后，
// 它失败的早退路径上 OnGraphStart=0 而 OnGraphEnd=1（独立审计实测）。
//
// 三方同款规则：LangGraph 把 run_manager 初始化成 None、on_chain_start 成功才赋值，
// 末尾 `if run_manager is not None: run_manager.on_chain_error(e)` 显式守卫。
//
// # 为什么白盒
//
// 第三条用例要触发「OnGraphStart 之前的装配失败」，唯一的入口是不导出的
// `withImportOverlay`。**不给它加导出 shim**——项目规约明写「不要为『能测』把内部函数
// export」，曾经加过一个 `ExportWithImportOverlayForTest`，两轮独立审计都把它报了出来。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type pairingHooks struct {
	NoOpOrchestrationHooks
	starts, ends atomic.Int32
	failStart    bool
}

func (h *pairingHooks) OnGraphStart(ctx context.Context, _ Channels) (context.Context, error) {
	h.starts.Add(1)
	if h.failStart {
		return ctx, errors.New("start 自己失败")
	}
	return ctx, nil
}

func (h *pairingHooks) OnGraphEnd(ctx context.Context, _ Channels, _ error) context.Context {
	h.ends.Add(1)
	return ctx
}

func pairingGraph(t *testing.T) *Graph {
	t.Helper()
	b := NewBuilder().WithName("pairing")
	b.AddNode("n", func(_ context.Context, _ Channels) ([]ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(StartNode, "n")
	b.AddEdge("n", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestGraphHooks_EndOnlyAfterStart(t *testing.T) {
	g := pairingGraph(t)

	t.Run("正常跑完：1/1", func(t *testing.T) {
		h := &pairingHooks{}
		if err := Run(WithOrchHooks(context.Background(), h), g, NewChannelManager()); err != nil {
			t.Fatalf("run: %v", err)
		}
		if s, e := h.starts.Load(), h.ends.Load(); s != 1 || e != 1 {
			t.Errorf("正常路径应 1/1，实际 start=%d end=%d", s, e)
		}
	})

	t.Run("start 自己失败：仍 1/1（既有配对承诺）", func(t *testing.T) {
		h := &pairingHooks{failStart: true}
		if err := Run(WithOrchHooks(context.Background(), h), g, NewChannelManager()); err == nil {
			t.Fatal("start 返 error 时 Run 应失败")
		}
		if s, e := h.starts.Load(), h.ends.Load(); s != 1 || e != 1 {
			t.Errorf("start 失败时 end 仍该 fire（否则 span/资源泄漏），实际 start=%d end=%d", s, e)
		}
	})

	t.Run("start 之前的装配失败：0/0，不该有没有开头的结尾", func(t *testing.T) {
		h := &pairingHooks{}
		// 未注册的 channel 塞进 import baseline——那道早退排在 OnGraphStart 之前
		stray := LastValue[int]("__pairing_stray__")
		err := Run(WithOrchHooks(context.Background(), h), g, NewChannelManager(),
			withImportOverlay(Write(stray, 1)))
		if err == nil {
			t.Fatal("fixture 没搭对：import overlay 含未注册 channel 应让 Run 失败——" +
				"不失败的话下面两条断言在「什么都没发生」上恒真")
		}
		s, e := h.starts.Load(), h.ends.Load()
		if s != 0 {
			t.Fatalf("fixture 没搭对：这道早退应排在 OnGraphStart 之前，实际 start=%d", s)
		}
		if e != 0 {
			t.Errorf("start 一次都没 fire，end 不该 fire（hook 拿到没有开头的结尾 ⟹ "+
				"start/end 成对管的 span / metric inc-dec / 资源 release 会 double-release），"+
				"实际 end=%d", e)
		}
	})
}
