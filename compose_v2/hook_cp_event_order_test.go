package compose_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// orderRecorder 按发生顺序记下 OnGraphEnd 与 framework 级 cp event。
type orderRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	mu    sync.Mutex
	order []string
}

func (r *orderRecorder) add(s string) {
	r.mu.Lock()
	r.order = append(r.order, s)
	r.mu.Unlock()
}

func (r *orderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *orderRecorder) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	r.add("OnGraphEnd")
	return ctx
}

func (r *orderRecorder) OnNodeEvent(ctx context.Context, nodeID compose_v2.NodeID, ev compose_v2.Event) context.Context {
	// framework 级 cp event 的 nodeID 是空串（业务自定义 NodeFunc 内调 cper 才非空）
	if ev.Type == compose_v2.CpEventType && nodeID == "" {
		r.add("cp:" + ev.Content)
	}
	return ctx
}

// TestHookOrder_OnGraphEndFiresBeforeCpEvents 守 cp event 与 OnGraphEnd 的**相对次序**。
//
// # 承诺在哪
//
// `graph_loop.go` 的 `WithEmitCpEvent` godoc 明写这是**业务订阅契约**：
//
//	timing 契约: cp.save / cp.cleanup fire 时序在 OnGraphEnd defer 之后……
//	业务订阅时若依"OnGraphEnd 收 = Run 完全结束"假设关 SSE / commit metric,
//	会在收到 cp.save/cleanup 时困惑
//
// 机制是 defer LIFO：cleanup **先注册 ⟹ 后跑**，OnGraphEnd **后注册 ⟹ 先跑**。
// 同一处还挂着第二条契约——「OnGraphEnd 看 cleanup **前**的 runErr；cleanup 可 wrap runErr」，
// 两条由同一个注册顺序担保。
//
// # 为什么补这个哨兵
//
// 承诺明写在 godoc 里、业务照它写订阅代码，但**全仓没有任何用例断言过这一维**：把两个
// defer 的注册顺序对调（两条契约同时抹平）后跑全仓 `go test ./...` 照样全绿。
//
// 这跟前缀消歧被无边界替换抹平那次是同一形状——**框架承诺了某种可区分性/顺序性，却没有
// 任何断言守它**，于是它只能靠人工比对代码发现。
//
// sanity-revert：把 OnGraphEnd 的 defer 注册挪到 cleanup defer **之前**（graph_loop.go
// 里两个 defer 块换位）→ 本用例红，且红在「OnGraphEnd 该在 cp:cleanup 之前」。
func TestHookOrder_OnGraphEndFiresBeforeCpEvents(t *testing.T) {
	rec := &orderRecorder{}

	b := compose_v2.NewBuilder().WithName("cp_event_order")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("cp_order_sid"),
		compose_v2.WithHooks(rec),
		compose_v2.WithEmitCpEvent(true),
	); err != nil {
		t.Fatalf("Run 该成功: %v", err)
	}

	order := rec.snapshot()
	t.Logf("次序: %s", strings.Join(order, " → "))

	endAt, cleanupAt := -1, -1
	for i, s := range order {
		switch {
		case s == "OnGraphEnd" && endAt < 0:
			endAt = i
		case strings.HasPrefix(s, "cp:cleanup") && cleanupAt < 0:
			cleanupAt = i
		}
	}

	// **承重 1**：两件事都得真发生——否则下面的次序比较在空集上恒真（否定式断言的集合
	// 在「什么都没发生」时全部成立，见 CLAUDE.md）。
	if endAt < 0 {
		t.Fatalf("OnGraphEnd 没 fire，次序 = %v", order)
	}
	if cleanupAt < 0 {
		t.Fatalf("成功路径该 fire cp:cleanup（DeleteSubtree 清子树），次序 = %v", order)
	}

	// **承重 2**：OnGraphEnd 必须在 cp 事件之前——这是 godoc 里写给业务的订阅契约。
	if endAt > cleanupAt {
		t.Errorf("OnGraphEnd 该在 cp:cleanup **之前** fire（defer LIFO：cleanup 先注册后跑），"+
			"实际次序 = %v\n"+
			"  业务按「OnGraphEnd 收到 = Run 完全结束」关 SSE / commit metric，"+
			"次序反过来就会在关掉之后又收到 cp 事件", order)
	}
}
