// 同 NodeID 多并发实例的 span / chunk 归属 (ISSUE-96 侧面 6).
//
// SendFrom 同 Target 投 N 次 → 一个超步里 N 个实例并发跑同一个 NodeID. span registry
// 和 chunk buffer 的 key 都只到 NodeID 粒度时, N 个实例落同一个槽:
//   - 后开的 span 覆盖先开的 → in-flight 只剩 1 个, 另外 N-1 个永远 end 不掉 (既漏
//     导出、又占 registry 直到 sweep)
//   - 先结束的实例把**后开那个**的 span 给 end 了 → duration 张冠李戴
//   - chunk 落同一 buffer → N 路流式输出逐 token 编织成一段乱码
//
// **测试必须走真的 graph 引擎**: 手工造实例身份再直调 OnNodeStart 观察不到"身份是在
// fire hook 之前还是之后进 ctx 的"——把注入挪回修复前的位置, 那种写法照样 PASS.
// **并且要有并发屏障**: 没有它 Go scheduler 会自然串行化, 每个实例发完 chunk 就立刻
// flush, 共享 buffer 也看不出问题.
package observability

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

// realPathOrchAdapter 把 agent_v2 视角的 OrchestrationHooks 适配到 compose_v2 引擎
// 要的形状（多一个 Channels 参数）——复刻 agent_v2.composeHooksAdapter 的最小子集。
//
// **为什么测试要绕这一下**：手工造实例身份再直调 OnNodeStart 的写法，结构上观察不到
// "身份是在 fire OnNodeStart 之前还是之后进 ctx 的"——把注入挪回 hook 之后（即修复
// 前的位置），那种写法照样 PASS。要钉住注入时机，只能让**真的 graph 引擎**去跑。
type realPathOrchAdapter struct {
	inner interface {
		OnGraphStart(context.Context) (context.Context, error)
		OnGraphEnd(context.Context, error) context.Context
		OnNodeStart(context.Context, string) (context.Context, error)
		OnNodeEnd(context.Context, string, error) context.Context
		OnNodeEvent(context.Context, string, agent_v2.Event) context.Context
	}
}

func (a realPathOrchAdapter) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return a.inner.OnGraphStart(ctx)
}
func (a realPathOrchAdapter) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	return a.inner.OnGraphEnd(ctx, err)
}
func (a realPathOrchAdapter) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	return a.inner.OnNodeStart(ctx, string(id))
}
func (a realPathOrchAdapter) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	return a.inner.OnNodeEnd(ctx, string(id), err)
}
func (a realPathOrchAdapter) OnNodeEvent(ctx context.Context, id compose_v2.NodeID, ev compose_v2.Event) context.Context {
	return a.inner.OnNodeEvent(ctx, string(id), agent_v2.Event{
		Type: ev.Type, Content: ev.Content, Metadata: ev.Metadata,
	})
}

// TestNodeInstance_RealGraphPath_SpansAndChunks 走**真的 graph 引擎**（compose_v2.Run
// + SendFrom fan-out），钉住手工调 hook 观察不到的两件事：
//
//  1. **实例身份必须在 fire OnNodeStart 之前进 ctx** —— 晚一步观测层就读不到，N 个
//     并发实例全落同一个 span 槽。
//  2. **chunk buffer 也要按实例分槽** —— 否则 N 路流式输出逐 token 编织成一段乱码。
//
// sanity-revert：把 graph_loop 里 WithNodeInstanceID 挪到 withNodeInterruptCounter
// 那行（修复前的位置）→ 只导出 1 个 worker span；去掉 chunkBufferKey 的实例维度 →
// 两路 chunk 合进同一个 streaming_response event。
func TestNodeInstance_RealGraphPath_SpansAndChunks(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	const width = 3
	b := compose_v2.NewBuilder().WithName("real_fanout")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ds := make([]compose_v2.SendDirective, 0, width)
		for i := 0; i < width; i++ {
			ds = append(ds, compose_v2.SendDirective{Target: "worker"})
		}
		compose_v2.SendFrom(ctx, ds...)
		return nil, nil
	})
	// **屏障：强制三个实例真并发**。没有它 Go scheduler 会自然串行化——每个实例
	// 发完 chunk 就立刻 OnNodeEnd flush 掉，共享同一个 buffer 也看不出问题（实测
	// 去掉 chunkBufferKey 的实例维度，无屏障版本照样 PASS）。有屏障后三个实例都
	// 发完 chunk 才开始收尾，共享 buffer 时第一个 flush 会把三条全卷走。
	barrier := make(chan struct{})
	var arrived atomic.Int32
	b.AddNode("worker", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			inst := compose_v2.GetCurrentInstanceID(ctx)
			sink.Send(compose_v2.Event{Type: "chunk", Content: "tok_" + inst})
		}
		if arrived.Add(1) == int32(width) {
			close(barrier)
		}
		select {
		case <-barrier:
		case <-time.After(5 * time.Second): // 并发度不足时别永久卡死，让断言去报错
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	adapter := realPathOrchAdapter{inner: obs.OrchHooks()}
	ctx := compose_v2.WithOrchHooks(compose_v2.WithGraphNS(context.Background(), ""), adapter)
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.Close()
	time.Sleep(500 * time.Millisecond)

	spans := srv.Spans()
	var workers int
	for _, sp := range spans {
		if sp.Name == "gen_ai.workflow.node.worker" {
			workers++
		}
	}
	if workers != width {
		t.Errorf("真实 graph 路径下 %d 个并发实例应各得 1 个 node span，实际 %d 个\n"+
			"  实例身份晚于 OnNodeStart 进 ctx 时观测层读不到 → 全落同一个 span 槽\n"+
			"  实际导出的 span ops: %v", width, workers, spanOps(spans))
	}

	// chunk 分槽：每个 worker 实例的 chunk 应各自成一个 streaming_response
	// （落同一 buffer 会被拼成一条，含多个 tok_）
	var streamEvents int
	for _, sp := range spans {
		if sp.Name != "gen_ai.workflow.node.worker" {
			continue
		}
		for _, ev := range sp.Events {
			if ev.Name == "streaming_response" {
				streamEvents++
			}
		}
	}
	if streamEvents != width {
		t.Errorf("%d 个实例的 chunk 应分别成 %d 个 streaming_response event，实际 %d 个\n"+
			"  chunkBufferKey 少了实例维度 → N 路 chunk 落同一 buffer 逐 token 编织",
			width, width, streamEvents)
	}

	// 导出侧：gen_ai.node.id 必须**同一 trace 内唯一**（spec AI_TRACE_SPEC 对该字段
	// 的原话是"前端执行树节点 ID"）。registry 分了槽 ≠ 导出的 id 也分了——前者决定
	// span 存不存在，后者决定前端建树时它们是不是同一个节点。
	//
	// OTel 原生 SpanId 和 crew.dotted_order 各自本来就唯一，所以这条只影响按
	// node.id / node.parent_id **字符串**建树的消费者：N 个并发兄弟会塌成一个节点。
	seen := map[string]int{}
	for _, sp := range spans {
		if sp.Name == "gen_ai.workflow.node.worker" {
			seen[findAttrString(sp, "gen_ai.node.id")]++
		}
	}
	if len(seen) != width {
		t.Errorf("%d 个并发实例应有 %d 个不同的 gen_ai.node.id，实际 %d 个: %v\n"+
			"  按 node.id 字符串建执行树的前端会把这些兄弟塌成一个节点",
			width, width, len(seen), seen)
	}
}
