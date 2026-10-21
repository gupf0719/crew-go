// v2_typedgraph_demo 演示 compose_v2.Channel[T] 类型安全 — v2 channel-based 用泛型替代
// v1 时代 GraphState string-keyed map.
//
// **核心优势** (vs v1 typed state):
//   - 编译期类型检查 — Get/Write 拼错类型直接编译失败 (v1 是 runtime panic)
//   - 4 种 reducer 内置: LastValue / Append / Sum / OnceFlag — 业务表达多写并发语义
//   - Custom reducer 业务自定义合并逻辑 (e.g., max / merge map)
//   - Channel 名跨包字符串拼写错只在 builder 阶段错 (Compile 时验证)
//
// **本 demo 4 场景**:
//  1. LastValue[T] — 最常用, 单写者最后写胜出
//  2. Append[T] — fan-out 多分支并行 append 自动按写顺序拼
//  3. Sum — 多分支加数 (e.g., 并行任务统计)
//  4. Custom — 业务定义合并 (本例: 取多分支结果中 score 最高的)
//
// 跑法:
//
//	go run ./example/graph/v2_typedgraph_demo
//
// 全场景 PASS + 退出码 0 表示通过.
package main

import (
	"context"
	"fmt"
	"os"

	"crew-go/compose_v2"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("v2_typedgraph_demo PASS")
}

func run() error {
	if err := scenarioLastValue(); err != nil {
		return fmt.Errorf("场景 1 LastValue: %w", err)
	}
	if err := scenarioAppend(); err != nil {
		return fmt.Errorf("场景 2 Append: %w", err)
	}
	if err := scenarioSum(); err != nil {
		return fmt.Errorf("场景 3 Sum: %w", err)
	}
	if err := scenarioCustom(); err != nil {
		return fmt.Errorf("场景 4 Custom (max-by-score): %w", err)
	}
	return nil
}

// scenarioLastValue: LastValue[T] 单节点写胜出 — 最常用 reducer
func scenarioLastValue() error {
	ctx := context.Background()

	// 业务自定义类型 — Channel[T] 任意 T 都可
	type Doc struct {
		Title string
		Body  string
	}

	docCh := compose_v2.LastValue[Doc]("doc")
	outCh := compose_v2.LastValue[string]("out")

	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, docCh)
	bld = compose_v2.WithChannel(bld, outCh)

	bld.AddNode("write", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{
			compose_v2.Write(docCh, Doc{Title: "Hello", Body: "World"}),
		}, nil
	})
	bld.AddNode("read", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		doc, ok := compose_v2.Get(chs, docCh)
		if !ok {
			return nil, fmt.Errorf("doc channel 空")
		}
		out := fmt.Sprintf("%s: %s", doc.Title, doc.Body)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "write")
	bld.AddEdge("write", "read")
	bld.AddEdge("read", compose_v2.EndNode)

	g, err := bld.Compile()
	if err != nil {
		return fmt.Errorf("Compile: %w", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	out, _ := compose_v2.Get(cm.Channels(), outCh)
	if out != "Hello: World" {
		return fmt.Errorf("expect 'Hello: World', got %q", out)
	}
	fmt.Printf("场景 1 OK: LastValue[Doc] — out=%q\n", out)
	return nil
}

// scenarioAppend: Append[T] 多分支 fan-out 自动拼装
//
// 注: Append[T] 创建 *Channel[[]T] (slice type) — 写时传 []T{value} 单元素 slice,
// 框架自动 concat 多分支写入. Get 返完整 []T.
func scenarioAppend() error {
	ctx := context.Background()

	collectCh := compose_v2.Append[string]("collected")

	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, collectCh)

	// 3 个并发分支各 Append 一条 — fan-out 从 Start 直连 3 个节点
	for _, label := range []string{"a", "b", "c"} {
		l := label
		nodeID := compose_v2.NodeID("write_" + l)
		bld.AddNode(nodeID, func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{
				compose_v2.Write(collectCh, []string{"item_" + l}),
			}, nil
		})
		bld.AddEdge(compose_v2.StartNode, nodeID)
		bld.AddEdge(nodeID, compose_v2.EndNode)
	}

	g, err := bld.Compile()
	if err != nil {
		return fmt.Errorf("Compile: %w", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	items, _ := compose_v2.Get(cm.Channels(), collectCh)
	if len(items) != 3 {
		return fmt.Errorf("expect 3 items appended, got %d: %v", len(items), items)
	}
	fmt.Printf("场景 2 OK: Append[string] 多分支 fan-out — items=%v\n", items)
	return nil
}

// scenarioSum: Sum 多分支加数 (e.g., 并行任务计数)
func scenarioSum() error {
	ctx := context.Background()

	totalCh := compose_v2.Sum("total")
	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, totalCh)

	values := map[string]int{"a": 10, "b": 20, "c": 30}
	for label, val := range values {
		l, v := label, val
		nodeID := compose_v2.NodeID("add_" + l)
		bld.AddNode(nodeID, func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(totalCh, v)}, nil
		})
		bld.AddEdge(compose_v2.StartNode, nodeID)
		bld.AddEdge(nodeID, compose_v2.EndNode)
	}

	g, err := bld.Compile()
	if err != nil {
		return fmt.Errorf("Compile: %w", err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	total, _ := compose_v2.Get(cm.Channels(), totalCh)
	if total != 60 {
		return fmt.Errorf("expect total=60, got %d", total)
	}
	fmt.Printf("场景 3 OK: Sum 多分支加数 — total=%d\n", total)
	return nil
}

// scenarioCustom: Custom reducer 业务定义合并 — 本例取多分支 score 最高的 Result
func scenarioCustom() error {
	ctx := context.Background()

	type Result struct {
		Name  string
		Score int
	}

	// Custom reducer: 取 score 最大的
	bestCh := compose_v2.Custom("best",
		func(prev, next Result) Result {
			if next.Score > prev.Score {
				return next
			}
			return prev
		},
		Result{}, // zero value
	)

	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, bestCh)

	candidates := []Result{
		{"alice", 85},
		{"bob", 92},
		{"carol", 78},
	}
	for _, c := range candidates {
		r := c
		nodeID := compose_v2.NodeID("vote_" + r.Name)
		bld.AddNode(nodeID, func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(bestCh, r)}, nil
		})
		bld.AddEdge(compose_v2.StartNode, nodeID)
		bld.AddEdge(nodeID, compose_v2.EndNode)
	}

	g, err := bld.Compile()
	if err != nil {
		return fmt.Errorf("Compile: %w", err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	best, _ := compose_v2.Get(cm.Channels(), bestCh)
	if best.Name != "bob" || best.Score != 92 {
		return fmt.Errorf("expect best={bob, 92}, got %+v", best)
	}
	fmt.Printf("场景 4 OK: Custom (max-by-score) — best=%+v\n", best)
	return nil
}
