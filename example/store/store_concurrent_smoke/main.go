// store_concurrent_smoke 验证 store + WithStoreMemory 在 N goroutine 并发跑同
// shared store 时不出 race / 数据交叉污染。
//
// **为什么需要**：commit c40bc71 audit Agent 1 race 视角 LOW-2 指出——
// agent_v2 内零测试同时跑 N goroutine + WithStoreMemory + shared store + race
// flag。本 smoke 是该 gap 的端到端补全。
//
// 跑法：
//
//	go run -race ./example/store/store_concurrent_smoke
//
// 验证要点：
//  1. race detector 全程不报警
//  2. 跨 user 数据无污染（user_A 不在 user_B 的 ns 看到 message）
//  3. 每 user 预置 + 本 Run 写入条目数 = 期望值
//  4. spy store 计 Put 次数 = goroutine 数（每 Run commitFinal 1 次 Put）
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// stubModel 返单条 assistant message——对应 BasicAgent 一轮即结束（无 tool_call）。
type stubModel struct {
	response string
}

func (m *stubModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.response), nil
}

func (m *stubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *stubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// countingStore 包装 store.Store 计 Put 调用次数 + 记录每次 Put 的 ns + key。
// 用于验证：(1) Put 调用计数 = goroutine 数；(2) 跨 user 数据不污染（每个
// ns 应只被对应 user 的 goroutine 写）。
type countingStore struct {
	store.Store
	puts     atomic.Int64
	mu       sync.Mutex
	perNS    map[string]int // bucketKey -> put count
	violated atomic.Bool    // 跨 user 污染 flag
}

func newCountingStore() *countingStore {
	return &countingStore{
		Store: store.NewInMemoryStore(),
		perNS: make(map[string]int),
	}
}

func (s *countingStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	s.puts.Add(1)
	s.mu.Lock()
	s.perNS[ns.String()]++
	s.mu.Unlock()
	return s.Store.Put(ctx, ns, key, value, opts...)
}

func main() {
	const numUsers = 20
	const preItemsPerUser = 3

	cs := newCountingStore()
	ctx := context.Background()

	// 1. 预置每 user 3 条历史 message
	for i := 0; i < numUsers; i++ {
		userID := fmt.Sprintf("user_%02d", i)
		ns := store.NS("users", userID, "memories")
		for j := 0; j < preItemsPerUser; j++ {
			key := fmt.Sprintf("seed_%d", j)
			msg := schema.UserMessage(fmt.Sprintf("seed message for %s #%d", userID, j))
			if err := cs.Put(ctx, ns, key, msg); err != nil {
				fmt.Fprintf(os.Stderr, "preseed put failed for %s: %v\n", userID, err)
				os.Exit(1)
			}
		}
	}
	seedPuts := cs.puts.Load()
	fmt.Printf("预置完成: %d users × %d items = %d puts\n", numUsers, preItemsPerUser, seedPuts)

	// 2. 启动 N goroutine 并发跑 BasicAgent.Run
	var wg sync.WaitGroup
	errCh := make(chan error, numUsers)
	for i := 0; i < numUsers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			userID := fmt.Sprintf("user_%02d", idx)
			ns := store.NS("users", userID, "memories")
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName(fmt.Sprintf("agent_%s", userID)),
				agent_v2.WithModel(&stubModel{response: fmt.Sprintf("done for %s", userID)}),
				agent_v2.WithStore(cs),
				agent_v2.WithStoreMemory(ns, nil),
			)
			sess := agent_v2.NewRunSession()
			_, err := ag.Run(ctx, sess, schema.UserMessage(fmt.Sprintf("hi from %s", userID)))
			if err != nil {
				errCh <- fmt.Errorf("user %s Run: %w", userID, err)
				return
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	// 3. 收集 Run 错误
	var runErrs []error
	for e := range errCh {
		runErrs = append(runErrs, e)
	}
	if len(runErrs) > 0 {
		fmt.Fprintf(os.Stderr, "Run 错误 %d 个:\n", len(runErrs))
		for _, e := range runErrs {
			fmt.Fprintf(os.Stderr, "  - %v\n", e)
		}
		os.Exit(1)
	}

	// 4. 验证总 Put 次数 = seed + numUsers
	expectedPuts := int64(seedPuts) + int64(numUsers)
	actualPuts := cs.puts.Load()
	fmt.Printf("Put 总次数: 期望 %d (seed %d + run %d)，实际 %d\n",
		expectedPuts, seedPuts, numUsers, actualPuts)
	if actualPuts != expectedPuts {
		fmt.Fprintf(os.Stderr, "FAIL: Put 次数不符——可能有 race / sub-agent 协议泄漏\n")
		os.Exit(1)
	}

	// 5. 验证每 user ns 被 put 次数 = preItemsPerUser + 1（commitFinal）
	cs.mu.Lock()
	defer cs.mu.Unlock()
	expectedPerNS := preItemsPerUser + 1
	for nsStr, count := range cs.perNS {
		if count != expectedPerNS {
			fmt.Fprintf(os.Stderr,
				"FAIL: ns %s put 次数=%d 不符（期望 %d）——跨 user 污染\n",
				nsStr, count, expectedPerNS)
			cs.violated.Store(true)
		}
	}
	if cs.violated.Load() {
		os.Exit(1)
	}
	fmt.Printf("每 user ns put 次数验证: 全 %d users × %d 次 ✓\n", numUsers, expectedPerNS)

	// 6. 验证 Search 跨 user 不污染——读 user_00 的 ns 不应看到 user_01 的 message
	for i := 0; i < numUsers; i++ {
		userID := fmt.Sprintf("user_%02d", i)
		ns := store.NS("users", userID, "memories")
		items, err := cs.Search(ctx, ns)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: Search %s: %v\n", userID, err)
			os.Exit(1)
		}
		// preseed 3 + commitFinal 1 = 4 items 期望
		if len(items) != preItemsPerUser+1 {
			fmt.Fprintf(os.Stderr,
				"FAIL: user %s Search 返 %d items（期望 %d）\n",
				userID, len(items), preItemsPerUser+1)
			os.Exit(1)
		}
		// 验证每条 item 的 ns 都是 userID 自己的——跨 user 污染检测
		for _, it := range items {
			if len(it.Namespace) >= 2 && it.Namespace[1] != userID {
				fmt.Fprintf(os.Stderr,
					"FAIL: 跨 user 污染——user %s ns 含 %s 的 item: %s\n",
					userID, it.Namespace[1], it.Key)
				os.Exit(1)
			}
		}
	}
	fmt.Printf("跨 user 数据隔离验证: 全 %d users Search 结果只含自己 ns 的 item ✓\n", numUsers)

	fmt.Println()
	fmt.Println("=== 场景 1: 20 goroutine 独立 user PASS ===")
	fmt.Printf("  - %d goroutine 并发 BasicAgent.Run + shared countingStore\n", numUsers)
	fmt.Printf("  - 每 user 独立 ns（users/<uid>/memories）\n")
	fmt.Printf("  - Put 总数 %d / 期望 %d ✓\n", actualPuts, expectedPuts)
	fmt.Printf("  - 跨 user 数据无污染（每 ns 严格隔离）\n")

	// === 场景 2: 50 goroutine 共享同 user ns（高竞争）===
	fmt.Println()
	scenario2(ctx)

	// === 场景 3: ParallelAgent 嵌套 BasicAgent + WithStoreMemory ===
	fmt.Println()
	scenario3(ctx)

	fmt.Println()
	fmt.Println("=== 全 3 场景 PASS ===")
	fmt.Println("  用 `go run -race` 跑额外 race detector 验证")
}

// scenario2 高竞争场景：50 goroutine 共享同一 user_id 同一 ns 并发 Put 同 store。
// 验证 InMemoryStore.mu 序列化所有 Put + 计数无丢失。
func scenario2(ctx context.Context) {
	const numConcurrent = 50
	cs := newCountingStore()
	ns := store.NS("users", "shared_user", "memories")

	var wg sync.WaitGroup
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName(fmt.Sprintf("ag_%d", idx)),
				agent_v2.WithModel(&stubModel{response: fmt.Sprintf("resp_%d", idx)}),
				agent_v2.WithStore(cs),
				agent_v2.WithStoreMemory(ns, nil),
			)
			sess := agent_v2.NewRunSession()
			if _, err := ag.Run(ctx, sess, schema.UserMessage(fmt.Sprintf("input_%d", idx))); err != nil {
				fmt.Fprintf(os.Stderr, "scenario2 Run %d 错: %v\n", idx, err)
				os.Exit(1)
			}
		}(i)
	}
	wg.Wait()

	actualPuts := cs.puts.Load()
	if actualPuts != int64(numConcurrent) {
		fmt.Fprintf(os.Stderr, "FAIL scenario2: 期望 %d Put 次（每 goroutine 1 commitFinal Put），实际 %d——race 致 Put 丢失\n",
			numConcurrent, actualPuts)
		os.Exit(1)
	}
	// Search 验证 store 内 item 数 = numConcurrent（每 Run key=RunID_assistant_N 唯一）
	items, err := cs.Search(ctx, ns, store.WithLimit(numConcurrent*2))
	if err != nil {
		fmt.Fprintf(os.Stderr, "scenario2 Search: %v\n", err)
		os.Exit(1)
	}
	if len(items) != numConcurrent {
		fmt.Fprintf(os.Stderr,
			"FAIL scenario2: store 内 item 数=%d，期望 %d——可能 key collision 致覆盖\n",
			len(items), numConcurrent)
		os.Exit(1)
	}
	fmt.Printf("=== 场景 2: %d goroutine 同 ns 高竞争 PASS ===\n", numConcurrent)
	fmt.Printf("  - 共享 ns（users/shared_user/memories）+ 50 goroutine 并发 commitFinal Put\n")
	fmt.Printf("  - Put 计数 %d / 期望 %d ✓（InMemoryStore mu 串行化无丢失）\n", actualPuts, numConcurrent)
	fmt.Printf("  - Search 返 %d items（每 Run RunID 唯一无 key collision）✓\n", len(items))
}

// scenario3 ParallelAgent 嵌套 BasicAgent + 共享 WithStoreMemory ns——验证
// **5 个分支各写各的、一条都不丢**。
//
// 这条守的东西换过一次：原先验的是「sub-agent 一条都不许写」（期望 0 Put），守的是
// 那道 `IsSubAgentStoreScope` 开关。而那道开关防的不是「成员不该有记忆」，是**键里
// 没有写入者**——同一次 Run 里 5 个分支的键逐字相同（本函数原注释自己写着这是
// 「高碰撞场景」）。它在拿「不让别人写」模拟「大家写不同的地方」，代价是业务给分支
// 配的 WithStoreMemory 静默失效。
//
// 键里加上写入者位置之后，验的就是真正想要的：5 次写入、5 个键、内容各自可辨。
func scenario3(ctx context.Context) {
	const numBranches = 5
	cs := newCountingStore()
	ns := store.NS("parallel", "test", "memories")

	// 构造 5 个 BasicAgent 作 ParallelAgent 分支，每个都挂 WithStoreMemory 同 ns
	parallel := agent_v2.NewParallelAgent(agent_v2.WithParallelName("parallel_smoke"))
	for i := 0; i < numBranches; i++ {
		idx := i
		child := agent_v2.NewBasicAgent(
			agent_v2.WithName(fmt.Sprintf("branch_%d", idx)),
			agent_v2.WithModel(&stubModel{response: fmt.Sprintf("branch_%d done", idx)}),
			agent_v2.WithStore(cs),
			agent_v2.WithStoreMemory(ns, nil),
		)
		parallel = parallel.AddBranch(fmt.Sprintf("branch_%d", idx), child)
	}

	sess := agent_v2.NewRunSession()
	if _, err := parallel.Run(ctx, sess, schema.UserMessage("fan-out")); err != nil {
		fmt.Fprintf(os.Stderr, "scenario3 Run 错: %v\n", err)
		os.Exit(1)
	}

	actualPuts := cs.puts.Load()
	if actualPuts != numBranches {
		fmt.Fprintf(os.Stderr,
			"FAIL scenario3: 每个分支都配了 WithStoreMemory，应各写一次共 %d 次 Put，实际 %d\n",
			numBranches, actualPuts)
		os.Exit(1)
	}
	// **承重的是这一条**：写了 N 次不代表存下 N 条——键撞了就是写 5 次存 1 条。
	items, serr := cs.Search(ctx, ns)
	if serr != nil {
		fmt.Fprintf(os.Stderr, "scenario3 Search 错: %v\n", serr)
		os.Exit(1)
	}
	if len(items) != numBranches {
		keys := make([]string, 0, len(items))
		for _, it := range items {
			keys = append(keys, it.Key)
		}
		fmt.Fprintf(os.Stderr,
			"FAIL scenario3: %d 次写入只存下 %d 条 %v——键撞了，后写静默覆盖先写\n",
			actualPuts, len(items), keys)
		os.Exit(1)
	}
	fmt.Printf("=== 场景 3: ParallelAgent 嵌套 5 BasicAgent PASS ===\n")
	fmt.Printf("  - ParallelAgent 5 分支并发 fan-out\n")
	fmt.Printf("  - 每分支 BasicAgent + WithStoreMemory 同 ns（曾经的高碰撞场景）\n")
	fmt.Printf("  - Put %d 次、存下 %d 条 ✓ 键带写入者位置，互不覆盖\n", actualPuts, len(items))
	for _, it := range items {
		fmt.Printf("      key=%s\n", it.Key)
	}
}
