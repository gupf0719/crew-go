package compose_v2_test

// 「终局失败 → 主动清 cp 子树」（ErrRunAborted）的回归测试。
//
// **Bug 历史**：CondFailAbort 档最初实现成「清 frontier → Run 出口守卫跳过 SaveNS」，
// 误以为"不写 cp"就等于"没有 cp"。但 cp 是 (sid, ns) **单槽覆盖式**——跳过写只是不覆盖，
// **先前已存在的 cp 原样幸存且 Status=Pending** → auto-load 照样捡起。三条可达路径：
//   ① 默认 durability=exit + 先前 HITL 中断 → 旧档幸存 → 重跑已成功节点、重问已答问题
//   ② 子图 Abort（子 cp 跳过写但父 cp 照存）→ livelock，永不收敛到终局失败
//   ③ durability=sync/async → 上一超步的中途 cp 幸存 → Resume 重跑整个上一超步
//
// **fix**：Abort 路径 wrap ErrRunAborted，Run 出口 defer `errors.Is` 命中时**主动
// DeleteSubtree**（与 cleanupOnSuccess 同款 purgeExitSlot）。
//
// 下面三个测试分别锁这三条路径——现有测试只覆盖「fresh Run 无旧 cp」，恰好全避开了。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAbortPurge_Path1_PriorHITLCheckpointPurged 锁路径 ①（最主流的 HITL 流程）：
// Run1 节点中断存 cp → Resume 答题、节点过了、但下游 cond 报错（Abort 档）→
// **先前那个 HITL cp 必须被清掉**，否则业务再 Resume 会重跑已成功的 A、被重新问已答的题。
//
// sanity-revert：graph_loop.go 出口 defer 删掉 `errors.Is(runErr, ErrRunAborted)` 的
// purge 分支（退回"跳过写"）→ 旧 cp 幸存 → 本测试 fail。
func TestAbortPurge_Path1_PriorHITLCheckpointPurged(t *testing.T) {
	aRuns := 0
	b := compose_v2.NewBuilder().WithName("abort_p1")
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		aRuns++
		if _, ok := compose_v2.GetResumeAnswer(ctx, "p1_q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "p1_q", Question: "approve?"},
			}
		}
		return nil, nil
	})
	b.AddNode("C", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "A")
	b.AddEdge("C", compose_v2.EndNode)
	// A 的 cond 恒报业务错 → 默认 CondFailAbort → 终局失败
	b.AddConditionalEdge("A", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", errors.New("cond: upstream data invalid")
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：A 中断 → 存 HITL cp（frontier=[A], pending=1）
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("p1_sid"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run1 应中断, got %v", err)
	}
	if cp, _ := cper.LoadNS(ctx, "p1_sid", ""); cp == nil {
		t.Fatal("Run1 应存 HITL cp")
	}

	// Run 2（Resume 答题）：A 过了 → cond 报错 → Abort → **必须把 Run1 的 HITL cp 清掉**
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("p1_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"p1_q": schema.UserMessage("yes")}))
	if err == nil {
		t.Fatal("Run2 应因 cond 报错失败")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("Abort 档的 err 应命中 ErrRunAborted（业务据此判'终局、别再 Resume'）, got %v", err)
	}

	// **核心断言**：先前的 HITL cp 必须已被清除
	cp, _ := cper.LoadNS(ctx, "p1_sid", "")
	if cp != nil {
		t.Fatalf("路径①回归：终局失败后先前的 HITL cp 仍幸存（CurrentFrontier=%v pending=%d）→ "+
			"业务 auto-load 会捡起它、重跑已成功的 A、**被重新问已经答过的问题**。\n"+
			"sanity-revert: 删 graph_loop.go 出口 defer 的 errors.Is(runErr, ErrRunAborted) purge 分支 → 本测试 fail",
			cp.CurrentFrontier, len(cp.PendingInterrupts))
	}

	// 佐证：旧 cp 已清 → 业务再跑同 sid 是干净的 fresh run（A 重新问一次是预期的新会话，
	// 而非"带着旧档重跑"）。这里只验 cp 状态，不重复跑。
	if aRuns != 2 {
		t.Logf("A 执行 %d 次（Run1 中断 + Run2 Resume），供参考", aRuns)
	}
}

// TestAbortPurge_Path3_DurabilityMidRunCheckpointPurged 锁路径 ③：durability=sync 每超步
// 写中途 cp → 后续 cond 报错 Abort → **中途 cp 也必须被清**，否则 Resume 重跑整个上一超步。
// 无需 HITL 即可触发。
func TestAbortPurge_Path3_DurabilityMidRunCheckpointPurged(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("abort_p3")
	b.AddNode("up", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("mid", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("down", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "up")
	b.AddEdge("up", "mid") // 静态边：让 up→mid 形成一个超步边界，durability=sync 会写中途 cp
	b.AddEdge("down", compose_v2.EndNode)
	// mid 的 cond 报错 → 默认 Abort
	b.AddConditionalEdge("mid", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", errors.New("cond: invalid")
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("p3_sid"),
		compose_v2.WithDurability(compose_v2.DurabilitySync)) // 每超步写中途 cp
	if err == nil {
		t.Fatal("应因 cond 报错失败")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("err 应命中 ErrRunAborted, got %v", err)
	}

	// **核心断言**：durability 写的中途 cp 也必须被清
	cp, _ := cper.LoadNS(ctx, "p3_sid", "")
	if cp != nil {
		t.Fatalf("路径③回归：durability=sync 的中途 cp 在终局失败后仍幸存（CurrentFrontier=%v）→ "+
			"业务 Resume 会重跑整个上一超步。终局失败必须主动清，不能只靠'跳过出口写'",
			cp.CurrentFrontier)
	}
}

// TestAbortPurge_Path2_SubgraphAbortPropagatesToParent 锁路径 ②（最严重的一条）：
// **子图终局失败必须沿错误链向上传播，让父图也清 cp**。
//
// **Bug 历史（livelock）**：子图 Abort 只"跳过写"时——子 cp 不写，但**父图照存**（父
// frontier=[sub] 非空 → 父 SaveNS）→ 业务看父 cp 以为可 Resume → Resume → 子图重跑 →
// 又中断问同一个问题 → 答 → 子 cond 又报错 → …… 无限循环，**永远收敛不到终局失败**。
// 实测曾 6 轮里同一个已答问题被问 3 次。
//
// **fix 机制**：子 Run 的 abort 错经 `subgraph.go` 的 `fmt.Errorf("subgraph %q: %w", id, runErr)`
// 冒泡进父 NodeFunc → 父 Run 出口 defer 同样 `errors.Is(runErr, ErrRunAborted)` 命中 →
// 父 cp 子树一并清。**终局性逐层向上传播，不需要每层显式协调**。
//
// sanity-revert：删出口 defer 的 purge 分支 → 父 cp 幸存 → 本测试 fail。
func TestAbortPurge_Path2_SubgraphAbortPropagatesToParent(t *testing.T) {
	subDone := compose_v2.LastValue[string]("__p2_sub_done__")

	// 子图：inner 首跑 HITL 中断（存下子 cp），Resume 后放行；其 cond 报业务错 → Abort
	subB := compose_v2.NewBuilder().WithName("p2_sub")
	subB.AddNode("inner", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetResumeAnswer(ctx, "p2_q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "p2_q", Question: "sub approve?"},
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(subDone, "ok")}, nil
	})
	subB.AddNode("inner2", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	subB.AddEdge(compose_v2.StartNode, "inner")
	subB.AddEdge("inner2", compose_v2.EndNode)
	subB.AddConditionalEdge("inner", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", errors.New("sub cond: upstream data invalid") // 默认 Abort → 子图终局失败
	})
	subB = compose_v2.WithChannel(subB, subDone)
	sub, err := subB.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}

	// 父图：Start → sub → End
	parentB := compose_v2.NewBuilder().WithName("p2_parent")
	parentB.AddSubgraphNode("sub", sub)
	parentB.AddEdge(compose_v2.StartNode, "sub")
	parentB.AddEdge("sub", compose_v2.EndNode)
	parentG, err := parentB.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：子图 inner 中断 → 父 + 子都存 cp
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, parentG, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("p2_sid"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run1 应中断, got %v", err)
	}
	if cp, _ := cper.LoadNS(ctx, "p2_sid", ""); cp == nil {
		t.Fatal("Run1 父 cp 应存在（本测试的可观测前提）")
	}

	// Run 2（Resume 答题）：inner 放行 → 子 cond 报错 → 子图 Abort → **终局性须传播到父**
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, parentG, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("p2_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"p2_q": schema.UserMessage("yes")}))
	if err == nil {
		t.Fatal("Run2 应因子图 cond 报错失败")
	}
	// 断言 1：sentinel 穿透子图 wrap（subgraph.go 用 %w wrap 子 runErr）
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("子图的 ErrRunAborted 应沿错误链冒泡到父 Run 返回值（subgraph %%w wrap）, got %v", err)
	}

	// 断言 2（**核心**）：父 cp 也必须被清 —— 否则业务看父 cp 以为可 Resume → livelock
	cp, _ := cper.LoadNS(ctx, "p2_sid", "")
	if cp != nil {
		t.Fatalf("路径②回归（livelock）：子图终局失败后**父 cp 仍幸存**（CurrentFrontier=%v）→ "+
			"业务据此 Resume → 子图重跑 → 又问同一个已答的问题 → 无限循环，永远收敛不到终局失败。\n"+
			"终局性必须沿错误链逐层向上传播（子 abort 错 → 父出口 defer errors.Is 命中 → 父也清）",
			cp.CurrentFrontier)
	}
	// 断言 3：子 cp 也不该残留（父 DeleteSubtree(sid,"") 清整子树）
	if subCp, _ := cper.LoadNS(ctx, "p2_sid", "sub"); subCp != nil {
		t.Errorf("子 cp 也应随父子树一并清, 仍在: %v", subCp.CurrentFrontier)
	}
}

// TestAbortPurge_ErrRunAbortedPropagatesThroughWrap 验 sentinel 在 wrap 链里可穿透，
// 且 Abort 档**保留原始业务错**（双 %w）——业务既能判"终局"又能拿到根因。
func TestAbortPurge_ErrRunAbortedPropagatesThroughWrap(t *testing.T) {
	bizErr := errors.New("my upstream produced garbage")
	b := compose_v2.NewBuilder().WithName("abort_wrap")
	b.AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("x", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("x", compose_v2.EndNode)
	b.AddConditionalEdge("n", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", bizErr
	})
	g, _ := b.Compile()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm)

	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应命中 ErrRunAborted（业务判终局）, got %v", err)
	}
	if !errors.Is(err, bizErr) {
		t.Errorf("应同时保留原始业务错（双 %%w，业务拿根因诊断）, got %v", err)
	}
	// cond 报错不是 HITL
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("cond 报错不该是 InterruptError, got %v", err)
	}
}

// TestAbortPurge_CondInterruptAlsoAborts 验 cond 抛 InterruptError（协议违规）也走 Abort
// 清 cp——重跑必然同样违规，留 cp 无意义。且不能让 IsInterrupt=true（否则 caller 当 HITL
// 等答案 → 无 ID 可配 → Resume 死锁）。
//
// **反假绿设计**（CLAUDE.md 5 类炸弹 #2「早退兜底盖过 fix 路径」）：必须让 Run 之前
// **已存在一个 cp**，"清没清"才可观测——若用 fresh run（无旧 cp），Abort 跳过写时本来
// 就没 cp 可捡，`cp==nil` 与 purge 分支在不在**毫无关系**，sanity-revert 照样通过（本
// 测试初版就踩了这个，靠 revert 实测抓出）。故先跑一轮 HITL 中断存下 cp。
func TestAbortPurge_CondInterruptAlsoAborts(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("abort_cond_int")
	// n 首跑 HITL 中断（存下 cp，制造"先前已有 cp"的可观测前提），Resume 后放行
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetResumeAnswer(ctx, "ci_q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ci_q", Question: "go?"},
			}
		}
		return nil, nil
	})
	b.AddNode("x", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("x", compose_v2.EndNode)
	// n 的 cond 抛 interrupt —— 协议违规，恒 Abort（不受 CondFailPolicy 影响）
	b.AddConditionalEdge("n", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "bad", Question: "?"},
		}
	})
	g, _ := b.Compile()
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：n 中断 → 存下 cp（"先前已有 cp"的前提）
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ci_sid"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run1 应中断（制造先前 cp）, got %v", err)
	}
	if cp, _ := cper.LoadNS(ctx, "ci_sid", ""); cp == nil {
		t.Fatal("Run1 应存下 cp —— 否则本测试的 purge 断言不可观测（假绿）")
	}

	// Run 2（Resume）：n 放行 → cond 抛 interrupt → 协议违规 → 恒 Abort → 必须清掉 Run1 的 cp
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ci_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"ci_q": schema.UserMessage("yes")}))
	if err == nil {
		t.Fatal("cond interrupt 应被拒")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("cond interrupt 应转非-interrupt（防 caller 当 HITL 等答案 → 无 ID 可配 → Resume 死锁）, got IsInterrupt=true")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("cond interrupt 是协议违规=终局失败, err 应命中 ErrRunAborted, got %v", err)
	}
	// **核心断言**：先前的 cp 必须被清（重跑必然同样违规，留着只会被 auto-load 捡起空转）
	if cp, _ := cper.LoadNS(ctx, "ci_sid", ""); cp != nil {
		t.Errorf("协议违规=终局失败应清 cp，实际先前的 cp 仍幸存: CurrentFrontier=%v pending=%d",
			cp.CurrentFrontier, len(cp.PendingInterrupts))
	}
}
