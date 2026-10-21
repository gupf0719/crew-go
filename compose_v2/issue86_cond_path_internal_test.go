package compose_v2

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestCondInterrupt_AlwaysRejected 验 **cond 禁止 interrupt** 协议（引擎深审 F-A 根治）：
// cond fn 抛 InterruptError 时框架一律 fail-fast 拒绝，**不管 Query.InterruptID 填没填**。
//
// **协议演进**（为什么"自填 ID 就合法"的豁免没了）：
//   - R25 audit HIGH-3（旧）：cond 抛 interrupt → framework 自动补 visit-1 ID → 跨 Resume 漂
//   - ISSUE-86 方案 D2（旧）：空 ID 拒 / **自填 ID 允许**（视作 sub-agent 透传形态）
//   - **本次（根治）**：**一律拒**——cond 是纯路由判断，中断的本质是「要新输入」，而 cond
//     需要新输入才能判断 = 判断所需的信息还没备齐 = 上游 node 没尽责。且 cond 是纯函数、
//     Resume 后要重新执行，答案还得从 channel 读——绕一圈还是得让上游备好。
//     HITL 必须在 **node 内**抛（nodeID + visit 协议稳定）；RoutingAgent 的 classifier/Router
//     已改在 classify node 内跑，原生支持 HITL。
//
// **关键断言 1（防 Resume 死锁）**：返回的**不是** InterruptError——若 IsInterrupt 仍 true，
// caller 会当 HITL 等答案，但 framework 已不 recordInterrupt（pending 空）→ 无 ID 可配 →
// Resume 永久死锁。
//
// sanity-revert：graph_loop.go cond 路径恢复 `recordInterrupt(q)` + 返 `%w err` →
// IsInterrupt 变 true → 本测试 fail。
func TestCondInterrupt_AlwaysRejected(t *testing.T) {
	cases := []struct {
		name  string
		query *AskQuery
	}{
		{"empty_id", &AskQuery{Question: "approve?"}},
		// D2 旧协议允许自填 ID 的 cond interrupt，新协议同样拒
		{"explicit_id", &AskQuery{Question: "approve?", InterruptID: "my_workflow_approval_step_1"}},
		// 连 Query 都不填（业务笔误）——新协议下同样一律拒（旧协议有独立的"空 query"分支）
		{"nil_query", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bld := NewBuilder().WithName("test_cond_reject_" + tc.name)
			noopFn := func(_ context.Context, _ Channels) ([]ChannelWrite, error) {
				return nil, nil
			}
			// **fan-out 拓扑**（反假绿）：a/b 都成功 → a 的 cond 抛 interrupt → frontier 应
			// 从 [a,b] **收缩**到 [a]。单节点拓扑下 frontier 本来就是 [a]、收缩断言测不出
			// fix 是否生效——ISSUE-86 audit HIGH-2 记录过的假绿陷阱（5 类炸弹 #2）。
			bld.AddNode("a", noopFn)
			bld.AddNode("b", noopFn)
			bld.AddNode("x", noopFn)
			bld.AddEdge(StartNode, "a")
			bld.AddEdge(StartNode, "b")
			bld.AddEdge("b", "x")
			bld.AddEdge("x", EndNode)
			q := tc.query
			bld.AddConditionalEdge("a", func(_ context.Context, _ Channels) (NodeID, error) {
				return "", &InterruptError{Query: q}
			})

			g, _ := bld.Compile()
			cm := NewChannelManager()
			cper := NewInMemoryCheckpointer()
			ctx := context.Background()
			runErr := Run(ctx, g, cm, WithCheckpointer(cper), WithSessionID("ci_"+tc.name))
			if runErr == nil {
				t.Fatalf("cond 抛 InterruptError 应 fail-fast 拒绝, got nil")
			}
			// 断言 1（核心）：转成**非-interrupt** error，防 caller 当 HITL 等答案 → Resume 死锁
			if _, isInt := IsInterrupt(runErr); isInt {
				t.Errorf("cond interrupt 应转成非-interrupt error（否则 caller 当 HITL 等答案但 "+
					"PendingInterrupts 空 → 无 ID 可配 → Resume 永久死锁）, got IsInterrupt=true: %v", runErr)
			}
			// 断言 2：error msg 给显式引导（改用 node）
			if !strings.Contains(runErr.Error(), "cond fn 是纯路由判断、不能中断") {
				t.Errorf("error msg 应含 cond 禁 interrupt 的显式引导, got %q", runErr.Error())
			}
			// 断言 3：没走 record 路径（与断言 1 互为佐证）
			if pending := cm.PendingInterrupts(); len(pending) != 0 {
				t.Errorf("cond interrupt 被拒后不应 recordInterrupt, got %v", pending)
			}
			// 断言 4：**终局**——cond interrupt 是协议违规（业务代码 bug），恒按 Abort 处理
			// （不受 CondFailPolicy 影响）：重跑必然同样违规，给 Resume 无意义。
			//
			// **断言的是结果不是机制**：终局性由 `ErrRunAborted` 触发 Run 出口清 cp 子树
			// 保证——跟节点返回 ErrRunAborted 时同一条路。曾经断言 `cm.frontierAtExit` 为空
			// （cond 失败路径自己清 frontier），那是出边还是独立相位时的实现细节；出边归成员
			// 之后，cond 失败就是成员失败，frontier 里留着那个待重跑的成员，而**是否可续**
			// 由存档在不在决定。
			if !errors.Is(runErr, ErrRunAborted) {
				t.Errorf("cond interrupt 应终局（%%w ErrRunAborted → 不重试 + 清 cp 子树）, got %v", runErr)
			}
			left, _ := cper.LoadAllInSession(ctx, "ci_"+tc.name)
			if len(left) != 0 {
				t.Errorf("终局失败应清光 cp 子树（否则 auto-load 捡起来原地再炸一次）, 残留 %d 份", len(left))
			}
		})
	}
}

// TestISSUE86_MED1_MED3_CondCtxErrFrontierUpdate 验 cond **报错**（非 interrupt）时的
// 两档 CondFailPolicy 行为：
//
//   - **默认 CondFailAbort**：终局——`%w ErrRunAborted` → Run 出口清 cp 子树 → 无从 Resume
//   - **显式 CondFailRerunFrom**：存 Pending cp，frontier **收缩到 [失败的那个成员]** →
//     业务 Resume 只重跑它，不牵连成功兄弟副作用双跑
//
// 两档都断言 cm.PendingInterrupts 仍空（cond 报错不是 HITL，业务无 query 可配）。
//
// **两档断言的东西不同，这是有意的**：Abort 断言**结果**（存档没了），因为它的终局性
// 由清子树保证，frontier 里留没留东西不影响；RerunFrom 断言 **frontier 的精度**（只收缩
// 到失败成员），因为那正是它要给的东西。
//
// **假绿炸弹拆除（ISSUE-86 audit HIGH-2）**：拓扑必须是 fan-out `split → [a,b,c]`。单节点
// 拓扑下 frontier 本来就是 [a]，"收缩"这件事没有可观测差异（CLAUDE.md 5 类炸弹 #2）。
func TestISSUE86_MED1_MED3_CondCtxErrFrontierUpdate(t *testing.T) {
	// buildFanoutCondErrGraph 造 split → fan-out [a,b,c]；a 的 cond 抛业务错。
	buildFanoutCondErrGraph := func(name string, opts ...CondOption) *Graph {
		bld := NewBuilder().WithName(name)
		noopFn := func(_ context.Context, _ Channels) ([]ChannelWrite, error) {
			return nil, nil
		}
		bld.AddNode("split", noopFn)
		bld.AddNode("a", noopFn)
		bld.AddNode("b", noopFn)
		bld.AddNode("c", noopFn)
		bld.AddEdge(StartNode, "split")
		bld.AddEdge("split", "a")
		bld.AddEdge("split", "b")
		bld.AddEdge("split", "c")
		bld.AddConditionalEdge("a", func(_ context.Context, _ Channels) (NodeID, error) {
			return "", errors.New("biz cond fail")
		}, opts...)
		g, _ := bld.Compile()
		return g
	}

	// ── 档 1：默认 CondFailAbort —— 终局失败，存档清光，不给 Resume ──
	t.Run("default_abort", func(t *testing.T) {
		g := buildFanoutCondErrGraph("test_cond_biz_err_abort")
		cm := NewChannelManager()
		cper := NewInMemoryCheckpointer()
		ctx := context.Background()
		runErr := Run(ctx, g, cm, WithCheckpointer(cper), WithSessionID("abort_sid"))
		if runErr == nil {
			t.Fatalf("Run 应抛 cond 业务错, got nil")
		}
		if !strings.Contains(runErr.Error(), "biz cond fail") {
			t.Errorf("err 应含 'biz cond fail'（原错要能 unwrap 出来）, got %v", runErr)
		}
		// **核心断言**：终局 + 存档清光 → 业务无从 Resume
		//（cond 报错 = 上游数据坏，默认档假定它是确定性的，重跑白烧）
		if !errors.Is(runErr, ErrRunAborted) {
			t.Errorf("默认 CondFailAbort 应 %%w ErrRunAborted（不重试 + 清 cp 子树）, got %v\n"+
				"sanity-revert: out_edges.go wrapCondFailure 的 default 分支去掉 ErrRunAborted → 本断言 fail",
				runErr)
		}
		if left, _ := cper.LoadAllInSession(ctx, "abort_sid"); len(left) != 0 {
			t.Errorf("默认 CondFailAbort 应清光 cp 子树（否则 auto-load 捡起来原地再炸）, 残留 %d 份", len(left))
		}
		if pending := cm.PendingInterrupts(); len(pending) != 0 {
			t.Errorf("cond 报错不是 HITL, PendingInterrupts 应空, got %v", pending)
		}
	})

	// ── 档 2：显式 CondFailRerunFrom —— frontier 收缩到 [a]（ISSUE-86 MED-3 精度）──
	t.Run("explicit_rerun_from", func(t *testing.T) {
		g := buildFanoutCondErrGraph("test_cond_biz_err_rerun", WithCondFailPolicy(CondFailRerunFrom))
		cm := NewChannelManager()
		cper := NewInMemoryCheckpointer()
		ctx := context.Background()
		runErr := Run(ctx, g, cm, WithCheckpointer(cper), WithSessionID("rerun_sid"))
		if runErr == nil {
			t.Fatalf("Run 应抛 cond 业务错, got nil")
		}
		// **核心断言**：frontier 应**从 fan-out [a,b,c] 收缩到 [a]**——只重跑 a（让它重新
		// 产出、cond 重评），不牵连成功兄弟 b/c 副作用双跑。单节点拓扑测不出此收缩（假绿
		// 炸弹 #2），故用 fan-out。
		//
		// 现在这个收缩不再由 cond 路径自己 `updateFrontier([from])` 做，而是**自然发生**：
		// a 的 cond 失败 ⟹ a 进 FailedBranches ⟹ frontier = 失败成员集。b/c 的写入照常提交、
		// 身份记入 completed。
		frontier := cm.frontierAtExit
		if len(frontier) != 1 || frontier[0].NodeID != "a" {
			t.Errorf("CondFailRerunFrom 档 frontier 应**从 fan-out [a,b,c] 收缩到 [a]**, got %v\n"+
				"sanity-revert: out_edges.go 里把 evalOutEdges 的错误改成 nil（cond 失败不算成员失败）→ "+
				"a 不进 FailedBranches → 本断言 fail", frontier)
		}
		// **正向断言**：还得真存下可续的档——只断言 frontier 的话，「什么都没存」也会绿
		if !errors.Is(runErr, ErrRunAborted) {
			// RerunFrom 明确不是终局：它的语义就是"存着等 Resume 重跑"
			if cp, _ := cper.LoadNS(ctx, "rerun_sid", ""); cp == nil || !cp.HasResumableWork() {
				t.Errorf("CondFailRerunFrom 应留下可续的存档, got %+v", cp)
			}
		} else {
			t.Errorf("CondFailRerunFrom 不该是终局失败, got %v", runErr)
		}
		if pending := cm.PendingInterrupts(); len(pending) != 0 {
			t.Errorf("cond 报错不是 HITL, PendingInterrupts 应空, got %v", pending)
		}
	})
}
