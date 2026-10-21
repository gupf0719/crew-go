package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestApproval_ApproveAlwaysDoesNotLeakAcrossTasks 钉住一个**已修缺陷**的回归线（实施笔记 R6）。
//
// 缺陷原样：`approve_always` 的规则集写在 `RunSession` KV（`approvedToolsKey`），**曾经零清除点**
// ——只有读和写，没有任何 delete/reset。而「复用同一个 RunSession」正是 HITL 的标准范式
// （`WithResumeSession` 的整个存在理由）。**已修**：每个顶层 Run 进来先清一次规则
// （`agent_helpers.go` 的 `enterRunTask` 一带）。
//
// 后果：用户在**任务 A** 里对某个带副作用的工具打了「以后都别问我」，这条规则会跟着
// session 活到**任务 B、C、D**——即便任务 A 是被放弃的。用户以为自己授权的是「这件事」，
// 实际授权的是「这个 session 里的这个工具，永远」。
//
// ⚠️ 这里原先写着「『本调用已批』那份按稳定 InterruptID 存、换个任务自然不命中，
// 只有规则集会跨任务恒命中」——**那句话是错的，后来实测推翻了**：稳定 InterruptID 的
// 五个维度 `(graphNS, nodeID, visit, branchIdx, idx)` 在新的顶层调用里**全部重置**，
// 两个任务的第一个工具调用算出同一个 ID，照样命中。两份记忆当时都在泄漏。
//
// 现在两份各归各位：「本调用已批」搬进了 tools 节点的私有中断态
// （`toolCallMemo.Approved`，新任务是新 cp、天然为空，隔离是结构性的，见
// `TestApproval_PerCallMemoryStructurallyIsolated`）；规则集仍在 session KV，靠顶层 Run
// 的清除点——但它的 key 是「工具名 + 参数指纹」纯内容键，那个清除点是**产品决策**
// （授权该活多久）不是正确性补丁。
//
// 本用例守的是规则集那一半。
//
// ⚠️ 修复时判据必须与顶层 `WithFreshRun` 一致——那处已经在
// 「什么算一个新任务」上达成一致，第三处再自己定义一遍必然分叉。
func TestApproval_ApproveAlwaysDoesNotLeakAcrossTasks(t *testing.T) {
	rt := &recordTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), // 任务 A 第 1 轮
			schema.AssistantMessage("A 完成"),
			oneToolCall("transfer_money"), // 任务 B 第 1 轮——本该重新问
			schema.AssistantMessage("B 完成"),
		})),
	)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession() // ← 业务复用同一个 session（HITL 标准范式）

	// ── 任务 A：用户打 approve_always ──
	_, err := ag.Run(ctx, sess, schema.UserMessage("转 5000 给 acct-9"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应抛审批中断，got %v", err)
	}
	if _, err = ag.Resume(ctx, sess.RunID,
		// **必须复用 session**——这正是 R6 描述的场景。不传的话 Resume 新建空 session，
		// 规则写进临时对象随即消失（`approval.go` 那段注释写明了），根本走不到跨任务。
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(
			map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways),
			})); err != nil {
		t.Fatalf("任务 A Resume: %v", err)
	}
	callsAfterA := rt.calls.Load()
	if callsAfterA != 1 {
		t.Fatalf("任务 A 应执行一次转账，实际 %d", callsAfterA)
	}

	// ── 任务 B：**完全不同的任务**，同一个 session ──
	_, err = ag.Run(ctx, sess, schema.UserMessage("帮我看下这个月话费"))

	// 承重①：任务 B **必须重新要审批**。用户在任务 A 里说「以后别再问了」，语境是任务 A；
	// 他放弃 A、转头发起 B 时那句话不该继续生效。
	if _, stillAsk := compose_v2.IsInterrupt(err); !stillAsk {
		t.Fatalf("任务 B 没有重新要审批——任务 A 的授权泄漏过来了。err=%v", err)
	}
	// 承重②：泄漏的判据是**工具有没有真的执行**，不能只看有没有中断
	if got := rt.calls.Load(); got != callsAfterA {
		t.Fatalf("任务 B 里工具被静默执行了（调用数 %d → %d），用户全程没被问第二次",
			callsAfterA, got)
	}
}

// TestApproval_PerCallMemoryStructurallyIsolated 钉住 per-call 审批记忆的**结构隔离**。
//
// 这条跟上面那个的区别是：它只打 `approve`（这一次），**不打 approve_always**，
// 所以走的是「这次调用批过没有」那份记忆，跟 don't-ask-again 规则集无关。
//
// # 它验的是「不靠清除动作」
//
// 那份记忆原先按**稳定 InterruptID** 存 `RunSession` KV。ID 的五个维度
// `(graphNS, nodeID, visit, branchIdx, idx)` **在新的顶层调用里全部重置** ⟹ 任务 B 的
// 第一个工具调用跟任务 A 的算出同一个 ID ⟹ 静默命中、不再询问。
//
// 现在它搬进了 tools 节点的**节点私有中断态**（`toolCallMemo.Approved`）：
// **新任务 = 新 cp = 无 NodeStates**，隔离是结构性的。
//
// ⚠️ 所以本用例**故意不依赖 `clearApprovalRulesForNewTask`**——那个清除点是给规则集
// （`approvedToolsKey`，生命周期是产品决策）留的；per-call 这份不该再需要它。
// 把清除点整个删掉本用例仍应绿；红了说明隔离又退回靠清除动作了。
//
// ⚠️⚠️ **fixture 的承重点：两个任务的 memoKey 必须逐字相同。**
// `oneToolCall` 硬编码 `ID: "tc1"`，两个任务的工具名和批内下标也一样，所以
// `memoKey(tc.ID, idx, name)` 在任务 A 和任务 B 里算出的是**同一个键**。
// 这是有意的——键相同意味着「隔离」只可能来自**容器**（新任务 = 新 cp = 空 NodeStates），
// 不可能来自「键碰巧不一样」。
//
// 如果哪天把 fixture 改成两个任务用不同的 tc.ID，本用例会**继续绿**，但它就不再证明
// 结构隔离了——那时绿的原因是键不同，而那是碰巧，不是设计保证。改 fixture 前先想这条。
// （这是本项目「炸弹 fixture 第 1 类：过滤后元素恰好原位」的同款陷阱，反过来用。）
func TestApproval_PerCallMemoryStructurallyIsolated(t *testing.T) {
	rt := &recordTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), // 任务 A
			schema.AssistantMessage("A 完成"),
			oneToolCall("transfer_money"), // 任务 B——本该重新问
			schema.AssistantMessage("B 完成"),
		})),
	)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// ── 任务 A：只批这一次 ──
	_, err := ag.Run(ctx, sess, schema.UserMessage("转 5000 给 acct-9"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应抛审批中断，got %v", err)
	}
	if _, err = ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove),
		})); err != nil {
		t.Fatalf("任务 A Resume: %v", err)
	}
	callsAfterA := rt.calls.Load()
	if callsAfterA != 1 {
		t.Fatalf("任务 A 应执行一次，实际 %d", callsAfterA)
	}

	// ── 任务 B：完全不同的任务，同一个 session ──
	_, err = ag.Run(ctx, sess, schema.UserMessage("帮我看下这个月话费"))

	// 承重①：必须重新要审批
	if _, stillAsk := compose_v2.IsInterrupt(err); !stillAsk {
		t.Fatalf("任务 B 没有重新要审批——per-call 记忆泄漏过来了。err=%v", err)
	}
	// 承重②：判据是工具有没有真执行，不能只看有没有中断
	if got := rt.calls.Load(); got != callsAfterA {
		t.Fatalf("任务 B 里工具被静默执行了（%d → %d）", callsAfterA, got)
	}
}

// TestApproval_ClearFiresWhenTopLevelIsNotBasicAgent 钉住「清除点九个编排都要接」。
//
// 缺陷（audit 实测，pre-existing）：清除点原先只在 `BasicAgent` 和 `GraphAgent` 上。
// 业务把 BasicAgent 包在 Sequential / Parallel / Loop / Routing / Reflect / Handoff / Plan
// 里是很常见的用法，那时**里层的 BasicAgent 因为 graphNS 非空跳过自己那次调用** ⟹
// 清除点一次都不触发 ⟹ 任务 A 打的 approve_always 在任务 B 里静默放行，err 全程 nil。
//
// 这是 CLAUDE.md「协议变更四复证」那条教训的又一实例——**加新协议必 grep 所有调用点**，
// 只接一部分就是下一轮的 finding。
func TestApproval_ClearFiresWhenTopLevelIsNotBasicAgent(t *testing.T) {
	rt := &recordTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"),
			schema.AssistantMessage("A 完成"),
			oneToolCall("transfer_money"),
			schema.AssistantMessage("B 完成"),
		})),
	)
	// **顶层是 Sequential 而不是 BasicAgent**——这正是原先漏掉的形状
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("w", inner)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// 任务 A：打 approve_always
	_, err := seq.Run(ctx, sess, schema.UserMessage("转 5000"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应抛审批中断，got %v", err)
	}
	if _, err = seq.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways),
		})); err != nil {
		t.Fatalf("任务 A Resume: %v", err)
	}
	callsAfterA := rt.calls.Load()
	if callsAfterA != 1 {
		t.Fatalf("任务 A 应执行一次，实际 %d", callsAfterA)
	}

	// 任务 B：全新任务，同 session
	_, err = seq.Run(ctx, sess, schema.UserMessage("看下话费"))

	// 承重：顶层不是 BasicAgent 时清除点照样要触发
	if _, stillAsk := compose_v2.IsInterrupt(err); !stillAsk {
		t.Fatalf("顶层是 Sequential 时清除点没触发——任务 A 的授权泄漏了。err=%v", err)
	}
	if got := rt.calls.Load(); got != callsAfterA {
		t.Fatalf("任务 B 里工具被静默执行了（%d → %d）", callsAfterA, got)
	}
}

// TestApproval_AnswerToScopeMapping 钉住「用户答了哪个词 → 拿到多大范围」这条映射。
//
// # 为什么必须单独钉
//
// 审计做了两个变异，**全套测试都绿**：
//   - 把 `approve_always`（「我批的是这件事」）改成发**工具级**授权 → 绿
//   - 把 `approve_always_tool` 降级成参数级 → 绿
//
// 也就是说框架可以**把最宽的权限静默发出去而测试一声不吭**。而拆这两档的整个理由就是
// 「用户心里的『一直允许』通常指这件事而不是这个能力，合成一个答案时框架只能猜，
// 猜宽了就是把用户没打算给的权限给出去」——那个理由此前没有任何哨兵。
//
// 既有的 `TestToolApprovedRule` 是白盒直接调 `markToolApproved`，验的是**机制**
// （给定 toolWide 参数存对了没）；它证明不了「答案 → 参数」这一步没接反。
func TestApproval_AnswerToScopeMapping(t *testing.T) {
	// 两次调用参数不同：`{"x":1}`（oneToolCall 固定）vs `{"y":2}`
	otherArgs := func() *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc1", Type: "function", Function: schema.FunctionCall{
				Name: "transfer_money", Arguments: `{"y":2}`}},
		})
	}

	run := func(t *testing.T, answer string) (secondAsked bool, calls int32) {
		t.Helper()
		rt := &recordTool{name: "transfer_money"}
		tk := agent_v2_newToolkit()
		tk.Register(rt)
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(8),
			agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				oneToolCall("transfer_money"), // 第 1 次：{"x":1}
				otherArgs(),                   // 第 2 次：**同工具、不同参数**
				schema.AssistantMessage("完成"),
			})),
		)
		cp := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
		sess := agent_v2.NewRunSession()

		_, err := ag.Run(ctx, sess, schema.UserMessage("转账"))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("第 1 次应抛审批中断，got %v", err)
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage(answer),
			}))
		// 第 2 次调用参数不同：按答案的语义，approve_always 应重新问、approve_always_tool 不该问
		_, secondAsked = compose_v2.IsInterrupt(err)
		return secondAsked, rt.calls.Load()
	}

	t.Run("approve_always 只批这件事——换了参数必须重新问", func(t *testing.T) {
		asked, calls := run(t, agent_v2.ApprovalOptionApproveAlways)
		if !asked {
			t.Fatalf("换了参数是另一件事，必须重新问；实际静默放行（工具已跑 %d 次）\n"+
				"——这说明 approve_always 被接成了工具级授权，把用户没打算给的权限发出去了", calls)
		}
	})

	t.Run("approve_always_tool 批整个工具——换了参数不该再问", func(t *testing.T) {
		asked, _ := run(t, agent_v2.ApprovalOptionApproveAlwaysTool)
		if asked {
			t.Fatal("用户显式选了「这个工具的任何调用」，换个参数不该再问；" +
				"——这说明 approve_always_tool 被接成了参数级")
		}
	})
}

// TestApproval_NestedNoLeakAcrossTasks 钉住**嵌套**下的跨任务隔离。
//
// # 这条是搬家引入的回归，差分实测坐实
//
//	搬家前：任务 B  approvals=1  工具调用 1→1   ← 重新要审批
//	搬家后：任务 B  approvals=0  工具调用 1→2   ← 一句没问，转账直接执行
//
// 机理：搬家前 per-call 记忆在 `RunSession` KV，而 session 的 values **父子共享** ⟹
// 顶层那次清除顺带把整棵嵌套树清了。搬进各层自己的 cp 之后那层附带保护没了——
// 只有顶层 Run 走 `WithFreshRun`，**嵌套刻意保留 auto-load**（拿掉它嵌套 HITL 从
// 12/12 掉到 3/12），所以被放弃任务的**子 cp** 会被下一个任务捡起来。
//
// 「新任务 = 新 cp = 无 NodeStates」这个论断**只在顶层成立**，我当时写成了无限定的。
//
// 修法：给记录打**任务世代**（`toolCallMemo.ApprovedEpoch`）。世代存在 session 上、
// 父子共享，顶层每开新任务 bump 一次全树立刻可见；cp 里的旧记录仍躺着，但说不出话。
//
// ⚠️ 审批闸门原本是那个更大的 pre-existing 问题（被放弃任务的子 cp 吞掉新任务）
// **唯一会让人察觉的信号**。这条用例守的是信号本身。
func TestApproval_NestedNoLeakAcrossTasks(t *testing.T) {
	st := &selfHITLTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(st)

	inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("transfer_money"), // 任务 A 第 1 轮
			oneToolCall("transfer_money"), // 任务 B 第 1 轮——本该重新问
			schema.AssistantMessage("完成"),
		})),
	)
	// 顶层 Sequential（子 ns 是**静态**注册名——审计标出 100% 命中的那一档）
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("w", inner)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// ── 任务 A：批准 → 工具执行 → 工具**自己再中断** → 用户就此放弃 ──
	//
	// ⚠️ **必须停在「放弃」这一步**：任务 A 若成功收尾，框架成功后的自动清理（`DeleteSubtree`，仓里通称 cleanupOnSuccess）会把子 cp 删掉，
	// 根本没有残留可泄漏——那样这条用例测的是「没有残留时不泄漏」，恒真。
	// （第一版 fixture 就是这么写的，变异**不红**才发现。第 6 类炸弹：
	// fixture 没搭对场景 ⟹ 假阴。）
	_, err := seq.Run(ctx, sess, schema.UserMessage("转 5000"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Reason != "tool_approval" {
		t.Fatalf("任务 A 应先抛审批中断，got %v", err)
	}
	_, err = seq.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove),
		}))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Reason != "tool_self_ask" {
		t.Fatalf("批准后工具应执行并抛自身中断（留下带 Approved 的挂起子 cp），got %v", err)
	}
	callsAfterA := st.calls.Load()
	if callsAfterA != 1 {
		t.Fatalf("任务 A 工具应执行一次，实际 %d", callsAfterA)
	}
	// ← 用户就此放弃，不 Resume ie2

	// ── 任务 B：全新任务，同 session ──
	_, err = seq.Run(ctx, sess, schema.UserMessage("查话费"))

	// 承重①：必须重新要审批。放弃任务留下的子 cp 里带着 Approved，
	// 没有世代比对的话它会被原样捡回来。
	ieB, stillAsk := compose_v2.IsInterrupt(err)
	if !stillAsk || ieB.Reason != "tool_approval" {
		t.Fatalf("嵌套下任务 B 没有重新要审批——被放弃任务的子 cp 把它吞了。err=%v", err)
	}
	// 承重②：判据是工具有没有真执行
	if got := st.calls.Load(); got != callsAfterA {
		t.Fatalf("嵌套下任务 B 里工具被静默执行了（%d → %d）", callsAfterA, got)
	}
	// 承重②：判据是工具有没有真执行
	if got := st.calls.Load(); got != callsAfterA {
		t.Fatalf("嵌套下任务 B 里工具被静默执行了（%d → %d）", callsAfterA, got)
	}
}
