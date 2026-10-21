package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// countExitEvents 数 session 里的 ExitEvent（`Action.Exit`）。
func countExitEvents(sess *agent_v2.RunSession) int {
	n := 0
	for _, e := range sess.Events() {
		if e != nil && e.Action != nil && e.Action.Exit {
			n++
		}
	}
	return n
}

// TestStoreMemory_EmptyContent_StillEmitsExitEvent 钉「空内容收尾时，跳过的只是记忆
// 写入，ExitEvent 照发」。
//
// ⚠️ **断言顺序是设计过的，别调**。第一版把「取 FinalMessage 验 Content 为空」当作
// 前提放在最前面，结果是**哑弹**：`FinalMessage` 扫的就是 `Action.Exit`，所以
// 「ExitEvent 被吞」⟺「FinalMessage 返 nil」⟹ 前提检查先 `t.Fatal`，承重的那条
// 永远执行不到。更坏的是它打出的是本仓表示「这条用例没测到东西」的标准措辞，
// 下一个人会去修 fixture，bug 原样留着。
//
// 现在：**承重断言不依赖任何会被这个 bug 破坏的量**——工具被调过（前提，独立于事件流）
// → ExitEvent 计数（承重）→ 再从事件里取 Content 验空（后置前提）。
func TestStoreMemory_EmptyContent_StillEmitsExitEvent(t *testing.T) {
	var called int
	silent := tool.NewFuncTool("silent", "returns nothing",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			called++
			r := tool.Success("") // 返空 ⟹ 最终答复 Content 为空
			r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
			return r, nil
		})
	tk := tool.NewToolkit()
	tk.Register(silent)

	st := store.NewInMemoryStore()
	ns := store.NS("mem")
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "c1", Function: schema.FunctionCall{Name: "silent", Arguments: "{}"}}}),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, nil),
		agent_v2.WithMaxRounds(3),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ① 前提：确实走到了「return-direct 工具返空输出」这条路。
	//    **独立于事件流**——ExitEvent 被吞时这条照样成立，所以它不会遮住下面那条。
	if called != 1 {
		t.Fatalf("fixture 前提没成立：silent 工具被调 %d 次（应为 1），这条用例要的是空内容收尾", called)
	}

	// ② **承重断言**：ExitEvent 照发。守卫写成 `return nil` 时这里为 0。
	if got := countExitEvents(sess); got != 1 {
		t.Errorf("空内容收尾时 ExitEvent 应恰好 1 条，实际 %d 条"+
			"——守卫应只跳过记忆写入，不该从 commitFinal 返回", got)
	}

	// ③ 空串没进 store（跳过写入的那一半）
	items, _ := st.Search(context.Background(), ns, store.WithLimit(10))
	if len(items) != 0 {
		t.Errorf("空内容不该写进长期记忆，实际写了 %d 条", len(items))
	}

	// ④ 后置前提：那条 ExitEvent 携带的答复 Content 确实是空的。放在 ② 之后——
	//    它依赖事件存在，不能拿来当 ② 的门槛。
	if final := agent_v2.FinalMessage(sess.Events()); final != nil && final.Content != "" {
		t.Errorf("fixture 前提没成立：最终答复 Content 不空（%q）", final.Content)
	}
}

// TestCommitFinal_ExitEventAfterStorePut 钉 commitFinal 的**顺序契约**：
// StoreMemory 的 `store.Put` 必须在 `AddEvent(ExitEvent)` **之前**。
//
// # 为什么必须有这条
//
// 那个顺序是一次更早的 audit 修出来的，理由写在 `commitFinal` 里：Put 抛中断时
// ExitEvent 不 add、Resume 重跑 Put 成功后才 add；顺序反过来的话 Resume 重跑
// commitFinal 会让 ExitEvent **第二次** AddEvent ⟹ dup。
//
// **但那次修完没留哨兵**：把 AddEvent 挪到 StoreMemory 块之前，全仓测试零红
// （本轮变异实测），而真实后果是 HITL 闭环下退出事件从 1 条变 2 条、`err` 全程 nil。
// 后来又有人（我）在这块加守卫时没读那段注释、把 ExitEvent 整个吞掉——注释拦不住，
// 因为改代码的人往往不看它。
//
// # 两个方向都要钉
//
// 只钉「Resume 后有 1 条」的话，「被吞」（0 条）和「dup」（2 条）只抓得到一种。
func TestCommitFinal_ExitEventAfterStorePut(t *testing.T) {
	const interruptID = "gate_1"
	// 在 Put 上挂一道 HITL gate：第一次 Put 抛中断，带答案 Resume 后放行。
	gated := &gateOnPutStore{Store: store.NewInMemoryStore(), interruptID: interruptID}

	ns := store.NS("mem")
	newAgent := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("bot"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantMessage("记住：用户偏好深色主题"),
			})),
			agent_v2.WithStore(gated),
			agent_v2.WithStoreMemory(ns, nil),
			agent_v2.WithMaxRounds(3),
		)
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	// ── Run：Put 抛中断 ⟹ 本轮**不该**有 ExitEvent ──
	_, err := newAgent().Run(ctx, sess, schema.UserMessage("我喜欢深色主题"))
	if _, isInterrupt := compose_v2.IsInterrupt(err); !isInterrupt {
		t.Fatalf("fixture 前提没成立：Put 上的 gate 没抛出中断，err=%v", err)
	}
	if got := countExitEvents(sess); got != 0 {
		t.Errorf("Put 抛中断的那一轮不该发 ExitEvent，实际 %d 条"+
			"——AddEvent 跑到 Put 前面了？（顺序契约见 commitFinal）", got)
	}

	// ── Resume：Put 成功 ⟹ 恰好 1 条，不能 dup ──
	if _, err := newAgent().Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			interruptID: schema.UserMessage("ok"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := countExitEvents(sess); got != 1 {
		t.Errorf("Resume 完成后 ExitEvent 应恰好 1 条，实际 %d 条"+
			"（0 = 被吞；≥2 = Resume 重跑 commitFinal 时重复 add）", got)
	}
}

// gateOnPutStore 在第一次 Put 上抛 InterruptError 做 HITL gate——业务在自实现 Store
// 后端里这么做是框架文档明确支持的用法。带答案 Resume 后放行。
type gateOnPutStore struct {
	store.Store
	interruptID string
}

func (g *gateOnPutStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	if _, answered := compose_v2.GetResumeAnswerText(ctx, g.interruptID); !answered {
		return &compose_v2.InterruptError{
			Reason: "业务在 Store 后端做人在回路 gate",
			Query:  &compose_v2.AskQuery{InterruptID: g.interruptID, Question: "批准写入长期记忆吗"},
		}
	}
	return g.Store.Put(ctx, ns, key, value, opts...)
}
