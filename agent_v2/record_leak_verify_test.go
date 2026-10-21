package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// directExitToolName / directExitToolkit 造一个「一调即出」的业务工具。
//
// 本文件要的只是**「本轮以带 tool_call 的 assistant 收尾」这个形状**（return-direct），
// 跟转交语义无关。原先借 `transfer_to_agent` 摆这个形状，而它在没有 HandoffAgent 接的
// 场合已经改成软失败回灌（LLM 接着自己作答）⟹ 摆不出 return-direct 了。业务自己声明
// ReturnDirectMetaKey 是这个形状的**通用**来源，也更贴 godoc 里说的触发人群。
const directExitToolName = "finish_directly"

func directExitToolkit() *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(directExitToolName, "一调即出", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return &tool.Result{
				Success:  true,
				Output:   "已收尾",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true},
			}, nil
		}))
	return tk
}

func directExitCall(callID string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: callID, Type: "function",
		Function: schema.FunctionCall{Name: directExitToolName, Arguments: `{}`},
	}})
}

// TestRecordLeak_NotUsedAsFinalAnswer 钉住 dbcd524 引入的一条回归的修复（审计视角二 HIGH-1）。
//
// `dropDanglingToolCalls` 给零结果的工具轮补的那条文本记录是**内部记账**，只该出现在
// 喂给模型的 prior 里。但它恰好是「assistant + 零 tool_call」，而当年那个挑最终答复的
// `lastAssistant`（**已删**，见下）首选判据正是这个 ⟹ 当本轮**以带 tool_call 的
// assistant 收尾**（return-direct：
// transfer_to_agent / route_to / 业务声明 ReturnDirectMetaKey）时，倒扫会跳过 tool_result、
// 跳过那条带 tc 的 assistant，**命中这条记账文本**。
//
// 后果链（三处一起中招）：
//   - `FinalMessage(events)` 返回它 ⟹ 业务拿到的「答案」是一句内部记账
//   - `commitFinal` 的 `st.Put` 把它写进长期记忆
//   - 11 个编排边界的 `settleHistorySlot(..., final)` 把它写进 append-only 历史（删不掉）
//
// 命中人群正是这个分支的主力：实测 31 次命中里 22 次是 `transfer_to_agent`，而
// return-direct 恰恰**就是**「以 tool_call 收尾」这个触发条件本身。
//
// **现行机制不是「跳过它」，是「它进不来」**：最终产出由决策点直接写进 `basicFinalCh`，
// `commitFinal` 读的就是那条 channel（`basic_agent.go` 的 `last := in.Final`）——记账文本
// 在 preamble 侧合成、根本进不了那条 channel，于是防护从「靠谓词记得跳过」变成结构上不
// 可能。当年那个倒扫 `basicMessagesCh`（含 prior）挑最终答复的 helper 已作为死代码删除。
//
// `UnresolvedCallsRecordAnnotation` 仍然要打、`IsUnresolvedCallsRecord` 仍然要调——
// 但只剩在**还得靠扫消息挑一条代表**的地方（observability 的 agent_output、ToolFilter
// 的 lastInput，以及业务自己扫 messages 的代码）。
//
// 本用例守的是结果不是手法：**内部记账不许成为最终答复**。把 `last := in.Final` 变异回
// 倒扫版，它会红并指出「内部记账文本变成了最终答复」。
func TestRecordLeak_NotUsedAsFinalAnswer(t *testing.T) {
	st := store.NewInMemoryStore()
	ns := store.NS("memories", "u1")

	// Run 1：return-direct 出口 ⟹ Exit 是带悬空 tc 的 assistant ⟹ 被 st.Put 进长期记忆
	run1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			directExitCall("tc_x1"),
		})),
		agent_v2.WithToolkit(directExitToolkit()),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 50}),
		agent_v2.WithMaxRounds(3),
	)
	if _, err := run1.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Run 2：新 session、同 store ⟹ preamble Search 注入那条悬空 assistant ⟹ 清洗成记账文本。
	// 本轮**同样以 return-direct 收尾**，于是倒扫命中那条记账文本。
	run2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			directExitCall("tc_x2"),
		})),
		agent_v2.WithToolkit(directExitToolkit()),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 50}),
		agent_v2.WithMaxRounds(3),
	)
	ev, err := run2.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("再来"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	final := agent_v2.FinalMessage(ev)
	if final == nil {
		t.Fatal("Run 2 没有最终消息")
	}
	t.Logf("Run 2 最终答复：%.120q", final.Content)

	// 承重①：内部记账文本绝不能成为答复（它还会被 st.Put 进长期记忆 + 写进 append-only 历史）
	if strings.Contains(final.Content, "[历史记录]") {
		t.Fatalf("内部记账文本变成了最终答复：%q", final.Content)
	}
	// 承重②：**必须有一条实打实的答复**——不能连它一起跳没了
	// （返 nil 会让 commitFinal 静默早退、整个 Run 没有最终消息）。
	//
	// ⚠️ 这里一度钉的是「回落到那条带 tool_call 的 assistant」。2026-08-28 之后
	// 业务 return-direct 工具的答复改为**取工具产出**，那个形态不再成立——
	// 但承重的从来不是形态，是「有答复且不是记账文本」。钉形态会让每次合理演进都误报。
	if strings.TrimSpace(final.Content) == "" {
		t.Fatalf("最终答复是空的——跳过记账文本时把真答复也一起跳没了。tool_calls=%v",
			schema.GetToolCalls(final))
	}

	// ⚠️ **本用例的 fixture 里刻意没有别的普通 assistant**。加一条（比如先跑一轮普通
	// 问答）会让 `final` 恒等于那条陈旧答复、倒扫**根本走不到记账文本** ⟹ 承重①退化成
	// 恒真。那条「陈旧答复遮蔽本轮出口」是**另一个**缺陷，归
	// TestStaleAssistantShadowsThisRoundExit 管——**别把两个 fixture 合到一起**。
}

// TestStaleAssistantShadowsThisRoundExit 钉住一条**已修缺陷**的修复。
//
// 当年那个挑最终答复的 helper 扫的是整条 `basicMessagesCh`——**含 prior**，而它不区分
// 「本轮产出」和「历史消息」。于是本轮**以带 tool_call 的 assistant 收尾**时
// （return-direct：transfer_to_agent / route_to / 业务声明 ReturnDirectMetaKey），
// 第一趟跳过它，继续往前撞上**上一轮的普通答复**，把那条当成本轮的答案。
//
// 后果三处一起中招且 err 全程为 nil：
//   - 调用方拿到的「本轮答案」是另一段对话的陈旧答复
//   - `commitFinal` 的 `st.Put` 把它再存一份 ⟹ 同一句话在 store 里自我复制，每多跑一轮
//     return-direct 就多一份
//   - 11 个编排边界的 `settleHistorySlot(..., final)` 把它写进 append-only 历史
//
// **RoutingAgent 上是同一根因的另一张脸**：classifier 只要有过一次普通文本答复
// （「我不确定」很常见），此后每轮的 `route_to` 决策都被那条旧文本遮蔽 ⟹
// `extractRouteTarget` 拿到不在 branchSet 的文本 ⟹ 静默走 fallback。
//
// **修法**：加 `basicFinalCh`——决策点（model 节点）在产生本轮产出的那一刻把它写进
// 专用 channel，出口读那条 channel 而不是倒扫消息列表。**prior 结构上进不去**（只有
// model / tools 节点会写它），这是「结构上不可能」而不是「加了道防御」。
//
// 这不是新范式：其余 6 个编排本来就是「决策点写、出口读」，只有 BasicAgent 在倒扫
// ——而它恰恰是唯一需要处理「prior 和本轮产出混在一起」的那个。
func TestStaleAssistantShadowsThisRoundExit(t *testing.T) {
	st := store.NewInMemoryStore()
	ns := store.NS("memories", "u1")
	mk := func(resp []*schema.Message, opts ...agent_v2.Option) agent_v2.Agent {
		base := []agent_v2.Option{
			agent_v2.WithName("bot"), agent_v2.WithModel(newStubModel(resp)),
			agent_v2.WithStore(st),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 50}),
			agent_v2.WithMaxRounds(3),
		}
		return agent_v2.NewBasicAgent(append(base, opts...)...)
	}

	// Run 1：一条最普通的文本答复进长期记忆
	if _, err := mk([]*schema.Message{schema.AssistantMessage("你的余额是 100 元")}).
		Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查余额")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	// Run 2：本轮 return-direct 收尾——它的出口才是本轮的答案
	ev, err := mk([]*schema.Message{directExitCall("tc_x")}, agent_v2.WithToolkit(directExitToolkit())).
		Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("直接收尾"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	final := agent_v2.FinalMessage(ev)
	if final == nil {
		t.Fatal("Run 2 没有最终消息")
	}
	// 承重①：绝不能拿到上一轮的陈旧答复
	if final.Content == "你的余额是 100 元" {
		t.Fatalf("本轮 return-direct 的出口被上一轮的陈旧答复顶替了（err==nil，全程静默）")
	}
	// 承重②：必须拿到本轮的实打实答复——不能矫枉过正返 nil / 返空。
	//
	// ⚠️ 一度钉的是「那条带 tool_call 的 assistant」。2026-08-28 起业务 return-direct
	// 的答复改为**取工具产出**，形态变了而承重的没变。Handoff 的接力语义（后接看前接
	// 说了什么）由框架工具那条路保证——transfer 是框架工具，走的仍是旧形态。
	if strings.TrimSpace(final.Content) == "" {
		t.Fatalf("没拿到本轮的答复——陈旧的 assistant 遮蔽了本轮出口，或本轮答复被跳没了")
	}
}
