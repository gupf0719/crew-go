package observability

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// capAssembly 是只记录「LLM 实收到的消息流」的桩模型——它拿到的就是
// buildInitialMessages 的真产出，不是测试自己手搓的形态。
type capAssembly struct {
	mu    sync.Mutex
	first []*schema.Message
}

func (c *capAssembly) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	c.mu.Lock()
	if c.first == nil {
		c.first = append([]*schema.Message(nil), msgs...)
	}
	c.mu.Unlock()
	return schema.AssistantMessage("这是真答复"), nil
}

func (c *capAssembly) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := c.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

func (c *capAssembly) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return c, nil }

var _ model.ChatModel = (*capAssembly)(nil)

// TestLastAssistantMessage_InjectedMemoryIsNotTheFinalAnswer 钉住「注入的长期记忆
// 不得冒充 agent 的最终答复」。
//
// `agent_v2.IsUnresolvedCallsRecord` 的 godoc 说的那类**冒充者**曾经有两个：框架合成的记账文本
// （已由同目录 `last_assistant_internal_test.go` 钉住），和 **StoreMemory 注入的长期
// 记忆**——它当年是「assistant + 零 tool_call」，形状与记账文本一模一样，而谓词只读
// 记账文本那把钥匙、挡不住它。实测过后果：模型还没产出真答复就退出时（中断 / API 报错），
// `agent_output` event 的 `gen_ai_content` 上报的是**用户自己的长期记忆**，冒充成
// 「agent 这次给我的回答」——而出事的恰好是有人在看 trace 排障的时候。
//
// 现在注入用的是 system role，这个冒充在**结构上**不成立了。这条哨兵钉的就是那个结构：
//
//   - 它走**真装配**（真 BasicAgent + 真 StoreMemory，桩模型收到的 msgs 就是
//     `buildInitialMessages` 的产出），不手搓消息流——手搓的话断言的是我对装配的记忆，
//     不是装配本身。
//   - 它走**真谓词**（`lastAssistantMessage` 本尊），不复制一份判据。
//
// ⚠️ **它跟 `sql` 那条跨后端哨兵不重叠**：那条钉的是「两类后端注入的东西逐字一致」，
// 把注入改成 assistant 但**两边一致**时它照样绿，而这条会红。两条钉的是两个不变量。
func TestLastAssistantMessage_InjectedMemoryIsNotTheFinalAnswer(t *testing.T) {
	const memText = "用户偏好深色主题"

	st := store.NewInMemoryStore()
	ns := store.NS("obs_mem_probe")
	if err := st.Put(context.Background(), ns, "pref", memText); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cm := &capAssembly{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("mem_agent"),
		agent_v2.WithModel(cm),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
		agent_v2.WithMaxRounds(2),
	)
	// query 与记忆有词面重叠，否则框架侧打分为 0 不注入——那样这条用例会变成空转。
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("我的主题偏好是什么")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 前提断言：记忆真的注进去了。它不承重（下面那条才承重），但没有它，
	// 一次「注入根本没发生」的退化会让这条用例恒绿。
	injected := false
	for _, m := range cm.first {
		if m != nil && m.Content == memText {
			injected = true
		}
	}
	if !injected {
		t.Fatalf("前提没搭对：记忆没被注入，本用例什么也没测。LLM 实收 %d 条", len(cm.first))
	}

	// 承重断言：模型还没产出任何 assistant 之前，这条消息流里不能有任何东西
	// 够格当「agent 的最终答复」。
	if got := lastAssistantMessage(cm.first); got != nil {
		t.Fatalf("注入的长期记忆冒充了 agent 的最终答复，会被当成 gen_ai_content 上报：role=%s content=%q",
			got.Role, got.Content)
	}
}
