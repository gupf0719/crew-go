package agent_v2

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// capSummarizer 记录摘要 LLM 实收的 input。
type capSummarizer struct {
	mu  sync.Mutex
	got [][]*schema.Message
}

func (c *capSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	c.mu.Lock()
	c.got = append(c.got, append([]*schema.Message(nil), msgs...))
	c.mu.Unlock()
	return schema.AssistantMessage("<summary>摘要正文</summary>"), nil
}

func (c *capSummarizer) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := c.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}

func (c *capSummarizer) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return c, nil }

var _ model.ChatModel = (*capSummarizer)(nil)

// TestAutoCompact_SummarizerSysPromptIsNotInjectedMemory 钉住「用户的长期记忆不得成为
// 摘要模型的 system prompt」。
//
// 摘要调用回落用 agent 自己的 system prompt，而回落的取法曾是**按下标**取前导 system
// 块的第 0 条。那个块里除了 persona 还装着**注入的长期记忆**（它也是 system、也在前导），
// 业务不配 `WithSystemPrompt` 时记忆就排在第 0 位 ⟹ 用户的隐私数据被当成
// 「你是什么样的助手」发给摘要模型。
//
// 这条用**真装配**（真 BasicAgent + 真 `store.Store`，`buildInitialMessages` 的产出直接
// 喂 [AutoCompactRun]）而不是手搓消息流——手搓的话断言的是我对装配顺序的记忆。
func TestAutoCompact_SummarizerSysPromptIsNotInjectedMemory(t *testing.T) {
	const secret = "4321"
	const memText = "用户的信用卡尾号 " + secret + "，住在朝阳区"

	st := store.NewInMemoryStore()
	ns := store.NS("summarizer_sysprompt")
	if err := st.Put(context.Background(), ns, "pref", memText); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 故意不配 WithSystemPrompt——那正是注入记忆会排到前导块第 0 位的条件。
	a := NewBasicAgent(
		WithName("probe"),
		WithStore(st),
		WithStoreMemory(ns, &StoreMemoryConfig{SearchLimit: 5}),
	)
	sess := NewRunSession()
	ctx := WithSession(store.WithContext(context.Background(), st), sess)
	// query 与记忆需有词面重叠，否则框架侧打分为 0 根本不注入。
	_, msgs, err := a.buildInitialMessages(ctx, sess, schema.UserMessage("我的信用卡尾号是多少"))
	if err != nil {
		t.Fatalf("buildInitialMessages: %v", err)
	}

	// 前提断言：记忆真被注入、且真排在第 0 位。它不承重，但少了它，一次「注入没发生」
	// 或「装配序变了」的退化会让下面那条恒真。
	if len(msgs) == 0 || msgs[0].Role != schema.RoleSystem || !strings.Contains(msgs[0].Content, secret) {
		t.Fatalf("前提没搭对：注入的记忆不在前导 system 第 0 位，本用例什么也没测。实际 %d 条", len(msgs))
	}

	// 撑出足够对话让摘要真跑（阈值走 conv 部分字符估算）。填充内容不含 secret。
	full := append([]*schema.Message(nil), msgs...)
	for i := 0; i < 6; i++ {
		full = append(full, schema.UserMessage(fmt.Sprintf("闲聊%d %s", i, strings.Repeat("x", 20000))))
		full = append(full, schema.AssistantMessage(fmt.Sprintf("回应%d %s", i, strings.Repeat("y", 20000))))
	}

	cm := &capSummarizer{}
	cfg := &AutoCompactConfig{Enabled: true, Model: cm, ModelContextWindow: 40000}
	if _, err := AutoCompactRun(ctx, cfg, full); err != nil {
		t.Fatalf("AutoCompactRun: %v", err)
	}
	if len(cm.got) == 0 {
		t.Fatal("前提没搭对：摘要 LLM 一次都没被调用，本用例什么也没测")
	}

	// 承重断言：摘要调用的 input 里，任何 system 消息都不得含用户的长期记忆。
	for _, in := range cm.got {
		for i, m := range in {
			if m.Role == schema.RoleSystem && strings.Contains(m.Content, secret) {
				t.Fatalf("用户的长期记忆被当成摘要模型的 system prompt 发出去了：input[%d] = %q", i, m.Content)
			}
		}
	}
}
