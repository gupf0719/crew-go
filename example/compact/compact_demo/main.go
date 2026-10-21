// compact_demo 演示 4 层上下文压缩里的第 2 层 AutoCompact（LLM 摘要早期对话）——用小
// ModelContextWindow + 每轮返长内容堆 token 触发压缩；经 OrchestrationHooks.OnNodeEvent
// 订阅 compaction event，实时看到「何时触发、压缩前后 token、丢/留几条、耗时」。
//
// sink-gated：接了 OrchHooks 就自动收到 compact event，无需单独开关。真实业务接
// observability 时这些 event 自动转成 metric/span。
//
// 自包含（mock 主 LLM + mock 摘要 model），直接跑:
//
//	go run ./example/compact/compact_demo
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

func main() {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("compact_bot"),
		agent_v2.WithModel(&longModel{}), // 每轮返长内容堆 token
		agent_v2.WithSystemPrompt("你是助手。"),
		agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
			Enabled:             true,
			Model:               &summaryModel{}, // 摘要用 mock（真实场景推荐用便宜 model）
			ModelContextWindow:  2000,
			SummaryMaxTokens:    400,
			SummaryBufferTokens: 400, // 阈值 = 2000 - 400 - 400 = 1200 tokens
			// ⚠️ **微缩窗口的 demo 必须显式调小它**：默认 5000，而本 demo 的可压缩区
			// 只有几百 token ⟹ 成本守卫会在调摘要之前早退 ⟹ 每轮都打印
			// 「丢 0 / 留 N 条、token 一点没降」，而下面还在说「摘要替掉早期对话」。
			MinCompactableTokens: 1,
		}),
	)

	// 订阅 compaction event（sink-gated：接 OrchHooks 自动收到）
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), &compactWatcher{})
	sess := agent_v2.NewRunSession()

	fmt.Println("=== 多轮对话堆 token（阈值 1200），超阈值自动触发 AutoCompact ===")
	for i := 1; i <= 6; i++ {
		// 每轮是一个独立任务：换一枚 RunID，values / events 一路攒着（见
		// RunSession.RunID 的契约）。本 demo 没配 checkpointer 所以换不换都跑得通，
		// 照规矩写是因为**配了 checkpointer 之后不换就会静默删掉上个任务的挂起存档**，
		// 而那时症状离这行代码很远。
		sess.NextTask()
		if _, err := ag.Run(ctx, sess, schema.UserMessage(fmt.Sprintf("第 %d 轮，随便聊聊", i))); err != nil {
			log.Fatalf("round %d: %v", i, err)
		}
		fmt.Printf("  round %d done, history=%d 条\n", i, len(sess.History()))
	}
	fmt.Println("=== 观察上面的 [compact:auto:*] 行——AutoCompact 在超阈值那轮触发，摘要替掉早期对话 ===")
}

// compactWatcher 订阅 compaction event（compose_v2.OrchestrationHooks.OnNodeEvent）。
type compactWatcher struct {
	agent_v2.NoOpOrchestrationHooks
}

func (compactWatcher) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type != agent_v2.CompactionEventType {
		return ctx
	}
	action, ok := ev.Metadata["action"].(*agent_v2.CompactionAction)
	if !ok {
		return ctx
	}
	switch action.Type {
	case agent_v2.CompactionActionBefore:
		if b := action.Before; b != nil {
			fmt.Printf("    🔻 [compact:%s:before] 触发=%s，压缩前 %d tokens\n", action.Layer, b.Trigger, b.InputTokens)
		}
	case agent_v2.CompactionActionAfter:
		if a := action.After; a != nil {
			fmt.Printf("    ✅ [compact:%s:after]  丢 %d / 留 %d 条，%d→%d tokens，耗时 %dms\n",
				action.Layer, a.Dropped, a.Kept, a.InputTokens, a.OutputTokens, a.DurationMs)
		}
	}
	return ctx
}

// longModel 每轮返长内容（堆 token 触发压缩）。
type longModel struct{}

func (longModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return longModel{}, nil }
func (longModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("好的。" + strings.Repeat("这是一段用来堆积上下文 token 的填充内容。", 30)), nil
}
func (m longModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, _ := m.Generate(ctx, msgs, opts...)
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

// summaryModel 是 AutoCompact 的摘要 model（返短摘要，替掉早期对话）。
type summaryModel struct{}

func (summaryModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return summaryModel{}, nil
}
func (summaryModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// AutoCompact 从响应里按 <summary>...</summary> 提取摘要（见 autocompact.go summaryRe）
	return schema.AssistantMessage("<summary>前面几轮是闲聊寒暄，无关键业务信息需保留。</summary>"), nil
}
func (m summaryModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, _ := m.Generate(ctx, msgs, opts...)
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}
