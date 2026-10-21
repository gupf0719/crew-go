package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// tooLongUntilMarker 按**输入内容**决定行为：只要还没看到紧急压缩补的 marker 就报超长。
// 不按调用次数——一轮里实际调几次取决于流式那条路怎么走（建流失败曾经还会再调一次
// Generate），按次数计的桩会把要测的那一次错位，被测的重试路径根本跑不到。
//
// 报的错**不含可解析的 token 缺口**，逼 emergencyDrop 走 dropPercent 那条路——
// 那条路每次只丢一组，才凑得出「丢 1 组 + 补 1 条 marker ⟹ 条数不变」这个形态。
type tooLongUntilMarker struct {
	mu    sync.Mutex
	calls int
}

func (m *tooLongUntilMarker) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	for _, x := range msgs {
		if x != nil && strings.Contains(x.Content, "[continued after context") {
			return schema.AssistantMessage("压缩之后答出来了"), nil
		}
	}
	return nil, errors.New("prompt is too long")
}

func (m *tooLongUntilMarker) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := m.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *tooLongUntilMarker) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*tooLongUntilMarker)(nil)

func seedAsst(sess *agent_v2.RunSession, agent, runID, content string) {
	msg := schema.AssistantMessage(content)
	msg.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: runID}
	sess.AddEvent(agent_v2.NewOutputEvent(agent, msg))
}

// TestReactiveCompact_ProgressJudgedByTokensNotCount 钉住「紧急压缩的『还能不能再压』
// 按 token 判，不按消息条数」。
//
// **bug 历史**：判据曾是 `len(compacted) >= len(curMsgs)` ⟹ 当场 break、把这次压缩丢掉。
// 而 `emergencyDropOldestGroups` 删完若首条是 assistant 会**补一条 user marker**——
// 丢 1 组补 1 条时**条数不变**，于是一次真实有效的压缩被整个丢弃、原始的 prompt-too-long
// 直接返给业务。实测那个形态：条数 3→3，token **2681→12**。
//
// 它还挡着下一步：内容级压缩（把工具结果换占位符 / 截断超长单条）的特征恰恰是条数不变、
// 体积变小 ⟹ 必然命中 `>=`，接进来也白接且静默。
//
// ⚠️ **承重的是「Run 有没有成功」，不是「模型被调了几次」**——调用次数还受别的机制影响：
// 建流失败曾经会再调一次 Generate，那时判据退回按条数，模型**照样被调 2 次**，只是两次都
// 拿着未压缩的消息、都报超长。所以下面那条 `calls < 2` 只是个前提检查（模型真被调过），
// 别把它当主断言。
func TestReactiveCompact_ProgressJudgedByTokensNotCount(t *testing.T) {
	llm := &tooLongUntilMarker{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"),
		agent_v2.WithModel(llm),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true}),
		agent_v2.WithMaxRounds(3),
	)
	// 造出 [assistant(r1 大), assistant(r2 小), user] —— 三组，dropPercent 50% 只丢最旧一组，
	// 丢完首条是 assistant ⟹ 补 marker ⟹ 条数不变。
	sess := agent_v2.NewRunSession()
	seedAsst(sess, "probe", "r1", "很旧很长的内容 "+strings.Repeat("x", 8000))
	seedAsst(sess, "probe", "r2", "较近的短内容")

	_, err := ag.Run(context.Background(), sess, schema.UserMessage("当前问题"))
	if err != nil {
		t.Fatalf("Run 应当在压缩之后成功，实际 err=%v（模型被调 %d 次）", err, llm.calls)
	}
	// 前提检查（不承重，理由见上）：模型确实被调用过不止一次。
	if llm.calls < 2 {
		t.Fatalf("前提没搭对：模型只被调了 %d 次，重试那条路没跑到", llm.calls)
	}
}
