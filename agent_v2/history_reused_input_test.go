package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 业务**复用同一个消息对象**二次提交时，判据把它当成同一条——这是**契约**。
//
// 一个消息对象 = 一次出现。复用它就是声明「这是同一条」，要表达「新的一轮」就新建一条
// （`schema.UserMessage` 每次调用铸新身份）。成熟框架同样这么取：按消息身份判，复用对象
// 同样塌成一条，且都没有为这个形状写测试——它们把它当契约不当 bug。
//
// # 为什么不做成「每次都是新一轮」
//
// 试过。判据换成「这个 Run 里有人记过这个指针吗」之后确实修好了复用对象这条，但引入
// 5 个回归、3 个是 HIGH：判据变成**全局**的（不分哪条流）而 prior 在 SelfOnly 下按归属
// 过滤——兄弟分支一记我就被判「有了」，可我的 prior 里一个字都没有：Parallel 三分支里
// 1~2 条拿到**空 prompt**、Handoff SelfOnly 第 2/3 跳全空、边界补记先记的话本地分支也空。
// 而且指针跨不过 cp，Resume 后本轮问题在 prompt 里出现两遍。
//
// 那次的教训是：判据的**作用域**必须跟它服务的那个问题同维度。这里的问题是「我这条流的
// prior 里有没有它」，prior 本身就是按流过滤过的，直接扫它两个维度一起对。
func TestHistory_ReusedInputObject_TreatedAsSameOccurrence(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	var prompts []string
	mk := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("bot"),
			agent_v2.WithModel(&reusePromptRecorder{out: &prompts}),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reuse")

	// (a) 复用同一个对象：判成同一条，prompt 不重复追加
	shared := schema.UserMessage("固定指令")
	for i := 0; i < 3; i++ {
		if _, err := mk().Run(ctx, agent_v2.NewRunSession(), shared); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
	}
	for i, p := range prompts {
		t.Logf("复用对象 Run %d prompt: %s", i+1, p)
	}
	if n := strings.Count(prompts[len(prompts)-1], "固定指令"); n != 1 {
		t.Errorf("复用对象时同一条输入在 prompt 里应恰好 1 次（契约：一个对象 = 一次出现），实际 %d 次", n)
	}

	// (b) 对照组：每次新建消息 = 每次都是新的一轮，全都进 prompt
	prompts = nil
	ctx2 := compose_v2.WithThreadIDCtx(context.Background(), "conv-fresh")
	for i, q := range []string{"问题A", "问题B", "问题C"} {
		if _, err := mk().Run(ctx2, agent_v2.NewRunSession(), schema.UserMessage(q)); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
	}
	last := prompts[len(prompts)-1]
	t.Logf("新建消息 第 3 轮 prompt: %s", last)
	if !strings.HasSuffix(last, "[user]问题C|") {
		t.Errorf("每次新建消息时第 3 轮该以本轮问题结尾——契约的另一半塌了：%s", last)
	}
}

// reusePromptRecorder 记下每轮实际喂给模型的消息流。
type reusePromptRecorder struct{ out *[]string }

func (m *reusePromptRecorder) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	var b strings.Builder
	for _, x := range msgs {
		if x.Role == schema.RoleSystem {
			continue
		}
		fmt.Fprintf(&b, "[%s]%s|", x.Role, strings.TrimSpace(x.Content))
	}
	*m.out = append(*m.out, b.String())
	return schema.AssistantMessage("ack"), nil
}
func (m *reusePromptRecorder) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *reusePromptRecorder) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
