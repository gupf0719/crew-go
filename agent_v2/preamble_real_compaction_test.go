package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/prompt"
	"crew-go/schema"
	"crew-go/tool"
)

// ptlModel 完全按**输入内容**决定行为（不按调用次数）——一轮里实际调几次取决于流式那条路
// 怎么走（建流失败曾经还会再调一次 Generate），按次数计的桩会把要测的那一次错位、让重试
// 那条路根本没跑到。
type ptlModel struct {
	mu   sync.Mutex
	last []*schema.Message
}

func (m *ptlModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.last = append([]*schema.Message(nil), msgs...)
	nTool, total := 0, 0
	for _, x := range msgs {
		if x.Role == schema.RoleTool {
			nTool++
		}
		total += len(x.Content)
	}
	m.mu.Unlock()
	// 见到紧急压缩补的那条 marker 就收尾——它是「压缩已经真跑过」的内容证据，
	// 用它当收敛条件比按次数计数可靠（降级路径会把次数打乱）。
	for _, x := range msgs {
		if strings.Contains(x.Content, "[continued after context") {
			return schema.AssistantMessage("最终答复"), nil
		}
	}
	if nTool < 2 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: fmt.Sprintf("c%d", nTool), Type: "function",
			Function: schema.FunctionCall{Name: "bulk", Arguments: `{}`},
		}}), nil
	}
	if total > 20000 {
		return nil, errors.New(`{"error":{"code":"context_length_exceeded","message":` +
			`"This model's maximum context length is 8192 tokens. However, your messages resulted in 50000 tokens."}}`)
	}
	return schema.AssistantMessage("最终答复"), nil
}

func (m *ptlModel) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := m.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}
func (m *ptlModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type bulkTool struct{}

func (bulkTool) Name() string                { return "bulk" }
func (bulkTool) Description() string         { return "产出大块内容" }
func (bulkTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (bulkTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	return tool.Success(strings.Repeat("z", 15000)), nil
}

// TestPreamble_SurvivesRealEmergencyCompaction 是这组里**唯一真的跑了一遍压缩**的哨兵。
//
// 前面两条钉的是「分流」那一半（前言进没进被压缩的流、hook 改写生不生效）。它们在
// 「劈回/透传那一半整个失效」的退化下**照样绿**——因为那个退化不改变它们断言的量。
// 独立审计实测过这一点，所以这条是补上的：承重的量是**「压缩真跑一遍之后，模型还收不收得到前言」**。
//
// fixture 的三处讲究，都是踩过才知道的：
//   - 桩模型按**输入内容**决定行为，不按调用次数——`streamOrGenerate` 有 Stream→Generate
//     降级，按次数计的桩会让第一次的错误被降级调用吃掉，重试那条路**根本没跑到**
//     （此前写过一版就是这样，变异下照样绿）。
//   - 用真实的 provider 错误串，否则 `ParsePromptTooLongGap` 解析不出 gap、走 dropPercent
//     兜底，测的就不是同一条路。
//   - 先用工具往返撑出**多个 API 轮次**，否则 `emergencyDropOldestGroups` 只有一组可丢、
//     直接返原 messages，被测路径压根不执行。
func TestPreamble_SurvivesRealEmergencyCompaction(t *testing.T) {
	tmpl := prompt.FromMessages(prompt.System("TEMPLATE SYSTEM"),
		prompt.User("fewshot q"), prompt.Assistant("fewshot a"))
	tk := tool.NewToolkit()
	tk.Register(bulkTool{})
	m := &ptlModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
		agent_v2.WithPromptTemplate(tmpl, nil),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true}),
		agent_v2.WithMaxRounds(6),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("开始")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 前提：紧急压缩真的发生过。没有它，下面那条断言在「压缩压根没触发」时恒真。
	if !containsContent(m.last, "[continued after context") {
		t.Fatalf("前提没搭对：没看到紧急压缩补的 marker，压缩那条路没跑到，本用例什么也没测。"+
			"模型实收：%s", renderRoles(m.last))
	}

	// 承重：压缩跑完之后，四条前言必须一条不少地还在模型的入参里。
	for _, w := range []string{"TEMPLATE SYSTEM", "REAL SYSPROMPT", "fewshot q", "fewshot a"} {
		if !containsContent(m.last, w) {
			t.Errorf("紧急压缩之后模型收不到 %q——agent 的定义被静默丢了。实收：%s",
				w, renderRoles(m.last))
		}
	}
}
