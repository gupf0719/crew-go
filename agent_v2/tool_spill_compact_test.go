package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// spillThenGrepModel：第 1 次调 dump（大输出、被转存），之后 rounds 次调 grep 把上下文撑过阈值，再收尾。
type spillThenGrepModel struct {
	mu     sync.Mutex
	rounds int
	calls  int
	inputs [][]*schema.Message
}

func (m *spillThenGrepModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	switch {
	case m.calls == 1:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "d1", Type: "function",
			Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}}}), nil
	case m.calls <= m.rounds+1:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("g%d", m.calls), Type: "function",
			Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}}), nil
	default:
		return schema.AssistantMessage("完成"), nil
	}
}
func (m *spillThenGrepModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *spillThenGrepModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func spillDumpGrepToolkit() *tool.Toolkit {
	tk := grepToolkit()
	out := bigOutput(3000)
	tk.Register(tool.NewFuncTool("dump", "导出", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: out}, nil
		}))
	return tk
}

// AutoCompact 把带取回 id 的预览（或 MicroCompact 清理后留下的取回占位符）送进摘要输入；摘要不保留 id 时，
// 之后的模型输入里就再也没有这个 id——全文还在 store 里，模型取不回了。对照：MicroCompact offload 的占位符
// 在 AutoCompact 里按整组保留（groupContainsProtectedMarker），转存的没有。
func TestToolResultSpill_IDSurvivesAutoCompact(t *testing.T) {
	for _, withMicro := range []bool{false, true} {
		t.Run(fmt.Sprintf("micro=%v", withMicro), func(t *testing.T) {
			const rounds = 80
			m := &spillThenGrepModel{rounds: rounds}
			s := &countingSummarizer{}
			st := store.NewInMemoryStore()
			opts := []agent_v2.Option{agent_v2.WithAutoCompact(smallWindow(s))}
			if withMicro {
				opts = append(opts, agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true}))
			}
			ag := spillAgent(m, st, spillDumpGrepToolkit(), append(opts, agent_v2.WithMaxRounds(rounds+5))...)
			ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-ac")
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出再查")); err != nil {
				t.Fatal(err)
			}
			id := ""
			if mm := spillIDRe.FindStringSubmatch(inputText(m.inputs[1])); mm != nil {
				id = mm[1]
			}
			if id == "" || s.count() == 0 {
				t.Fatalf("fixture 没搭对：id=%q 摘要次数=%d", id, s.count())
			}
			inSummaryInput := false
			for _, in := range s.inputs {
				if strings.Contains(inputText(in), id) {
					inSummaryInput = true
				}
			}
			last := inputText(m.inputs[len(m.inputs)-1])
			t.Logf("摘要 %d 次；id 进过摘要输入=%v；最后一次模型输入含 id=%v", s.count(), inSummaryInput, strings.Contains(last, id))
			if !strings.Contains(last, id) {
				t.Fatalf("AutoCompact 之后模型输入里没有取回 id %s 了——全文还在 store 里，模型不知道怎么取回", id)
			}
		})
	}
}

// ReactiveCompact 紧急丢组：还没被 MicroCompact 清理的转存预览组不受保护，丢掉之后取回 id 就没了。
// 对照：同样位置换成 MicroCompact 清理后的占位符（带 MicroCompactClearedAnnotation）时受保护、留下。
func TestToolResultSpill_IDSurvivesReactiveCompact(t *testing.T) {
	build := func(cleared bool) []*schema.Message {
		notice := `【工具输出过大，已转存】dump 返回了 120008 字节（3001 行）。要看完整内容，调用 read_tool_output，参数 id="sp_deadbeef00000001"` + strings.Repeat("预览", 200)
		tr := schema.ToolResultMessage("d1", notice)
		tr.Annotations = map[string]any{agent_v2.ToolResultSpillAnnotation: "sp_deadbeef00000001"}
		if cleared {
			tr.Content = `[较早的工具输出已清理；全文仍可用 read_tool_output 取回，id="sp_deadbeef00000001"]`
			tr.Annotations[agent_v2.MicroCompactClearedAnnotation] = true
		}
		msgs := []*schema.Message{
			schema.UserMessage("导出订单"),
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "d1", Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}}}),
			tr,
			schema.AssistantMessage("导出好了"),
		}
		for i := 0; i < 6; i++ {
			msgs = append(msgs, schema.UserMessage("再聊一句 "+strings.Repeat("话", 300)), schema.AssistantMessage("好 "+strings.Repeat("答", 300)))
		}
		return msgs
	}
	for _, cleared := range []bool{true, false} {
		out, err := agent_v2.ReactiveCompactRun(context.Background(), &agent_v2.ReactiveCompactConfig{Enabled: true}, build(cleared),
			errors.New("prompt is too long: 9000 tokens > 8000 maximum"))
		if err != nil {
			t.Fatal(err)
		}
		has := false
		for _, m := range out {
			if strings.Contains(m.Content, "sp_deadbeef00000001") {
				has = true
			}
		}
		t.Logf("cleared=%v：紧急压缩后 %d 条消息，含取回 id=%v", cleared, len(out), has)
		if !has {
			t.Errorf("cleared=%v：ReactiveCompact 丢掉了带取回 id 的那组——全文还在 store 里，模型取不回了", cleared)
		}
	}
}

// TestToolResultSpill_IDSurvivesRepeatedAutoCompact：摘要了不止一次时取回 id 也还在——第一次摘要把转存的结果摘掉、
// 附上索引；之后每一次增量摘要把上一份摘要也摘掉，索引要从它的标注接着带（摘要模型不一定保留正文里那段）。
func TestToolResultSpill_IDSurvivesRepeatedAutoCompact(t *testing.T) {
	const rounds = 240
	m := &spillThenGrepModel{rounds: rounds}
	s := &countingSummarizer{}
	ag := spillAgent(m, store.NewInMemoryStore(), spillDumpGrepToolkit(),
		agent_v2.WithAutoCompact(smallWindow(s)), agent_v2.WithMaxRounds(rounds+5))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-ac2")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出再查")); err != nil {
		t.Fatal(err)
	}
	id := ""
	if mm := spillIDRe.FindStringSubmatch(inputText(m.inputs[1])); mm != nil {
		id = mm[1]
	}
	if id == "" || s.count() < 3 {
		t.Fatalf("fixture 没搭对：id=%q 摘要次数=%d（要至少 3 次）", id, s.count())
	}
	if last := inputText(m.inputs[len(m.inputs)-1]); !strings.Contains(last, id) {
		t.Fatalf("摘要了 %d 次之后模型输入里没有取回 id %s 了", s.count(), id)
	}
}
