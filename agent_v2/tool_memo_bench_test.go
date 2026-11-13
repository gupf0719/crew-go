package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type toolLedgerBenchModel struct {
	reply *schema.Message
}

func (m toolLedgerBenchModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if in[len(in)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	response := *m.reply
	return &response, nil
}
func (toolLedgerBenchModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m toolLedgerBenchModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func BenchmarkBasicAgentToolLedger(b *testing.B) {
	for _, width := range []int{8, 32, 64, 128} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			result := strings.Repeat("x", 4096)
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("work", "work", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
				return tool.Success(result), nil
			}))
			names := make([]string, width)
			for i := range names {
				names[i] = "work"
			}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("ledger"), agent_v2.WithToolkit(tk), agent_v2.WithModel(toolLedgerBenchModel{toolCallsMessage(names...)}))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("run")); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
