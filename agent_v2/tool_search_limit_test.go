package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

var cappedRetrieverSignal = compose_v2.NewSignalKind("test.capped-tool-retriever")

type rejectToolSearchHook struct{ hook.NoOpHooks }

func (rejectToolSearchHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, output *schema.ToolOutput) (context.Context, error) {
	if output.CallId == "veto" {
		output.Result = &schema.ToolCallResult{ErrorCode: "vetoed", Error: "不采用这次检索"}
	}
	return ctx, nil
}

func TestToolSearchLimitSettlesInOrderAndSurvivesResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	kit := tool.NewToolkit()
	var lookupCalls, exportCalls atomic.Int32
	kit.Register(tool.NewFuncTool("inventory_lookup", "查询库存", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookupCalls.Add(1)
			return tool.Success("库存已查"), nil
		}))
	kit.Register(tool.NewFuncTool("inventory_export", "导出库存", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			exportCalls.Add(1)
			return tool.Success("库存已导出"), nil
		}))
	kit.Register(tool.NewFuncTool("inventory_preview", "预览库存", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return nil, errors.New("unapproved preview tool was executed")
		}))
	exportRetrieved := make(chan struct{})
	retriever := func(ctx context.Context, query string, _ []tool.Tool) []string {
		switch query {
		case "control":
			compose_v2.EmitSignal(ctx, cappedRetrieverSignal, compose_v2.Signal{Dest: "test", Value: "disallowed"})
			return []string{"inventory_preview"}
		case "veto":
			return []string{"inventory_preview"}
		case "lookup":
			select {
			case <-exportRetrieved:
			case <-ctx.Done():
				return nil
			}
			return []string{"inventory_lookup", "inventory_lookup"}
		case "export":
			close(exportRetrieved)
			return []string{"inventory_export"}
		case "repeat":
			return []string{"inventory_lookup", "inventory_export"}
		default:
			return []string{"inventory_export"}
		}
	}
	search := func(id, query string) schema.ToolCall {
		return schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: "tool_search", Arguments: fmt.Sprintf(`{"query":%q}`, query)}}
	}
	hasTool := func(tools []*schema.ToolInfo, name string) bool {
		return slices.ContainsFunc(tools, func(info *schema.ToolInfo) bool { return info.Name == name })
	}
	receipt := func(messages []*schema.Message, id string) string {
		for _, message := range messages {
			if schema.GetToolResultID(message) == id {
				return message.Content
			}
		}
		return ""
	}
	var rounds atomic.Int32
	m := observedToolModel{generate: func(ctx context.Context, messages []*schema.Message, tools []*schema.ToolInfo) (*schema.Message, error) {
		if n := rounds.Add(1); n == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				search("control", "control"), search("veto", "veto"), search("grant", "lookup"), search("over", "export"),
			}), nil
		} else if n == 2 || n == 3 {
			if !hasTool(tools, "inventory_lookup") || hasTool(tools, "inventory_export") || hasTool(tools, "inventory_preview") ||
				!strings.Contains(receipt(messages, "grant"), "inventory_lookup") || !strings.Contains(receipt(messages, "over"), "未解锁") ||
				!strings.Contains(receipt(messages, "veto"), "vetoed") || !strings.Contains(receipt(messages, "control"), "编排控制信号") {
				return nil, fmt.Errorf("unexpected settled inventory tools or receipts: %s", concatContent(messages))
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认查询？"}); err != nil {
				return nil, err
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{search("repeat", "repeat")}), nil
		} else if n == 4 {
			got := receipt(messages, "repeat")
			if !strings.Contains(got, "inventory_lookup") || strings.Contains(got, "inventory_export") || !strings.Contains(got, "未解锁") ||
				!hasTool(tools, "inventory_lookup") || hasTool(tools, "inventory_export") {
				return nil, fmt.Errorf("resume changed the reveal budget: %q", got)
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "lookup-call", Function: schema.FunctionCall{Name: "inventory_lookup", Arguments: "{}"}},
				{ID: "export-call", Function: schema.FunctionCall{Name: "inventory_export", Arguments: "{}"}},
			}), nil
		}
		if !strings.Contains(receipt(messages, "lookup-call"), "库存已查") {
			return nil, errors.New("the admitted tool did not execute")
		}
		return schema.AssistantMessage("done"), nil
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("budgeted-inventory"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithHooks(rejectToolSearchHook{}), agent_v2.WithMaxRounds(8),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{
			Enabled: true, Deferred: func(tool.Tool) bool { return true }, Retriever: retriever, MaxRevealed: 1,
		}))
	session := agent_v2.NewRunSession()
	_, err := a.Run(ctx, session, schema.UserMessage("盘点"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("检索结算后应挂起: %v", err)
	}
	_, err = a.Resume(ctx, session.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	}))
	if err != nil || lookupCalls.Load() != 1 || exportCalls.Load() != 0 {
		t.Fatalf("续跑后 lookup=%d export=%d err=%v", lookupCalls.Load(), exportCalls.Load(), err)
	}
	newModel := &boundRecordingModel{script: func(n int) *schema.Message {
		switch n {
		case 0:
			return schema.AssistantToolCallMessage([]schema.ToolCall{search("new-export", "new task")})
		case 1:
			return oneCall("inventory_export", "{}")
		default:
			return schema.AssistantMessage("done")
		}
	}}
	_, err = a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出"), agent_v2.WithRunModel(newModel))
	if err != nil || exportCalls.Load() != 1 || slices.Contains(newModel.boundAt(0), "inventory_lookup") {
		t.Fatalf("新任务应从空解锁集开始并可解锁导出: export=%d err=%v", exportCalls.Load(), err)
	}
}
