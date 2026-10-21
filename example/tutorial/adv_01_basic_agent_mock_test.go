package tutorial_test

// adv_01 mock 镜像：BasicAgent 多 tool_call 电商客服。
// 验证 ReAct 多轮 tool_call 路径 + Hooks 触发计数。

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

func TestAdv_01_BasicAgent_Mock(t *testing.T) {
	queryOrder := tool.NewFuncTool(
		"query_order",
		"根据订单 ID 查询订单状态、商品名称和取消原因",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"type": "string"},
			},
			"required": []string{"order_id"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			b, _ := json.Marshal(map[string]any{
				"status": "已取消", "product": "无线键盘", "cancel_reason": "仓库库存不足",
			})
			_ = args
			return tool.Success(string(b)), nil
		},
	)
	checkInventory := tool.NewFuncTool(
		"check_inventory", "查询库存",
		map[string]any{"type": "object", "properties": map[string]any{
			"product_name": map[string]any{"type": "string"},
		}, "required": []string{"product_name"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("无线键盘 暂时缺货，预计 7 个工作日补货"), nil
		},
	)
	submitRefund := tool.NewFuncTool(
		"submit_refund", "提交退款",
		map[string]any{"type": "object", "properties": map[string]any{
			"order_id": map[string]any{"type": "string"},
			"reason":   map[string]any{"type": "string"},
		}, "required": []string{"order_id", "reason"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("退款已受理：3~5 个工作日退回"), nil
		},
	)
	tk := tool.NewToolkit()
	tk.RegisterAll(queryOrder, checkInventory, submitRefund)

	// 脚本化 mock：4 轮序列
	// Round 0: query_order(ORD-001234)
	// Round 1: check_inventory(无线键盘)
	// Round 2: submit_refund(...)
	// Round 3: 最终答复
	round := int32(0)
	stub := &stubModel{
		reply: func(_, _ string, _ []*schema.Message) *schema.Message {
			r := atomic.AddInt32(&round, 1) - 1
			switch r {
			case 0:
				return schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: "tc1", Type: "function",
					Function: schema.FunctionCall{Name: "query_order", Arguments: `{"order_id":"ORD-001234"}`},
				}})
			case 1:
				return schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: "tc2", Type: "function",
					Function: schema.FunctionCall{Name: "check_inventory", Arguments: `{"product_name":"无线键盘"}`},
				}})
			case 2:
				return schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: "tc3", Type: "function",
					Function: schema.FunctionCall{Name: "submit_refund", Arguments: `{"order_id":"ORD-001234","reason":"缺货取消"}`},
				}})
			default:
				return schema.AssistantMessage("订单 ORD-001234 因仓库无线键盘缺货已取消，退款已受理，3~5 个工作日到账。")
			}
		},
	}

	th := newTimingHookMock(t)
	ah := &auditHookMock{}
	chained := hook.NewChainHooks(th, ah)
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("customer_service"),
		agent_v2.WithSystemPrompt("电商客服"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(chained),
		agent_v2.WithMaxRounds(8),
	)

	events, err := a.Run(ctx60sMock(t), nil,
		schema.UserMessage("我的订单 ORD-001234 被取消了，能帮我查一下原因并自动申请退款吗？"))
	if err != nil {
		t.Fatalf("BasicAgent 运行失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("FinalMessage nil")
	}
	t.Logf("模型 %d 次, 工具 %d 次", th.modelCalls, th.toolStarts)
	t.Logf("审计: %v", ah.snapshot())
	t.Logf("最终: %s", out.Content)

	if th.toolStarts != 3 {
		t.Errorf("期望 3 次工具调用（query/check/refund），实际 %d", th.toolStarts)
	}
	if th.modelCalls < 4 {
		t.Errorf("期望至少 4 次 LLM 调用，实际 %d", th.modelCalls)
	}
	if len(ah.snapshot()) != 3 {
		t.Errorf("审计应 3 条，实际 %d", len(ah.snapshot()))
	}
}

// timingHookMock / auditHookMock：mock 版 hook（adv_helpers 的同名类型在
// integration tag 后看不到）。简化版，只记 model/tool 调用计数 + 审计。
type timingHookMock struct {
	hook.NoOpHooks
	t          *testing.T
	modelCalls int32
	toolStarts int32
}

func newTimingHookMock(t *testing.T) *timingHookMock { return &timingHookMock{t: t} }

func (h *timingHookMock) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	atomic.AddInt32(&h.modelCalls, 1)
	return ctx, nil
}
func (h *timingHookMock) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	atomic.AddInt32(&h.toolStarts, 1)
	return ctx, nil
}

type auditHookMock struct {
	hook.NoOpHooks
	mu      sync.Mutex
	entries []string
}

func (h *auditHookMock) OnToolEnd(ctx context.Context, info *hook.AgentInfo, output *schema.ToolOutput) (context.Context, error) {
	if output == nil || output.Result == nil {
		return ctx, nil
	}
	h.mu.Lock()
	h.entries = append(h.entries, fmt.Sprintf("[%s] → %s", info.Name, output.Result.Output))
	h.mu.Unlock()
	return ctx, nil
}
func (h *auditHookMock) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]string, len(h.entries))
	copy(cp, h.entries)
	return cp
}
