//go:build integration

package tutorial_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// ──────────────────────────────────────────────────────────────────
// 1. BasicAgent（进阶）— 多工具电商客服
//
// timingHook / auditHook：Hook 是"框架回调"模式。
// 用户定义回调方法，框架（reactLoop）在 LLM 调用、工具调用等
// 生命周期节点自动触发对应方法——使用者只需通过
// agent_v2.WithHooks(h) 注册，无需在业务代码中手动调用。
// ──────────────────────────────────────────────────────────────────

func TestAdv_01_BasicAgent(t *testing.T) {
	provider := requireAPIKey(t)

	// queryOrder 模拟订单数据库查询
	queryOrder := tool.NewFuncTool(
		"query_order",
		"根据订单 ID 查询订单状态、商品名称和取消原因",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"type": "string", "description": "订单 ID，格式 ORD-XXXXXX"},
			},
			"required": []string{"order_id"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			orderID, _ := args["order_id"].(string)
			db := map[string]map[string]any{
				"ORD-001234": {"status": "已取消", "product": "无线键盘", "price": 299.0, "cancel_reason": "仓库库存不足"},
				"ORD-005678": {"status": "已发货", "product": "机械鼠标", "price": 199.0, "tracking": "SF1234567890"},
			}
			if order, ok := db[orderID]; ok {
				b, err := json.Marshal(order)
				if err != nil {
					return tool.Failure("marshal_error", err.Error()), nil
				}
				return tool.Success(string(b)), nil
			}
			return tool.Failure("not_found", fmt.Sprintf("订单 %s 不存在", orderID)), nil
		},
	)

	// checkInventory 查询库存
	checkInventory := tool.NewFuncTool(
		"check_inventory",
		"查询指定商品的当前库存数量及补货预期",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"product_name": map[string]any{"type": "string", "description": "商品名称"},
			},
			"required": []string{"product_name"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			name, _ := args["product_name"].(string)
			type stockInfo struct {
				qty     int
				restock string
			}
			stock := map[string]stockInfo{
				"无线键盘": {0, "预计 7 个工作日补货"},
				"机械鼠标": {47, ""},
			}
			if s, ok := stock[name]; ok {
				if s.qty == 0 {
					return tool.Success(fmt.Sprintf("%s 暂时缺货，%s", name, s.restock)), nil
				}
				return tool.Success(fmt.Sprintf("%s 有货，剩余 %d 件", name, s.qty)), nil
			}
			return tool.Success(name + " 库存信息暂未录入"), nil
		},
	)

	// submitRefund 提交退款申请（写操作）
	submitRefund := tool.NewFuncTool(
		"submit_refund",
		"为已取消订单提交退款申请，返回预计到账时间",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_id": map[string]any{"type": "string", "description": "订单 ID"},
				"reason":   map[string]any{"type": "string", "description": "退款原因"},
			},
			"required": []string{"order_id", "reason"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			orderID, _ := args["order_id"].(string)
			return tool.Success(
				fmt.Sprintf("退款已受理：订单 %s 款项将在 3~5 个工作日退回原支付方式", orderID),
			), nil
		},
	)

	tk := tool.NewToolkit()
	tk.RegisterAll(queryOrder, checkInventory, submitRefund)

	// 串联 timing 统计 + audit 审计两个 Hook
	th := newTimingHook(t)
	ah := &auditHook{}
	chainedHook := hook.NewChainHooks(th, ah)

	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("customer_service"),
		agent_v2.WithSystemPrompt(
			"你是专业的电商客服机器人。处理用户问题时必须：\n"+
				"1. 先调用 query_order 查询订单状态\n"+
				"2. 如果是缺货取消，调用 check_inventory 确认库存\n"+
				"3. 对于取消订单，自动调用 submit_refund 提交退款\n"+
				"所有步骤必须通过工具完成，不得凭空回答。最终告知处理结果和预期时间。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(chainedHook),
		agent_v2.WithMaxRounds(8),
	)

	events, err := a.Run(ctx60s(t), nil,
		schema.UserMessage("我的订单 ORD-001234 被取消了，能帮我查一下原因并自动申请退款吗？"))
	if err != nil {
		t.Fatalf("BasicAgent 运行失败: %v", err)
	}

	out := mustFinalMessageV2(t, events)
	t.Logf("模型调用: %d 次，工具调用: %d 次，输入 token: %d，输出 token: %d",
		th.modelCalls, th.toolStarts, th.totalInputTok, th.totalOutputTok)
	t.Logf("审计条目: %v", ah.snapshot())
	t.Logf("最终回复: %s", out.Content)

	if th.toolStarts == 0 {
		t.Error("期望至少调用一次工具")
	}
	if out.Content == "" {
		t.Error("输出不应为空")
	}
}
