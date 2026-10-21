// tool_middleware_demo 演示 WithToolMiddleware 的四条契约（mock 模型，不需要 API key）：
//
//	[1] 观测：中间件包住每次业务工具调用，先注册的在最外
//	[2] 短路：不调 next 直接给结果，工具体一次都不跑
//	[3] 吞掉中断会被框架挡住：业务写 `if err != nil { return fallback, nil }` 这一句
//	    把 HITL 吞掉时，框架不让它静默成功——把原中断顶回给调用方（IsInterrupt 仍为 true，
//	    Resume 照样闭环），并在错误正文里指名中间件吞了它
//	[4] 老实传：中断原样传出去 → 调用方拿到 InterruptError → Resume 答完 → 工具收尾
//	[5] 框架自带的工具（ask_user 等）不过中间件
//
// 跑：go run ./example/harness/tool_middleware_demo
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// scriptedModel 是脚本化 LLM：本轮已有 tool_result 就收尾，否则发一次指定工具的 tool_call。
// 按内容路由而不是按调用序号——Resume 时 model 节点不重跑，序号会错位。
type scriptedModel struct {
	toolName string
	args     string
}

func (m *scriptedModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("[mock] 结果：" + msgs[len(msgs)-1].Content), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc_1", Type: "function",
		Function: schema.FunctionCall{Name: m.toolName, Arguments: m.args},
	}}), nil
}

func (m *scriptedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *scriptedModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// priceTool 是带副作用计数的业务工具；runs 记它的函数体真正跑了几次。
func priceTool(runs *int) tool.Tool {
	return tool.NewFuncTool("get_price", "查商品价格。参数 sku。",
		map[string]any{"type": "object", "properties": map[string]any{"sku": map[string]any{"type": "string"}}},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			*runs++
			return tool.Success(fmt.Sprintf("%v 售价 ¥238.00", args["sku"])), nil
		})
}

// approvalTool 在执行副作用之前先问人——问的动作走 compose_v2.Ask（算 ID / 查答案 / 没答案才抛中断）。
func approvalTool(shipped *int) tool.Tool {
	return tool.NewFuncTool("ship_order", "发货。参数 order。",
		map[string]any{"type": "object", "properties": map[string]any{"order": map[string]any{"type": "string"}}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("确认给订单 %v 发货？", args["order"])})
			if err != nil {
				return nil, err // 中断：原样返回，框架据此存档挂起
			}
			*shipped++
			return tool.Success(fmt.Sprintf("订单 %v 已发货（审批答复：%s）", args["order"], ans.Content)), nil
		})
}

func newAgent(tk *tool.Toolkit, m model.ChatModel, mws ...agent_v2.ToolMiddleware) *agent_v2.BasicAgent {
	opts := []agent_v2.Option{
		agent_v2.WithName("mw_demo"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4),
	}
	if len(mws) > 0 {
		opts = append(opts, agent_v2.WithToolMiddleware(mws...))
	}
	return agent_v2.NewBasicAgent(opts...)
}

func finalText(events []*agent_v2.AgentEvent) string {
	if f := agent_v2.FinalMessage(events); f != nil {
		return f.Content
	}
	return ""
}

func main() {
	ctx := context.Background()
	pass := true
	check := func(cond bool, name string) {
		if cond {
			fmt.Printf("  ✓ %s\n", name)
		} else {
			fmt.Printf("  ✗ %s\n", name)
			pass = false
		}
	}

	// ── [1] 观测：先注册的在最外 ──
	fmt.Println("=== [1] 观测中间件：计时 + 顺序（先注册的最外）===")
	{
		var mu sync.Mutex // 一轮 N≥2 个工具调用时中间件在 N 个 goroutine 里并发跑——持状态要加锁
		var order []string
		var runs int
		tag := func(name string) agent_v2.ToolMiddleware {
			return func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, toolName string, args map[string]any) (*tool.Result, error) {
					start := time.Now()
					mu.Lock()
					order = append(order, name+"→")
					mu.Unlock()
					res, err := next(ctx, toolName, args)
					mu.Lock()
					order = append(order, "←"+name)
					mu.Unlock()
					fmt.Printf("    [%s] %s 耗时 %s\n", name, toolName, time.Since(start).Round(time.Microsecond))
					return res, err
				}
			}
		}
		tk := tool.NewToolkit()
		tk.Register(priceTool(&runs))
		ag := newAgent(tk, &scriptedModel{toolName: "get_price", args: `{"sku":"A-1"}`}, tag("outer"), tag("inner"))
		ev, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A-1 多少钱"))
		got := strings.Join(order, " ")
		fmt.Printf("  顺序: %s\n  final: %s\n", got, finalText(ev))
		check(err == nil && got == "outer→ inner→ ←inner ←outer", "先注册的在最外，链完整往返")
		check(runs == 1 && strings.Contains(finalText(ev), "238"), "工具体跑了 1 次，结果回到模型")
	}

	// ── [2] 短路：不调 next ──
	fmt.Println("\n=== [2] 短路：缓存命中直接给结果，工具体不跑 ===")
	{
		var runs int
		tk := tool.NewToolkit()
		tk.Register(priceTool(&runs))
		cache := map[string]*tool.Result{"get_price": tool.Success("A-1 售价 ¥199.00（缓存）")}
		ag := newAgent(tk, &scriptedModel{toolName: "get_price", args: `{"sku":"A-1"}`},
			func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, toolName string, args map[string]any) (*tool.Result, error) {
					if hit, ok := cache[toolName]; ok {
						return hit, nil // 短路。⚠️ 它会被记进 memo 并在续跑时重放，别返「本次特有」的东西
					}
					return next(ctx, toolName, args)
				}
			})
		ev, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A-1 多少钱"))
		fmt.Printf("  工具体跑了 %d 次  final: %s\n", runs, finalText(ev))
		check(err == nil && runs == 0 && strings.Contains(finalText(ev), "缓存"), "短路后工具体 0 次，模型拿到缓存值")
	}

	// 中断要存档，下面三个场景都带 checkpointer
	cpCtx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())

	// ── [3] 吞掉中断会被框架挡住 ──
	fmt.Println("\n=== [3] 吞中断被挡：`if err != nil { return fallback, nil }` 这一句不会静默通过 ===")
	{
		var shipped int
		tk := tool.NewToolkit()
		tk.Register(approvalTool(&shipped))
		ag := newAgent(tk, &scriptedModel{toolName: "ship_order", args: `{"order":"O-77"}`},
			func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, toolName string, args map[string]any) (*tool.Result, error) {
					res, err := next(ctx, toolName, args)
					if err != nil {
						return tool.Success("工具出错了，先当成功处理"), nil // ← 业务最容易写的那句
					}
					return res, nil
				}
			})
		sess := agent_v2.NewRunSession()
		_, err := ag.Run(cpCtx, sess, schema.UserMessage("给 O-77 发货"))
		fmt.Printf("  Run err: %v\n", err)
		ie, isInt := compose_v2.IsInterrupt(err)
		check(err != nil && strings.Contains(err.Error(), "绝不能吞"), "错误正文指名：中间件吞掉了一个绝不能吞的错误")
		check(isInt, "原中断被顶回给调用方（IsInterrupt=true）——没有变成「工具成功」")
		check(shipped == 0, "副作用没发生（发货 0 次）")
		if isInt {
			// 中断没丢 ⟹ 挂起照样存了档，答完照样收尾。吞的那一句只是被框架绕过去了。
			q := ie.AllQueries()[0]
			ev, rerr := ag.Resume(cpCtx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				q.InterruptID: schema.UserMessage("批准"),
			}))
			fmt.Printf("  Resume final: %s\n", finalText(ev))
			check(rerr == nil && shipped == 1, "Resume 照样闭环：发货 1 次")
		}
	}

	// ── [4] 老实传：中断 → Resume → 收尾 ──
	fmt.Println("\n=== [4] 老实传：中断原样传出 → Resume 答完 → 工具收尾 ===")
	{
		var shipped int
		var seen []string
		var mu sync.Mutex
		tk := tool.NewToolkit()
		tk.Register(approvalTool(&shipped))
		ag := newAgent(tk, &scriptedModel{toolName: "ship_order", args: `{"order":"O-77"}`},
			func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, toolName string, args map[string]any) (*tool.Result, error) {
					mu.Lock()
					seen = append(seen, toolName)
					mu.Unlock()
					return next(ctx, toolName, args) // 原样返回，含中断
				}
			})
		sess := agent_v2.NewRunSession()
		_, err := ag.Run(cpCtx, sess, schema.UserMessage("给 O-77 发货"))
		ie, isInt := compose_v2.IsInterrupt(err)
		check(isInt && shipped == 0, "Run 拿到 InterruptError，副作用未发生")
		if !isInt {
			fmt.Printf("  Run err: %v\n", err)
		} else {
			q := ie.AllQueries()[0]
			fmt.Printf("  挂起提问: %q (id=%s…)\n", q.Question, q.InterruptID[:8])
			ev, rerr := ag.Resume(cpCtx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				q.InterruptID: schema.UserMessage("批准"),
			}))
			fmt.Printf("  Resume final: %s\n  中间件看到的调用: %v\n", finalText(ev), seen)
			check(rerr == nil && shipped == 1 && strings.Contains(finalText(ev), "批准"), "Resume 后发货 1 次，审批答复进了结果")
			check(len(seen) == 2, "中间件在 Run 与 Resume 各见到一次调用（工具体是二阶段：问 → 答后做）")
		}
	}

	// ── [5] 框架工具不过链 ──
	fmt.Println("\n=== [5] 框架自带工具（ask_user）不过中间件 ===")
	{
		var seen []string
		var mu sync.Mutex
		tk := tool.NewToolkit()
		tk.Register(agent_v2.NewAskUserTool())
		ag := newAgent(tk, &scriptedModel{toolName: agent_v2.AskUserToolName, args: `{"question":"要哪种规格？"}`},
			func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, toolName string, args map[string]any) (*tool.Result, error) {
					mu.Lock()
					seen = append(seen, toolName)
					mu.Unlock()
					return next(ctx, toolName, args)
				}
			})
		_, err := ag.Run(cpCtx, agent_v2.NewRunSession(), schema.UserMessage("下单"))
		_, isInt := compose_v2.IsInterrupt(err)
		fmt.Printf("  中间件看到的调用: %v（应为空）  IsInterrupt=%v\n", seen, isInt)
		check(isInt && len(seen) == 0, "ask_user 挂起了，但中间件一次都没被调")
	}

	fmt.Println()
	if !pass {
		fmt.Println("tool_middleware_demo FAIL")
		os.Exit(1)
	}
	fmt.Println("tool_middleware_demo PASS")
}
