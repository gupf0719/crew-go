package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func mwAgent(t *testing.T, tk *tool.Toolkit, mws ...agent_v2.ToolMiddleware) *agent_v2.BasicAgent {
	opts := []agent_v2.Option{
		agent_v2.WithName("bot"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "biz", Arguments: "{}"}}}),
			schema.AssistantMessage("好了"),
		})),
	}
	if len(mws) > 0 {
		opts = append(opts, agent_v2.WithToolMiddleware(mws...))
	}
	return agent_v2.NewBasicAgent(opts...)
}

func bizTool(runs *int) tool.Tool {
	return tool.NewFuncTool("biz", "干活", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			*runs++
			return tool.Success("真结果"), nil
		})
}

// 工具中间件的五条契约，每条一格。
//
// # 这个 seam 存在的理由
//
// 业务想包装工具，Go 里最自然的写法是嵌入 `tool.Tool` 再重写 `Call`——但**可选接口不穿过
// 嵌入**：`StreamableTool`（流式）和 `Categorizer`（trace span 的组件类型）会静默丢掉，
// 框架靠类型断言识别它们，嵌入出来的新类型不实现。中间件包的是**调用**不是对象，
// 那两个接口结构上碰不到（见 TestToolMiddleware_KeepsOptionalInterfaces）。
func TestToolMiddleware_Contracts(t *testing.T) {
	// ① 顺序：先注册的最外
	t.Run("先注册的最外", func(t *testing.T) {
		var order []string
		var runs int
		tk := tool.NewToolkit()
		tk.Register(bizTool(&runs))
		mk := func(tag string) agent_v2.ToolMiddleware {
			return func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
					order = append(order, "→"+tag)
					r, e := next(ctx, n, a)
					order = append(order, tag+"→")
					return r, e
				}
			}
		}
		ag := mwAgent(t, tk, mk("A"), mk("B"))
		_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
		got := strings.Join(order, " ")
		t.Logf("  %s", got)
		if got != "→A →B B→ A→" {
			t.Errorf("先注册应最外，实际 %q", got)
		}
	})

	// ② 短路：不调 next，工具不跑
	t.Run("短路", func(t *testing.T) {
		var runs int
		tk := tool.NewToolkit()
		tk.Register(bizTool(&runs))
		ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(context.Context, string, map[string]any) (*tool.Result, error) {
				return tool.Success("缓存命中"), nil
			}
		})
		ev, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
		t.Logf("  工具跑了 %d 次  err=%v  final=%q", runs, err, agent_v2.FinalMessage(ev).Content)
		if runs != 0 {
			t.Errorf("短路后工具不该跑，实际 %d 次", runs)
		}
	})

	// ③ 框架工具不过链
	t.Run("框架工具不过链", func(t *testing.T) {
		var seen []string
		tk := tool.NewToolkit()
		tk.Register(agent_v2.NewAskUserTool())
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("bot"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
			agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
				return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
					seen = append(seen, n)
					return next(ctx, n, a)
				}
			}),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: "tc1", Type: "function",
					Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"?"}`}}}),
				schema.AssistantMessage("ok"),
			})))
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, _ = ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		t.Logf("  中间件看到的工具 = %v（应为空）", seen)
		if len(seen) != 0 {
			t.Errorf("框架工具不该过链，实际看到 %v", seen)
		}
	})

	// ④ 吞掉中断会被挡住
	t.Run("吞中断被挡", func(t *testing.T) {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("biz", "问", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
				return nil, err
			}))
		ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
				if _, err := next(ctx, n, a); err != nil {
					return tool.Success("我把错吞了"), nil // ← 业务最容易写的那句
				}
				return tool.Success("ok"), nil
			}
		})
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		t.Logf("  err=%v", err)
		if err == nil || !strings.Contains(err.Error(), "绝不能吞") {
			t.Errorf("吞中断该被挡住，实际 err=%v", err)
		}
	})

	// ⑤ 吞掉**终局失败**同样被挡住
	//
	// ⚠️ 这条不是 ④ 的重复：这道 guard 的集合从「控制流信号」扩成了
	// [compose_v2.MustPropagate]（多了 `ErrRunAborted` / `ErrCheckpointNotResumable`），
	// 而**扩了集合不补用例，新成员就是零覆盖**——本仓规约「每个借用点必须有一个 N=2 的
	// 用例」正是为此。中断那条走的是 `IsControlFlowSignal` 那一档，换掉判据它照样绿。
	t.Run("吞终局失败被挡", func(t *testing.T) {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("biz", "违反协议", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return nil, fmt.Errorf("业务违反协议: %w", compose_v2.ErrRunAborted)
			}))
		ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
				if _, err := next(ctx, n, a); err != nil {
					return tool.Success("我把错吞了"), nil
				}
				return tool.Success("ok"), nil
			}
		})
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		t.Logf("  err=%v", err)
		if err == nil || !strings.Contains(err.Error(), "绝不能吞") {
			t.Errorf("吞终局失败该被挡住，实际 err=%v", err)
		}
	})

	// ⑤ 中断原样传出去不受影响
	t.Run("中断原样传出", func(t *testing.T) {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("biz", "问", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
				return nil, err
			}))
		var mu sync.Mutex
		ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
				mu.Lock()
				defer mu.Unlock()
				return next(ctx, n, a) // 老实传
			}
		})
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		ie, isInt := compose_v2.IsInterrupt(err)
		t.Logf("  IsInterrupt=%v  q=%q", isInt, func() string {
			if ie != nil && ie.Query != nil {
				return ie.Query.Question
			}
			return ""
		}())
		if !isInt {
			t.Errorf("中断该原样传出，实际 %v", err)
		}
	})
}

// richTool 同时实现两个可选接口
type richTool struct{ tool.Tool }

func (richTool) StreamCall(context.Context, map[string]any) (*schema.StreamReader[*tool.Result], error) {
	return nil, nil
}
func (richTool) Category() string { return tool.CategoryRetrieval }

// TestToolMiddleware_KeepsOptionalInterfaces 挂中间件不影响工具的可选接口。
//
// 这是这个 seam 存在的**全部理由**：换成「嵌入 tool.Tool 包一层」的写法，实测
// `可流式 true→false`、`有组件类型 true→false`，而且没有任何报错——流式降级、
// trace span 的组件类型丢失，全是静默的。
func TestToolMiddleware_KeepsOptionalInterfaces(t *testing.T) {
	base := tool.NewFuncTool("biz", "干活", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil })
	rich := richTool{Tool: base}
	tk := tool.NewToolkit()
	tk.Register(rich)

	var saw bool
	ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			saw = true
			return next(ctx, n, a)
		}
	})
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !saw {
		t.Fatal("中间件没被调到——这一格什么都没验")
	}
	// 承重：注册进 toolkit 的那个对象仍然实现两个可选接口
	got := tk.Get("biz")
	if _, ok := got.(tool.StreamableTool); !ok {
		t.Error("挂中间件后工具不再是 StreamableTool——流式会静默降级")
	}
	if _, ok := got.(tool.Categorizer); !ok {
		t.Error("挂中间件后工具不再是 Categorizer——trace span 的组件类型会丢")
	}
}

// TestToolMiddleware_NextOnlyOnce 调第二次 next 必须被拒。
//
// 这条跟「吞掉中断」同类：业务一句 `for i := 0; i < 3; i++ { next(...) }`（最自然的重试
// 写法）就能触发，而后果是静默的——中断身份含一维「本次调用里第几次问」，读一次 tick
// 一次。实测调两次拿到两个身份，用户答后一个；续跑时第一次 attempt 又拿回前一个、又中断，
// **永不收敛**。
func TestToolMiddleware_NextOnlyOnce(t *testing.T) {
	var runs int
	tk := tool.NewToolkit()
	tk.Register(bizTool(&runs))
	ag := mwAgent(t, tk, func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			_, _ = next(ctx, n, a) // 业务想「重试」
			return next(ctx, n, a)
		}
	})
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err == nil || !strings.Contains(err.Error(), "至多一次") {
		t.Fatalf("调两次 next 该被拒，实际 err=%v（工具跑了 %d 次）", err, runs)
	}
	t.Logf("  被拒: %v", err)
}

// TestToolMiddleware_OwnTimeoutDegradeNotAccused 钉住**中间件自带超时 + 降级不被诬告**。
//
// ⚠️ audit 实测坐实的先存缺陷：guard 判「这个错能不能吞」时用的是**链内最深处**递下来的
// ctx。中间件自己 `context.WithTimeout` 再调 next（`WithToolTimeout` 出现之前做单工具
// 超时的唯一写法）⟹ 派生 ctx 到期 ⟹ 按那个 ctx 判「任何错都必传」⟹ 中间件把超时转
// tool.Failure 的**正常降级**被报成「吞掉了绝不能吞的错误」，整个节点挂。
//
// 派生 ctx 之死 ≠ 调用方之死。判据 ctx 换成进链之前的调用方 ctx 之后，这条必须绿。
func TestToolMiddleware_OwnTimeoutDegradeNotAccused(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("slow", "慢工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			select {
			case <-time.After(5 * time.Second):
				return tool.Success("慢慢跑完了"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))
	mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			c, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
			defer cancel()
			res, err := next(c, n, a)
			if err != nil {
				return tool.Failure(tool.ErrCodeInternal, "工具超时，已降级"), nil
			}
			return res, nil
		}
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolMiddleware(mw),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("slow"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("中间件的正常降级被诬告、整个节点挂了: %v", err)
	}
	if fm := agent_v2.FinalMessage(evs); fm == nil || fm.Content != "final" {
		t.Errorf("降级后该继续 ReAct 到正常收尾，实际 final=%+v", fm)
	}
}

// TestToolMiddleware_CallerCancelBizErrorMustPropagate 钉住**调用方真喊停时，
// 业务错也必须穿过中间件**——guard 的 ctx 档在这一侧的正向收益（audit 指出它此前零哨兵）。
//
// 调用方 ctx 已取消时，「换条路再试」没有意义，任何错都该停——中间件把它吞成降级结果
// 会让 Run 顶着死 ctx 继续跑。
func TestToolMiddleware_CallerCancelBizErrorMustPropagate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("biz", "干活", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			cancel() // 模拟：工具执行期间调用方喊停，工具随后返一个普通业务错
			return nil, errors.New("下游拒了")
		}))
	mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(c context.Context, n string, a map[string]any) (*tool.Result, error) {
			if _, err := next(c, n, a); err != nil {
				return tool.Success("我把错吞了"), nil // 业务最容易写的那句
			}
			return tool.Success("ok"), nil
		}
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolMiddleware(mw),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("biz"), schema.AssistantMessage("final")})),
	)
	runCtx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(runCtx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "绝不能吞") {
		t.Errorf("调用方已取消，中间件吞业务错该被挡住，实际 err=%v", err)
	}
}
