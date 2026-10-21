package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// ToolEndpoint 是一次工具调用。中间件拿到它当作「继续往下走」。
type ToolEndpoint func(ctx context.Context, name string, args map[string]any) (*tool.Result, error)

// ToolMiddleware 包住一次工具调用——观测 / 短路 / 挂起，业务不碰 Tool 对象。
//
// # 为什么不是「包装 tool.Tool」
//
// Go 的可选接口**不穿过嵌入**：业务写 `struct{ tool.Tool }` 再重写 `Call`，
// `StreamableTool`（流式）和 `Categorizer`（trace span 的组件类型）会**静默丢掉**——
// 框架靠类型断言识别它们，嵌入出来的新类型不实现。实测：`可流式 true→false`、
// `有组件类型 true→false`，没有任何报错。
//
// 中间件包的是**调用**不是对象，所以那两个接口结构上碰不到。
//
// # 契约
//
//  1. **`next` 至多调一次。** 调两次 = 同一个提问换两个身份（中断身份含一维「本次调用
//     里第几次问」，读一次就 tick 一次）——实测重试一轮后同一个问题拿到新 ID，用户答过的
//     那条永远配不上、会被重新问一遍。要重试请在工具体内做，只重试不提问的那段；重试的
//     是嵌套的子 agent 调用时用 `compose_v2.RunWithRetry` 包住它（手写循环每次都算新调用、
//     子 agent 从头跑，见 `RunSubAgent`）。
//  2. **框架自带的工具不过中间件。** 它们的真产出不在 tool_result 里（`transfer_to_agent`
//     的决定是交给上层的上行信号、`tool_search` 写的是揭示集），短路掉是**静默失败**：err=nil、
//     tool_result 齐全，只是转交没发生。同一道题 memo 判过一次，这里给同样的答案。
//  3. **持状态的中间件必须 thread-safe。** 一轮 N≥2 个工具调用时，同一条链在 N 个
//     goroutine 里并发跑。
//  4. **里面调 `compose_v2.Ask` 要么恒调要么恒不调，别按条件调。** 条件调会移动工具体的
//     中断身份（同一维计数器），两次续跑之间条件一变，工具体的 ID 就漂了、答案静默错配。
//  5. **改 args 不会重新触发审批**——审批排在中间件外面。要改参数用
//     `hook.Hooks.OnToolStart`，那里是消费档且在审批之前。
//  6. **交给 `next` 的 ctx 从收到的那个派生。** 框架按采纳钩子返回的 ctx 的规则接住它
//     （`compose_v2.EnterHook`，规则同 CallHook）：派生的，你推的值、调用方可配的（软取消、环境）、
//     超时都生效；外来的（存下来的、`context.Background()`）只借你推的业务值，工具体照样在这次
//     调用的位置上、按这次请求的环境与取消跑。
//
// # 顺序
//
//	memo 命中 → 渐进披露 gate → 审批 → [ 中间件链 ] → 工具
//
// 先注册的在最外。中间件在 memo / 审批**里面**是有意的：在外面的话，一个计数中间件会在
// 每次续跑重复计数、一个会提问的中间件会对**已经跑完的**工具重新问人，而那正是 memo
// 存在的理由。
type ToolMiddleware func(next ToolEndpoint) ToolEndpoint

// chainToolMiddlewares 把链烘焙成一个 endpoint。先注册的最外。
func chainToolMiddlewares(mws []ToolMiddleware, final ToolEndpoint) ToolEndpoint {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		final = mws[i](final)
	}
	return final
}

// guardControlFlow 挡住「中间件把中断吞了」。
//
// 业务写 `if err != nil { return fallback, nil }` 这一句就能把 HITL 吞掉，而且 err=nil、
// tool_result 齐全——**静默**。同款缺陷在外部框架里真实存在（它们的中断是异常，新加一层
// catch 忘了豁免控制流就中招，实测：中断被转成 error tool_result 喂回 LLM，还附一句
// "Please fix your mistakes"）。
//
// 我们的中断是 error 值不是异常，所以框架看得见——记下内层有没有返过控制流信号，链返回后
// 若「返了成功但内层其实中断了」就把原信号顶回去。
// callerCtx 是**进入中间件链之前、也在工具超时派生之前**的调用方 ctx——判「这个错能不能
// 吞」时 ctx 那一档问的就是它。
//
// ⚠️ **这里一度捕获链内最深处递下来的 ctx（audit 实测坐实是错的）**：中间件自己
// `context.WithTimeout` 再调 next 是合法模式（`WithToolTimeout` 出现之前做单工具超时的
// 唯一写法），它派生的 ctx 到期 ⟹ 最深处 ctx.Err() != nil ⟹ 按那个 ctx 判，中间件的
// **正常降级**（超时转 tool.Failure）被诬告成「吞掉了绝不能吞的错误」，整个节点挂掉。
// 派生 ctx 之死 ≠ 调用方之死——同一族错误第三次出现，前两次在谓词内部
// （[compose_v2.IsRetryFutile] / [compose_v2.MustPropagate] 的 ctx 档），这次是喂给
// 谓词的**实参**拿错了归属。
func guardControlFlow(callerCtx context.Context, inner ToolEndpoint) (ToolEndpoint, func(*tool.Result, error) (*tool.Result, error)) {
	var swallowed error
	var calls int
	wrapped := func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
		// **第二次调用直接拒**——不是洁癖，是它静默毁 HITL：中断身份含一维「本次调用里
		// 第几次问」，读一次 tick 一次。实测调两次拿到 b511041b… / b511051b…，用户答的是
		// 后一个；续跑时第一次 attempt 又拿回前一个身份、又中断 ⟹ 永不收敛。
		//
		// 只在**这一层**计数（每个 tool_call 一份 guard），所以并发的兄弟调用互不影响。
		calls++
		if calls > 1 {
			return nil, fmt.Errorf("tool middleware 调了 %d 次 next——契约是至多一次。"+
				"每多调一次，工具里的提问就换一个身份，用户答过的那条永远配不上（实测永不收敛）。"+
				"要重试请在工具体内做（重试嵌套的子 agent 调用时用 compose_v2.RunWithRetry 包住）", calls)
		}
		res, err := inner(ctx, name, args)
		// **判据 ctx 用 callerCtx 不用链内的 ctx**：中断 / 终局失败 / 坏存档几档跟 ctx
		// 无关照旧；ctx 档变成「**调用方**喊停时任何错都必传」——中间件自己派生的超时
		// （链内 ctx 死、callerCtx 活）不再触发记录，它随后的降级是合法的。
		if err != nil && compose_v2.MustPropagate(callerCtx, err) {
			swallowed = err
		}
		return res, err
	}
	check := func(res *tool.Result, err error) (*tool.Result, error) {
		if swallowed == nil {
			return res, err
		}
		if err != nil && compose_v2.MustPropagate(callerCtx, err) {
			return res, err // 原样传出去了，正常
		}
		return nil, fmt.Errorf("tool middleware 吞掉了一个绝不能吞的错误（中断 / 取消 / "+
			"终局失败 / 坏存档）——它必须原样返回，否则挂起永远等不到答案、"+
			"协议违规会被当成普通工具失败喂回模型: %w", swallowed)
	}
	return wrapped, check
}
