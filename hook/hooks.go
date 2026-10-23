package hook

import (
	"context"
	"fmt"

	specModel "crew-go/modules/model"

	"crew-go/schema"
)

// ────────────────────────────────────────────────────────────
// 生命周期类型
// ────────────────────────────────────────────────────────────

// AgentInfo 为 hook 回调标识代理和当前轮次.
// RunPath 是 Go SDK 扩展, 其余字段与规范生成的 hook.AgentInfo 对齐.
type AgentInfo struct {
	Name    string
	Type    string // "base"、"sequential"、"parallel" 等
	Round   int32
	RunPath []string // 从根执行路径, 例如 ["sequential","weather_agent","base"]
}

// AgentInput 传递给 OnAgentStart.
//
// Messages 里的输入是这个 agent 自己那份（Run 入口拷过）：原地改它、换掉它，这个 agent 用的就是改过的，调用方手里
// 那条与兄弟 agent 拿到的不受影响。
//
// **改写 Messages 时关键约束 (R22 audit MED-1)**:
// 业务 hook 改写消息 (如 PII filter / content redaction) 时**必须保留所有
// Annotations**——含 framework 内部 marker (SkillBodyAnnotation /
// StoreInjectedAnnotation 等), 否则跨层不变量破裂:
//   - 4 层压缩跨层防护按 marker 识别——丢 marker 后 skill body 被当普通历史压扁
//   - 计数截断按 marker 认亲（带 marker 的 tool_result 要连同声明它的 assistant
//     一起保留）——丢 marker 会切出孤儿 tool_result，出口再被丢弃 → silent bug
//
// **正确改写模式**:
//
//	for i, m := range in.Messages {
//	    newM := *m // shallow clone 保 Annotations 指向同 map
//	    newM.Content = redactPII(m.Content)
//	    in.Messages[i] = &newM
//	}
type AgentInput struct {
	Messages []*schema.Message `json:"messages"`
	Extra    map[string]any    `json:"extra,omitempty"`
}

// AgentOutput 传递给 OnAgentEnd（观测终点）.
//
// **Messages 只读**：OnAgentEnd 在 Run/Resume 返回 events 后才触发（defer），
// 改写 Messages 不生效——见 Hooks 接口"消费语义"[只读]档.
type AgentOutput struct {
	Messages []*schema.Message `json:"messages"`
	Extra    map[string]any    `json:"extra,omitempty"`
}

// ────────────────────────────────────────────────────────────
// Hooks 接口 (ctx-based)
// ────────────────────────────────────────────────────────────

// Hooks 定义代理执行的生命周期回调.
//
// ─── ctx-based 协议 (audit² 改: 每个 hook 方法签名 OnStart(ctx)→ctx) ───
// 每个 hook 方法返 context.Context：链里交给下一个 hook，整条链返回之后框架接着用（哪几个方法的、
// 怎么用见下文「ctx 返回值」）。业务 hook 内可 `ctx = context.WithValue(ctx, spanKey{}, span)`
// 让后续 hook 拿到 (典型 OTel trace 模式: OnStart 内 `ctx, span := tracer.Start(ctx)`,
// OnEnd 内 ctx 取 span + span.End()). 返回的 ctx 要**从入参派生**。
//
// **业务无 ctx mutation 需求时**: 直接 `return ctx, nil` 一行即可.
//
// ─── panic 语义 ───
// 业务 hook panic = 业务 bug, framework **不做 per-hook isolation**（链里排在它后面的不再调）。框架在调用
// 钩子的地方兜住、不让 goroutine 崩：OnAgentStart / OnModel* / OnTool*（跑在工作之前或改得了结果）与
// OnError（调用已经失败）panic 转成这次调用的错误；OnAgentEnd 跑在工作之后、只作观测，调用成功时它 panic 就地兜住、
// 带栈记一条 ERROR，结果不变（转成错误的话调用方的重试 / 续跑会把已经做完的再做一次），调用失败时并到
// 那个错误上。业务想 panic-safe 自己在 hook 内 defer recover().
//
// ─── 消费语义（按字段分档，不是"所有 hook 一律可改"）───
// hook 入参是指针，但**改了是否生效取决于框架是否在该 hook 后消费该字段**。
// 业务实现 hook 前必看本表——可纯观测，也可改写，但改写仅在 [消费] 档字段生效。
//
// [消费] 框架读改写后的值喂下游，业务改写生效：
//
//	OnAgentStart                input.Messages —— 整个调用的原始输入（可做 PII filter / 模板包装；Resume 时为空）
//	OnModelStart                input.Preamble —— agent 前言（system prompt / 提示词模板渲染结果，含 skill 正文）；改内容 / 删条 / 整段换都生效
//	OnModelStart                input.Messages —— 被上下文压缩处置的那条对话流（⚠️ 它不是最终发出去的那份：四层压缩在本 hook 之后才跑）
//	OnModelEnd                  output.Message —— 替换本轮输出（写 channel + 前端 reset_stream event）。改写在这一轮第一次
//	                            处理完时定下：之后这次执行在提交之前作废（节点成功之后的钩子 Goexit、提交出错）、续跑时这一轮
//	                            不再处理——不调模型，OnModelStart / OnModelEnd 都不触发，交出定下的答复与它当时做出的决定。
//	                            模型答复之后、这一轮处理完之前停下的看原因：因为等人或被叫停（中断、取消）停下的，续跑沿用这次
//	                            调用的答复（不再调模型、不再带用量）把这一轮处理完——这一遍 ModelInput.Extra / ModelOutput.Extra
//	                            标着 ModelResponseReusedKey，按调用计数、计时、计费的钩子据此不再记一次（它们要排在会停下的
//	                            钩子前面）；钩子报错拒掉、把答复置空、panic 的，续跑重新调模型换一份，OnModelEnd 里对新答复的
//	                            提问换新 ID（见 agent_v2 的 modelCallRecord）
//	OnModelEndStream            返回值 reader  —— 以返回的 reader 替换流（不得为 nil：返回 nil 或报错都让这次模型调用失败）
//	OnToolStart                 input.Name / input.ArgumentsJSON —— 改写后喂业务工具调用（参数注入 / 工具重定向）；
//	                            只在这一批调用第一次交给工具执行时生效：之后的执行（中断续跑、失败续跑）沿用那一次定下的内容，
//	                            这两个字段的改写对它们不生效——用户答过的、审批批过的、已经执行过的都是那一份（钩子照样
//	                            触发，返回的 ctx、错误与中断照常算数；这一批处理完之后才作废的调用续跑时不再处理、钩子不触发，
//	                            见 OnToolEnd）；定下的内容随中断档、失败档落盘，每次执行都要新取的值
//	                            走钩子返回的 ctx 或工具中间件（见 OnToolStart 方法注释）
//	OnToolEnd                   output.Result.* —— 改写后写回 LLM 历史（结果脱敏 / 注入缓存），in-place 改 content 保留 message Annotations marker；
//	                            Success 同样算数：改成失败，这次调用做出的转交 / 路由决定作废、框架那件控制工具（transfer_to_agent /
//	                            route_to）不短路（业务 return-direct 工具照常以失败内容收尾）；改成成功则决定算数。Success 为假时模型看到的
//	                            一律是失败格式（没给 Error 就拿 Output 当说明；给了 Error 时内容按 (ErrorCode, Error) 渲染，只改 Output
//	                            不生效——要改失败结果给模型看的说明，比如脱敏，就改 Error）——⚠️ 整体替换
//	                            `out.Result = &schema.ToolCallResult{...}` 时记得填 Success：零值 false 按失败处理。final_output
//	                            被改成失败等同校验失败：回炉、计入 WithOutputSchema 的重试预算。改写在这一批第一次处理完时
//	                            定下：之后这次执行在提交之前作废（节点成功之后的钩子 Goexit、提交出错）、续跑时这几个调用不再
//	                            处理——工具不执行，OnToolStart / OnToolEnd 都不触发，交出定下的结果与钩子当时做出的决定；这一批
//	                            没处理完就停下的（同批别的调用中断、钩子报错），续跑时照常交给钩子处理
//	                            （BasicAgent 的 tools 节点在全部 OnToolEnd 之后才定这批调用的决定。同一批里决定被排在前面的调用盖掉的那几个，
//	                            结果在那时被换成一条「没有生效」的失败结果——钩子看到的不是最终进历史的那一条。钩子自己在 OnToolEnd 里做出的
//	                            转交 / 路由决定排在这批调用的决定之后：它是看过结果才做的，后者为准）
//	                            一批 tool_call 没整体成功（有调用中断或硬失败，或者某个调用的 OnToolEnd 钩子报错 / 中断）时，已经有
//	                            结局、还没收尾的调用照样各自触发，Extra 带 agent_v2.ToolBatchFailedExtraKey：跑完的交它的结果，硬失败的
//	                            交框架合成的失败结果（agent_v2.ToolCallErrCode）；挂起的（等人回答、等审批）不触发。这时这批的结果整体
//	                            作废，改写不被消费，钩子的错误与 panic 只记日志——给用户展示结果的钩子据这个标记跳过、等续跑那一次。续跑时
//	                            跑完的那几个从工具账重放、再触发一次，Extra 带 agent_v2.ToolReplayedExtraKey（改写照常消费）——按调用计数
//	                            或计时的钩子据它跳过。续跑时同一批的每个调用再触发一次 OnToolStart，ToolInput.Extra 带
//	                            agent_v2.ToolRedispatchedExtraKey——把调用转发给前端的钩子据它不再重开同一个调用
//
// [只读] 观测 / 统计 / 配对锚点字段，框架不消费，改写不生效（by design；tool/model 层是 observation-only）：
//
//	OnModelStart input.Tools —— 框架当前不填充（恒 nil）；动态增删工具不走 hook
//	OnModelStart input.Config —— 本次请求的模型名与按次参数（BasicAgent 按「按次 model.WithModel > 客户端 model.ModelNamer」填），观测用；发请求不读它
//	OnModelEnd   output.Usage —— LLM 返回的 token 统计，事实值
//	OnToolStart input.CallId / OnToolEnd output.CallId —— tool_call ↔ tool_result 配对锚点，改它破坏配对（OpenAI 422），仅供观测关联（此字段不应在 tool callback 路径被改写）
//	OnAgentEnd  output.Messages —— Run/Resume 已返回 events，此 hook 是事后通知（defer）
//	OnError     err.Code / err.Message —— 框架返回原始 error，CommError 仅快照
//
// ctx 返回值：链里每个 hook 返回的 ctx 交给下一个 hook（ChainHooks）；整条链返回之后，框架用开始一侧的返回值，
// **只用于它包住的那次调用**：
//
//	OnAgentStart     这次 Run / Resume（OnAgentEnd 收的也是它）
//	OnModelStart     这次模型调用（OnModelEnd 收的也是它）
//	OnModelEndStream 这次流式读取
//	OnToolStart(i)   第 i 个 tool_call 的执行——工具体、工具中间件、它调起的嵌套 agent（OnToolEnd(i) 收的也是它）；
//	                 同一批别的调用、别的调用的钩子都看不到
//
// 结束一侧（OnModelEnd / OnToolEnd / OnAgentEnd）与 OnError 的返回值不往后流。曾经 OnToolStart 的返回值赋回整批共用的
// ctx：b 的钩子收到 a 放的值，a 的工具体带着 b 的值在跑（实测）。怎么采纳（`compose_v2.CallHook`）：
//
//	框架的身份与位置（所在位置、session、任务代号、会话 ID、历史写入器、转交收件人……）—— 一律取框架的，推了也不算
//	返回的 ctx 从入参派生 —— 你推的值、调用方可配的（checkpointer、编排钩子、调用主体、store、软取消……）、
//	                         取消与截止时间都生效（从它之后才读 ctx 的地方起；本层钩子之前已定下的不变）
//	不是从入参派生的（存下来的、context.Background()）—— 只借你推的业务值；可配的与取消、截止时间
//	                         取这次请求的，你在那个 ctx 上加的超时不生效（首次出现时告警一次）
//
// 业务值框架分不出哪些是 span、哪些是凭证，照用钩子返回的那份：**别跨请求存 ctx**——存下的 ctx 会把
// 那个请求的凭证带进这一次。
//
// ⚠️ 链内部不做这层处理：链里前一个 hook 返回存下来的 ctx 时，后一个 hook 收到的就是它。
// observability 的钩子手动装进链时要排第一（`obs.Install` 已经这么装）。
//
// 触发点 / 频次：
//
//	OnAgentStart     agent_v2.{编排}.Run/Resume top-level（图执行前），整个调用 1 次
//	OnModelStart/End 紧贴 LLM 调用前后（modelNode）；OnModelEndStream 流式返回（modelFn 内）
//	OnToolStart/End  每个 tool 调用前后（toolsNode 串行）
//	OnAgentEnd       Run/Resume defer 1 次（ISSUE-82 单一所有权；HITL Run + Resume 闭环算 2 次）
//	OnError          任一阶段错误
type Hooks interface {
	OnAgentStart(ctx context.Context, info *AgentInfo, input *AgentInput) (context.Context, error)
	OnAgentEnd(ctx context.Context, info *AgentInfo, output *AgentOutput) context.Context
	OnModelStart(ctx context.Context, info *AgentInfo, input *schema.ModelInput) (context.Context, error)
	OnModelEnd(ctx context.Context, info *AgentInfo, output *schema.ModelOutput) (context.Context, error)
	// OnModelEndStream 允许 hook 包装或拦截流输出.
	//
	// 返回的 reader 只有交出 io.EOF 才算这条流正常结束；交出别的错误——包括关掉底层流之后读到的
	// schema.ErrStreamCanceled——这次模型调用按失败处理。要在半路截断、把已有内容当完整输出，
	// 就在自己的 Recv 里返回 io.EOF。这类失败默认可重试：按确定的规则掐流（如内容审核）时，配了
	// 重试的编排会重跑到次数用完，想让它不重试就在 Recv 里返回实现了 compose_v2.NonRetryable 的错误。
	OnModelEndStream(ctx context.Context, info *AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error)
	// OnToolStart 在**整批** tool_call 派发之前串行触发一次/个，位置在工具执行单位
	// **外面**——拿不到 memo、审批状态、批量合并。
	//
	// ⚠️ **别在这里向人提问**（`compose_v2.Ask`）。它能跑通，但绕过三样东西：
	// `toolCallMemo`（已跑成功的工具不重跑）、`approve_always`（别再问了）、
	// `MergeInterrupts`（一批多个问题一次问完）。
	//
	// 要在工具执行前拦人审批，用 `agent_v2.WithApprovalMode(ApprovalAsk)`
	// （配 `WithApprovalHeuristic` 定规则）——它跑在执行单位**里面**，三样都有。
	//
	// 要包装工具调用（观测 / 短路 / 挂起），用 `agent_v2.WithToolMiddleware`——它包的是
	// **调用**不是 Tool 对象，所以工具的可选接口（流式 / 组件类型）不会被静默丢掉。
	//
	// ⚠️ 改写只在这一批调用第一次交给工具执行时生效（BasicAgent 的 tools 节点，见 Hooks 接口 godoc 的
	// 消费表）：续跑时钩子照样触发、收到的是模型给的原样，但对 `Name` / `ArgumentsJSON` 的改写不生效，执行的
	// 是第一次那份——用户答过的问题、批过的审批、交给嵌套 agent（`agent_v2.AgentAsTool`）的输入续跑都不变。
	// 这一批处理完（OnToolEnd 之后）才作废的调用续跑时不再处理，本钩子也不触发。
	//
	// 改写就是改这次调用的内容：定下的那份随中断档、失败档落盘（续跑要按它执行）。所以**每次执行都要新取的值**
	// （凭证、短时令牌、trace_id）别写进参数——写进去会落盘，短时令牌还会被冻在第一次的值上。放钩子返回的 ctx
	// （只用于这次执行、不落盘），或用 `agent_v2.WithToolMiddleware` 在每次执行时注入（也不落盘；它改的参数
	// 不经审批）。
	//
	// 代价：改写让这次调用出错时，修好钩子再续跑也不生效——这一批按第一次的内容执行。恢复办法是放弃这个任务、
	// 换一个 RunID 重开（`agent_v2.RunSession` 的 `NextTask`）。
	OnToolStart(ctx context.Context, info *AgentInfo, input *schema.ToolInput) (context.Context, error)
	OnToolEnd(ctx context.Context, info *AgentInfo, output *schema.ToolOutput) (context.Context, error)
	OnError(ctx context.Context, info *AgentInfo, err *schema.CommError) context.Context
}

// ────────────────────────────────────────────────────────────
// NoOpHooks
// ────────────────────────────────────────────────────────────

// NoOpHooks 是 Hooks 的空实现, 3 个用途:
//  1. framework 默认值 (agent_v2.Config.Hooks 字段) 让 framework 调 .OnXxx 不 nil panic
//  2. 业务部分实现时嵌入 (`type myHook struct{ NoOpHooks }`, 只覆写关心的方法,
//     其他 7 method 自动走 NoOp 默认)
//  3. defensive return (业务 nil Observability 等场景返 NoOp 保 caller 安全)
type NoOpHooks struct{}

func (NoOpHooks) OnAgentStart(ctx context.Context, _ *AgentInfo, _ *AgentInput) (context.Context, error) {
	return ctx, nil
}
func (NoOpHooks) OnAgentEnd(ctx context.Context, _ *AgentInfo, _ *AgentOutput) context.Context {
	return ctx
}
func (NoOpHooks) OnModelStart(ctx context.Context, _ *AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	return ctx, nil
}
func (NoOpHooks) OnModelEnd(ctx context.Context, _ *AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	return ctx, nil
}
func (NoOpHooks) OnModelEndStream(ctx context.Context, _ *AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	return ctx, sr, nil
}
func (NoOpHooks) OnToolStart(ctx context.Context, _ *AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	return ctx, nil
}
func (NoOpHooks) OnToolEnd(ctx context.Context, _ *AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	return ctx, nil
}
func (NoOpHooks) OnError(ctx context.Context, _ *AgentInfo, _ *schema.CommError) context.Context {
	return ctx
}

// ────────────────────────────────────────────────────────────
// ChainHooks
// ────────────────────────────────────────────────────────────

// ChainHooks 串联 N 个 Hooks — 业务一次想挂多个独立 hook (typical: SSE adapter +
// audit logger + observability + billing).
//
// **ctx propagation**: hook 链式串 — 每个 hook 返的 ctx 给下一个 hook 用. 业务在
// hook A 内 push 的 span 让 hook B 拿到.
//
// **panic 语义**: 一个 hook panic = 业务 bug, 中断剩余 hook; 处置见包文档「panic 语义」.
//
// **错误路径的 ctx 语义**（8 个方法一致）: 链上任一 hook 返 err 时,
// ChainHooks 回**入参** ctx 而不是那个 hook 返的 newCtx. 理由: hook 返 err 即
// 声明本次失败, 把"自认失败但改了一半"的 ctx 传给 caller 会让 caller 的错误路径
// 拿到一个半成品——而 caller 的常规写法正是"错误路径用入参 ctx". 于是框架 caller
// 恒拿到稳定的"hook 链失败 ⟹ 回到调用前那个 ctx"。
type ChainHooks struct {
	hooks []Hooks
}

// NewChainHooks 创建 ChainHooks. 自动过滤 nil / typed nil 元素.
//
// 业务 `var h MyHooks; NewChainHooks(h, observer)` 时 h 是 typed nil → 静默过滤
// (与 untyped nil 等价 — caller 显式传 NoOp 而非 nil 时仍正常工作).
func NewChainHooks(hooks ...Hooks) *ChainHooks {
	filtered := make([]Hooks, 0, len(hooks))
	for _, h := range hooks {
		if isHooksNil(h) {
			continue
		}
		filtered = append(filtered, h)
	}
	return &ChainHooks{hooks: filtered}
}

func (c *ChainHooks) OnAgentStart(ctx context.Context, info *AgentInfo, input *AgentInput) (context.Context, error) {
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnAgentStart(ctx, info, input)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainHooks) OnAgentEnd(ctx context.Context, info *AgentInfo, output *AgentOutput) context.Context {
	for _, h := range c.hooks {
		ctx = h.OnAgentEnd(ctx, info, output)
	}
	return ctx
}

func (c *ChainHooks) OnModelStart(ctx context.Context, info *AgentInfo, input *schema.ModelInput) (context.Context, error) {
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnModelStart(ctx, info, input)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainHooks) OnModelEnd(ctx context.Context, info *AgentInfo, output *schema.ModelOutput) (context.Context, error) {
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnModelEnd(ctx, info, output)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainHooks) OnModelEndStream(ctx context.Context, info *AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	// wrappedStack 记录每次成功 wrap 后的 stream, 出错时逆序全部关闭,
	// 避免中间 wrapped stream 持有的资源泄漏.
	original := sr
	wrappedStack := make([]specModel.MessageStreamReader, 0, len(c.hooks))
	cleanup := func() {
		for i := len(wrappedStack) - 1; i >= 0; i-- {
			if wrappedStack[i] != nil {
				_ = wrappedStack[i].Close()
			}
		}
		if original != nil {
			_ = original.Close()
		}
	}
	// **audit³ A4 fix**: hook panic 跳过 cleanup() 让 wrappedStack stream 泄漏.
	// 函数顶部 defer recover + 重抛——cleanup 必跑, panic 仍冒泡让 framework
	// outer recover (safeNodeRun) 转 err.
	defer func() {
		if r := recover(); r != nil {
			cleanup()
			panic(r) // 重抛让 framework outer recover 兜底
		}
	}()
	for _, h := range c.hooks {
		newCtx, wrapped, err := h.OnModelEndStream(ctx, info, sr)
		if err != nil {
			cleanup()
			return ctx, nil, err
		}
		// hook 返 (nil, nil) 是协议违例 — fail-fast.
		if wrapped == nil {
			cleanup()
			return ctx, nil, fmt.Errorf("hook OnModelEndStream returned nil reader without error (协议违例)")
		}
		wrappedStack = append(wrappedStack, wrapped)
		sr = wrapped
		ctx = newCtx
	}
	return ctx, sr, nil
}

func (c *ChainHooks) OnToolStart(ctx context.Context, info *AgentInfo, input *schema.ToolInput) (context.Context, error) {
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnToolStart(ctx, info, input)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainHooks) OnToolEnd(ctx context.Context, info *AgentInfo, output *schema.ToolOutput) (context.Context, error) {
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnToolEnd(ctx, info, output)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainHooks) OnError(ctx context.Context, info *AgentInfo, err *schema.CommError) context.Context {
	for _, h := range c.hooks {
		ctx = h.OnError(ctx, info, err)
	}
	return ctx
}
