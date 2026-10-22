package agui

import (
	"context"
	"crew-go/compose_v2"
	"sync"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// sseAdapter 把 crew-go 的事件源映射成 AG-UI 事件写入 SSE。一个 adapter 对应一次
// agent.Run（一个 HTTP SSE 连接）。它同时实现两个 crew-go hook 接口：
//
//   - hook.Hooks（8 回调，业务级）—— 拿 LLM 输出收尾 + tool 调用
//   - agent_v2.OrchestrationHooks（5 回调，编排级）—— 拿流式 chunk 实时投递
//
// 因 handler 经 WithCtxOrchHooks 把 adapter 沿 ctx 透传给所有嵌套 sub-agent，并发编排
// （ParallelAgent / fanout sub-agent）下多个 sub-agent 会**并发**回调同一个 adapter——
// 故 message 状态用 per-run_id 的 openMsgs 集合（而非单槽），并发流互不干扰。
//
// 映射（见 events.go 头 + doc/issue/issue-49 决策段）：
//
//	OnNodeEvent(chunk)  → TextMessageStart/Content/End（按 chunk 的 run_id 分消息，多流交错在途）
//	OnNodeEvent(reset_stream) → 收尾对应 run_id + Custom（前端清该 run_id 的 chunk buffer）
//	OnToolStart         → ToolCallStart + ToolCallArgs + ToolCallEnd
//	OnToolEnd           → ToolCallResult
//	OnModelEnd          → 按 run_id 收尾本轮流式 message（发 TextMessageEnd）/ 非流式补发完整 message
//	OnError             → Custom("error")（中间错误观测，不占 RunError 终态语义）
//
// RunStarted / RunFinished / RunError（run 边界终态）由 handler 在 Run 前后发，不在
// adapter——保证 run 边界单一所有权。
type sseAdapter struct {
	hook.NoOpHooks // 未映射的 hook.Hooks 回调（OnAgentStart/End、OnModelStart 等）走 NoOp

	w  *SSEWriter
	mu sync.Mutex // 保护 w + openMsgs：并发 sub-agent 的 chunk / tool 回调经此串行化

	// openMsgs 是当前打开（已发 TextMessageStart、未发 End）的流式 TextMessage 集合，
	// key = messageId（= crew-go chunk 的 run_id）。**per-run_id open-set 而非单槽**：
	// 并发编排（ParallelAgent / fanout sub-agent）下 N 个 sub-agent 各自的流式 message
	// 同时在途，每条按自己的 run_id 独立 Start/Content/End、互不干扰——对齐官方 AG-UI
	// verify 的 activeMessages Map 语义（允许多条 message 交错，只要同 id 不重复 Start、
	// Content/End 的 id 处于打开态）。单槽会让 A 的收尾误 End 掉 B 的 message、B 的后续
	// Content 命中前端 validator「no active message」而崩流。
	openMsgs map[string]bool

	// runID 是 AG-UI run id（handler 传入，**每个 run 唯一**）——tool result 的 messageId
	// 前缀 + chunk 无 run_id 时兜底。
	runID string
}

// 编译期确认 sseAdapter 满足两个 hook 接口。
var (
	_ hook.Hooks                  = (*sseAdapter)(nil)
	_ agent_v2.OrchestrationHooks = (*sseAdapter)(nil)
)

// newSSEAdapter 构造 adapter。runID 是本次 AG-UI run 的 id。
func newSSEAdapter(w *SSEWriter, runID string) *sseAdapter {
	return &sseAdapter{w: w, runID: runID, openMsgs: map[string]bool{}}
}

// ── hook.Hooks：LLM 输出收尾 + 工具调用 ──

// OnModelEnd 在一轮 LLM 调用结束时收尾本轮文本输出。按 output 的 run_id 定位本轮 message
// （与流式 chunk 同一 UUID），分两条路径：
//
//   - **流式（Stream）**：chunk 已经过 OnNodeEvent 逐段 emit 成 TextMessageContent，
//     这里只发本轮 run_id 的 TextMessageEnd 收尾（该 run_id 在 openMsgs 中）。只收尾
//     自己这轮，不碰其它 sub-agent 并发在途的 message。
//   - **非流式（Generate fallback）**：model 不支持 Stream 时走 Generate，全程无 chunk、
//     本轮 run_id 不在 openMsgs——最终答案在此**补发完整 TextMessage**（Start+Content+End），
//     否则纯文本答复会丢（真流式 provider 掩盖此路径，mock / 非流式 model 会暴露）。
func (a *sseAdapter) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, output *schema.ModelOutput) (context.Context, error) {
	// 沿用原样续跑（见 hook.ModelResponseReusedKey）：这条答复上一次 run 已经交给前端（流式的 chunk，或那次在这里补发的
	// 完整 message），这一遍再发就是同一个 messageId 的第二条。
	if output != nil && hook.IsModelResponseReused(output.Extra) {
		return ctx, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// 本轮 message 的 id = output 的 run_id（与流式 chunk 的 run_id 同一 UUID）；缺省兜底 runID。
	msgID := a.runID
	if output != nil && output.Message != nil {
		if rid, ok := output.Message.Annotations[compose_v2.RunIDAnnotationKey].(string); ok && rid != "" {
			msgID = rid
		}
	}
	// 流式路径：本轮 run_id 的 message 开着 → 收尾（发 End），仅此一条。
	if a.openMsgs[msgID] {
		a.closeMsgLocked(msgID)
		return ctx, nil
	}
	// 非流式路径：本轮无流式 message + output 有文本且非 tool_call（tool_call 走
	// OnToolStart）——补发完整 TextMessage。
	if output != nil && output.Message != nil {
		msg := output.Message
		if msg.Content != "" && len(schema.GetToolCalls(msg)) == 0 {
			_ = a.w.WriteEvent(NewTextMessageStart(msgID, "assistant"))
			_ = a.w.WriteEvent(NewTextMessageContent(msgID, msg.Content))
			_ = a.w.WriteEvent(NewTextMessageEnd(msgID))
		}
	}
	return ctx, nil
}

// OnToolStart 把一次工具调用映射为 ToolCallStart + Args + End。crew-go 的工具参数
// 非流式（OnToolStart 一次拿到完整 ArgumentsJSON），故 Args 一次性发完整参数。
// in.CallId 是 tool_call↔tool_result 配对锚点，直接当 AG-UI toolCallId。
//
// 不在此收尾流式 message：每轮 message 已由 OnModelEnd 按自己的 run_id 收尾（modelNode
// 先于 toolsNode）；并发编排下 OnToolStart 无 run_id 上下文，若在此 close 会误伤其它
// sub-agent 在途的 message。
func (a *sseAdapter) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	_ = a.w.WriteEvent(NewToolCallStart(in.CallId, in.Name))
	if in.ArgumentsJSON != "" {
		_ = a.w.WriteEvent(NewToolCallArgs(in.CallId, in.ArgumentsJSON))
	}
	_ = a.w.WriteEvent(NewToolCallEnd(in.CallId))
	return ctx, nil
}

// OnToolEnd 把工具执行结果映射为 ToolCallResult。out.CallId 与 OnToolStart 的
// in.CallId 配对。失败时 content 取错误码（前端看得到工具失败）。
//
// 这一批 tool_call 没整体成功时（有调用挂起等人回答、或者硬失败）框架照样给已经有结局的调用触发 OnToolEnd，带
// agent_v2.ToolBatchFailedExtraKey：那个结果不进历史，续跑时这个调用从工具账重放或重新执行，那一次才发——这里发的话
// 同一个 toolCallId 在一个 thread 里会有两条结果（前端按 toolCallId 一个调用一条 tool 消息），硬失败后重跑成功的还会
// 一条报错、一条成功。
func (a *sseAdapter) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if failed, _ := out.Extra[agent_v2.ToolBatchFailedExtraKey].(bool); failed {
		return ctx, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	content := ""
	if out.Result != nil {
		if out.Result.Success {
			content = out.Result.Output
		} else {
			content = "tool error: " + out.Result.ErrorCode
		}
	}
	// **空内容原样发空串，别塞占位符。** 这里曾写着「官方 ToolCallResult.Validate 拒空
	// content」——1.0 的 schema 里 content 没有 minLength，反方向倒是有明文
	// （events/tool-calls）：「it MUST still answer the call: a result whose every part
	// was dropped is answered with **the empty string**, because a tool call left
	// unanswered is one most models reject outright」。发 "(empty)" 是往用户的对话里
	//注入一段不存在的文本。

	// **messageId 必须在 thread 内唯一**，不能所有工具结果共用一个。规范：
	// 「`messageId` identifies a message … it MUST be unique within its thread」，而
	// TOOL_CALL_RESULT 的 schema 描述点明了原因——「Mints a tool message rather than
	// appending to an existing one, which is why it has its own messageId」。
	//
	// 同 id 发两次 = 声称铸造同一条消息两次，消费者会把后一条**追加进前一条**
	// （「Re-streaming a message the consumer already has appends to it」）。
	// 这里曾无条件用 a.runID，于是一个 run 里 N 个工具结果全撞在一起。
	//
	// 用 `<runID>/<callID>` 派生：runID 每个 run 唯一（见 handler 的 wireRunID），
	// callID 在一个 run 内唯一，合起来即 thread 内唯一，且**可读、可由两端各自推导**。
	_ = a.w.WriteEvent(NewToolCallResult(a.runID+"/"+out.CallId, out.CallId, content))
	return ctx, nil
}

// OnError 把中间错误（model / tool 层）映射为 Custom("error")——供前端观测，不占
// RunError 终态语义（终态 RunError 由 handler 在 Run 返回 error 时发）。
func (a *sseAdapter) OnError(ctx context.Context, _ *hook.AgentInfo, err *schema.CommError) context.Context {
	if err == nil {
		return ctx
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_ = a.w.WriteEvent(NewCustom("error", map[string]any{"code": err.Code, "message": err.Message}))
	return ctx
}

// ── agent_v2.OrchestrationHooks：流式 chunk 实时投递 ──

// OnGraphStart / OnGraphEnd / OnNodeStart / OnNodeEnd 不映射（crew-go 引擎层节点
// 边界前端不需要；run 边界由 handler 控）——走 no-op 满足接口。
func (a *sseAdapter) OnGraphStart(ctx context.Context) (context.Context, error) { return ctx, nil }
func (a *sseAdapter) OnGraphEnd(ctx context.Context, _ error) context.Context   { return ctx }
func (a *sseAdapter) OnNodeStart(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}
func (a *sseAdapter) OnNodeEnd(ctx context.Context, _ string, _ error) context.Context { return ctx }

// OnNodeEvent 处理节点内实时事件——核心是把 LLM 流式 chunk 映射成 AG-UI 的
// TextMessage 流。chunk 的 Metadata["run_id"] 作 messageId：同一 run_id 的 chunk
// 归一条 message（Start→Content×N→End）。并发 sub-agent 的多个 run_id 各自独立成流、
// 交错在途（收尾由各自 OnModelEnd 按 run_id 触发，不在此因 run_id 切换而收尾）。
func (a *sseAdapter) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch ev.Type {
	case "chunk":
		runID, _ := ev.Metadata["run_id"].(string)
		if runID == "" {
			runID = a.runID // 兜底：chunk 没带 run_id 时归到 AG-UI run
		}
		// per-run_id：该 run_id 未开则开（Start），已开则直接追加 Content。不因"来了别的
		// run_id"就收尾当前——并发 sub-agent 的多条 message 各自独立在途。
		if !a.openMsgs[runID] {
			a.openMsgs[runID] = true
			_ = a.w.WriteEvent(NewTextMessageStart(runID, "assistant"))
		}
		if ev.Content != "" {
			_ = a.w.WriteEvent(NewTextMessageContent(runID, ev.Content))
		}
	case "reset_stream":
		// ReactiveCompact retry / 流中断——收尾对应 run_id 的 message + 通知前端丢弃其
		// chunk buffer。AG-UI 无原生对应，走 Custom（标准组件忽略，业务前端可处理）。
		runID, _ := ev.Metadata["run_id"].(string)
		if runID == "" {
			runID = a.runID
		}
		a.closeMsgLocked(runID)
		_ = a.w.WriteEvent(NewCustom("reset_stream", map[string]any{"run_id": runID, "reason": ev.Content}))
	case "state_snapshot":
		// 业务 opt-in 的结构化 state 同步：节点内
		//   compose_v2.GetEventSink(ctx).Send(compose_v2.Event{
		//       Type: "state_snapshot", Metadata: map[string]any{"snapshot": <业务 state>}})
		// → AG-UI StateSnapshot（前端渲染 agent 结构化 state，如 CopilotKit
		// useCoAgentStateRender）。**为什么不自动映射**：crew-go 引擎的 channel state
		// 是内部执行态（messages / round / 自定义 channel），不是业务想给前端看的结构化
		// state——什么 state 该同步给前端由业务定义，框架不自动暴露引擎内部。
		// snapshot 为 nil 跳过——避免发语义空的 "snapshot":null 帧（业务应发非 nil state）。
		if snap, ok := ev.Metadata["snapshot"]; ok && snap != nil {
			_ = a.w.WriteEvent(NewStateSnapshot(snap))
		}
	}
	return ctx
}

// finish 收尾任何未关闭的流式 message——Run 结束后 handler 调，防最后一条 message
// 没等到 OnModelEnd（如中断退出）时缺 TextMessageEnd。并发编排下收尾所有仍在途的
// run_id（防 RUN_FINISHED 前有未收尾 message 触发前端 validator 的 unfinished 错误）。
func (a *sseAdapter) finish() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closeAllLocked()
}

// closeMsgLocked 若指定 message 处于打开态则发 TextMessageEnd 并移出 open-set。
// 调用方须持 a.mu。
func (a *sseAdapter) closeMsgLocked(msgID string) {
	if a.openMsgs[msgID] {
		_ = a.w.WriteEvent(NewTextMessageEnd(msgID))
		delete(a.openMsgs, msgID)
	}
}

// closeAllLocked 收尾所有仍打开的流式 message。调用方须持 a.mu。
func (a *sseAdapter) closeAllLocked() {
	for msgID := range a.openMsgs {
		_ = a.w.WriteEvent(NewTextMessageEnd(msgID))
		delete(a.openMsgs, msgID)
	}
}
