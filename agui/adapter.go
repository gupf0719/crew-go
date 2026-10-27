package agui

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
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

	// input 是请求带来的消息原文（AG-UI 1.0：消费方每次都发完整的对话），emitted 是这个 run 发出的消息，按发出顺序；
	// voided 是其中作废了的（流中途失败的那次模型调用，见 reset_stream）。撤回靠 MESSAGES_SNAPSHOT：消费方丢掉快照里
	// 没有的消息，所以快照必须重述它手上的每一条——请求里的原样带回，这个 run 发出的按消费方落库时的 id 重建。
	input    []json.RawMessage
	inputIDs map[string]bool
	// inputCallIDs 是请求带来的消息里已有的工具调用 id（不论装着它的那条消息 id 是什么）：官方客户端的 TOOL_CALL_START 在
	// 所有消息的工具调用里找同一个 id，找到就并进去，不另建消息
	inputCallIDs map[string]bool
	emitted      []*emittedMessage
	byID         map[string]*emittedMessage // 文本消息与工具结果按 id 认（工具调用每次发出单独一条，不进这里）
	// snapshotPending 标「有撤回或改写，快照还没发」：攒到下一个入口（下一个钩子事件进来、改状态之前）、或这个 run 收尾时
	// 发一份。一次尝试作废时每次模型调用各来一个撤回，逐个发的话每个都重述整场对话，长对话下发送量近似平方增长。
	// 有在途的流式消息时不发、等它们都收尾：快照落在一条文本流中间时，官方客户端的事件压缩回放把它排到那条流之后，回放
	// 出来那条消息只剩快照那一刻的半截。
	snapshotPending bool
}

// emittedMessage 是这个 run 发给前端的一条消息，id 与官方客户端落库时用的一致：文本消息是 messageId（模型调用的
// run_id），工具调用没带 parentMessageId 时客户端建一条 id = toolCallId 的 assistant 消息来装，工具结果是 messageId。
type emittedMessage struct {
	id         string
	producer   string // 出自哪次模型调用（run_id）：文本消息是它自己的 id，工具调用与结果取 agent_v2.ToolModelRunIDExtraKey
	role       string
	content    strings.Builder
	toolName   string
	toolArgs   string
	toolCallID string // 工具调用 / 工具结果所属的调用
	isToolCall bool
	voided     bool
}

// snapshotJSON 按 AG-UI 1.0 的 Message 形状编码。
func (m *emittedMessage) snapshotJSON() json.RawMessage {
	var v any
	switch {
	case m.isToolCall:
		v = map[string]any{"id": m.id, "role": "assistant", "toolCalls": []map[string]any{{
			"id": m.toolCallID, "type": "function",
			"function": map[string]any{"name": m.toolName, "arguments": m.toolArgs},
		}}}
	case m.role == "tool":
		v = map[string]any{"id": m.id, "role": "tool", "toolCallId": m.toolCallID, "content": m.content.String()}
	default:
		v = map[string]any{"id": m.id, "role": m.role, "content": m.content.String()}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil // 只有字符串字段，编不成 JSON 不会发生
	}
	return b
}

// 编译期确认 sseAdapter 满足两个 hook 接口。
var (
	_ hook.Hooks                  = (*sseAdapter)(nil)
	_ agent_v2.OrchestrationHooks = (*sseAdapter)(nil)
)

// newSSEAdapter 构造 adapter。runID 是本次 AG-UI run 的 id，input 是请求带来的消息（撤回消息时重述它们）。
func newSSEAdapter(w *SSEWriter, runID string, input []AGUIMessage) *sseAdapter {
	a := &sseAdapter{w: w, runID: runID, openMsgs: map[string]bool{}, byID: map[string]*emittedMessage{},
		inputIDs: map[string]bool{}, inputCallIDs: map[string]bool{}}
	for _, m := range input {
		if len(m.raw) == 0 {
			continue
		}
		a.input = append(a.input, m.raw)
		if id := inputMessageID(m.raw); id != "" {
			a.inputIDs[id] = true
		}
		var calls struct {
			ToolCalls []struct {
				ID string `json:"id"`
			} `json:"toolCalls"`
		}
		if json.Unmarshal(m.raw, &calls) == nil {
			for _, tc := range calls.ToolCalls {
				if tc.ID != "" {
					a.inputCallIDs[tc.ID] = true
				}
			}
		}
	}
	return a
}

// recordLocked 记下这个 run 发出的一条消息。文本消息与工具结果按 id 认（同 id 已记过、没作废时返回已有的那条——流式片段
// 接在同一条后面）；工具调用每次发出单独记一条：provider 会复用 tool_call ID，它们出自不同的模型调用、各自作废（快照里
// 同一个 id 只重述一份，见 voidLocked）。调用方须持 a.mu。
func (a *sseAdapter) recordLocked(m *emittedMessage) *emittedMessage {
	if !m.isToolCall {
		if old, ok := a.byID[m.id]; ok && !old.voided {
			return old
		}
		a.byID[m.id] = m
	}
	a.emitted = append(a.emitted, m)
	return m
}

// producerOf 取工具调用 / 结果出自哪次模型调用（见 agent_v2.ToolModelRunIDExtraKey）。
func producerOf(extra map[string]any) string {
	id, _ := extra[agent_v2.ToolModelRunIDExtraKey].(string)
	return id
}

// voidLocked 撤回这个 run 里一次模型调用（run_id）发出的消息——它的文本，连同它产出的工具调用与这些调用的结果：发
// MESSAGES_SNAPSHOT，快照里是请求带来的消息加这个 run 发出、没作废的消息，消费方据此把作废的丢掉。请求没带消息时不发——
// 快照之外的消息会被消费方丢掉，而我们不知道它手上有什么。调用方须持 a.mu。
//
// 续跑时作废的是上一个请求已经交给前端的答复：它在请求带来的对话里，从快照要重述的原文里去掉（曾经只看这个 run 发出的，
// 被审核钩子拒掉的答复留在前端、下一轮又作为前文回到模型的上下文里，审计实测）。原文里它的工具调用与结果不带出处，去不掉。
func (a *sseAdapter) voidLocked(runID string) {
	if runID == "" {
		return
	}
	changed := false
	for _, e := range a.emitted {
		if !e.voided && e.producer == runID {
			e.voided, changed = true, true
		}
	}
	if a.inputIDs[runID] {
		kept := a.input[:0]
		for _, raw := range a.input {
			if inputMessageID(raw) == runID {
				changed = true
				continue
			}
			kept = append(kept, raw)
		}
		a.input = kept
		delete(a.inputIDs, runID)
	}
	if changed {
		a.snapshotLocked()
	}
}

// inputMessageID 取请求带来的一条消息的 id（解不开时返回空串）。
func inputMessageID(raw json.RawMessage) string {
	var head struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &head) != nil {
		return ""
	}
	return head.ID
}

// replaceLocked 把这个 run 里一次模型调用（run_id）的文本消息换成 content（OnModelEnd 钩子改写了答复）：记过这条就改它的
// 内容、发快照纠正前端手上的；没发过（流式只出了工具调用、或适配层排在钩子前面且没发文本）就照常发一条完整的文本消息。
// 调用方须持 a.mu。
func (a *sseAdapter) replaceLocked(runID, content string) {
	if m := a.byID[runID]; m != nil && !m.voided && !m.isToolCall && m.role == "assistant" {
		if m.content.String() == content {
			return
		}
		m.content.Reset()
		m.content.WriteString(content)
		a.snapshotLocked()
		return
	}
	// 续跑时改写的是上一个请求已经交给前端的消息：它在请求带来的对话里。改那条原文、发快照——流式重发同一个 id 在客户端
	// 是追加（官方客户端：消息已存在时 START 不新建、CONTENT 接在后面），曾经得到「原文 + 改写」（审计实测）
	if a.inputIDs[runID] {
		if a.rewriteInputLocked(runID, content) {
			a.snapshotLocked()
		}
		return
	}
	if content == "" {
		return
	}
	a.flushSnapshotLocked() // 先发攒着的快照：它不能已经含有下面要发的这一条
	a.recordLocked(&emittedMessage{id: runID, producer: runID, role: "assistant"}).content.WriteString(content)
	a.writeLocked(NewTextMessageStart(runID, "assistant"))
	a.writeLocked(NewTextMessageContent(runID, content))
	a.writeLocked(NewTextMessageEnd(runID))
}

// rewriteInputLocked 把请求带来的 id 为 id 的消息的正文换成 content（就地改快照要重述的原文；客户端违约重复了同一个 id 时
// 每一条都改，只改第一条的话快照里还留着原文）；一条都没改时返回假。调用方须持 a.mu。
func (a *sseAdapter) rewriteInputLocked(id, content string) bool {
	changed := false
	for i, raw := range a.input {
		var m map[string]any
		if json.Unmarshal(raw, &m) != nil || m["id"] != id {
			continue
		}
		if old, ok := m["content"].(string); ok && old == content {
			continue
		}
		m["content"] = content
		b, err := json.Marshal(m)
		if err != nil {
			continue
		}
		a.input[i] = b
		changed = true
	}
	return changed
}

// snapshotLocked 记下「要发一份快照」，攒到下一次往外写别的事件或 run 收尾时发（flushSnapshotLocked）。调用方须持 a.mu。
func (a *sseAdapter) snapshotLocked() { a.snapshotPending = true }

// writeLocked 往外写一个事件。攒着的快照由各入口在改状态之前发（见 flushSnapshotLocked 的调用处），这里不发：入口多是先把
// 要发的消息记进状态再写事件，在这里发的话快照里已经有这个事件，增量又发一遍（审计实测前端出现 "FINALFINAL"、工具参数
// 拼成两份）。调用方须持 a.mu。
func (a *sseAdapter) writeLocked(ev Event) {
	_ = a.w.WriteEvent(ev)
}

// flushSnapshotLocked 发攒着的 MESSAGES_SNAPSHOT：请求带来的消息原样加上这个 run 发出、没作废的消息。请求没带消息时不发——
// 快照之外的消息会被消费方丢掉，而我们不知道它手上有什么。调用方须持 a.mu。
func (a *sseAdapter) flushSnapshotLocked() {
	if !a.snapshotPending || len(a.openMsgs) > 0 {
		return
	}
	a.snapshotPending = false
	if len(a.input) == 0 {
		return
	}
	msgs := make([]json.RawMessage, 0, len(a.input)+len(a.emitted))
	msgs = append(msgs, a.input...)
	// 同一个 tool_call ID 只重述一份：官方客户端的 TOOL_CALL_START 是幂等的，几次复用同一个 ID 的调用在它手上是一条消息
	calls := map[string]bool{}
	for _, e := range a.emitted {
		if e.voided || (e.isToolCall && calls[e.id]) {
			continue
		}
		if e.isToolCall {
			calls[e.id] = true
		}
		msgs = append(msgs, e.snapshotJSON())
	}
	_ = a.w.WriteEvent(NewMessagesSnapshot(msgs))
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
	a.flushSnapshotLocked() // 在这次要改的状态记进去之前：快照里不能已经有这个入口接着要发的事件
	// 本轮 message 的 id = 这次模型调用的 run_id（与流式 chunk 的 run_id 同一 UUID）：先取框架放在 Extra 上的（排在前面的
	// 钩子可能换掉了 Message，新消息不带标注，见 hook.ModelRunIDKey），再取消息标注，都没有时兜底 runID。
	msgID := a.runID
	if output != nil {
		if rid := hook.ModelRunIDOf(output.Extra); rid != "" {
			msgID = rid
		} else if output.Message != nil {
			if rid, ok := output.Message.Annotations[compose_v2.RunIDAnnotationKey].(string); ok && rid != "" {
				msgID = rid
			}
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
			a.recordLocked(&emittedMessage{id: msgID, producer: msgID, role: "assistant"}).content.WriteString(msg.Content)
			a.writeLocked(NewTextMessageStart(msgID, "assistant"))
			a.writeLocked(NewTextMessageContent(msgID, msg.Content))
			a.writeLocked(NewTextMessageEnd(msgID))
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
	a.flushSnapshotLocked() // 在这次要改的状态记进去之前：快照里不能已经有这个入口接着要发的事件
	// 续跑时同一批调用的再次派发（框架标 agent_v2.ToolRedispatchedExtraKey）：上一个 run 已经发过它们的 START / ARGS / END，
	// 再发就是重开同一个调用、参数追加，前端的参数文本变成两份。结果照常在执行的那一次发（见 OnToolEnd）。按 toolCallId
	// 记「发过没有」不行：provider 在不同轮次复用同一个 ID 时，后一个调用的 START 会被吞掉、结果挂到前一个头上（审计实测）。
	if again, _ := in.Extra[agent_v2.ToolRedispatchedExtraKey].(bool); again {
		return ctx, nil
	}
	// provider 跨轮复用 tool_call ID 时：同一个 ID 已经在请求带来的消息里（以前的 run 发出的，或外来历史里装在别的 id 的
	// 消息下——带 parentMessageId 的），客户端把这次的调用并进它手上那条（官方客户端的 TOOL_CALL_START 在所有消息的工具
	// 调用里找同一个 id），快照照原样重述那条、这里不另记；曾经只认消息 id 等于 toolCallId 的那种，外来历史下快照里同一个
	// 工具调用出现两份。这个 run 里复用的每次单独记一条（各自作废），快照里同一个 ID 只重述一份
	if !a.inputIDs[in.CallId] && !a.inputCallIDs[in.CallId] {
		a.recordLocked(&emittedMessage{id: in.CallId, producer: producerOf(in.Extra), role: "assistant", isToolCall: true,
			toolCallID: in.CallId, toolName: in.Name, toolArgs: in.ArgumentsJSON})
	}
	a.writeLocked(NewToolCallStart(in.CallId, in.Name))
	if in.ArgumentsJSON != "" {
		a.writeLocked(NewToolCallArgs(in.CallId, in.ArgumentsJSON))
	}
	a.writeLocked(NewToolCallEnd(in.CallId))
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
	a.flushSnapshotLocked() // 在这次要改的状态记进去之前：快照里不能已经有这个入口接着要发的事件
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
	// provider 在一个 run 里复用 tool_call ID 时 `<runID>/<callID>` 会撞：加序号保 thread 内唯一。撞了的话消费方把后一条
	// 当成同一条消息（规范：重发已有的消息是追加），快照里也会把两次结果拼成一条（审计实测 "RaRb"）
	resultID := a.runID + "/" + out.CallId
	for n := 2; a.byID[resultID] != nil || a.inputIDs[resultID]; n++ {
		resultID = a.runID + "/" + out.CallId + "#" + strconv.Itoa(n)
	}
	a.recordLocked(&emittedMessage{id: resultID, producer: producerOf(out.Extra), role: "tool", toolCallID: out.CallId}).content.WriteString(content)
	a.writeLocked(NewToolCallResult(resultID, out.CallId, content))
	return ctx, nil
}

// OnError 把中间错误（model / tool 层）映射为 Custom([CustomEventError])——供前端观测，不占
// RunError 终态语义（终态 RunError 由 handler 在 Run 返回 error 时发）。
func (a *sseAdapter) OnError(ctx context.Context, _ *hook.AgentInfo, err *schema.CommError) context.Context {
	if err == nil {
		return ctx
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushSnapshotLocked() // 在这次要改的状态记进去之前：快照里不能已经有这个入口接着要发的事件
	a.writeLocked(NewCustom(CustomEventError, map[string]any{"code": err.Code, "message": err.Message}))
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
	// 在这次要改的状态记进去之前发攒着的快照；撤回事件自己只往里攒，连着来的几个只发一份
	if ev.Type != "reset_stream" {
		a.flushSnapshotLocked()
	}

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
			a.writeLocked(NewTextMessageStart(runID, "assistant"))
		}
		m := a.recordLocked(&emittedMessage{id: runID, producer: runID, role: "assistant"})
		if ev.Content != "" {
			m.content.WriteString(ev.Content)
			a.writeLocked(NewTextMessageContent(runID, ev.Content))
		}
	case "reset_stream":
		// 一次模型调用流中途失败，它已交出的片段作废：收尾对应 run_id 的 message，再发 MESSAGES_SNAPSHOT 撤回它
		// （AG-UI 的标准办法，标准前端照做，见 voidLocked）；自定义事件留着给按 run_id 自己管缓冲的业务前端。曾经只发
		// 自定义事件，标准前端忽略它，半截那条一直留在对话里（实测）。
		runID, _ := ev.Metadata["run_id"].(string)
		if runID == "" {
			runID = a.runID
		}
		a.closeMsgLocked(runID)
		if replacement, ok := ev.Metadata["replacement"].(string); ok {
			// 钩子改写了答复：不是作废，用新正文替换那条消息
			a.replaceLocked(runID, replacement)
		} else {
			a.voidLocked(runID)
		}
		// 自定义事件立即发、不带出快照：连着来的几个撤回只发一份快照
		_ = a.w.WriteEvent(NewCustom(CustomEventResetStream, map[string]any{"run_id": runID, "reason": ev.Content}))
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
			a.writeLocked(NewStateSnapshot(snap))
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
	a.flushSnapshotLocked() // 在这次要改的状态记进去之前：快照里不能已经有这个入口接着要发的事件
	a.closeAllLocked()
	a.flushSnapshotLocked() // 撤回发生在最后一个事件之后（没有别的事件带出它）时在这里发
}

// closeMsgLocked 若指定 message 处于打开态则发 TextMessageEnd 并移出 open-set。
// 调用方须持 a.mu。
func (a *sseAdapter) closeMsgLocked(msgID string) {
	if a.openMsgs[msgID] {
		a.writeLocked(NewTextMessageEnd(msgID))
		delete(a.openMsgs, msgID)
		a.flushSnapshotLocked() // 等着在途消息收尾的快照
	}
}

// closeAllLocked 收尾所有仍打开的流式 message。调用方须持 a.mu。
func (a *sseAdapter) closeAllLocked() {
	for msgID := range a.openMsgs {
		a.writeLocked(NewTextMessageEnd(msgID))
		delete(a.openMsgs, msgID)
	}
	a.flushSnapshotLocked()
}
