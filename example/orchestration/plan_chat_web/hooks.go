package main

import (
	"context"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// interruptHook 在可落档的位置检查会话的暂停请求。顶层 OnAgentStart 跳过检查，
// 子 agent 启动和模型调用结束时可以中断，交给引擎保存存档。
type interruptHook struct {
	hook.NoOpHooks
	requested func(context.Context) (bool, error)
}

func (h *interruptHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if !compose_v2.WithinNodeScope(ctx) {
		return ctx, nil // 顶层启动还没有可以落档的节点。
	}
	return h.maybeInterrupt(ctx, info, "OnAgentStart")
}

// OnModelEnd 在模型调用结束后、工具派发前检查暂停请求。
func (h *interruptHook) OnModelEnd(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	_, err := h.maybeInterrupt(ctx, info, "OnModelEnd")
	return ctx, err
}

// maybeInterrupt 把暂停请求转换为可落档的中断。
func (h *interruptHook) maybeInterrupt(ctx context.Context, info *hook.AgentInfo, triggerPoint string) (context.Context, error) {
	requested, err := h.requested(ctx)
	if err != nil {
		return ctx, err
	}
	if requested {
		return ctx, &compose_v2.InterruptError{
			Reason: "user requested pause",
			Query: &compose_v2.AskQuery{
				// **自填一个稳定名字**：框架不再替业务补空 ID。
				//
				// 暂停钮这种形态不走 `compose_v2.Ask`——它不在节点里（hook 跑在 agent 层），
				// 而且抛完**不回查**（ID 只当 SSE payload 外带给前端，续跑走
				// `WithResumeUserText`）。这类「不指向具体提问」的挂起自填一个常量即可。
				InterruptID: "pause",
				Question:    "已挂起，请点继续或换方向。",
			},
			Data: map[string]any{
				"agent":   info.Name,
				"round":   info.Round,
				"trigger": triggerPoint, // 业务 SSE 可观测到 OnAgentStart vs OnModelEnd 哪个触发
			},
		}
	}
	return ctx, nil
}

// orchAdapter 把 v2 agent_v2.OrchestrationHooks 的 5 个回调转成 SSE 事件。
type orchAdapter struct {
	agent_v2.NoOpOrchestrationHooks // 嵌入：接口再长方法时不至于编译断；下面 5 个都实打实上报，全部覆盖
	sink                            *eventSink
}

var _ agent_v2.OrchestrationHooks = (*orchAdapter)(nil)

func (o *orchAdapter) OnGraphStart(ctx context.Context) (context.Context, error) {
	o.sink.send("graph_start", map[string]any{})
	return ctx, nil
}

func (o *orchAdapter) OnGraphEnd(ctx context.Context, err error) context.Context {
	data := map[string]any{}
	if err != nil {
		data["err"] = err.Error()
	}
	o.sink.send("graph_end", data)
	return ctx
}

func (o *orchAdapter) OnNodeStart(ctx context.Context, name string) (context.Context, error) {
	o.sink.send("node_start", map[string]any{"node": name})
	return ctx, nil
}

func (o *orchAdapter) OnNodeEnd(ctx context.Context, name string, err error) context.Context {
	data := map[string]any{"node": name}
	if err != nil {
		data["err"] = err.Error()
	}
	o.sink.send("node_end", data)
	return ctx
}

// OnNodeEvent 实时投递节点内 event——v2 ev 是简化的 Event{Type/Content/Metadata}。
func (o *orchAdapter) OnNodeEvent(ctx context.Context, name string, ev agent_v2.Event) context.Context {
	data := map[string]any{
		"node": name,
		"type": ev.Type,
	}
	if ev.Content != "" {
		data["content"] = ev.Content
	}
	for k, v := range ev.Metadata {
		data[k] = v
	}
	o.sink.send("node_event", data)
	return ctx
}

// hookAdapter 把 hook.Hooks 的回调转成 SSE 事件，暴露 run_id / 消息内容。
type hookAdapter struct {
	hook.NoOpHooks
	sink *eventSink
}

var _ hook.Hooks = (*hookAdapter)(nil)

func (h *hookAdapter) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if info.Round != 0 {
		return ctx, nil
	}
	data := map[string]any{
		"name":  info.Name,
		"type":  info.Type,
		"path":  info.RunPath,
		"round": info.Round,
	}
	if len(in.Messages) > 0 {
		last := in.Messages[len(in.Messages)-1]
		data["input_role"] = string(last.Role)
		data["input"] = preview(last.Content, 160)
	}
	h.sink.send("agent_start", data)
	return ctx, nil
}

func (h *hookAdapter) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.sink.send("agent_end", map[string]any{
		"name":  info.Name,
		"round": info.Round,
	})
	return ctx
}

func (h *hookAdapter) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	// 暂停之后继续：沿用暂停前那次答复，这一遍不是新的模型调用，前端已经收到过（hookAdapter 排在 intHook 前面）
	if hook.IsModelResponseReused(in.Extra) {
		return ctx, nil
	}
	h.sink.send("model_start", map[string]any{
		"name":  info.Name,
		"round": info.Round,
		"msgs":  len(in.Messages),
	})
	return ctx, nil
}

func (h *hookAdapter) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out == nil || out.Message == nil || hook.IsModelResponseReused(out.Extra) {
		return ctx, nil
	}
	msg := out.Message
	data := map[string]any{
		"name":  info.Name,
		"round": info.Round,
	}
	if rid, ok := msg.Annotations["run_id"].(string); ok && rid != "" {
		data["run_id"] = rid
	}
	if calls := schema.GetToolCalls(msg); len(calls) > 0 {
		names := make([]string, 0, len(calls))
		for _, c := range calls {
			names = append(names, c.Function.Name)
		}
		data["tool_calls"] = names
	} else {
		data["content"] = preview(msg.Content, 200)
	}
	h.sink.send("model_end", data)
	return ctx, nil
}

func (h *hookAdapter) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	h.sink.send("tool_start", map[string]any{
		"agent": info.Name,
		"name":  in.Name,
		"args":  preview(in.ArgumentsJSON, 120),
	})
	return ctx, nil
}

func (h *hookAdapter) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	data := map[string]any{"agent": info.Name}
	if out.Result != nil {
		data["ok"] = out.Result.Success
		if !out.Result.Success {
			data["err_code"] = out.Result.ErrorCode
		} else {
			data["result"] = preview(out.Result.Output, 120)
		}
	}
	h.sink.send("tool_end", data)
	return ctx, nil
}

func (h *hookAdapter) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	if err == nil {
		return ctx
	}
	h.sink.send("error", map[string]any{
		"agent":   info.Name,
		"code":    err.Code,
		"message": err.Message,
	})
	return ctx
}

// ── 小工具 ──

func preview(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	r := []rune(s)
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}
