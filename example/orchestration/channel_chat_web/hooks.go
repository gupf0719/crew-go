package main

import (
	"context"
	"strings"
	"sync/atomic"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// interruptHook 在 flag 被点亮时立即抛 *compose_v2.InterruptError，触发框架自动 SaveNS cp。
// 挂到所有 sub-agent 和 planner 的 hooks 链上，OnAgentStart 是每轮 LLM 调用前最早
// 的检查点，触发后续 chatModel.Generate 不会被调用——挂起颗粒度 = 一轮 LLM。
type interruptHook struct {
	hook.NoOpHooks
	flag *atomic.Bool
}

func (h *interruptHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.flag != nil && h.flag.Load() {
		return ctx, &compose_v2.InterruptError{
			Reason: "user requested pause",
			Query: &compose_v2.AskQuery{
				// 自填稳定名字——框架不再替业务补空 ID。暂停钮不在节点里（hook 跑在
				// agent 层）且抛完不回查，这类挂起自填常量即可，别套 `compose_v2.Ask`。
				InterruptID: "pause:" + info.Name,
				Question:    "user requested pause - " + info.Name,
			},
		}
	}
	return ctx, nil
}

// orchAdapter 把 agent_v2.OrchestrationHooks 的 5 个回调转成 SSE 事件。
// 用 "graph_*" / "node_*" 前缀区分于 hook.Hooks 的 "agent_*" / "model_*" / "tool_*"。
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

func (o *orchAdapter) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	o.sink.send("node_start", map[string]any{"node": nodeID})
	return ctx, nil
}

func (o *orchAdapter) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	data := map[string]any{"node": nodeID}
	if err != nil {
		data["err"] = err.Error()
	}
	o.sink.send("node_end", data)
	return ctx
}

// OnNodeEvent 实时投递节点内 EventSink.Send 发射的事件——典型场景 LLM 流式 chunk
// + ReactiveCompact retry reset_stream。
func (o *orchAdapter) OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context {
	data := map[string]any{
		"node": nodeID,
	}
	if ev.Type != "" {
		data["type"] = ev.Type
	}
	if ev.Content != "" {
		data["content"] = ev.Content
	}
	if ev.Metadata != nil {
		for k, v := range ev.Metadata {
			data[k] = v
		}
	}
	// chunk 类型：前端按 run_id 累加渲染
	if ev.Type == "chunk" {
		data["is_chunk"] = true
	}
	// reset_stream 类型：前端清此 run_id 已累 chunk buffer（ReactiveCompact retry 触发）
	if ev.Type == "reset_stream" {
		data["reset_stream"] = true
	}
	o.sink.send("node_event", data)
	return ctx
}

// hookAdapter 把 hook.Hooks 的回调转成 SSE 事件，暴露 run_id / 消息内容。
// 嵌入 NoOpHooks 保持轻量，只覆盖实际要上报的回调。
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
	h.sink.send("model_start", map[string]any{
		"name":  info.Name,
		"round": info.Round,
		"msgs":  len(in.Messages),
	})
	return ctx, nil
}

func (h *hookAdapter) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out == nil || out.Message == nil {
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

func lastUser(msgs []*schema.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			return msgs[i].Content
		}
	}
	return ""
}

func preview(s any, n int) string {
	str, _ := s.(string)
	str = strings.ReplaceAll(str, "\n", " ")
	r := []rune(str)
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}
