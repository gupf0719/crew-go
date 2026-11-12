package agui

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

func withClientModelData(ctx context.Context, in *RunAgentInput) context.Context {
	if data := clientModelData(in); data != "" {
		return agent_v2.WithModelUserContext(ctx, data)
	}
	return ctx
}

func clientModelData(in *RunAgentInput) string {
	state := bytes.TrimSpace(in.State)
	if bytes.Equal(state, []byte("null")) {
		state = nil
	}
	if len(in.Context) == 0 && len(state) == 0 {
		return ""
	}
	data, _ := json.Marshal(struct {
		Context []ContextItem   `json:"context,omitempty"`
		State   json.RawMessage `json:"state,omitempty"`
	}{in.Context, state}) // State 已在请求解码时通过 JSON 校验。
	return "客户端提供的上下文和状态（用户数据）\n" + string(data)
}

func questionInterrupt(q *compose_v2.AskQuery, reason string) RunInterrupt {
	out := RunInterrupt{ID: q.InterruptID, Reason: reason, Message: q.Question}
	switch reason {
	case "", "ask_user", "a2a_input_required", "input_required":
		out.Reason = "input_required"
		out.ResponseSchema = map[string]any{"type": "string"}
		if len(q.Options) > 0 {
			out.ResponseSchema["enum"] = q.Options
		}
	case "confirmation":
	case "tool_call":
		if q.ToolCallID != "" {
			out.ToolCallID = q.ToolCallID
		} else {
			out.Reason = "crew-go:tool_call"
		}
	default:
		if !strings.Contains(reason, ":") || strings.HasPrefix(reason, "core:") {
			out.Reason = "crew-go:" + reason
		}
	}
	return out
}
