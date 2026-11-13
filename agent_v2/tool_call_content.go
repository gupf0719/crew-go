package agent_v2

import (
	"context"
	"log/slog"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// fixedToolCall 是一次工具调用被固定下来的内容：`OnToolStart` 改写之后的工具名与参数。
type fixedToolCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// fixedToolCalls 是 tools 节点这一批工具调用定下的内容。Rewritten 按批内下标对齐：被 `OnToolStart` 改写过的调用
// 记改写后的内容，没改写的是 nil（续跑时用模型给的原样）。
//
// 只记改写过的：没改写的那份就在存档里那条 assistant 消息上，再存一份是白占存档——参数大的时候（交给子 agent 的
// 整段原文）每份中断档里多一份副本（实测）。记录本身不能省：它在就表示「这一批已经定下了」，续跑时钩子换了改写
// 也不生效。
type fixedToolCalls struct {
	Rewritten []*fixedToolCall `json:"rewritten"`
}

// fixedToolCallsStep 是固定内容在 tools 节点这次执行的步骤记录里的键（compose_v2.SaveStepResult）。
//
// 旧存档的节点私有态保留给批次工具账的迁移。
const fixedToolCallsStep = "agent_v2.fixed_tool_calls"

func init() {
	compose_v2.RegisterValueType[*fixedToolCalls]("*agent_v2.fixedToolCalls")
}

// fixedToolCallContentOf 读这一批定下的调用内容（只读，不记）：这一批已经交给工具执行过时返回定下的那份（original 的
// 拷贝、换上改写过的工具名与参数），否则返回 false。OnToolStart 之前要知道工具账的键时用（定稿的工具跳过钩子）。
func fixedToolCallContentOf(ctx context.Context, original []schema.ToolCall) ([]schema.ToolCall, bool) {
	rec, ok := compose_v2.GetStepResult[*fixedToolCalls](ctx, fixedToolCallsStep)
	if !ok || rec == nil || len(rec.Rewritten) != len(original) {
		return nil, false
	}
	out := append([]schema.ToolCall(nil), original...)
	for i := range out {
		if r := rec.Rewritten[i]; r != nil {
			out[i].Function.Name, out[i].Function.Arguments = r.Name, r.Arguments
		}
	}
	return out, true
}

// saveToolCallContent 记录首次派发的改写，续跑时复用同一份内容以匹配审批、提问和工具账。
func saveToolCallContent(ctx context.Context, toolCalls, original []schema.ToolCall) {
	rec := &fixedToolCalls{Rewritten: make([]*fixedToolCall, len(toolCalls))}
	for i, tc := range toolCalls {
		if tc.Function.Name != original[i].Function.Name || tc.Function.Arguments != original[i].Function.Arguments {
			rec.Rewritten[i] = &fixedToolCall{Name: tc.Function.Name, Arguments: tc.Function.Arguments}
		}
	}
	if err := compose_v2.SaveStepResult(ctx, fixedToolCallsStep, rec, compose_v2.WithStreams(nil)); err != nil {
		slog.WarnContext(ctx, "agent_v2: 存这一批工具调用的内容失败，续跑时会按钩子那一次的改写执行", "err", err)
	}
}
