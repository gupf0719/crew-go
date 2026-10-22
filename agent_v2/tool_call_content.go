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
// 用步骤记录、不用 SaveNodeState：节点私有态那一格归 tools fn 记工具账（toolCallMemo），在这里写会把账覆盖掉。
const fixedToolCallsStep = "agent_v2.fixed_tool_calls"

func init() {
	compose_v2.RegisterValueType[*fixedToolCalls]("*agent_v2.fixedToolCalls")
}

// fixToolCallContent 让一批工具调用的内容只定一次：这一批第一次交给工具执行时（全部 OnToolStart 跑完、改写之后）
// 记下每个调用的工具名与参数；之后的执行（中断续跑、失败续跑）把 toolCalls 换回记下的那一份，`OnToolStart` 在这些
// 执行里对 Name / ArgumentsJSON 的改写不生效。钩子照样触发，它返回的 ctx、错误与中断照常算数（这一批处理完之后才
// 作废的调用除外：续跑时不再处理，钩子不触发，见 fixedToolResults）。
//
// toolCalls 是这一次改写之后的（就地换回），original 是模型给的原样（同一条 assistant 消息，每次执行都相同）。
//
// # 为什么
//
// 一次调用在续跑之间可能被改写成别的内容（钩子注入时间戳、请求号，或改了工具名），而这次调用身上已经发生过的事
// 都是对第一次那份内容做的（以下均实测）：
//   - 用户的答复：工具自己的提问（compose_v2.Ask）按位置认答案、不按内容。用户确认的是退 100，续跑时参数被改成
//     1000，同一个位置上那一问拿到那个确认，退了 1000；用户回答了 ask_user 的问题，续跑时 ask_user 被改名成要审批
//     的业务工具，那个回答被当成了审批放行。
//   - 审批：启发式判过的、用户在审批问题里看到并批过的是第一次那份。内容不固定时审批怎么绑定都有反例：只按调用
//     沿用结论，判的是 100、执行的是 1000；按内容重判，钩子每次注入新值时每次续跑都重判，启发式一改口就推翻用户
//     的拒绝；改了工具名，wipe_all 拿着对 refund 的批准执行。
//   - 工具账：已经成功的调用续跑时不再执行（toolCallMemo.Done），账是按第一次的内容记的。
//
// 确定性的改写（每次都改成同样的值）不受影响。
//
// # 为什么在这里、对整批一起做
//
// 这是 `OnToolStart` 的改写被消费的那一处：在这里换回来，下游——工具执行与审批（tools fn）、OnToolEnd 的相位、这批
// 调用做出的决定的裁决——看到的都是同一份，业务自己的 tools fn（低阶 builder）也一样。曾经只在审批生效时、在 tools fn
// 里逐个调用固定：工具账按没换回的内容记，续跑时被改了名的调用对不上账，已经成功的退款又执行一遍（审计实测）；
// 审批关掉时工具自己的提问照样拿着旧答复执行新内容。
//
// 全部 OnToolStart 都返回之后才定：中途有钩子报错或中断时这一批还没交给工具，下次执行重新改写、重新定。
//
// 记录跟着节点的这一次执行走（同 toolCallMemo）：节点成功后自然不可达，下一轮的新一批重新定。存不下时只 warn，退化成
// 续跑时按那一次的改写执行（工具账与工具体的私有态都带工具名，被改了名时不命中、重跑，错在安全侧）。
//
// # 代价与该放哪儿的值
//
// 改写过的内容随中断档、失败档落盘。每次执行都要新取的值（凭证、短时令牌、trace_id）因此别在 OnToolStart 里写进参数
// ——会落盘，短时令牌还会冻在第一次的值上；放钩子返回的 ctx，或用工具中间件在每次执行时注入（见 hook.Hooks 的
// OnToolStart 方法注释）。给每个调用都注入一个字段的钩子会让每个调用的整份参数另存一份。改写让这次调用出错时，
// 修好钩子再续跑也不生效，只能放弃这个任务重开。
// fixedToolCallContentOf 读这一批定下的调用内容（只读，不记）：这一批已经交给工具执行过时返回定下的那份（original 的
// 拷贝、换上改写过的工具名与参数），否则返回 false。OnToolStart 之前要知道工具账的键时用（见 fixedToolResults）。
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

func fixToolCallContent(ctx context.Context, toolCalls, original []schema.ToolCall) {
	if len(original) != len(toolCalls) {
		return // 同一条消息取出来的两份，长度必然相同
	}
	if rec, ok := compose_v2.GetStepResult[*fixedToolCalls](ctx, fixedToolCallsStep); ok && rec != nil && len(rec.Rewritten) == len(toolCalls) {
		for i := range toolCalls {
			c := fixedToolCall{Name: original[i].Function.Name, Arguments: original[i].Function.Arguments}
			if r := rec.Rewritten[i]; r != nil {
				c = *r
			}
			toolCalls[i].Function.Name, toolCalls[i].Function.Arguments = c.Name, c.Arguments
		}
		return
	}
	rec := &fixedToolCalls{Rewritten: make([]*fixedToolCall, len(toolCalls))}
	for i, tc := range toolCalls {
		if tc.Function.Name != original[i].Function.Name || tc.Function.Arguments != original[i].Function.Arguments {
			rec.Rewritten[i] = &fixedToolCall{Name: tc.Function.Name, Arguments: tc.Function.Arguments}
		}
	}
	if err := compose_v2.SaveStepResult(ctx, fixedToolCallsStep, rec); err != nil {
		slog.WarnContext(ctx, "agent_v2: 存这一批工具调用的内容失败，续跑时会按钩子那一次的改写执行", "err", err)
	}
}
