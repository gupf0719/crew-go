package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// SupersededDecisionErrCode 是「这次调用做出的决定被同一批里排在前面的调用盖掉了」的 tool error code
// （业务级软失败：模型据此知道这一次没生效，不 abort 整个 Run）。
const SupersededDecisionErrCode = "decision_superseded"

// ── 同一批 tool_call 做出的上行信号：钩子之后判作废、同一件事上第一个生效 ──
//
// 一批 tool_call 是模型一条回复里同时发出的。它们做出的转交 / 路由决定在 OnToolEnd 跑完之后才定：
//
//   - **作废看钩子改写之后的结果**：OnToolEnd 的 output.Result 是 [消费] 档，模型最后看到的是钩子改写
//     之后的那条。结果是失败（超时、工具声明的 tool.Failure、中间件或钩子改成失败），这次调用做出的
//     决定就不算；钩子改成成功就算。曾经作废在钩子之前判：钩子改成失败时模型看到「失败」、对话照样
//     转走；改成成功时模型以为转成了、决定却已作废（审计实测）。「算不算失败」与模型看到的内容由同一份
//     改写后的 Result 推出（renderHookedToolResult）。
//   - **同一件事上第一个生效**：后面给出不同值的调用改写成一条失败结果，告诉模型这一次没有生效；那个
//     调用整个不算，它别的决定也不生效（见 compose_v2.JoinSignalsFirstWins）。曾经报冲突、整个 Run
//     失败，而这批 tool_call 续跑时从工具账原样重放、必然再撞，任务永远过不去（审计实测：续跑 3 次都撞
//     同一个冲突，批里另一个工具先问了人的话答完照样卡死）。
//
// 只管框架自己的 tools 节点（makeToolsFn）：它把每个调用收到的信号登记进 builder 放在 ctx 上的收集器，
// builder 在 OnToolEnd 之后调 finishToolBatchSignals。业务自写的 toolsFn 不经收集器，工具做出的决定
// 直接交给节点这次执行（同一段执行里先后交的，后者为准）。
type toolBatchSignals struct {
	// perCall 下标与 tool_call 一一对应：那个调用期间收到的上行信号（原样，作废在这里之后判）。
	perCall []compose_v2.Signals
	filled  bool
}

type toolBatchSignalsKey struct{}

func withToolBatchSignals(ctx context.Context, b *toolBatchSignals) context.Context {
	return context.WithValue(ctx, toolBatchSignalsKey{}, b)
}

func toolBatchSignalsFrom(ctx context.Context) *toolBatchSignals {
	b, _ := ctx.Value(toolBatchSignalsKey{}).(*toolBatchSignals)
	return b
}

// finishToolBatchSignals 定下这批 tool_call 的决定并交给 ctx 上正在收的那段执行（tools 节点这次执行）：
// failedAfterHook[i] 为真的作废；其余按顺序合并，被盖掉的那几个调用的结果换成一条失败结果。
//
// 被盖掉的那个调用**整个**不算：它别的、没冲突的决定也不生效（见 compose_v2.JoinSignalsFirstWins）——
// 结果说失败、另一个决定却照样生效，两边对同一次调用的结论相反。
//
// 失败结果里**带上这次调用原本的结果**：做出决定的不只是 transfer / route_to，业务工具也能经嵌套调用
// 做出（查了客户信息、顺带定了路由）。只说「没有生效」的话模型丢了那份产出，多半会再调一次，副作用
// 跟着重放。框架那件控制工具不带：它的原结果就是「已转交给 X」这句确认，跟「没有生效」自相矛盾。一个
// 调用有几个决定被盖掉时合成一条，不层层套。
//
// 换的是**新对象**，不就地改——工具账里存的消息会被原样交出来，就地改会连账一起改掉。results 的前
// len(toolCalls) 条与 toolCalls 一一对应；对不上时只合并、不改写结果。
func finishToolBatchSignals(ctx context.Context, toolCalls []schema.ToolCall, results []*schema.Message,
	perCall []compose_v2.Signals, failedAfterHook []bool) {
	if len(perCall) == 0 {
		return
	}
	branches := make([]compose_v2.Signals, len(perCall))
	for i, s := range perCall {
		if i < len(failedAfterHook) && failedAfterHook[i] {
			continue // 模型看到的是「失败」，这次调用做出的决定不算
		}
		branches[i] = s
	}
	merged, superseded := compose_v2.JoinSignalsFirstWins(branches...)
	aligned := len(toolCalls) == len(perCall) && len(results) >= len(toolCalls)
	var notes map[int][]string // 被盖掉的调用下标 → 它被盖掉的那几个决定
	var order []int
	for _, sp := range superseded {
		slog.WarnContext(ctx, "agent_v2: 同一批 tool_call 对同一件事做出了不同的决定，只保留排在前面的那个",
			"kind", sp.Kind, "kept", sp.Kept.Value, "dropped", sp.Dropped.Value, "dropped_call_index", sp.Branch)
		if !aligned {
			continue
		}
		if notes == nil {
			notes = make(map[int][]string, len(superseded))
		}
		if _, seen := notes[sp.Branch]; !seen {
			order = append(order, sp.Branch)
		}
		notes[sp.Branch] = append(notes[sp.Branch], fmt.Sprintf("「%s」（生效的是排在前面的「%s」）", sp.Dropped.Value, sp.Kept.Value))
	}
	for _, i := range order {
		var b strings.Builder
		b.WriteString("这次调用做出的决定都没有生效：")
		b.WriteString(strings.Join(notes[i], "、"))
		b.WriteString("——同一批调用里排在前面的那个已经对同一件事做了不同的决定，只能保留一个。")
		if orig := results[i]; orig != nil && orig.Content != "" && !isFrameworkControlResult(orig) {
			b.WriteString("这次调用本身的结果：")
			b.WriteString(orig.Content)
		}
		tc := toolCalls[i]
		results[i] = failedToolResult(tc.ID, tc.Function.Name, SupersededDecisionErrCode, b.String())
	}
	compose_v2.EmitSignals(ctx, merged)
}

// isFrameworkControlResult 报一条 tool_result 是不是框架那件控制工具（transfer_to_agent / route_to）
// 的结果——makeToolsFn 给 return-direct 结果盖 ReturnDirectMintedAnnotation 时判过身份。
func isFrameworkControlResult(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	minted, _ := m.Annotations[ReturnDirectMintedAnnotation].(bool)
	return minted
}

// isFailedToolResult 报一条 tool_result 是不是失败结果（框架合成的失败与工具声明的 tool.Failure 都带
// 失败标注，见 markToolFailure）。
func isFailedToolResult(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	_, failed := m.Annotations[toolErrorCodeAnnotation]
	return failed
}
