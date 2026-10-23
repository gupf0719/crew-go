package agent_v2

import (
	"context"
	"log/slog"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// basicModelCallStep 是 model 节点这次执行里那次模型调用的记录在节点私有态里的键（见 [modelCallRecord]）。
const basicModelCallStep = "crew_model_call"

// basicToolsHistoryStep 记「tools 节点这次执行已经把这一批结果记进会话历史」：处理完之后作废、续跑重放时不再记一份。
const basicToolsHistoryStep = "crew_tools_history_recorded"

// modelCallRecord 是 model 节点这次执行里那次模型调用的记录，记在节点私有态里：模型调用成功那一刻记下原样，这一轮
// 处理完（OnModelEnd 之后）定下（Final）。
//
// 引擎的契约是「成功的执行可能在提交之前作废、重跑」（节点体成功之后的钩子 Goexit、提交出错；成功之后的观测钩子
// panic 就地兜住、不再让执行作废，见 compose_v2 的 notifyNodeEnd），节点要靠私有态让重跑幂等。不记的话重跑时模型再调
// 一次、答出另一份：作废的那份已经记进会话历史，历史里多出一条模型「说过」却从没进对话的答复（带着没有结果的
// 工具调用）；OnModelEnd 钩子问过人的，用户批准的是第一份、续跑时批准落到用户从没看过的第二份上（均实测）。
//
// 三条不变量：
//
//   - **一次模型调用的结果在这一轮第一次处理完时定下，定下了就不再处理**：之后这次执行作废、续跑时不调模型，
//     OnModelStart / OnModelEnd 都不触发，交出定下的那份——消息流、轮次、压缩备忘、最终答复的写入都跟那次一样，
//     处理时做出的上行信号原样交出，这一轮的会话历史不重记、但照首跑那样登记成「这次调用里记过」（编排边界补记据此
//     不再替它记一份，见 noteHistoryRecorded）。再交给钩子的只能是处理完的那份：不幂等的钩子（解析
//     答复里的 JSON、给答复打序号）在它上面报错或叠加改写，续跑就永远过不去、或结果跟着变（审计实测）；两个钩子
//     一起不触发，钩子开的 span 也不落单。同 tools 节点的工具账（见 fixedToolResults）。
//   - **模型答复之后、这一轮处理完之前停下的，原样只在等人或被叫停时留着**：钩子因为要等人或被叫停（中断、取消
//     ——compose_v2.MustPropagate 为真）停下时，续跑沿用这次调用的原样答复、不再调模型，把这一轮处理完——钩子问过
//     人的话，用户答的就是这一份。别的结局一律作废、续跑重新调模型换一份：钩子报错拒掉（业务拒掉不合格的答复时，
//     续跑本来就该换一份）、把答复置空、panic（钩子、事件投递）。拿不准这份答复还能不能用时宁可重新生成：内容触发
//     的钩子 panic 留着原样的话，续跑永远过不去（审计实测）。模型调用之前停下的（OnModelStart 报错或中断、模型
//     报错）没有记录，续跑照常调模型。作废原样时推进 OnModelEnd 那一格的重做代际（compose_v2.AdvanceRerunGeneration，
//     模型钩子各在自己的相位里跑，见 modelHookCtx）：那一格里对新答复的提问换新 ID——不换的话，调用方累积传的旧答案
//     （对上一份的批准）按同一个 ID 兑付给用户从没看过的新一份（审计实测，`err == nil`）；那一格的普通记录照常拿回
//     （钩子里与答复无关的扣费这类嵌套调用不重做，按 Rerunnable 记的作废——同条件边否决重做）。OnModelStart 问的是
//     输入，重新调模型不改变输入，它那一格不推进，答过的不再问。
//   - **一次模型调用的用量只交给 OnModelEnd 一次**：沿用原样时不再带用量——上一次 OnModelEnd 停下之前，排在前面的
//     钩子（观测链装在最前）已经收到过。沿用原样的那一遍在 ModelInput.Extra / ModelOutput.Extra 上标
//     hook.ModelResponseReusedKey：这一遍没有模型调用，按调用计数、计时的钩子（框架自带的观测）与把答复交给前端的钩子
//     （agui 适配层）据此不再记一次、不再发一遍。
//
// 按 Rerunnable 记：这一轮被要求重新产出（否决重做）时作废。
type modelCallRecord struct {
	// Notes 是这一轮送进来的后台任务通知（模型是看着它们答的）。
	Notes []*schema.Message `json:"notes,omitempty"`
	// Inter 是这一轮调模型之前接进来的运行中插话（模型是看着它们答的，见 Inbox）；Late 是模型给出最终答复之后
	// 再取到的那几条（接在答复后面，Final 时有意义）。
	Inter []*schema.Message `json:"inter,omitempty"`
	Late  []*schema.Message `json:"late,omitempty"`
	// Out 是模型的答复：Final 为假时是模型刚返回的原样，为真时是 OnModelEnd 处理完的那份。
	Out *schema.Message `json:"out"`
	// Compaction 是这一轮 AutoCompact 摘要出的备忘（没摘要为 nil）。
	Compaction *compactionMemo `json:"compaction,omitempty"`
	// Boundary 是这一轮要写进会话历史的压缩边界（没摘要、或 OnModelStart 钩子动了消息列表时为 nil）。在模型调用
	// 那一刻定：沿用原样续跑时 OnModelStart 再触发一次，它这次动没动消息列表跟那次答复无关。
	Boundary *schema.Message `json:"boundary,omitempty"`
	// Final 报这一轮已经处理完、定下了。
	Final bool `json:"final,omitempty"`
	// FinalRound 报这份答复是不是在软降落轮生成的（模型被告知 tool_choice=none / 只留 final_output 的那一轮，见
	// withFinalRound）。沿用原样、重放定下的那一轮时 tools 节点据它判这批调用执不执行——不能按续跑时的预算现算：
	// 续跑调整了轮次预算（WithResumeMaxRounds）时现算的值跟这份答复生成时不同，缩预算会把可以调工具的一轮当成软降落轮
	// （硬调不执行、随后被「至多一个软降落轮」的闸拦下，一次收尾都没有），调大预算会执行软降落轮里硬调的工具。
	FinalRound bool `json:"final_round,omitempty"`
	// Signals 是处理这次调用时做出的上行信号（钩子与模型里做出的，多数为空；Final 时有意义）。
	Signals compose_v2.Signals `json:"signals,omitzero"`
}

func init() {
	// 随存档落盘 ⟹ 必须注册（同 toolCallMemo）
	compose_v2.RegisterValueType[*modelCallRecord]("*agent_v2.modelCallRecord")
}

// modelCallOf 读这次执行里那次模型调用的记录；没有（第一次执行、原样被作废过、被否决重做）时返回 false。每次调用
// 都是一份新解码的拷贝：交给钩子的那份就地改了也碰不到要写回的那份。
func modelCallOf(ctx context.Context) (*modelCallRecord, bool) {
	rec, ok := compose_v2.GetStepResult[*modelCallRecord](ctx, basicModelCallStep)
	if !ok || rec == nil || rec.Out == nil {
		return nil, false
	}
	return rec, true
}

// saveModelCall 记下这次模型调用（原样或定下的那份）。存不下时续跑会再调一次模型（没有记录时的行为）。
func saveModelCall(ctx context.Context, rec *modelCallRecord) {
	if err := compose_v2.SaveStepResult(ctx, basicModelCallStep, rec, compose_v2.Rerunnable()); err != nil {
		slog.WarnContext(ctx, "agent_v2: 记下这次模型调用失败，续跑时会再调一次模型", "err", err)
	}
}

// clearModelCall 作废记下的原样：这一轮既没处理完、也不是因为等人或被叫停而停下（钩子报错、置空、panic），续跑
// 重新调模型。OnModelEnd 处理过这份答复（可能问过人），那一格的产出跟着作废：推进它的重做代际（见 modelCallRecord）。
func clearModelCall(ctx context.Context) {
	saveModelCall(ctx, (*modelCallRecord)(nil))
	compose_v2.AdvanceRerunGeneration(modelHookCtx(ctx, "OnModelEnd"))
}

// modelHistoryRecords 是 model 节点这一轮要记进会话历史的消息：送进来的通知与插话排在最前（边界里的替代历史已经
// 含着它们，排在边界之后的话下一次 Run 读回时多一份），摘要过就先记压缩边界（见 compaction_boundary.go），然后是
// 答复，最后是答复之后才取到的插话（插话是用户的原话，照记）。
func modelHistoryRecords(notes, inter []*schema.Message, boundary, out *schema.Message, late []*schema.Message) []*schema.Message {
	recorded := make([]*schema.Message, 0, len(notes)+len(inter)+2+len(late))
	recorded = append(append(recorded, notes...), inter...)
	if boundary != nil {
		recorded = append(recorded, boundary)
	}
	return append(append(recorded, out), late...)
}

// basicModelWrites 是 model 节点这一轮的写入：通知、插话与答复进消息流、轮次加一、摘要过就更新压缩备忘、答复交给
// basicFinalCh。接进了插话时重复调用的计数链从头数（用户插话改变了语境，之后的重复调用不算原地打转），插话另记进
// basicInterjectionsCh。
//
// **本轮产出交给 basicFinalCh**，而不是让下游事后回消息列表里倒扫找——那个列表含 prior，倒扫分不出「本轮产出」和
// 「上一次调用的答复」（见 basicFinalCh 声明处）。**无条件写**（不只在「无 tool_call」时写）：channel 是 LastValue，
// 多轮 ReAct 里最后一轮的产出自然胜出。而最后一轮有两种收尾——无 tool_call 终止、或调了 return-direct 工具终止——
// **两种都该由这条 assistant 代表本轮**：无 tool_call 时它就是模型的答复；return-direct 时它是模型在转交 / 路由**之前
// 说的话**，丢掉它会真丢东西——session events 只存 Exit 这一条，Handoff 的后接 agent 完全靠它看前接说了什么
// （Rewrite 模式的接力语义）。
func basicModelWrites(notes, inter []*schema.Message, out *schema.Message, late []*schema.Message, compaction *compactionMemo) []compose_v2.ChannelWrite {
	produced := make([]*schema.Message, 0, len(notes)+len(inter)+1+len(late))
	produced = append(append(append(append(produced, notes...), inter...), out), late...)
	writes := []compose_v2.ChannelWrite{
		compose_v2.Write(basicMessagesCh, produced),
		compose_v2.Write(basicRoundCh, 1), // 累加 round
	}
	if len(inter)+len(late) > 0 {
		writes = append(writes,
			compose_v2.Write(basicRepeatChainCh, &repeatToolState{}),
			compose_v2.Write(basicInterjectionsCh, append(append(make([]*schema.Message, 0, len(inter)+len(late)), inter...), late...)))
	}
	if compaction != nil {
		writes = append(writes, compose_v2.Write(basicCompactionCh, compaction))
	}
	return append(writes, compose_v2.Write(basicFinalCh, out))
}
