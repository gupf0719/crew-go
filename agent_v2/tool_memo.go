package agent_v2

import (
	"context"
	"log/slog"
	"strconv"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// toolCallMemo 是 tools 节点的**节点私有中断态**载荷——记「本轮哪些 tool_call 已经跑完、
// 产出是什么」，让中断重跑时跳过已成功的那几个、不重放副作用。
//
// 载体机制见 `compose_v2/node_state.go`（键、生命周期、为什么不需要清除点）；用法见
// `makeToolsFn` 里 memo 那段注释。
//
// **为什么整条 Message 都存而不只存 content**：tool_result 上挂着 4 层压缩的跨层 marker、
// return-direct 标记、失败的 code/message（`ContentBlocks[0].Toolresult`）——只存文本会
// 让重跑那批静默丢标记，压缩和 return-direct 行为跟首跑不一致。
type toolCallMemo struct {
	// Done 键是 `(tool_call ID, 批内下标, 工具名)` 三维拼串——**不是裸 tool_call ID**。
	// 键的构造和三个维度各自的理由见 `basic_agent.go` 的 `memoKey`；一句话概括：
	// 框架别处不假设 tc.ID 唯一（同批重复 ID 会让后跑的静默命中前一个），而工具名
	// 在固定内容没存下来时可能被 `OnToolStart` 改写（同一个 (ID,下标) 两次 Resume 指向不同工具）。
	Done map[string]*toolMemoEntry `json:"done,omitempty"`

	// Approved 是「这次调用用户已经批过了」的集合，键与 Done 同一把（`memoKey`）。
	//
	// # 为什么放这里，而不是像原先那样放 RunSession KV
	//
	// 它原先按**稳定 InterruptID**（`(graphNS, nodeID, visit, branchIdx, idx)` 哈希）存在
	// session 上。那个 ID 的五个维度**在新的顶层调用里全部重置** ⟹ 任务 B 的第一个工具
	// 调用跟任务 A 的算出同一个 ID ⟹ 任务 A 批过的在任务 B 里被静默命中。实测坐实。
	//
	// 对照六家的结论是：**没有任何一家把位置坐标用在寿命超过一个 checkpoint 的容器上**。
	// 用位置坐标的那家只往 `pending_writes` 里放（寿命=一个超步），而寿命最长的两家
	// （一个会话 / 跨进程落盘）键都做成了内容化的。「长寿容器 + 位置键」这个组合在六家里
	// 是空集——那正是我们原先的形状。所以病根不是「键不够独特」，是**把只在一个
	// checkpoint 内有意义的键，放进了活得比 checkpoint 长的容器**。
	//
	// 搬进 NodeState 之后三件事同时成立，而且都是**结构上**成立、不靠谁记得清：
	//   - 键是 `(NodeID, InstanceID, Visit)` 而 visit **只在成功路径递增** ⟹ 中断重跑时
	//     键完全重合（批过的不重复问），节点成功后旧 memo 自然不可达；
	//   - **顶层 BasicAgent 下**「新任务 = 新 cp = 无 NodeStates」⟹ 跨任务泄漏不可能发生。
	//     ⚠️ 嵌套时只有顶层 Run 会 `WithFreshRun`，嵌套刻意保留 auto-load（拿掉它嵌套 HITL
	//     直接从 12/12 掉到 3/12，那条注释就在 `runGraph` 里）。被放弃的任务留下的子 cp 不会被
	//     下一个任务捡起来，靠的是存档里的任务代号：auto-load 只认本任务的（见
	//     `compose_v2.Checkpoint.BelongsToTask`）。这一维引入之前确实会被捡起——新任务的输入
	//     根本没进图、整个被旧 cp 吞掉，审批复用只是它最显眼的症状；
	//   - 键里编了 tc.ID（`memoKey`），即使容器意外活长了也不会误命中。
	//
	// 曾经靠一个显式清除点（顶层 Run 时清 session KV）兜这件事——那是**正确性补丁**：
	// 谁忘了调、或「任务边界」的判据跟别处走岔，缺陷立刻静默复现。对照下来没有一家把
	// 正确性押在必须记得调的清除点上。那个清除点现已降级为纵深防御。
	Approved map[string]bool `json:"approved,omitempty"`

	// Decided 是「这次调用的审批结论」（启发式与 ApprovalMode 合出的 Allow / Ask / Deny），键与 Done / Approved
	// 同一把。审批生效时结论第一次形成就写进来，中断续跑、失败续跑时重放、不再按配置重新形成——见
	// [approvalDecisionOf]。重放成立的前提是内容不变，由 fixToolCallContent 保证。
	//
	// 放这里的理由同 Approved：寿命是「这一次调用」，随 cp 走，新任务的 NodeStates 是空的。
	Decided map[string]ApprovalDecision `json:"decided,omitempty"`
}

// toolMemoEntry 一个已完成 tool_call 的产出。
type toolMemoEntry struct {
	// ToolMsg 是 tool_result 消息（`role=tool`，带 CallId 配对锚点）。
	ToolMsg *schema.Message `json:"tool_msg,omitempty"`
	// BodyMsg 是 P2.D inline skill body 那条附加 UserMessage（多数工具为 nil）。
	BodyMsg *schema.Message `json:"body_msg,omitempty"`
	// Signals 是这次调用做出的上行信号（转交 / 路由决定，多数工具为空）——工具自己做出的
	// （transfer_to_agent / route_to），以及它里面经 RunSubAgent 等嵌套调用交上来的。
	//
	// 命中记账、函数体被跳过时由框架原样交出——否则 tool_result 齐全而决定丢了，且静默
	// （业务工具里经 RunSubAgent 定的路由，在同批兄弟工具中断续跑之后落到兜底，实测）。
	// 记的是框架在这次调用期间**收到的**，不靠工具自己声明：转交 / 路由只能经框架那几个入口
	// 做出（见 side_signal.go）——手写一份信号 JSON 冒充不了，因为收件人由引擎铸、算不出来。
	// 结果是失败的（超时、tool.Failure）也照记——作废在 OnToolEnd 之后按钩子改写后的结果判，命中的是原样
	// （这一批没处理完就停下了）时钩子照样 fire、按同一套规则重判（见 tool_batch_signals.go）；定下了的
	// （Final）不再交给钩子，按定下时的结论判。
	Signals compose_v2.Signals `json:"signals,omitzero"`
	// Final 报 ToolMsg 是这一批处理完（OnToolEnd、批内裁决、按大小处理）之后定下的那份，不是工具刚返回的原样——
	// 见 [finalizeToolMemo]。
	Final bool `json:"final,omitempty"`
	// HookSignals 是定下时这次调用的 OnToolEnd 钩子做出的上行信号（只在 Final 时有意义）：定下了的续跑时不再交给
	// 钩子（见 fixedToolResults），这里记的原样交出。
	HookSignals compose_v2.Signals `json:"hook_signals,omitzero"`
}

// shouldMemoize 判断这次 tool_call 的产出能不能进重放记账。
//
// **判据是「这次调用的副作用能不能重放」，不是「工具叫什么名字」。**
//
//   - 做出了上行信号的（转交 / 路由决定）：那半随记账进 cp，命中时原样交出 ⟹ 可以进
//     记账，函数体不必重跑。
//
//   - 没做出上行信号的框架元工具：产出不止 tool_result 而框架无从重放 ⟹ 排除在记账外，靠
//     函数体重跑补回来。命中跳过的后果是**静默**的：err=nil、tool_result 齐全，
//     只是事没发生。
//
//     ⚠️ `tool_search` **仍必须排除，但理由已经变了**：它的已解锁集现在进 channel
//     state（随 cp 走），不再是当初那条「副作用不进 cp」。现在的理由是**写 channel
//     这一步发生在工具体里**——命中记账跳过函数体，就没人往 channel 写。写错理由
//     下一个人很可能顺手把它从名单里"优化"掉，那会让同批中断场景静默退化。
//     判据动之前先看 tool_search_resume_test.go 那四条哨兵。
//
//   - 业务工具：默认进记账；它里面经嵌套调用交上来的决定同样随记账走。
//
// minted 是「这次派发的那件是不是框架铸的」（[isFrameworkMinted]），不是按名字判——业务工具恰好叫
// plan / respond / final_output 时按名字判会被排除在记账外，兄弟工具中断续跑后它再执行一次（实测）。
func shouldMemoize(minted bool, signals compose_v2.Signals) bool {
	return signals.Len() > 0 || !minted
}

// frameworkControlTool 是「这件工具是框架自己铸的」的**封闭标记**——接口方法未导出，
// 业务实现不了。return-direct 收尾时据它区分「这是框架那件 transfer_to_agent / route_to」与业务
// 自己注册的同名工具（见 [ReturnDirectMintedAnnotation]）。`final_output` 不盖这个章——它不是控制工具，
// 它的产出就是答复——它有自己的封闭标记（frameworkFinalOutput，见 finalOutputAttemptAnnotation）。
//
// # 为什么不能用工具名判
//
// **那些名字业务占得到**：没被授予转交时，框架刻意不挂软失败桩（`basic_agent.go` 那段注释写明
// 理由——否则任何一个恰好叫 `transfer_to_agent` 的业务工具会被框架静默顶掉），于是业务那件
// 同名工具留在 toolkit 里；`route_to` 同理（没被授予且业务占了这个名字时不挂桩）。
type frameworkControlTool interface{ isFrameworkControlTool() }

// frameworkTool 把框架自己铸的工具打上封闭标记。
type frameworkTool struct{ tool.Tool }

func (frameworkTool) isFrameworkControlTool() {}

// markFrameworkTool 给框架铸的控制流工具盖章。**新增框架控制流工具时必须包一层**，
// 否则 return-direct 收尾时它被当成业务工具。
func markFrameworkTool(t tool.Tool) tool.Tool { return frameworkTool{Tool: t} }

// isFrameworkControlToolIn 判断 toolkit 里这个名字下挂的是不是框架那件控制工具（transfer_to_agent /
// route_to）。「是不是框架铸的」这个更宽的问题用 [isFrameworkMintedIn]。
func isFrameworkControlToolIn(tk *tool.Toolkit, name string) bool {
	if tk == nil {
		return false
	}
	_, ok := tk.Get(name).(frameworkControlTool)
	return ok
}

func init() {
	// 跨 cp 边界持久化 ⟹ 必须注册（同 channel value 一套 tagged JSON 协议）。
	compose_v2.RegisterValueType[*toolCallMemo]("*agent_v2.toolCallMemo")
}

// toolMemoKey 是工具账里一条记录的键：`(tc.ID, 批内下标, 工具名)`，理由见 makeToolsFn 里 memoKey 那段。
func toolMemoKey(callID string, idx int, toolName string) string {
	return callID + "#" + strconv.Itoa(idx) + "#" + toolName
}

// fixedToolResults 报这一批里哪些调用的结果已经定下（见 finalizeToolMemo），以及定下时它们的 OnToolEnd 钩子做出的决定。
//
// 定下了的续跑时**不再处理**：工具不执行（工具账命中）、OnToolStart / OnToolEnd 都不触发——那一步的处理首跑已经做完。
// 再交给钩子的只能是处理完的那份（转存说明、钩子自己改写过的结果），不幂等的钩子在它上面报错，续跑就永远过不去
// （审计实测）；两个钩子一起不触发，钩子在 OnToolStart 开的 span 也不会落单。
//
// 在 OnToolStart 之前判：工具账的键用这一批定下的调用内容（见 fixedToolCallContentOf），这一批没交给工具执行过时
// 没有定下的。original 是模型给的原样。
func fixedToolResults(ctx context.Context, original []schema.ToolCall) ([]bool, []compose_v2.Signals) {
	content, ok := fixedToolCallContentOf(ctx, original)
	if !ok {
		return nil, nil
	}
	memo, ok := compose_v2.GetNodeState[*toolCallMemo](ctx)
	if !ok || memo == nil || len(memo.Done) == 0 {
		return nil, nil
	}
	fixed := make([]bool, len(content))
	sigs := make([]compose_v2.Signals, len(content))
	for i, tc := range content {
		if e := memo.Done[toolMemoKey(tc.ID, i, tc.Function.Name)]; e != nil && e.Final && e.ToolMsg != nil {
			fixed[i], sigs[i] = true, e.HookSignals
		}
	}
	return fixed, sigs
}

// finalizeToolMemo 在这一批调用都处理完（OnToolEnd、批内裁决、按大小处理）之后，把这次执行的工具账里这几个调用的
// 产出换成处理完的那份、标为定下（Final），连同当时 OnToolEnd 钩子做出的决定（hookSignals[i]）重存一次。
//
// 三条不变量：
//
//   - **一次调用的结果在这一批第一次处理完时定下**：之后这次执行在提交之前作废（节点成功之后的钩子 Goexit、提交
//     出错）、续跑时不再处理（见 fixedToolResults）——工具不执行、OnToolStart / OnToolEnd 不触发，交出定下的那份：
//     内容、结论（成没成）、钩子做出的决定都跟那次处理完的一样。不定下的话钩子在续跑时重新处理一遍，而工具账里留的
//     已经不是它第一次看到的东西（开了转存时是转存说明）：按全文做判断的钩子会把首跑判成功的翻成失败，模型看到的
//     结论跟着翻；定下了却照样交给钩子的话，不幂等的钩子在处理完的那份上报错，续跑永远过不去（两条均审计实测）。
//     被批内裁决换掉的那几条定下的是替换后的失败结果：续跑时它按失败判、它的决定作废，生效的仍是首跑排在前面的那个。
//   - **处理完之前作废的照常重新处理**：同批别的调用中断或出错、OnToolEnd 报错时走不到这里，工具账里还是工具刚返回的
//     原样，续跑时钩子在它上面第一次处理，改写照常生效。
//   - **一次 Run 里工具账不随轮数 × 原文大小增长**：开了转存时，工具账在每个调用成功那一刻记下的是「待处理」的全文
//     （转存在 OnToolEnd 之后才做，中断的那一批续跑时钩子要看到全文）；节点私有态在一次 Run 里不设清除点，每一轮的账
//     都留在内存里——实测 20 轮、每轮 2MB 的输出，Run 结束前多占 80MB。定下的是处理之后的那份，内存回到预览的量级。
//
// 没开转存也定下：结果定下之后不再变这条规矩跟结果多大、配没配转存无关。代价是每一批多存一次工具账。
func finalizeToolMemo(ctx context.Context, toolCalls []schema.ToolCall, results []*schema.Message, hookSignals []compose_v2.Signals) {
	memo, ok := compose_v2.GetNodeState[*toolCallMemo](ctx)
	if !ok || memo == nil || len(memo.Done) == 0 {
		return
	}
	changed := false
	for i, tc := range toolCalls {
		if i >= len(results) || results[i] == nil {
			continue
		}
		e := memo.Done[toolMemoKey(tc.ID, i, tc.Function.Name)]
		if e == nil || e.ToolMsg == nil || e.Final {
			continue
		}
		cp := *results[i]
		e.ToolMsg, e.Final = &cp, true
		if i < len(hookSignals) {
			e.HookSignals = hookSignals[i]
		}
		changed = true
	}
	if !changed {
		return
	}
	// 存不下时工具账停在定下之前：续跑照常重新处理（改动之前的行为），这一批转存前的全文留在内存里直到 Run 结束。
	if err := compose_v2.SaveNodeState(ctx, memo); err != nil {
		slog.WarnContext(ctx, "agent_v2: 定下工具账失败，续跑时这一批会重新处理", "err", err)
	}
}
