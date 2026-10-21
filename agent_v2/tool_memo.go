package agent_v2

import (
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
	// 结果是失败的（超时、tool.Failure）也照记——作废在 OnToolEnd 之后按钩子改写后的结果判，续跑命中
	// 时钩子照样 fire、按同一套规则重判（见 tool_batch_signals.go）。
	Signals compose_v2.Signals `json:"signals,omitzero"`
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
