package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync/atomic"

	"crew-go/schema"
)

// AskQuery 是 NodeFunc 抛 InterruptError 时附带的用户询问 payload。
//
// 字段：Question / Options / InterruptID / Reason 四个核心字段，外加
// ToolCallID / ToolName 两个**可选的**工具身份（空 = 这条挂起不指向任何待执行动作，
// 见下面那两个字段自己的说明）。ns 维度不在这里——cp 按 (sid, ns) 物理分离寻址。
//
// 所有字段必须 JSON-friendly——AskQuery 整体随 cp 持久化，不做类型保真。
type AskQuery struct {
	// Question 给 user 看的问题
	Question string `json:"question"`
	// Options 可选项；nil = 自由文本
	Options []string `json:"options,omitempty"`
	// InterruptID 这条挂起的稳定身份——调用方拿它配答案，跨 Resume 必须重现得出同一个值。
	//
	// **必填。框架不替你补**：空 ID 会被当场拒（`ErrRunAborted`，不可重试、存档不留）。
	// 同一个 `InterruptError` 里两条 query 用相同 ID 也拒——第二条会被静默丢弃，
	// 用户答一条却可能触发另一条。
	//
	// # 谁来填，取决于你走哪条路
	//
	//  1. **走 [Ask]（推荐，也是唯一能取到答案的路）**——**这个字段必须留空**，`Ask` 对
	//     非空值当场拒。ID 的一维是**本分支自己的计数器**，当前值只有正在跑的那段代码
	//     知道，所以「算」和「用」必须是同一次调用；任何第二处想重建都是猜（框架替业务
	//     猜过，实测比不补还糟，理由见 `Ask` godoc）。
	//     ⚠️ `Ask` **不写回**你传进去的对象，身份在框架自己的记录上——要读 ID 从抛出来的
	//     `InterruptError.Query` 上读。
	//  2. **自填**——只在**直接构造 `&InterruptError{Query: ...}`** 那条路上合法，语义是
	//     **给前端一个自己认得的标签**（典型是钩子相位的「暂停」按钮，它抛完不回查）。
	//     要保证跨 Resume 稳定、且同一批内互不相同。
	//
	// ⚠️ **为什么不让 `Ask` 认自填**：身份决定答案投给谁，选择权在业务手里时，两个**不同
	// 的提问**可以落在同一个答案位上——实测三种（跨超步 / 跨 visit / 同节点两问），全部
	// `err == nil`：用户只见过第一问，第二件事跟着第一件的授权办了。**名字可以业务给，
	// 路线只能框架定。** 详见 [NextAnswerSlot] godoc。
	InterruptID string `json:"interrupt_id,omitempty"`
	// Reason 业务可读的中断原因 (per-query, 跟 ie.Reason 区分)。
	//
	// **why 加字段** (audit #5 视角 8 F3 fix): `MergeInterrupts` 聚合 N 个 ie 时
	// 之前只保留聚合后的 `Reason: "merged N interrupts"`, 各 ie.Reason 全丢. fanout
	// 多 tc 抛 Interrupt 业务想区分 "tool_X 想确认 vs tool_Y 想确认" 拿不到. 加
	// per-query Reason 让 MergeInterrupts 拷贝 ie.Reason 到 query 保留上下文.
	//
	// **cp 持久化**: Reason 是 string, 跟 Question/Options 同款随 cp 持久化, 业务跨
	// Resume 拿 cp.PendingInterrupts 仍能读 (区别于 ie.Data any 跨 Resume 丢失).
	Reason string `json:"reason,omitempty"`

	// ToolCallID / ToolName 标出**这条挂起指向哪一次工具调用**——空表示它不指向任何
	// 待执行的动作（`ask_user` 那种纯问答就该留空）。
	//
	// **为什么需要**：两类挂起被放弃时的损失是不对称的。问答型放弃是对称损失（重问
	// 的代价只是一次提问）；而工具审批型，模型**已经在自己的输出里公告过那个
	// tool_call**、它的世界模型里那动作「已发起、等结果」——放弃之后既没执行也没
	// 拒绝，历史里还留下一条没有配对结果的 tool_call。要给它补一条「未执行」的合成
	// 结果，就得知道补给谁。
	//
	// **为什么不能从 InterruptID 反查**：`InterruptID` 是各维度的 fnv hash，单向。
	// tc.ID 确实是其中一维（`InterruptSite.ToolCallID`），但 hash 出来反不回去，
	// 所以要用就得存。
	//
	// **业务自定义工具想要同款能力**时自填即可（框架只在审批路径自动填）。
	ToolCallID string `json:"tool_call_id,omitempty"`
	ToolName   string `json:"tool_name,omitempty"`
}

// InterruptError 表示节点主动挂起等待用户输入——错误作为控制流模式。
//
// NodeFunc 抛此 error 时，Run 路径：
//  1. 记录当前节点 + AskQuery 到 cp（Status=Pending）
//  2. 不应用本节点的 writes（与失败分支同款，避免半完成状态）
//  3. 返 InterruptError 给调用方，调用方可通过 cm.Snapshot 拿到 cp 持久化
//
// 节点里**别手工拼这个结构**——走 [Ask]，它把「算身份 → 查答案 → 没有就抛」三步合成
// 一次调用（身份必须由框架按执行位置派生，理由见 [NextAnswerSlot]）：
//
//	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
//	if err != nil {
//	    return nil, err // 没答案 ⟹ err 是 InterruptError，原样冒泡
//	}
//	// 用 ans.Content 决定行为
//
// Resume 时调用方按**抛出来那条中断上的 ID** 提供答案（身份是框架派生的 16 位 hex，
// 业务这边不该有字面量）：
//
//	ie, _ := compose_v2.IsInterrupt(err)
//	answers := map[string]*schema.Message{
//	    ie.AllQueries()[0].InterruptID: schema.UserMessage("yes"),
//	}
//	compose_v2.Run(ctx, g, cm,
//	    compose_v2.WithResumeFrom(snap),
//	    compose_v2.WithResumeAnswers(answers))
type InterruptError struct {
	Reason string
	// Query 单 query 模式（HITL 单点中断，99% 业务场景）。**与 Queries 互斥**——
	// 业务只填其一，详见 AllQueries godoc。
	Query *AskQuery
	// Queries 多 query 模式（ISSUE-81：toolsFn fanout 多 tc 同时抛 Interrupt）——
	// **与 Query 互斥**，业务自填 InterruptError 时只填一个字段。两者都填时
	// Queries 优先（AllQueries 协议），Query 被忽略——业务可能误期望合并。
	// MergeInterrupts helper 把多个 InterruptError 聚合到 Queries 让 graph_loop
	// 能 record 全部 query 防 N-1 query 永久丢失（参考 audit Round 6 Bug 3
	// SendFrom 同款 fix）。
	Queries []*AskQuery
	// Data 是业务自定义元数据——**仅同进程透传**，NodeFunc 抛 InterruptError 时
	// 让 caller / hook 能拿到额外上下文（如 active agent 名 / round 号）。
	//
	// **⚠️ 跨 Resume 不持久化**：framework 把 `cm.pendingInterrupts []*AskQuery` 存
	// `cp.PendingInterrupts`（`compose_v2/serialization.go::Snapshot`）——**只存
	// AskQuery 全字段（Question/Options/InterruptID/Reason + 可选的 ToolCallID/
	// ToolName）**，不存 InterruptError 自身
	// 因此 Data 字段。Resume 时 caller 拿到的 InterruptError 是 nil（业务通过
	// `cp.PendingInterrupts` 读 AskQuery 不读 InterruptError.Data）。
	//
	// 业务想跨 Resume / 跨进程保留元数据:
	//   (a) 把元数据塞 AskQuery.Question / Options (业务自序列化为字符串)——cp
	//       持久化路径走 AskQuery 持续保留
	//   (b) 走 `store.Store` 后端 (Redis / PG / mem0) 跨 thread 持久化, 用
	//       AskQuery.InterruptID 作 store key 关联——见 [`doc/v2/store.md`](../../doc/v2/store.md)
	//
	// **不推荐** `RunSession.SetValue/GetValue` 跨 Resume——sess.values 是 per-sess
	// in-process KV 不走 cp (agent_v2/session.go:506 godoc 明示), 跨进程 Resume
	// (server 重启 / 分布式 worker / 长时间挂起后业务 caller 释放 RunSession)
	// 同样丢失。仅同进程 caller 持 RunSession 指针时可用作"InterruptError.Data
	// 的等价 in-process 透传"——但 InterruptError.Data 本身已是同进程透传, 用
	// SetValue 多此一举。
	//
	// audit follow-up #5: 此前 godoc 谎报 "cp 持久化按 JSON-friendly 序列化"
	// 实际从未实现, 业务依赖此承诺会静默失败 (Resume 后 Data nil).
	// audit follow-up #5 #2: 此前修法又错推荐 SetValue 跨 Resume 路径, sess.values
	// 同样不走 cp——本轮 fix 改为 store.Store 跨进程持久化层 + 删 SetValue 推荐.
	Data any

	// detached 为真表示这是一次独立运行（[DetachExecution] 之后开的）的根图挂起时交出去的中断，detachedSID
	// 是那个运行的 sid：它属于那个任务，所在任务的节点把它返回时引擎拒收（validateInterrupt）。挂在对象上而
	// 不只挂在外面那层包装上：业务常把 [IsInterrupt] 取出来的这个对象直接返回或再包一层，包装就丢了
	// （实测：曾经只认包装，那两种写法下外层把它当成自己的挂起收下，续跑每次换一个 ID 再问、永不收敛）。
	// 打在交出去的那份拷贝上、不改业务的原对象（见 detachedInterruptError.marked）。不进存档（存档只存
	// AskQuery）；[MergeInterrupts] 合并时往下传。
	detached    bool
	detachedSID string
}

// AllQueries 返此 InterruptError 含的所有 query。Queries 多 query 模式优先，
// 否则 fallback 到 [Query] 单 query 兼容路径。
//
// **why 协议**：framework 的 record / 校验路径都通过此 helper 统一访问，业务自填
// InterruptError 也走同协议（单 query 用 Query 字段，多 query 用 Queries），
// 不需 if-else 分两套。
//
// **重要：Query 与 Queries 字段互斥**（视角 D LOW-1 audit 文档化）：
//   - 业务自填 InterruptError 时**只填一个字段**：单 query 用 Query，多 query 用 Queries
//   - 两字段都填时 **Queries 优先**，Query 字段被忽略——业务可能误期望两者合并
//   - 框架 MergeInterrupts helper 自动维持互斥（聚合时只用 Queries 不用 Query）
//
// **严格非空协议**（R2 audit + R4-H MED-3 文档化）：framework graph_loop 在
// **node 路径**fail-fast 不允许 `len(AllQueries()) == 0`——业务抛
// `&InterruptError{Reason: "..."}` 但 Query / Queries 都不填会被 framework
// 直接转普通 error 返业务（非 HITL 控制流）。业务"暂停按钮"等无真 question 场景
// 必须填**虚拟 Query**（典型：`&AskQuery{Question: "已挂起..."}`）让 framework
// 识别为合法 HITL signal。
//
// **cond fn 路径不适用**：cond 是纯路由判断、**禁止 interrupt**（cond interrupt 会把
// 已成功成员拉回重跑、违反「成功成员不重跑」契约）——cond 抛任何 InterruptError
// 都被 graph_loop 一律转非-interrupt error 拒绝，不走本 helper。
func (e *InterruptError) AllQueries() []*AskQuery {
	if e == nil {
		return nil
	}
	if len(e.Queries) > 0 {
		return e.Queries
	}
	if e.Query != nil {
		return []*AskQuery{e.Query}
	}
	return nil
}

func (e *InterruptError) Error() string {
	// nil 守卫跟 AllQueries / Validate 对齐——它是三个方法里唯一漏的。
	//
	// **typed nil 是从业务侧真能进来的**：业务 tool / sub-agent 写
	// `var ie *InterruptError; return nil, ie`（CLAUDE.md 记的 Go interface typed-nil
	// 陷阱）时，`IsInterrupt` 会返 `(nil, true)`——于是这个 typed nil 被当成合法中断
	// 收进错误链。单条链上无害（`%w` 只记 wrap 关系不调 Error），但一旦跟别的错误
	// 一起进 `errors.Join`，`joinError.Error()` 会逐个调成员的 Error()，这里就炸。
	//
	// 实测后果不是 crash（fmt 自带 recover），是**整条文案被吞掉**变成
	// `%!v(PANIC=Error method: ...)`——中断信息和 stale 提示全没了，而观测层会把这坨
	// 记进 trace 的 error.message。语义（IsInterrupt / AllQueries / Resume）不受影响，
	// 纯粹毁诊断。
	if e == nil {
		return "interrupt: <nil>"
	}
	if e.Reason != "" {
		return fmt.Sprintf("interrupt: %s", e.Reason)
	}
	if len(e.Queries) > 0 {
		// **audit LOW fix**: Queries[0] 可能 nil（业务填含 nil 元素的 Queries）——裸
		// e.Queries[0].InterruptID deref 在**业务栈** panic（Error() 经 fmt.Errorf %w /
		// slog 触发时不受 framework recover 保护）。守卫返可读串（Validate 已在 framework
		// 路径 fail-fast，此守卫防未经 Validate 的裸 ie 调 Error()）。
		if e.Queries[0] != nil {
			return fmt.Sprintf("interrupt: %d ask user (first id=%s)", len(e.Queries), e.Queries[0].InterruptID)
		}
		return fmt.Sprintf("interrupt: %d ask user (first query nil)", len(e.Queries))
	}
	if e.Query != nil {
		return fmt.Sprintf("interrupt: ask user (id=%s, q=%q)", e.Query.InterruptID, e.Query.Question)
	}
	return "interrupt"
}

// PickFanoutError 从一组**并行子调用**的错误里挑出返给调用方的那**一个**。
//
// 判据是 `终局失败 > 坏存档 > 档没写成 > 中断 > 第一个业务错`。为什么中断优先、为什么业务错绝不聚合成多错误树，
// 完整理由在 `frontierResult.exitErr` 的 godoc——那是这套判据的权威说明。
//
// **这不是那份判据的拷贝，是同一份**：图引擎的超步仲裁和它一起走下面的
// `pickFanoutError`。框架里别处扇出（如长期记忆按写入者分片检索）用公开的这个入口。
//
// errs 按子调用的固定顺序传，nil 元素跳过。
//
// # 调用方必须知道的三件事
//
//  1. **≥2 个中断时返的是一个 [errors.Join] 复合体**（合并后的 InterruptError 打头 +
//     各分支原始链）。于是 `errors.Is` 在它上面是 **OR**——「有没有哪个分支是 X」，不再是
//     「这个错是不是 X」。一个中断分支的链上挂着 [ErrCheckpointStale]，整体就判 stale，兄弟那条
//     完全可答的干净中断跟着一起判死。这是有意的 fail-closed，但株连是真的。（引擎出口写档撞上
//     陈旧写回时返 [ErrCheckpointNotPersisted]、中断不在链上，走上面单独那一档。）
//  2. **元素应当来自互异的子调用**。合并**不去重**：N 个元素带同一个 `InterruptID` 会合成
//     一批撞 ID 的 query，被 `validateInterrupt` 当协议违规拒（`ErrRunAborted` + 清存档）。
//     一个逻辑提问被扇成 N 份时**别送进来**——在扇出侧当场停（记忆分片检索就是这么做的）。
//  3. **会改写入参**：[MergeInterrupts] 往元素的 `*AskQuery` 上写 `Reason`，且结果里的
//     `Queries` 与入参共享指针。别拿包级哨兵 `var ErrX = &InterruptError{...}` 当元素传。
func PickFanoutError(errs []error) error {
	var raws []error
	var firstErr error
	for _, e := range errs {
		if e == nil {
			continue
		}
		// **终局失败严格支配中断**，理由见 exitErr 的 godoc。
		//
		// 这一档曾经外包给调用方（godoc 写「自己在调用之前先判」），实测那是错的：
		// 框架自己的第一个调用方就没照做，而漏掉的后果完全静默——返中断 ⟹ 落 Pending 档
		// ⟹ **人被问了、人答了** ⟹ Resume 时 abort 赢 ⟹ 出口 purge 把刚给的答案连同
		// 兄弟分支的工作一起清掉。一次白问 + 一次销毁，信息量为零。
		//
		// 按本仓规约，「每个调用方各留一份框架判据的拷贝」是设计债的自白；判据搬进来，
		// 义务连同它必然产生的拷贝一起消失。
		if errors.Is(e, ErrRunAborted) {
			return e
		}
		if firstErr == nil {
			firstErr = e
		}
		if _, isInt := IsInterrupt(e); isInt {
			raws = append(raws, e)
		}
	}
	// **坏存档同样严格支配中断**（跟引擎层 `frontierResult.exitErr` 的阶梯一致）。
	//
	// 判据跟上面 `ErrRunAborted` 那档同一条：**回答这个问题有没有可能有用**。
	// 有中断的兄弟时报中断看着更「友好」，实际更糟——那个提问注定答不成（存档里没有可
	// 执行的东西），用户答完只会再撞同一份坏档。消费方（a2a server）从**返回值**算
	// `keepPending`，报中断就把任务挂成 input-required、把问题发给远端，等一个不可能成功
	// 的续发。**实测过**：工具甲返坏档 + 工具乙 `Ask`，Run 返中断、用户答了，Resume 才
	// 撞上坏档——那一问白问。
	//
	// ⚠️ 单独一遍循环（不并进上面那个）是因为**顺序承重**：`ErrRunAborted` 要严格排在它
	// 之前（前者捆绑清 cp 子树、是更终局的处置），而 `errs` 的下标序不保证两者的相对位置。
	//
	// ⚠️ 跟 `ErrRunAborted` 的区别在**后果**不在报告：那个会让 Run 出口主动清 cp 子树，
	// 而这份坏档**要留着排查**。这里只借用顺序，不借用后果。
	for _, e := range errs {
		if e != nil && errors.Is(e, ErrCheckpointNotResumable) {
			return e
		}
	}
	// **档没写成同样支配中断**（跟 `frontierResult.exitErr` 同一档、同一个顺序）：那个子调用的档
	// 没写成，用户答完兄弟的提问续跑，它也续不上。槽属于别人时它不带 ErrRunAborted，上面第一遍
	// 循环接不住它。
	for _, e := range errs {
		if e != nil && errors.Is(e, ErrCheckpointNotPersisted) {
			return e
		}
	}
	return pickFanoutError(raws, firstErr)
}

// pickFanoutError 是判据本体。interruptRaws 必须是抛了中断的那些子调用的**原始
// error**（不抠不重建——链上并列挂的第二 sentinel 靠它保住），firstErr 是按顺序
// 第一个非 nil 的错。
func pickFanoutError(interruptRaws []error, firstErr error) error {
	switch len(interruptRaws) {
	case 0:
		return firstErr
	case 1:
		return interruptRaws[0]
	default:
		ies := make([]*InterruptError, 0, len(interruptRaws))
		for _, raw := range interruptRaws {
			if ie, ok := IsInterrupt(raw); ok {
				ies = append(ies, ie)
			}
		}
		// 合并后的 ie 打头（`asInterrupt` 对多错误树 DFS 从左到右，放前面让 IsInterrupt
		// 最快命中），各分支原始链并列挂后面保住各自的第二 sentinel。这里聚合安全的
		// 理由见 exitErr——挂上去的每一条都来自抛中断的分支，全是控制流信号。
		joined := make([]error, 0, len(interruptRaws)+1)
		joined = append(joined, MergeInterrupts(ies))
		joined = append(joined, interruptRaws...)
		return errors.Join(joined...)
	}
}

// MergeInterrupts 把多个 InterruptError 聚合成单个 InterruptError——toolsFn fanout
// 化时多 tc 同时抛 Interrupt，需把 N 个 ie.Query 收集到单个 InterruptError.Queries
// 让 graph_loop 单一 NodeFunc return 路径仍能 record 全部 query 不丢。
//
// **协议**：
//   - len(ies)==0 返 nil
//   - len(ies)==1 直接返该 ie 不包装（保兼容 errors.As / IsInterrupt）
//   - len(ies)>=2 创建新 InterruptError 收集所有 ie.AllQueries() 到 Queries 字段
//
// **使用场景**：业务自定义 toolsFn / makeToolsFn fanout 多 tc 时；其它单 query
// 场景（preamble/postamble/单 tool 抛 Interrupt）直接用 `&InterruptError{Query: q}`
// 不需走此 helper。
func MergeInterrupts(ies []*InterruptError) *InterruptError {
	// audit¹¹ HIGH-1 fix: 过滤 nil 元素 — 业务自定义 toolsFn 并发抛 Interrupt 时
	// 收集 slice 可能含 nil 元素 (有些 goroutine 没抛 / err 不是 Interrupt). 旧代码
	// `for _, ie := range ies { queries = append(queries, ie.AllQueries()...) }` 不
	// 检 ie nil → ie.AllQueries() nil deref panic.
	filtered := make([]*InterruptError, 0, len(ies))
	for _, ie := range ies {
		if ie != nil {
			filtered = append(filtered, ie)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	// audit #5 视角 8 F3 fix: per-query 保留 ie.Reason — fanout 多 tc 业务区分
	// "tool_X 想确认 vs tool_Y 想确认" 上下文. 每 ie 的 query 未填 Reason 时拷贝
	// ie.Reason 上去 (业务自填 query.Reason 优先).
	// 注: ie.Data 仍跨 ie 边界丢失 (any json 不可逆, AskQuery 不存 Meta 字段),
	// 业务想跨 fanout 保 typed metadata 走 store.Store 用 InterruptID 作 key.
	//
	// **audit #5 阶段 1 follow-up MED-A fix**: 单 ie 早返路径**也**拷贝 ie.Reason —
	// 之前单 ie 直接 return filtered[0] 不拷贝, 跟多 ie 行为不一致让业务自定义
	// toolsFn 单 tc 抛 ie 时 q.Reason="" silent 丢失. 现单 / 多 ie 路径统一拷贝行为.
	propagateIEReasonToQueries := func(ie *InterruptError) {
		if ie.Reason == "" {
			return
		}
		for _, q := range ie.AllQueries() {
			if q != nil && q.Reason == "" {
				q.Reason = ie.Reason
			}
		}
	}
	if len(filtered) == 1 {
		propagateIEReasonToQueries(filtered[0])
		return filtered[0]
	}
	var queries []*AskQuery
	for _, ie := range filtered {
		propagateIEReasonToQueries(ie)
		queries = append(queries, ie.AllQueries()...)
	}
	merged := &InterruptError{
		Reason:  fmt.Sprintf("聚合 %d 个 InterruptError (fanout 多 tc 抛, ie.Reason 已拷贝到 query.Reason 见 godoc)", len(filtered)),
		Queries: queries,
	}
	// 合并进来的有一个是独立运行交出的中断，合并之后整个就不是这个任务答得了的（见 InterruptError.detached）
	for _, ie := range filtered {
		if ie.detached {
			merged.detached, merged.detachedSID = true, ie.detachedSID
			break
		}
	}
	return merged
}

// Validate 检查 InterruptError 字段一致性——Query 和 Queries 同时填时返 error.
//
// **why** (audit #5 视角 8 F8 fix): Query/Queries 互斥协议 godoc 已说 "Queries
// 优先 Query 被忽略", 但业务可能误填两者期望合并—— framework 在 graph_loop
// ie 路径 record 前调 Validate 让业务尽早发现 (而非 silent Query 被 ignored).
//
// 业务自填 Queries 是高级路径 (e.g., 业务自定义 fanout toolsFn 自管聚合),
// 99% 业务只填 Query —— Validate 在双填时 fail-fast 帮业务诊断.
//
// 返 nil 表示状态合法. 调用方: framework graph_loop / 业务测试自检.
func (e *InterruptError) Validate() error {
	if e == nil {
		return nil
	}
	if e.Query != nil && len(e.Queries) > 0 {
		return fmt.Errorf("InterruptError: Query and Queries are mutually exclusive (业务只填一个: 单 query 用 Query, 多 query fanout 用 Queries; godoc 说 Queries 优先 Query 被忽略 — Validate fail-fast 防 silent drop)")
	}
	// **audit LOW fix**: 拒绝 Queries 含 nil 元素——下游校验循环 (q.InterruptID)
	// 与 Error() (Queries[0].InterruptID) 都会 deref nil。framework 在
	// **node 路径** record 前先调 Validate fail-fast 挡住 deref（cond 路径已禁 interrupt，
	// 不再调本 helper——cond 抛任何 InterruptError 都被一律拒绝）。
	for i, q := range e.Queries {
		if q == nil {
			return fmt.Errorf("InterruptError: Queries[%d] is nil (业务填 Queries 不能含 nil 元素)", i)
		}
	}
	return nil
}

// IsInterrupt 检查 error 是否是 InterruptError。
func IsInterrupt(err error) (*InterruptError, bool) {
	if err == nil {
		return nil, false
	}
	var ie *InterruptError
	if asInterrupt(err, &ie) {
		return ie, true
	}
	return nil, false
}

func asInterrupt(err error, target **InterruptError) bool {
	for err != nil {
		if ie, ok := err.(*InterruptError); ok {
			*target = ie
			return true
		}
		// 独立运行交出的中断：交带记号的拷贝，不往下走到原对象（见 detachedInterruptError.marked）
		if d, ok := err.(*detachedInterruptError); ok && d.marked != nil {
			*target = d.marked
			return true
		}
		// 简化版 errors.As——手动走 Unwrap 链找 *InterruptError（只认这一种目标类型，
		// 不用 stdlib errors.As 省 reflect 开销）。**必须同时认单链 Unwrap() error 与
		// 多错误树 Unwrap() []error**（Go 1.20+ errors.Join）：漏 []error 分支会让
		// errors.Join 包的 InterruptError 检测不到——graph_loop.go 两条路都靠它：部分中断 +
		// 同超步 ApplyAll 失败走 `errors.Join(branchErr, applyErr)`，以及**多个分支同时
		// 中断**走 `exitErr` 的 `errors.Join(合并后的中断, 各中断分支的原始链...)`（后者
		// 挂原始链是为了保住各分支并列的第二 sentinel，`MergeInterrupts` 造的新 ie 没有
		// Unwrap、只返它等于把 N 条链全扔了）。漏此分支
		// 把 HITL 中断误判成致命错、Resume 闭环静默失效。语义对齐 stdlib errors.As：先试单链、再
		// DFS 多链、从左到右第一个命中即返（单链是多链的退化，行为一致）。
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if asInterrupt(e, target) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// ── ResumeContext + ctx 协议 ──

// ResumeContext 携带 Resume 时调用方注入的"用户答案"，节点函数通过
// GetResumeAnswer 按 InterruptID 取出。
//
// 设计对照现有 compose.ResumeContext：UserInputs 类型 map[string]*schema.Message
// 让答案保留完整 ContentBlocks/Annotations/Role 字段——支持多模态（图片/音频）
// + 元数据（trace_id/locale）+ 角色提示（user/tool）。
//
// **audit Round 9 Bug B1 修复**：之前 UserInputs map[string]string 只承载文本，
// agent_v2 conversion 层把 *schema.Message 强转 map[string]string 静默丢
// ContentBlocks/Annotations/Role/Refusal/Metadata 五个字段（基本可用性破坏）。
// 改为 map[string]*schema.Message 端到端保完整字段；helper GetResumeAnswerText
// 给只关心文本的业务方便提取。
type ResumeContext struct {
	// UserInputs 按 InterruptID 精确配对答案——**寻址型**：这条答案是给某个具体
	// 提问的。节点走 `Ask`（推荐，身份由框架派生）或 `NextAnswerSlot` 取自己那条。
	//
	// key = AskQuery.InterruptID（节点抛中断时填的 ID）
	// value = *schema.Message（完整保留 Role / Content / ContentBlocks /
	//         Annotations / Refusal 等所有字段）
	UserInputs map[string]*schema.Message

	// UserText 是**非寻址型**的「用户这轮说了什么」——不指向任何一个提问，谁想据此
	// 改主意谁读（`GetResumeUserText`）。典型消费者是 PlanAgent 的 planner：它不在
	// 节点里、没有自己的 InterruptID，要的是「用户中途说『换个方向』」这句话本身。
	//
	// # 为什么必须跟 UserInputs 分开
	//
	// 这两种语义对「要不要限定给我」的答案**正好相反**——寻址型必须限定（拿错就是
	// 「用户批准的是这件事、系统执行了另一件」），非寻址型不能限定（它压根没有对应的
	// 提问）。混在一个通道里的代价实测过两条：
	//
	//   - 曾有个 `GetResumeAnswerSingle` 取 map 里**任意一条**，于是非寻址型的读法
	//     会吃到别人的答案 —— 实测：任务 A 挂起被放弃、任务 B 挂起，用户答 A 的 ID，
	//     用它取答案的节点执行了 **B** 那件事，`err == nil`。已删，见下方 `GetResumeUserText`。
	//   - 非寻址型的负载只能拿**假 ID** 挤进寻址通道（业务约定的 `"_replan_"` 之类），
	//     而那个假 ID 跟真挂起并存时会让 `ask_user` 反复重抛中断、**死循环**。
	//
	// 分开之后两条一起消失：寻址通道只认真 ID，非寻址走这个字段。
	UserText string
}

type resumeCtxKey struct{}

// withResumeContext 注入 ResumeContext 到 ctx。仅供 Run 内部调用。
func withResumeContext(ctx context.Context, rc *ResumeContext) context.Context {
	if rc == nil {
		return ctx
	}
	return context.WithValue(ctx, resumeCtxKey{}, rc)
}

// WithResumeContextValue 把 ResumeContext 显式注入 ctx——**业务高级 escape hatch**。
// 标准 HITL 闭环（Agent.Resume + WithResumeUserInputs）框架已自动注入 ResumeContext，
// 业务通常不需调本 helper。仅业务**绕过标准 Resume 入口自管 cp round-trip**、或单测
// 构造 ResumeContext 时才手动 inject（注：原 godoc 举的 commitFinal store.Put 场景已被
// ISSUE-75 消除——commitFinal 迁入 graph postamble 节点后，节点内 ctx 已自带 ResumeContext）。
//
// **场景**：`compose_v2.Run` 内部 `withResumeContext(ctx, ...)` 仅修改 Run 函数局部
// ctx 变量（Go 值传递）——outer caller 的 ctx 不受影响。Run 返回后 commitFinal
// 等"graph 之外"的业务回调若想读 ResumeContext 必须 caller 手动 inject。
//
// rc=nil 时 no-op 返原 ctx。
func WithResumeContextValue(ctx context.Context, rc *ResumeContext) context.Context {
	if rc == nil {
		return ctx
	}
	return context.WithValue(ctx, resumeCtxKey{}, rc)
}

// StripResumeContext 显式移除 ctx 中的 ResumeContext——audit Round 9 Bug B2 修复。
//
// **场景**：嵌套 subgraph / 嵌入式 sub-agent 入口需要隔离父的 UserInputs，避免
// 业务用 sentinel key（如 PlanAgent `_replan_`）时父子互相吃对方答案。
//
// **R27b audit LOW-1 文档化**：当前框架默认行为：
//
//   - `compose_v2/subgraph.go`（低阶 AddSubgraphNode）默认 forward 父 ResumeContext
//     到子图；调 `WithSubgraphIsolatedResumeContext()` SubgraphOption 显式隔离
//
//   - **agent_v2 9 编排（高阶 sub-agent 嵌入）默认 forward**——框架不提供 9 编排级
//     RunOption（因大多数业务 sentinel key 是显式命名不冲突），业务想隔离时**在
//     自己的 NodeFunc 入口调本 helper 手动隔离**：
//
//     // 业务自定义编排（如 GraphAgent 内 NodeFunc）想给某子 agent 隔离
//     nodeFn := func(ctx context.Context, chs Channels) (...) {
//     isolatedCtx := compose_v2.StripResumeContext(ctx)
//     return subAgent.Run(isolatedCtx, sess, input)
//     }
//
//   - 业务也可用 `compose_v2.WithSubgraphIsolatedResumeContext()` 在 GraphAgent
//     内 AddSubgraphNode 时直接隔离（仅 AddSubgraphNode 路径，9 编排嵌套用上面的手动方式）
//
// 框架 auto-ID 路径（InterruptSite 编入 graphNS）父子 ID 不冲突——本 strip
// 不影响 auto-ID 路由正确性，只防 sentinel-key 串台。
func StripResumeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, resumeCtxKey{}, (*ResumeContext)(nil))
}

// GetResumeContext 从 ctx 读取 ResumeContext——节点函数可调用。
func GetResumeContext(ctx context.Context) (*ResumeContext, bool) {
	rc, ok := ctx.Value(resumeCtxKey{}).(*ResumeContext)
	return rc, ok && rc != nil
}

// NextAnswerSlot 取**本站点下一个提问位**的身份，连同那个位置上已经有的答案。
//
// 这是框架内按位置取答案的入口——`Ask` 内部用它，`ask_user` / 工具审批两处 HITL 也用它（它们要在
// 「读答案」和「构造中断」之间插自己的逻辑，用不了 `Ask` 那个一步到位的形态）。a2a 的 human gate
// 不用它：那一问的身份还要带上它问的远端任务与第几问（新远端任务的追问、同一个任务的下一问都是新
// 问题），它按位置（[CurrentInterruptSite]）取一次之后自己派生，见 `a2a.WithHumanGate`。
//
// # 为什么是「算 + 读」合成一个动作，而不是分开两个
//
// 身份的一维是**本节点自己的计数器**（同一个节点里问第几次），取一次就 ++。分成
// 「先算 ID、再拿 ID 去读」两步的话，第二处想重建那个 ID 只能猜——而框架替业务猜过，
// 实测比不补还糟（见 `Ask` godoc）。合成一个动作 ⟹ 算完立刻用，没有第二处。
//
// # 为什么没有「按我给的 ID 取」这个形态
//
// 这里曾经有过 `GetResumeAnswer(ctx, id)`：任意站点拿任意 key 去全局 map 里 pull。
// 那个形态的问题是**身份的选择权在业务手里**，而身份决定答案投给谁——于是两个不同的
// 提问可以落在同一个答案位上。实测漏三种：
//
//  1. **跨超步**：s1 问「确认转账」、s2 问「确认删除历史」，业务两处都填同一个 ID ⟹
//     用户只**见过**转账（s2 的问题从未呈现），Resume 后 s2 照样取到答案执行。
//  2. **跨 visit**：循环节点每轮用同一个常量 ID ⟹ 用户批一次，之后每轮静默通过。
//  3. **同节点两问**：一个节点里问两次、复用同一个 ID ⟹ 第二件事跟着第一件的授权执行。
//
// 三种都 `err == nil`。它们的共同前提是**业务能选 key**；key 只能由站点派生之后，
// 三种在结构上都不存在（节点不同 ⟹ NodeID 不同；轮次不同 ⟹ Visit 不同；
// 同节点第二问 ⟹ 计数器不同）。
//
// # 返回值的两条契约
//
// `id` 是这一位的身份（拿去填 `AskQuery.InterruptID`），`ans`/`ok` 是这一位上已有的
// 答案（Resume 重跑到同一位置时命中——那正是幂等重跑要的）。两条边角：
//
//   - **`ok == true` 时 `ans` 仍可能是 nil**：调用方传了 `map{"id": nil}`（前端反序列化
//     失败 / DB 字段 NULL）时 key 在、value 是 nil。**必须 `if ok && ans != nil` 双重
//     校验**，否则 deref-nil panic。框架三处 HITL 都这么守。
//   - **不在节点/工具里调时 `id` 是空串**：身份由执行位置派生，没有位置就派生不出。
//     空 ID 拿去填 `AskQuery` 会死在 `validateInterrupt`（`%w ErrRunAborted`，不重试 +
//     清死档）。`Ask` 在调本函数**之前**有 `inNodeExecution` 闸门，直接用本函数则没有
//     ——调用方自己判，或者干脆走 `Ask`。
func NextAnswerSlot(ctx context.Context) (id string, ans *schema.Message, ok bool) {
	id = CurrentInterruptSite(ctx).InterruptID()
	rc, has := GetResumeContext(ctx)
	if !has || rc.UserInputs == nil {
		return id, nil, false
	}
	// **已兑付的答案不再兑付第二次**（见 answer_consumption.go）：调用方累积传历次
	// 答案是预期形态，所以身份一旦漂移，后一个提问会被前一个的旧答案静默满足——
	// 用户根本没被问到而 `err == nil`。这里当作「没有答案」返回，让它照常去问；
	// 而「新提问落在已兑付的位上」这件事本身是身份算错的确证，由 validateInterrupt
	// 在随之抛出的中断上当场终局失败（那才是能让人看见并定位的地方）。
	if answerAlreadyConsumed(ctx, id) {
		return id, nil, false
	}
	a, found := rc.UserInputs[id]
	// 记账：**本次执行**兑付了这一位。
	//
	// **判据是「真的交出了一个能用的答案」，不是「key 在不在 map 里」**：调用方传
	// `map{"id": nil}`（前端反序列化失败 / DB 字段 NULL）是本函数上方明写支持的形态，
	// 语义是「当没答案、照常再问」。按 `found` 记的话那条路会变成：记账落下 → 调用方
	// fall through 抛中断（同一个 id）→ `validateInterrupt` 判它落在已兑付位上 →
	// 终局失败 + 清整棵存档子树。**会话被删掉，而不是再问一次**——三处框架 HITL 实测全中。
	//
	// 真正并入是在超步的提交点——读到就记同样是错的，节点读完仍可能失败，那次执行整体
	// 不算数，重跑时它必须还能拿到同一个答案。
	if found && a != nil {
		getAnswerLedger(ctx).record(id)
	}
	return id, a, found
}

// GetResumeAnswer 按 InterruptID 取答案。
//
// ⚠️ **这是「拿任意 key 去 pull」那条路，实测漏三种**（跨超步 / 跨 visit /
// 同节点两问，见 [NextAnswerSlot] godoc）——**新代码一律用 [Ask]**，它把身份的选择权
// 收回框架，三种泄漏结构上不存在。
//
// 保留它只为一个原因：`ResumeContext.UserInputs` 是公开字段，删掉这个函数关不上口子
// （业务照样能 `GetResumeContext(ctx).UserInputs[k]`）。要真关得先收窄 ResumeContext
// （动 observability 两处读点 + 59 个测试文件），那是另一笔账。
func GetResumeAnswer(ctx context.Context, interruptID string) (*schema.Message, bool) {
	rc, ok := GetResumeContext(ctx)
	if !ok || rc.UserInputs == nil {
		return nil, false
	}
	// **兑付记账对这条路同样生效**（见 answer_consumption.go）。
	//
	// 这条路正是「拿任意 key 去 pull」，也就是身份最容易撞的地方——业务两处写同一个常量
	// ID 时，第二个提问会被第一个的答案静默满足。哨兵实测过那个形态：用户只批准了
	// 「确认转账」，「删除全部历史」跟着执行、`err == nil`。
	//
	// 记账放在这里而不是只放在 `NextAnswerSlot`：那样这条路会成为整道兜底的绕过口。
	if answerAlreadyConsumed(ctx, interruptID) {
		return nil, false
	}
	ans, ok := rc.UserInputs[interruptID]
	// 同 NextAnswerSlot：判据是「真的交出了能用的答案」，不是「key 在不在」——
	// `map{"id": nil}` 是支持的形态（当没答案、照常再问），按 `ok` 记会把它变成
	// 终局失败 + 清存档。
	if ok && ans != nil {
		getAnswerLedger(ctx).record(interruptID)
	}
	return ans, ok
}

// ── 中断寻址坐标 + 每节点 interrupt counter（`Ask` / 业务自填 ID 都从这里取维度） ──

// nodeInterruptCounterKey 是 ctx 注入的 per-node 单调计数器键——节点入口框架
// 注入计数器，业务工具（如 ask_user）每次调用要 ID 时取并 ++ 拿稳定 ID。
//
// 解决问题：同节点内多次调 ask_user，每次的 InterruptID 必须不同（按调用顺序
// 精确配对答案）；之前 ask_user 内部用 GetResumeAnswerSingle 取 map 第一条，
// 多 pending 时取错答案串台。
type nodeInterruptCounterKey struct{}

// nodeIDKey 是 ctx 注入的当前执行节点 ID 键——业务工具用于算稳定 InterruptID
// 时 nodeID 维度区分（多节点同 idx 不串台）。
type nodeIDKey struct{}

// nodeVisitKey 是 ctx 注入的当前节点访问次数键（audit Round 3 Bug #1 修复）。
// 跨节点重访（BasicAgent ReAct 多轮 / LoopAgent body / PlanAgent 循环）让
// InterruptID 编入 visit 维度防串台。
type nodeVisitKey struct{}

// toolCallIdxKey 是 ctx 注入的**当前 tool_call 在 assistant message 里的下标**键
// （ISSUE-96 拆维度）。
//
// **为什么单独一个 key**：ISSUE-96 之前，图级"分支序号"和 tc 下标这两个完全不同的
// 语义挤在同一个 ctx key 上（`WithCurrentToolCallContext` 把前者**覆盖**成后者）：
//   - 图级分支序号 = 实例在 frontier 里的**位置** → 部分失败时 frontier 缩水、位置
//     重排 → **不稳**，已由 nodeInstanceIDKey（fresh-mint token）取代并删除
//   - 本 key = tool_call 在 assistant message `ToolCalls` 里的**下标** → 那条 message
//     存在 channel 里、随 cp 持久化，toolsFn 重跑时读的是同一份 → **稳**
//
// 挤在一起就没法只删不稳的那半——拆开正是为了让它单独留下。
type toolCallIdxKey struct{}

// nodeInstanceIDKey 是 ctx 注入的当前节点**实例身份**键（ISSUE-96）。
//
// 值来自 FrontierMember.InstanceID：**SendFrom 派生实例**为 fresh-mint 的不透明
// token（随 frontier 持久化、跨 Resume 稳定）；**静态边/cond 边节点**为空串
// （恒单实例，身份走 NodeID，跨访问代际走 visit）。详见 FrontierMember godoc。
type nodeInstanceIDKey struct{}

// currentToolCallIDKey 是 ctx 注入的当前 tool_call ID 键（ISSUE-78 修）。
// BasicAgent toolsFn 调单 tool 前 inject 让 AgentAsTool.Call / 业务工具能拿到 tc.ID
// 加 childNS 维度——同 LLM 轮多个同名 tool_call 分别独立 NS 不互覆 cp。
//
// 当前 toolsFn 串行执行 + early return 兜底场景下不出 bug，但加 tc.ID 维度后未来
// toolsFn fan-out 化直接 race-free，无需架构改动 AgentAsTool 自动安全。
type currentToolCallIDKey struct{}

// WithNodeInstanceID 把当前节点的**实例身份**注入 ctx（ISSUE-96）。
//
// 框架在 graph_loop 进节点时调用——**先于 OnNodeStart**，让观测层能把同一 NodeID
// 的 N 个并发实例分开（节点 span 上记 crew.node.instance）。
//
// 业务自己包装 graph 执行引擎时可复用；正常用 compose_v2.Run 不需要碰它。
func WithNodeInstanceID(ctx context.Context, instanceID string) context.Context {
	return context.WithValue(ctx, nodeInstanceIDKey{}, instanceID)
}

// withNodeInterruptCounter 注入 per-node 单调计数器 + 当前 nodeID + 当前 visit
// 到 ctx——graph_loop runNodeWithHooks 调用，每节点入口分配独立 counter。
//
// **实例身份不在这里注入**：它要早于 OnNodeStart（见 WithNodeInstanceID），而本
// helper 在 hook 之后才调。两处各自单一注入点，别再挤回来。
func withNodeInterruptCounter(ctx context.Context, nodeID NodeID, visit int) context.Context {
	var counter atomic.Int64
	ctx = context.WithValue(ctx, nodeInterruptCounterKey{}, &counter)
	ctx = context.WithValue(ctx, nodeIDKey{}, nodeID)
	ctx = context.WithValue(ctx, nodeVisitKey{}, visit)
	// 嵌套调用的次序计数器跟提问计数器同一个生命周期（见 NextSubCallIndex）
	ctx = withSubCallCounter(ctx)
	return ctx
}

type currentTaskIDKey struct{}

// withCurrentTaskID 把本次跑的任务代号推给节点——中断寻址要它做唯一一个跨任务的维度。
func withCurrentTaskID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, currentTaskIDKey{}, id)
}

// GetCurrentTaskID 返本次跑的任务代号（见 `Checkpoint.TaskID`）。
// 不在图执行路径上时返空串——退化成「所有任务共用位置派生的 ID」，是引入前的行为。
func GetCurrentTaskID(ctx context.Context) string {
	if v, ok := ctx.Value(currentTaskIDKey{}).(string); ok {
		return v
	}
	return ""
}

// GetCurrentInstanceID 返当前节点的**实例身份**（ISSUE-96）。
//
//   - **SendFrom 派生实例**：非空的不透明 token，随 frontier 持久化、跨 Resume 稳定。
//     业务不应解析它的内部结构（格式是实现细节）。
//   - **静态边 / cond 边节点**：空串——它们恒单实例（`evalOutEdges` / `assembleFrontier` 的去重集合
//     是 map 会去重），身份用 NodeID 即可，跨完成访问的代际由 `GetCurrentNodeVisit` 承担。
//   - ctx 没注入（业务在框架外直调 NodeFunc）：空串。
//
// **用它替代 GetCurrentNodeBranchIdx 做身份**：后者是「实例在 frontier 里的位置」，
// 部分失败时 frontier 缩成失败子集 → 位置重排 → 身份漂移（ISSUE-96 的根因）。
func GetCurrentInstanceID(ctx context.Context) string {
	id, _ := ctx.Value(nodeInstanceIDKey{}).(string)
	return id
}

// GetCurrentNodeID 返当前节点 ID——业务工具（如 ask_user）算稳定 InterruptID
// 时按 (graphNS, nodeID, idx) 防多节点串台。
//
// ctx 没注入（业务在框架外测试）→ 返空字符串。
func GetCurrentNodeID(ctx context.Context) NodeID {
	id, _ := ctx.Value(nodeIDKey{}).(NodeID)
	return id
}

// GetCurrentNodeVisit 返当前节点访问次数（第一次访问 0，第二次 1...）——业务
// 工具算 InterruptID 时编入此维度防跨多次访问串台（audit Round 3 Bug #1）。
//
// ctx 没注入（业务在框架外测试）→ 返 0 不报错。
func GetCurrentNodeVisit(ctx context.Context) int {
	v, _ := ctx.Value(nodeVisitKey{}).(int)
	return v
}

// WithCurrentToolCallContext 一站式注入 toolsFn 并发化 (ISSUE-81) 所需 ctx 协议——
// BasicAgent toolsFn 内并发调多个 tool 时各 goroutine 入口调用此 helper 让不同
// tc 走独立 NS / InterruptID / counter 维度防同节点 N 个 tc 同时抛 ask_user 时
// InterruptID 撞。
//
// **协议**（audit C MED-1 文档化）：业务自定义 fanout toolsFn 用此 helper 后，
// **业务工具提问一律走 [Ask]**——本 helper 推的 tc 下标那一维会被它读进身份，各分支
// 天然不串台。框架自带的 `ask_user` / 工具审批走的是 [NextAnswerSlot]（同一套身份，
// 只是它们要在「查答案」和「抛」之间插自己的逻辑，用不了 `Ask` 一步到位的形态）。
//
// ⚠️ **别自填 `query.InterruptID`**：`Ask` 对非空值当场拒，理由见 [NextAnswerSlot]。
//
// 引擎**没有**兜底：中断请求的 InterruptID 为空时当场拒（`%w ErrRunAborted`）。曾经有一条
// 「业务没填 ID 时按节点级 ctx 补」的兜底，已删——它**看不见节点内部的分支结构**：分支下标
// 那一维读到恒 0，序号取的是合并列表里的位置，而分支自己算的是「本分支计数器的下一个」，
// 两套坐标只在最简单的形态下碰巧对上。
//
// 试过让扇出方在交出中断之前替分支盖章（按「第 j 个 query 配第 j 个计数值」猜），实测
// **比不补还糟**：开了工具审批时审批层自己先 tick 过一次 ⟹ 全错位 ⟹ 工具体永不收敛；
// 「一个工具问两次」时第二问被盖上第一问已答过的 ID ⟹ 副作用**无界重放**。那条路已删。
//
// 根子是：计数器的当前值**只有正在跑的那段代码知道**，第二处想重建都是猜。`Ask` 把
// 「算 ID」和「用 ID」放进同一次调用，结构上不可能分叉。
//
// **counter 隔离协议**（audit A LOW-2 文档化）：每次调本 helper 都新建独立
// `atomic.Int64=0` counter 注入 ctx，**遮蔽** 父节点 `withNodeInterruptCounter`
// 的 counter——业务想"父 counter 跨多 tc 累计"语义（如所有 fanout tc 共享单调
// 序号）时**不要**依赖框架 `nodeInterruptCounterKey`，应在 fanout 入口自管
// `atomic.Int64` + 通过其他 ctx key 透传。
//
// **典型业务自管模式**:
//
//	type myCounterKey struct{}
//	counter := &atomic.Int64{}
//	for i, tc := range toolCalls {
//	    go func(i int, tc schema.ToolCall) {
//	        callCtx := compose_v2.WithCurrentToolCallContext(ctx, tc.ID, i)
//	        callCtx = context.WithValue(callCtx, myCounterKey{}, counter) // 自管
//	        // 业务工具内 counter.Add(1) 拿全局递增 idx,而非 NextInterruptIndex
//	        ...
//	    }(i, tc)
//	}
//
// **同时注入六样**：
//   - 「在 tool_call 里」的章（[InToolCall]）：tool_call ID 可能为空（有的 provider 不回），判「在不在
//     tool_call 里」看这枚章，不看 ID 非空
//   - tc.ID (currentToolCallIDKey)：AgentAsTool / skill_tool fork 派生 childNS 的一维
//     （ISSUE-78 协议）。它是模型给的，框架不假设它跨轮、批内唯一，所以派生时还叠上
//     tc 下标与 tools 节点的访问序号（见 `agent_v2.RunSubAgent`）
//   - tc 下标 (toolCallIdxKey)：ask_user 等业务工具算 InterruptID 时的一维——同
//     tools 节点 N 个 tc 并发各拿不同下标，让 InterruptID 自动按 tc 区分。**与图级
//     实例身份是两个正交维度**，此前挤在同一个 ctx key 上（ISSUE-96 拆开）
//   - 独立 counter (nodeInterruptCounterKey)：每 tc 入口换新 atomic.Int64=0 让
//     NextInterruptIndex 在每 tc 内独立计数（同 tc 内 sub-agent 多次 ask_user
//     仍按 0/1/2.. 计），不被父 counter race 影响
//   - 独立的嵌套调用次序计数器（[NextSubCallIndex]）：同一个 tc 里用同一份输入对同一个
//     子 agent 调两次时，两次各落一个存档槽（输入不同的两次按内容就分开了，用不到它）
//   - 节点私有态的 tool_call 分域：工具函数里的 [SaveNodeState] / [SaveStepResult] 记在这个
//     tool_call 名下，不跟同批别的 tool_call 串，也碰不到 tools 节点自己记的账（它在节点的
//     ctx 上记，不经本 helper）
//
// 业务自定义 toolsFn 想做并发化时**必须**在 goroutine 入口调此 helper，否则多
// 并发 ask_user_tool 算同 InterruptID 串台。
func WithCurrentToolCallContext(ctx context.Context, tcID string, toolCallIdx int) context.Context {
	ctx = context.WithValue(ctx, currentToolCallIDKey{}, tcID)
	ctx = context.WithValue(ctx, inToolCallKey{}, true)
	// ISSUE-96：注入**独立**的 tc 下标维度，不再覆盖 nodeBranchIdxKey（图级位置）。
	// 两者语义不同且都要用：tc 下标区分"同一个 tools 节点实例内的 N 个并发 tool_call"，
	// 图级实例身份（GetCurrentInstanceID）区分"同一 NodeID 的 N 个并发节点实例"。
	ctx = context.WithValue(ctx, toolCallIdxKey{}, toolCallIdx)
	var counter atomic.Int64
	ctx = context.WithValue(ctx, nodeInterruptCounterKey{}, &counter)
	// 每个 tool_call 各有自己的嵌套调用次序计数器——并发的 tool_call 不能共用一个
	// （谁拿到几号会取决于调度），理由同上面的提问计数器
	ctx = withSubCallCounter(ctx)
	// 节点私有态按 tool_call 分域。下标一起进分域：模型给的 tcID 批内可能重复
	if a := nodeStateAccessorFrom(ctx); a != nil {
		ctx = withAccessor(ctx, scopedOf(a, "tc\x00"+tcID+"\x00"+strconv.Itoa(toolCallIdx)))
	}
	return ctx
}

// GetCurrentToolCallIndex 返当前 tool_call 在 assistant message `ToolCalls` 里的下标
// （ISSUE-96 从 nodeBranchIdxKey 拆出）。
//
// **业务工具算稳定 InterruptID 时用它区分同轮的 N 个并发 tool_call**：N 个 tool_call
// 跑在**同一个** tools 节点实例里（节点内 fanout ≠ 图级 fanout），所以图级实例身份
// (`GetCurrentInstanceID`) 对它们**全相同**——必须靠本维度区分，否则 N 个 ask_user
// 会算出同一个 ID、`WithResumeAnswers` 只配得上一条。
//
// **跨 Resume 稳定**：下标锚在 assistant message 上，那条 message 存在 channel 里、
// 随 cp 持久化，toolsFn 重跑时读的是同一份，顺序不变。
//
// ctx 没注入（业务直调 tool.Call、不经 toolsFn）→ 返 0。
func GetCurrentToolCallIndex(ctx context.Context) int {
	v, _ := ctx.Value(toolCallIdxKey{}).(int)
	return v
}

// GetCurrentToolCallID 返当前 tool_call ID（BasicAgent toolsFn 注入的本轮 LLM
// emit 的 tool_call ID）——AgentAsTool.Call 等内部工具用此加 childNS 维度,让同
// LLM 轮多个同名 tool_call 分别独立 NS 不互覆 cp。
//
// 另一个写入方是 RoutingAgent 的决策方升级链：它借这几个维度给每一档一个独立的调用
// 身份，那里读到的是 "route_decider_<档位下标>"，以失败收尾后被重新调用时是
// "route_decider_<档位下标>_retry<代数>"。它不是 LLM 的 tool_call ID，跨任务可能相同。
//
// ctx 没注入返空字符串（业务 caller 直接调 tool.Call 不经 BasicAgent toolsFn）。
//
// **ISSUE-78 修**：之前 childNS 仅由 agent.Name() 派生—— toolsFn 串行 + early
// return 兜底未触发 bug，但未来 fan-out 化时变 HIGH。加此协议作预防性修复：嵌套调用
// 把 tc.ID（叠上批内下标与 tools 节点访问序号）哈希成 `#` 下标物理隔离——走标准 fan-out
// NS 协议，哈希成整数也避开了 tc.ID 里可能有的 ns 分隔符。派生细节见 `agent_v2.RunSubAgent`。
func GetCurrentToolCallID(ctx context.Context) string {
	id, _ := ctx.Value(currentToolCallIDKey{}).(string)
	return id
}

// inToolCallKey 是「当前在某个 tool_call 的执行里」的章，由 [WithCurrentToolCallContext] 盖；
// 进新图时清（clearInheritedNodePosition），交出一段执行时也清（[EnterPhase] / [HandOver]：那段
// 代码不是调起它的那次 tool_call 本身）。
type inToolCallKey struct{}

// InToolCall 报当前是不是在某个 tool_call 的执行里（[WithCurrentToolCallContext] 注入过）。
// tool_call 的位置维（[GetCurrentToolCallID]、[GetCurrentToolCallIndex]）在交出去的那段执行里
// 照留，那里发起的调用仍按它们区分位置；这枚章只回答「是不是那次 tool_call 本身」。
//
// 不能拿「[GetCurrentToolCallID] 非空」判：有的 provider 不回 tool_call ID，那时 ID 是空串，
// 但这次执行照样是某个 tool_call、照样有它的批内下标（[GetCurrentToolCallIndex]）。拿 ID 判
// 的话，同一批两个没有 ID 的 tool_call 算不出自己的位置，里面发起的嵌套调用撞成一次。
func InToolCall(ctx context.Context) bool {
	v, _ := ctx.Value(inToolCallKey{}).(bool)
	return v
}

// NextInterruptIndex 给业务工具用——每次返当前节点内 interrupt 调用的递增序号
// （第一次返 0，第二次 1...）。用于稳定 InterruptID 生成。
//
// ctx 没有 counter（业务在框架外测试 ask_user）→ 返 0 不报错。
func NextInterruptIndex(ctx context.Context) int64 {
	c, _ := ctx.Value(nodeInterruptCounterKey{}).(*atomic.Int64)
	if c == nil {
		return 0
	}
	return c.Add(1) - 1
}

// askSlotMark 是提问计数器在某一刻的值，给「同一次调用的重试」用（见 [RunWithRetry]），跟嵌套调用
// 计数器的 subCallMark 成对。
type askSlotMark struct {
	c *atomic.Int64
	v int64
}

// markAskSlots 记下 ctx 里提问计数器此刻的值。没有计数器（不在节点执行里）时返回零值，rewind 什么也不做。
func markAskSlots(ctx context.Context) askSlotMark {
	c, _ := ctx.Value(nodeInterruptCounterKey{}).(*atomic.Int64)
	if c == nil {
		return askSlotMark{}
	}
	return askSlotMark{c: c, v: c.Load()}
}

// rewind 把计数器退回记下的那一刻：重试的每次尝试都从同一个序号数起，同一个问题算出同一个 ID。
func (m askSlotMark) rewind() {
	if m.c != nil {
		m.c.Store(m.v)
	}
}

// InterruptSite 是一次 interrupt 的**发生位置**——算稳定 InterruptID 的全部维度。
//
// 每个维度回答一个"会不会串台"的问题（缺一个就有一类串台）：
//
//	GraphNS      不同子图 / 不同 fanout 分支的 ns 不撞
//	NodeID       同一图里不同节点不撞
//	InstanceID   **同一 NodeID 的 N 个并发实例**不撞（SendFrom 同 Target 投多次）
//	Visit        同一节点**跨完成访问**不撞（ReAct 多轮：第 1 轮问"目的地?"、
//	             第 2 轮问"日期?"）
//	ToolCallID   模型给的 tool_call ID。**可能为空**（有的 provider 不回），所以它不回答
//	             「在不在 tool_call 里」——那一维是 InToolCall
//	ToolCallIdx  **同一 tools 节点实例内的 N 个并发 tool_call** 不撞（节点内 fanout
//	             ≠ 图级 fanout：它们 InstanceID 全相同，只能靠本维度区分）。
//	             **跟 ToolCallID 并存不是冗余**：框架别处不假设 tc.ID 一批内唯一
//	             （见 basic_agent.go 的 memoKey），换掉下标会把那层保护拆了
//	InToolCall   在不在某个 tool_call 里（见字段注释）
//	Phase        在哪个相位里（见字段注释）
//	Idx          同一个相位里多次 interrupt 不撞（NextInterruptIndex 递增）
//	RerunGen     同一次节点执行**被否决之后重做**时不撞（见字段注释）
//	OuterRerunGens 嵌套图里的提问在**外层执行被否决之后重做**时不撞（见字段注释）
//
// **用结构体而非位置参数**：位置参数传错顺序编译器不报错、只会在生产上静默串台。
type InterruptSite struct {
	GraphNS     string
	NodeID      string
	InstanceID  string
	Visit       int
	ToolCallID  string
	ToolCallIdx int
	Idx         int64

	// InToolCall 是 [InToolCall]：提问发生在某个 tool_call 的执行里。
	//
	// ToolCallID 可能为空、ToolCallIdx 的 0 同时表示「第 0 个」与「不在 tool_call 里」，只靠这两维
	// 时，provider 不回 tool_call ID 的情况下 tools 节点自己（不在任何 tool_call 里）的提问与
	// tool_call #0 里的提问算出同一个 ID：用户答了前一问，后一问被判「答案已兑付」、整个 Run
	// 终局失败（实测）。为 false 时不计入哈希。
	InToolCall bool

	// Phase 是提问所在的相位（[SubCallScope]）：节点体为空；图的钩子与条件边、业务工具钩子
	// （[EnterPhase]）、嵌套调用的被调方在自己的图之外（[HandOver]）各有自己的名字。
	//
	// 被调方在自己的图之外提问时，ns 是被调方的、节点与 tool_call 维却是调用方的——这是第三种
	// 位置，前面各维分不开它：调用方的节点恰好叫 "tools" 时，它跟被调方自己 tools 节点钩子里的
	// 提问撞成同一个 ID（实测，不需要空 ID）。为空时不计入哈希。
	Phase string

	// TaskID 是**唯一一个跨任务的维度**——「这一次任务」的代号，新开时铸、续跑时从存档
	// 读回。
	//
	// 没有它时其余各维**全是任务内的量**（位置维与相位，外加重做代际与外层代际链），而它们在
	// 新任务里全部重置 ⟹ 同一 sid 下两个任务走到同一个地方，ID **确定性相等**（不是概率碰撞）。后果是**给任务 A 的答案被
	// 任务 B 的问题消费掉**，`err == nil`。
	//
	// 位置维度负责「跨 Resume 稳定」（重算得同值），这一维负责「跨任务唯一」——两个要求
	// 得由不同的东西承担，靠一个纯位置方案同时满足是不可能的：位置不变正是它稳定的原因，
	// 也正是它跨任务必撞的原因。
	TaskID string

	// RerunGen 是提问所在分域的有效重做代际（[RerunGeneration]）：节点的条件出边按
	// [CondFailRerunFrom] 否决它的产出、或这个分域产出的输入被嵌套图的入口条件边否决（节点因此
	// 失败）时推进。
	//
	// 否决不推进 visit（visit 只在成功路径递增），节点重做时其余各维跟被否决的那一次完全相同。
	// 没有这一维时，重做里在同一个位置问的问题跟被否决那次问的是同一个 ID：调用方累积传历次
	// 答案时，用户对「批准方案 v1 吗」的回答直接兑付给了 v2——v2 的问句从头到尾没出现，
	// `err == nil`（实测）。只传当前挂起那条答案的调用方本来就会被重新问到，这一维让两种写法
	// 结果一致。想让一个回答跨重做保留（比如「用哪个模板」），把它用 [SaveStepResult] 记住（不带
	// [Rerunnable]），重做时读回、不再问——那一问要放进自己的相位里问（[EnterPhase]）。跟后面的提问在
	// 同一个相位里问的话，续跑时跳过它会让后面的提问挪到它的 ID 上、拿到它的答案（见 [Ask]「按同样的次序问」）。
	//
	// 为 0 时不计入哈希：从没被否决过的提问，ID 跟没有这一维时相同。
	//
	// 按分域算（[RerunGeneration]）：嵌套图的入口被否决只让产出那份输入的分域重做时，同一次节点
	// 执行里别的分域（兄弟 tool_call、节点体）的提问不换 ID。被调方在自己的图之外（[HandOver] 交出去
	// 的那一段）问的从交接那一层数起：调用方那一层重做时，身份没变的那次调用里已答的问题不换 ID。
	RerunGen int

	// OuterRerunGens 是承载这张图的每一层外层执行（在嵌套图里时）此刻的重做代际，从外到里用点号
	// 连起来；全为 0 时为空、不计入哈希。
	//
	// RerunGen 只看提问所在那张图自己的节点执行。嵌套图有自己的 cm，外层节点被条件出边否决、重做
	// 时，嵌套图从头跑（它上次成功时存档已清），在同一个位置问同一个问题——没有这一维时 ID 跟被
	// 否决那次完全相同，调用方累积传的「批准 plan-v1」直接兑付给了 plan-v2（实测：节点体里直接跑
	// 嵌套图、AddSubgraphNode 两种形态都中，`err == nil`）。
	//
	// 经 [HandOver] 交过来的主图（`agent_v2.RunSubAgent` 等调起的 agent）里，承载它的那一项是交接那一格
	// 的代际，从交接那一层数起、不算调用方那一层：调用方重做时这次调用是不是同一次调用，由调用的身份
	// 决定。调用方所在那张图之外的各层照带——那几层被否决重做时调用方的图从头跑、这次调用的完成记录
	// 跟着没了，被调方重新产出，提问必须换新（曾经交接主图把整条链清空，对 plan-v1 的批准兑付给了
	// plan-v2，实测）。
	OuterRerunGens string
}

// Ask 是业务**同步向人提问**的推荐入口：算 ID → 查答案 → 有就返回、没有就抛中断。
//
// **身份由框架按执行位置派生，`AskQuery.InterruptID` 必须留空**——填了当场拒（返普通
// error 不是中断）。选择权在业务手里时两个不同的提问能落在同一个答案位上，实测三种
// 泄漏全部 `err == nil`，见 [NextAnswerSlot] godoc。想给前端一个自己认得的标签走另一
// 条路：直接构造 `&InterruptError{Query: ...}`，那条路上的 ID 不参与 `Ask` 的取答案。
//
// ⚠️ 不是所有 HITL 都能用它：「先问远端、远端说要人再问人」这种**分离形态**（a2a 的
// `RemoteAgent` 就是）里，「算 ID / 查答案」和「抛」被整个远程调用隔开，而且「没答案」
// 那支不是抛、是把原始输入发给远端。那种自己拼，别硬套。
//
//	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准转账 5000 吗"})
//	if err != nil {
//	    return nil, err // 中断沿调用链上抛，框架存档挂起
//	}
//	// 走到这里说明人已经答了，ans 就是答案
//
// # 为什么必须走这个入口
//
// 中断 ID 由一组坐标算出，其中一维是**本分支自己的计数器**——它的当前值**只有正在跑的
// 那段代码知道**。所以「算 ID」和「用 ID」必须是**同一次调用**，任何第二处想重建它都是猜。
//
// 曾经试过让框架在分支把中断交出去之后替它盖章（按「第 j 个 query 配第 j 个计数值」猜），
// 实测**比不盖还糟**：
//
//   - 只要开了工具审批，审批层自己先 tick 过一次，业务工具拿到的第一个值就是 1 而不是 0
//     ⟹ 盖的全错 ⟹ 工具体反复重跑、**永不收敛**
//   - 「一个工具问两次」（确认 → 扣款 → 再确认）这种标准审批形态下，第二问被盖上第一问
//     那个**已经答过**的 ID ⟹ 每轮「第一问命中 → 扣款 → 第二问查不到 → 再抛」⟹
//     **扣款次数无界**（实测 16 次仍未收敛，而不盖时只扣 1 次）
//
// ⟹ 那条路已删。框架自带的三处 HITL（`ask_user` / 工具审批 / a2a）之所以一直没事，正是
// 因为它们都是「同一次调用里算完就用」——本函数把那个模式提成了公开 API。
//
// # 多次提问、并发扇出都自动正确
//
// 每次 `Ask` 自己 tick 自己用，所以：同一个工具连问 N 次拿到 N 个不同 ID；一轮 N 个
// tool_call 并发提问时各自在自己的计数器上从 0 起，互不干扰。包在 [RunWithRetry] 里时每次尝试
// 从同一个序号数起，前一次尝试已经答过的不重问。
//
// # 按同样的次序问
//
// 同一个相位里的提问按发起次序编号。续跑时节点从头执行到这一问，次序跟上一次一样才拿得到同样的 ID。
// 按上一次执行记下的进度跳过某一问（答案已经用 [SaveStepResult] 记住、这次不问了），后面的提问就挪到
// 它的 ID 上：调用方累积传答案时，后一问拿到的是被跳过那一问的答案，`err == nil`（实测）。能被跳过的
// 那一问放进自己的相位里问（[EnterPhase]，名字在这次执行里唯一，比如「tpl」或「confirm:<工单号>」）：
// 它有自己的序号，跳不跳过都不动这个相位的。
//
//	tpl, ok := compose_v2.GetStepResult[string](ctx, "tpl")
//	if !ok {
//	    a, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "tpl"), &compose_v2.AskQuery{Question: "用哪个模板？"})
//	    if err != nil {
//	        return nil, err
//	    }
//	    // 记在节点体（ctx）上，不记在相位上：读的时候用的也是 ctx
//	    if err := compose_v2.SaveStepResult(ctx, "tpl", a.Content); err != nil {
//	        return nil, err
//	    }
//	    tpl = a.Content
//	}
//	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准用模板 " + tpl + " 发出吗？"})
//
// # 调用方拿到什么
//
// 抛出去的中断里 `Query.InterruptID` 已填好，调用方按它回答
// （`agent_v2.WithResumeUserInputs(map[InterruptID]*Message)`）。
func Ask(ctx context.Context, q *AskQuery) (*schema.Message, error) {
	if q == nil {
		return nil, fmt.Errorf("%w: compose_v2: Ask 需要非 nil 的 AskQuery", ErrRunAborted)
	}
	// **身份只由执行位置派生，不认调用方自填**——自填 ID 能让两个不同的提问共用一个
	// 答案位，实测三种泄漏（跨超步 / 跨 visit / 同节点两问），见 [NextAnswerSlot] godoc。
	if q.InterruptID != "" {
		return nil, fmt.Errorf("%w: compose_v2: Ask 不接受自填的 AskQuery.InterruptID %q"+
			"（ns=%q node=%q）——投递身份只能由执行位置派生，否则两个不同的提问会共用一个答案位。"+
			"想给前端一个自己认得的标签：别走 Ask，直接返 &InterruptError{Query: ...}",
			ErrRunAborted, q.InterruptID, GetGraphNS(ctx), GetCurrentNodeID(ctx))
	}
	// 自动派生的身份来自**执行位置**——不在节点里就没有位置可派生。
	if !inNodeExecution(ctx) {
		return nil, fmt.Errorf("%w: compose_v2: Ask 的身份要从执行位置派生，"+
			"而当前不在任何节点/工具里——把这次提问搬进节点。"+
			"（钩子相位想「暂停」的话那不是提问：直接返 &InterruptError{Query: ...}，"+
			"续跑走 WithResumeUserText）", ErrRunAborted)
	}
	// **唯一一次 tick**——算完立刻用，不给第二处重建的机会
	id, ans, ok := NextAnswerSlot(ctx)
	if ok && ans != nil {
		return ans, nil
	}
	// 框架自己的记录，与调用方的对象脱钩：调用方之后改 q 不影响已挂起的这一条，
	// 复用同一个 q 再问也不会带着上一问的身份。
	rec := *q
	rec.InterruptID = id
	if q.Options != nil {
		rec.Options = append([]string(nil), q.Options...)
	}
	return nil, &InterruptError{Reason: rec.Reason, Query: &rec}
}

// clearInheritedNodePosition 清掉从**外层图**继承来的节点位置维度。
//
// # 为什么需要
//
// 这些维度是普通 ctx value，会跨图边界继承：子图的 `Run` 收到的就是父节点的 ctx，
// 而写它们的 `withNodeInterruptCounter` 排在 `OnNodeStart` **之后** ⟹ 子图每个节点的
// `OnNodeStart` 读到的是**父节点的** NodeID。
//
// 实测后果不是「值不好看」而是 livelock：那个身份的 idx 维读的是**父节点的计数器**，
// tick 次数 = 「本次跑到这儿之前子图里已有几个节点触发过 OnNodeStart」；Resume 时子图
// frontier 缩成失败子集 ⟹ tick 次数变 ⟹ 身份变 ⟹ 用户答的那条永远配不上。
//
// 跟 `WithComponentKind` 同一手法：**总是覆盖（含空），不加 `if != "" ` 守卫**——
// 位置只作用于声明它的那一层。
func clearInheritedNodePosition(ctx context.Context) context.Context {
	// **不清 withinNodeScope 章**：它答的是「这一层之上有没有节点接得住中断」，嵌套图整张
	// 图（含 OnGraphStart / OnGraphEnd 这段图级钩子相位）都在外层那个节点的执行范围里，
	// 答案是「有」。清掉的话 agent_v2 会把图级钩子里发起的调用当成顶层新任务（见
	// withinNodeScopeKey 的说明）。要清的只有下面这些**身份**维度。
	ctx = context.WithValue(ctx, nodeInterruptCounterKey{}, (*atomic.Int64)(nil))
	ctx = context.WithValue(ctx, nodeIDKey{}, NodeID(""))
	ctx = context.WithValue(ctx, nodeVisitKey{}, 0)
	ctx = WithNodeInstanceID(ctx, "")
	ctx = context.WithValue(ctx, currentToolCallIDKey{}, "")
	ctx = context.WithValue(ctx, inToolCallKey{}, false)
	ctx = context.WithValue(ctx, toolCallIdxKey{}, 0)
	// 嵌套调用的次序计数器同理：新图的钩子相位若接着外层节点的数，序号取决于「本次跑到
	// 这儿之前子图里已有几个节点触发过钩子」，续跑时子图 frontier 缩了、序号就漂（实测：
	// 用户答过的问题被再问一次）。
	ctx = context.WithValue(ctx, subCallCounterKey{}, (*subCallCounter)(nil))
	// 节点私有态的访问器同理：它绑的是外层那个节点、外层那张图的 cm。新图里每个要记东西的
	// 相位都显式配自己的访问器（节点体在入口拿自己的；OnNodeStart / OnNodeEnd / 条件边拿本节点
	// 这次执行的相位分域；图级钩子拿外层节点执行的相位分域，见 compose_v2.Run 入口的 outerAcc），
	// 这里清掉的是「图层面」的 ctx——不让外层节点那一格随普通 ctx value 流进新图里任何没有显式
	// 配访问器的地方（曾经的实测后果：图级钩子里的 SaveNodeState 覆盖外层节点自己记的进度）。
	ctx = context.WithValue(ctx, nodeStateAccessorKey{}, (*nodeStateAccessor)(nil))
	return ctx
}

// inNodeExecution 判断当前 ctx 是不是在某个节点/工具的执行中——**自动派生身份的前提**。
//
// 判据是「节点入口推的那两样东西在不在」。图外（顶层 hook / 后台 goroutine / 普通函数）
// 拿不到它们，各维度取零值 ⟹ 算出一个**常量** ID。
func inNodeExecution(ctx context.Context) bool {
	// 取到 nil 指针 = 进新图时被显式清空的继承值，不算「在节点里」
	if c, ok := ctx.Value(nodeInterruptCounterKey{}).(*atomic.Int64); ok && c != nil {
		return true
	}
	id, ok := ctx.Value(nodeIDKey{}).(NodeID)
	return ok && id != ""
}

// CurrentInterruptSite 从 ctx 一站式取出当前 interrupt 位置的全部维度——**业务工具
// 算稳定 InterruptID 的推荐入口**。
//
// 注意它会调 `NextInterruptIndex(ctx)`（每次调用递增 Idx），所以**一次 interrupt 只
// 调一次**、把结果存下来复用；调两次会拿到两个不同的 ID。
//
// 不在节点/工具执行内调（图外 hook / 后台 goroutine / 普通函数）时各维取零值，
// `InterruptID()` 返空——见它自己的 godoc。
//
//	site := compose_v2.CurrentInterruptSite(ctx)
//	id := site.InterruptID()
func CurrentInterruptSite(ctx context.Context) InterruptSite {
	site := interruptSiteFromCtx(ctx)
	site.Idx = NextInterruptIndex(ctx)
	return site
}

// interruptSiteFromCtx 从 ctx 读齐各维度，**不动 idx 计数器**（`Idx` 留零值由调用方定）。
//
// # 为什么单独抽出来
//
// 它曾经有第二个调用方：引擎给「业务没自填 ID 的中断」补 ID 那条路（已删）。那处原先是
// **另写一遍字段列表**的字面量，漏过两次维度（`ToolCallIdx` / `TaskID`），后果是同节点多 tc
// 和跨任务的自动 ID 撞。抽出来是为了消灭那份会漂的平行拷贝。
//
// 补 ID 那条路删掉之后它只剩 `CurrentInterruptSite` 一个调用方，留着是因为「读维度」和
// 「推进计数器」本来就是两件事。
func interruptSiteFromCtx(ctx context.Context) InterruptSite {
	return InterruptSite{
		GraphNS:        GetGraphNS(ctx),
		NodeID:         string(GetCurrentNodeID(ctx)),
		InstanceID:     GetCurrentInstanceID(ctx),
		Visit:          GetCurrentNodeVisit(ctx),
		ToolCallID:     GetCurrentToolCallID(ctx),
		ToolCallIdx:    GetCurrentToolCallIndex(ctx),
		InToolCall:     InToolCall(ctx),
		Phase:          SubCallScope(ctx),
		TaskID:         GetCurrentTaskID(ctx),
		RerunGen:       RerunGeneration(ctx),
		OuterRerunGens: outerRerunGensID(ctx),
	}
}

// InterruptID 按位置的全部维度算稳定 ID——同一位置跨 retry / Resume 总是同一个值
// （业务工具 `GetResumeAnswer` 按此 ID 取上次答案）。
//
// 算法：fnv64a hash 取 16 hex chars——碰撞空间 ~10^19，业务规模够用。
// **InstanceID 与 Visit 互斥**（ISSUE-96 实测坑）：
//
//   - **有 InstanceID**（SendFrom 派生实例）：它已完整表达"哪一个待办"——**忽略 Visit**。
//     必须忽略：`visitCount` 是 **per-NodeID 共享计数器**，成功的兄弟实例会把它推高，
//     中断实例重跑时读到的值跟中断当时不同 → ID 漂移。实测 3 实例场景 Run1 的
//     `visit=0` 在 Resume 时变成 `visit=2`。
//   - **无 InstanceID**（静态边 / cond 边节点，恒单实例）：**用 Visit** 表达跨完成访问
//     的代际（ReAct 多轮问不同问题）。
//
// **注意计数器只由空身份成员推进**（守卫在 `advanceVisitsLocked`）：不能想当然认为
// "静态成员没有兄弟实例"——同一个 NodeID 可以**同时**是静态边目标和 SendFrom 目标，
// 混合 frontier 下两类成员并存。若让 fan-out 实例也推进，静态那个的 Visit 就被污染了。
//
// 两者各自解决"同一节点的不同待办怎么区分"，只是场景互补——同时编入反而让稳的那个
// 被不稳的那个带偏。
func (s InterruptSite) InterruptID() string {
	// 没有节点身份 = 不标识任何位置 ⟹ 没有 ID。
	//
	// ⚠️ 判据用 NodeID 而不是「各维全零」：其余各维都可能合法为零（根图 graphNS 空、
	// 静态边 InstanceID 空、首访 visit 0、非 tool-call 相位 tc.ID 空 + 下标 0、
	// 首问 idx 0、旧存档 TaskID 空），只有 NodeID 在节点执行内恒非空。
	//
	// 不返值而不是返一个 hash：全零维 hash 出来是个**全局常量**，两张图两个节点两个 sid
	// 都一样，业务拿它提问会跨任务串台，而它看着像个正常 ID。返空则落进已有的空 ID 拒绝。
	if s.NodeID == "" {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s.TaskID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(s.GraphNS))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(s.NodeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(s.InstanceID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(s.ToolCallID))
	_, _ = h.Write([]byte{0})
	// 有实例身份时 visit 维度置 0——见上方"互斥"说明
	visit := s.Visit
	if s.InstanceID != "" {
		visit = 0
	}
	_, _ = fmt.Fprintf(h, "%d:%d:%d", visit, s.ToolCallIdx, s.Idx)
	if s.RerunGen > 0 {
		_, _ = fmt.Fprintf(h, ":r%d", s.RerunGen)
	}
	if s.OuterRerunGens != "" {
		_, _ = fmt.Fprintf(h, ":e%s", s.OuterRerunGens)
	}
	if s.InToolCall {
		_, _ = h.Write([]byte(":t"))
	}
	// 相位名是不透明的串（可能含 NUL），带长度写入：它排在最后，前面各段也都不含 ':' 以外的
	// 分隔歧义，长度前缀让「相位名的内容」与「后面再加的段」永远分得开。
	if s.Phase != "" {
		_, _ = fmt.Fprintf(h, ":p%d:", len(s.Phase))
		_, _ = h.Write([]byte(s.Phase))
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// GetResumeUserText 取「用户这轮说了什么」——**非寻址型**负载（见 `ResumeContext.UserText`）。
//
// 典型消费者：PlanAgent 的 planner 想拿到用户在 Resume 时补的指示（「换个方向」）喂回
// 规划 prompt。它不在节点里、没有自己的 InterruptID，所以按 ID 取不到东西。
//
// ⚠️ **这个 API 不是「取答案」的便利写法**。节点要拿属于自己的那条答案，**只有**
// `GetResumeAnswer(ctx, myID)` 一条路——按自己的 ID 精确取。
//
// 它替换了曾经的 `GetResumeAnswerSingle`（取 map 里任意一条）。那个 API 的问题不是
// 「多挂起时行为未定义」这种边角，而是**它让「拿错别人的答案」成为可能**：实测同一
// 会话里任务 A 挂起被放弃、任务 B 挂起，用户答 A 的 ID，用它取答案的节点执行了 **B**
// 那件事、`err == nil`。给它加过滤治不了本——A 的 ID 合法地在挂起集里。所以删掉那个
// 能力本身：**现在节点结构上不可能拿到不属于自己的答案。**
func GetResumeUserText(ctx context.Context) (string, bool) {
	rc, ok := GetResumeContext(ctx)
	if !ok || rc.UserText == "" {
		return "", false
	}
	return rc.UserText, true
}

// ── RunOption 集成 ──

// WithResumeAnswers 配置 Resume 时的用户答案 map。
//
// 与 WithResumeFrom 配合使用：
//
//	answers := map[string]*schema.Message{
//	    "ask_1": schema.UserMessage("yes"),
//	    "ask_2": &schema.Message{Role: schema.RoleUser, Content: "Beijing",
//	        ContentBlocks: []*schema.ContentBlock{...}, // 多模态
//	        Annotations: map[string]any{"locale": "zh-CN"}}, // 元数据
//	}
//	compose_v2.Run(ctx, g, cm,
//	    compose_v2.WithResumeFrom(snap),
//	    compose_v2.WithResumeAnswers(answers))
//
// **audit Round 9 Bug B1 修复**：value 类型从 string 改 *schema.Message，业务
// 多模态 / 带元数据 / 带 role 的答案完整保留（之前 agent_v2 conversion 层强转
// string 静默丢 ContentBlocks/Annotations 等）。
func WithResumeAnswers(answers map[string]*schema.Message) RunOption {
	return func(o *runOptions) {
		o.resumeAnswers = answers
	}
}

// WithResumeUserText 传「用户这轮说了什么」——非寻址型负载，见 `ResumeContext.UserText`。
//
// 跟 `WithResumeAnswers` 是两条独立通道：那条按 InterruptID 精确投递给提问者，这条
// 不指向任何提问、谁想据此改主意谁读。**别再用假 ID 把它塞进前者**。
func WithResumeUserText(text string) RunOption {
	return func(o *runOptions) { o.resumeUserText = text }
}

// GetResumeAnswerText 是 [GetResumeAnswer] 的便利 helper——同款警告，新代码用 [Ask]。
func GetResumeAnswerText(ctx context.Context, interruptID string) (string, bool) {
	msg, ok := GetResumeAnswer(ctx, interruptID)
	if !ok || msg == nil {
		return "", false
	}
	return msg.Content, true
}

// withinNodeScopeKey 是「本层之上有个节点会接住中断并落档」的章。
//
// 它与 [GetCurrentNodeID] 那个身份章**刻意分开**：两者回答不同的问题，盖章时机
// 的要求正好相反。
//
//   - 身份章（nodeID / visit / counter）回答「中断身份的维度从哪来」，必须晚到
//     `OnNodeStart` **之后**才盖——hook 相位框架一维身份都不给、业务只能自填常量 ID，
//     那是刻意的设计（见 graph_loop 里 `withNodeInterruptCounter` 的调用点注释）。
//   - 本章回答「抛出去有没有人接」，在**进节点那一刻**就成立：`OnNodeStart` 的早退
//     路径同样会 `validateInterrupt` 并把中断冒泡上去落档。
//
// 合一的代价实测过：`OnNodeStart` 里跑的子运行，其 `OnGraphStart` 抛的中断被判成
// 「没人接得住」而拒绝——而外层早退路径明明接得住，结果是白拒一次、挂起档 0。
// 这正是本仓反复栽的「一个字段被两种语义共用 ⟹ 注定要逼某一方判错」的形状。
//
// **进新图时不清**（`clearInheritedNodePosition` 只清身份维度）：从某个节点体里调进来的
// 嵌套图，整张图——包括 `OnGraphStart` / `OnGraphEnd` 这段图级钩子相位——都在那个节点的
// 执行范围里，嵌套图 `OnGraphStart` 抛出的中断照样冒泡到那个节点落档（`OnGraphEnd` 没有
// 错误返回值，抛不出中断）。曾经进图就清：嵌套图 `OnGraphStart` 里发起的调用于是被
// agent_v2 当成**顶层新任务**——不续存档、整个任务的 approve_always 规则被清空（审计实测）。
// 顶层图没有外层节点，章本来就是 false，不受影响。
type withinNodeScopeKey struct{}

// nodeScope 是 withinNodeScopeKey 章的值：进节点那一刻记下的、这次 Run 的 session ID。
type nodeScope struct{ sessionID string }

// withinNodeScope 盖章。sessionID 在进节点那一刻取（runNodeWithHooks 入口），之后节点体、钩子
// 往 ctx 上推别的 sid 都改不了它。
func withinNodeScope(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, withinNodeScopeKey{}, nodeScope{sessionID: sessionID})
}

// WithinNodeScope 报「当前调用发生在某个节点的执行范围内」：节点体、它的 `OnNodeStart`，以及
// 从节点体里调进来的嵌套图（连同嵌套图的图级钩子相位）。在能抛中断的相位（节点体、
// `OnNodeStart`、嵌套图的 `OnGraphStart`）里抛出的中断有人接得住并落档；条件边求值也在这个
// 范围里，但它抛中断是协议违规，恒被拒。
//
// 供 `agent_v2` 判顶层 hook 抛的中断能不能落档（不能就必须响亮拒绝，而不是留下假暂停）。
// 「这次 Run 是不是这个任务的一部分」另看 [NodeScopeSessionID]。业务一般不需要它。
func WithinNodeScope(ctx context.Context) bool {
	_, ok := ctx.Value(withinNodeScopeKey{}).(nodeScope)
	return ok
}

// NodeScopeSessionID 返回当前节点执行范围所属那次 Run 的 session ID——进节点那一刻记下的，
// 业务之后在节点里往 ctx 上推别的 sid（[WithSessionIDCtx]）不改变它。ok 为 false 表示不在任何
// 节点的执行范围内；那次 Run 没配 session ID 时返回 ("", true)。
//
// 供 `agent_v2` 取「所在任务」的身份：节点里发起的 Run 是这个任务的一部分，session 的 RunID 必须
// 就是这个 sid，对不上入口直接拒；要开一个独立的任务先 `agent_v2.DetachTask`（它连同这枚章一起
// 剥掉）。图在进节点之前把自己的 sid 重新铺回 ctx，图级钩子（`OnGraphStart`）往 ctx 上推的别的
// sid 进不来。业务一般不需要它。
func NodeScopeSessionID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(withinNodeScopeKey{}).(nodeScope)
	return v.sessionID, ok
}
