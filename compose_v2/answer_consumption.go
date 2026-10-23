package compose_v2

import (
	"context"
	"sync"
)

// ── 答案一次性消费：一个答案只能满足一个提问 ──
//
// # 它防的是什么
//
// 中断寻址的身份由执行位置派生，位置维算错时**两个不同的提问会落在同一个答案位上**。
// 而调用方**累积传历次答案**是本框架文档化的预期形态（见 `warnUnmatchedAnswerIDs`
// 的 hint），于是身份一旦漂移，后一个提问会被前一个的旧答案**静默满足**——用户根本
// 没被问到，`err == nil`。
//
// 这不是假想：出边归成员那次改动里，代际推进点挪错一格就造出了这个形态，全套变异
// 验证 + 冒烟 + 真 LLM 测试全绿，靠独立审计才抓到（见 completed_sibling_visit_test.go）。
//
// # 为什么是「记账」而不是「换一种身份算法」
//
// 身份算法怎么改，都是在赌「这次算对了」。而**一个答案只能满足一个提问**是与身份算法
// 正交的事实：把它记下来，身份判据出错时会**当场暴露**，而不是静默生效。
// 这是对「我未来还会算错」的兜底，不是对某一个具体错的修补。
//
// ⚠️ 「当场暴露」的范围**就是下面「覆盖到哪儿为止」列的那几个入口**，不是所有路径。
// 这两段曾经互相打脸：这里写「任何一次」，下面自己承认裸读 `UserInputs` 绕得过去。
//
// # 记账不是被「累积传」逼出来的（实测推翻过一个结论）
//
// 累积传是**调用方的便利**（它不必记谁答过谁没答过），一次性消费是**引擎的记账**。
// 调用方照常传整张 map；引擎只是记住「这一位已经兑付过」。两者不冲突。
//
// 曾经以为「累积传是这份记账存在的**唯一**理由，改掉调用约定就不必记了」。**假的**——
// 暴露窗口有两个，改约定只关得掉其中一个：
//
//  1. **同一次执行内**：一个节点前后问两次、撞同一个 ID。那张答案 map 本来就在这次
//     Resume 里活着，**跟累不累积毫无关系**。关掉整道记账后实测：用户只批准「转账
//     5000」，紧接着的「删除全部历史」被**同一张票**放行，`err == nil`，存档还按
//     `reason=success` 清掉了。守它的 answer_consumption_within_exec_test.go
//     **只传了一轮答案**——那个 fixture 本身就是非累积传形态，照样承重。
//  2. **跨 Run**：上几轮的旧答案还躺在 map 里，被本轮的新提问撞上。这一个确实是
//     累积传打开的。
//
// 所以病因是**身份由执行位置派生、而位置维可能算错**；累积传只是把窗口从「一次 Resume
// 内」撑大到「整个任务生命周期」。**换调用约定关不掉第 1 个窗口。**
//
// # 消费点在提交点，不在读取点
//
// 读到就标记是错的：节点读了答案之后仍可能失败（业务错 / 后续中断 / 提交失败），
// 那次执行整体不算数，重跑时它**必须还能拿到同一个答案**。所以标记跟代际推进同一
// 时刻——「这一步成了」才算兑付。
//
// 一次执行之内也有「再做一遍」：[RunWithRetry] 重试同一次调用。后一次尝试同样必须拿得到
// 前一次尝试读过的答案，所以那里按尝试分层（见 retryLedger）。
//
// # 命中之后是**终局失败**，不是再问一次
//
// 一个已兑付的答案位再次出现，意味着两个不同的提问算出了同一个身份——那是框架的位置
// 维算错或业务自填 ID 撞车，**重跑必然一样**。再问一次会变成调用方答了又被拒的死循环；
// 报 `ErrRunAborted` 才能让人当场看见并定位（错误正文带节点 + ns + 那个身份）。
//
// # 覆盖到哪儿为止（如实说，别读成穷举）
//
// 记账挂在**读答案的入口**上，所以它覆盖的是走这些入口的路：
//
//   - [Ask] / [NextAnswerSlot]（推荐路径，身份由位置派生）
//   - 裸 [GetResumeAnswer] / [GetResumeAnswerText]（自填 ID 那条路——**恰恰最容易撞**，
//     所以必须覆盖，否则整道兜底有个绕过口）
//   - `OnNodeStart` hook 相位：账本的注入排在它**之前**（见 graph_loop 里 `withAnswerLedger`
//     的调用点），因为那一层框架一维身份都不给、业务只能自填常量 ID
//
// **不覆盖**：业务直接 `GetResumeContext(ctx).UserInputs[k]` 裸读那张 map。
// `ResumeContext.UserInputs` 是公开字段，绕过任何入口级机制——这条口子 [GetResumeAnswer]
// 的 godoc 也承认过（要真关得先收窄 ResumeContext）。**这不是遗漏，是已知边界**：
// 走裸读的业务同时也放弃了「一个答案只能满足一个提问」这条保证。

// answerLedgerKey 是节点 ctx 里「本次执行兑付了哪些答案位」的记录器键。
type answerLedgerKey struct{}

// answerLedger 收本次节点执行兑付掉的答案位，并持 cm 供「这一位兑付过没有」的查询。
//
// **一个 ctx 槽兼两职是有意的**：判定与记账必须看同一份账本，分成两个 ctx 值就又是
// 一对必须保持一致的东西。收本次的那半跟 [sendSink] 同款——节点执行期间往里记，
// 节点 return 后由 graph_loop 取出，**只有该成员最终成功、且本超步提交成功时**才并入 cm。
//
// # parent 链：账本跨得过层，但**不跨同超步的兄弟**
//
// 「一个答案只能兑付一次」这条不变量的作用域是整个 Run（答案 map 沿 ctx 活全程），
// 而 cm 是每层图各一个。没有链的时候，账本的可见域被错缩成「本层」：子图第 1 轮兑付、
// 成功、子 cp 连同兑付记录一起被清，父循环第 2 轮重进（或另一个子图撞同一个自填 ID）
// 时**谁都不记得那一位已经兑付过**——实测旧答案被静默复用，err == nil。
//
// ⚠️ **可见域如实说，别读成「整个 Run」**（这一段一度就是那么写的）。实际是三段：
// 本次执行的 in-flight → 本层 cm 已提交 → **祖先**已提交。链只往上走，**同一超步的
// 并行兄弟互相看不见**——它们各自的兑付还没提交进 cm，而彼此不在对方的父链上。
// 实测：两条并行分支用同一个自填常量 ID 各 pull 一次，一张票被兑付两次、err == nil。
// 这一格没有补机制（见 doc/code/todo.md 的条目）：走 [Ask] 时并行分支的 ID 天然
// 不同、结构上撞不了；能撞的只有「自填常量 ID + 并行」这条已被劝退的路
// （[GetResumeAnswer] 的 godoc 自陈是最容易撞的遗留路）。
//
// 链的两半（写与读各走各的层，提交纪律不变）：
//
//   - **读（判定）沿链上溯**：本次 in-flight → 本层 cm 已提交 → 父节点账本递归——
//     子层的提问能看见祖先已提交的兑付。[RunWithRetry] 的每次尝试在链上再垫一层，前面几次
//     尝试记的不在链上（同一次调用的同一批提问，见 retryLedger）。
//   - **写（提交）逐层上冒**：嵌套图成功出口把自己 cm 的兑付集 record 进外层节点
//     账本（[bubbleConsumedAnswersTo]），随外层成员成功 + 外层超步提交并入
//     外层 cm，最终随根 cp 持久化。**不是一份全局活账本**——那会把消费点从提交点
//     挪到读取点：外层超步失败重跑本该重新拿到同一批答案（读到不算兑付），全局
//     活账本会把没提交的兑付当真，重跑当场断粮。
type answerLedger struct {
	mu     sync.Mutex
	cm     *ChannelManager
	parent *answerLedger
	ids    []string
	// attempt 为真表示这是 [RunWithRetry] 某次尝试的那一层（见 retryLedger）。
	attempt bool
}

func withAnswerLedger(ctx context.Context, l *answerLedger) context.Context {
	return context.WithValue(ctx, answerLedgerKey{}, l)
}

func getAnswerLedger(ctx context.Context) *answerLedger {
	l, _ := ctx.Value(answerLedgerKey{}).(*answerLedger)
	return l
}

// consumed 报这一位是否已经兑付过——**两个来源都算**：
//
//  1. `cm` 里已并入的（更早超步 / 更早 Run 兑付的）
//  2. **本次执行自己刚兑付的**（还没到提交点，所以不在 cm 里）
//
// 第 2 条不能省。少了它，「同一次节点执行内问两次、两问撞同一个 ID」整条绕过——
// 实测：节点先读答案执行「转账 5000」，紧接着第二问算出同一个 ID、被**同一张票**放行
// 执行了「删除全部历史」，全程 `err == nil`。而它恰恰是自填 ID 最容易撞的形态
// （一个工具体里前后两个确认，业务顺手写了同一个常量）。
//
// 两个来源的生命周期不同、缺一不可：cm 那份跨超步跨 Run 活着，本次这份**随执行结束
// 而丢弃**——所以重跑时账本是新的，节点能重新读到它上次读过的那些答案（那正是
// 「读到不算兑付、提交点才算」要保的）。
//
// 不在节点执行里时（业务在框架外复用 NodeFunc）恒返 false——跟 [SendFrom] 一样
// 静默降级，不崩。
func (l *answerLedger) consumed(id string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	for _, seen := range l.ids {
		if seen == id {
			l.mu.Unlock()
			return true
		}
	}
	l.mu.Unlock()
	if l.cm.answerConsumed(id) {
		return true
	}
	// 沿父链上溯（见类型 godoc「parent 链」）：祖先层已提交的兑付对子层的提问同样
	// 算数。锁序安全：本层锁已放、cm 读锁已放才进父层；链是严格树无环。
	return l.parent.consumed(id)
}

// answerAlreadyConsumed 是给 ctx 持有方的查询入口——`validateInterrupt` 用它判
// 「新提问落在了一个已兑付的答案位上」。
func answerAlreadyConsumed(ctx context.Context, id string) bool {
	return getAnswerLedger(ctx).consumed(id)
}

// record 记下「这一位被兑付了」。同一位重复记只留一份。
func (l *answerLedger) record(id string) {
	if l == nil || id == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seen := range l.ids {
		if seen == id {
			return
		}
	}
	l.ids = append(l.ids, id)
}

// bubbleConsumedAnswersTo 把本层 cm 已提交的兑付集 record 进外层节点账本——
// 嵌套图**成功出口**调用（[Run] 顶部那道专用 defer，注册在 outer panic recover
// **之前**好让它 LIFO 殿后；别挪回 cleanup defer 里，那里 cper/sid 缺省会早退）。
//
// parent 是 Run 入口、ctx 被 `OnGraphStart` 返回值替换之前取下的那本账（见 Run 顶部）：
// 钩子返回一个不是从入参派生的 ctx 时，出口再从 ctx 上取就取到别处的账本或者 nil，兑付集
// 冒不上去。
//
// 只在成功出口冒是提交纪律的一部分：中断退出时记录已随本层 cp 持久化（Resume 恢复
// cm 时读回，最终还是从成功出口冒上去）；失败退出时外层成员不会成功、外层超步不会
// 提交，冒了也进不了账。根层（外层无账本）no-op。
//
// 冒的是**全集**（含从 cp 恢复的历史条目）：跨 Run 续跑的子图，早先 Run 兑付的位
// 同样属于本任务的已兑付集，外层必须看得见。record 去重，重复冒安全。
//
// 越过 [RunWithRetry] 的尝试层，记进外面那本账（见 retryLedger「嵌套图冒上来的不分层」）。
func bubbleConsumedAnswersTo(parent *answerLedger, cm *ChannelManager) {
	for parent != nil && parent.attempt {
		parent = parent.parent
	}
	if parent == nil || cm == nil {
		return
	}
	for _, id := range cm.consumedAnswersSnapshot() {
		parent.record(id)
	}
}

// retryLedger 管 [RunWithRetry] 一次调用里各次尝试的兑付。
//
// 重试的每次尝试是同一次调用再做一遍：在同一个位置问同一个问题、算出同一个 ID（提问序号随重试退回）。
// 前一次尝试已经兑付的答案，后一次尝试必须还拿得到。直接记进外层账本的话，后一次尝试取同一个 ID 被当成
// 「已兑付」、随后抛的中断落在已兑付的位上，整个任务终局失败（自填 ID 那条路实测）。
//
// 所以每次尝试记在自己那一层：读的时候沿链看得见尝试之外已记的（进入重试之前的、兄弟 tool_call 的），
// 看不见前面几次尝试记的。收尾（成功，或以不再重试的错退出）时各次尝试记下的一并记进外层账本，账本最终
// 的内容跟每次尝试都直接记进外层时相同。一次尝试记下的兑付要到 RunWithRetry 返回才对兄弟可见。
//
// # 嵌套图冒上来的不分层
//
// 分层只管重试会原样重做的那部分：fn 直接兑付的（[Ask]、[NextAnswerSlot]、[GetResumeAnswer]，包括交出去的
// 执行在自己的图之外问的）。嵌套图成功退出时冒上来的兑付（[bubbleConsumedAnswersTo]）越过尝试层，直接记进
// 外面那本账、立即可见。那张图在失败那次尝试里已经跑完，存档随成功清掉；后一次尝试要么拿回记录不再跑它
// （`agent_v2.RunSubAgent` 的完成记录），要么从头再跑——一次重生，内容可能重新生成（草稿 v1 变 v2），里面的提问
// 换新身份（见 nestedRegenKey），问的是新问题、拿不到对 v1 的批准。它的兑付对后一次尝试照旧可见，作为第二道保险：
// 万一重生的提问落在旧位置上，当场终局失败而不是兑付旧答案。这部分也分层的话，那个批准曾是静默兑付给 v2 的
// （实测，`err == nil`）。
//
// 不配重试（MaxRetries 为 0）或外层没有账本（不在节点执行里）时为 nil，各方法什么也不做。
type retryLedger struct {
	outer   *answerLedger
	current *answerLedger
	retired []string
}

func newRetryLedger(ctx context.Context) *retryLedger {
	outer := getAnswerLedger(ctx)
	if outer == nil {
		return nil
	}
	return &retryLedger{outer: outer}
}

// begin 开始一次尝试，返回它用的 ctx。前一次尝试（要重试才会走到这里）记下的收起来，这一次看不见。
func (r *retryLedger) begin(ctx context.Context) context.Context {
	if r == nil {
		return ctx
	}
	r.retired = append(r.retired, drainAnswerLedger(r.current)...)
	r.current = &answerLedger{parent: r.outer, attempt: true}
	return withAnswerLedger(ctx, r.current)
}

// settle 收尾：各次尝试记下的一并记进外层账本。
func (r *retryLedger) settle() {
	if r == nil {
		return
	}
	for _, id := range append(r.retired, drainAnswerLedger(r.current)...) {
		r.outer.record(id)
	}
	r.retired, r.current = nil, nil
}

// drain 取出并清空——节点 return 后由 graph_loop 调用。
func drainAnswerLedger(l *answerLedger) []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ids) == 0 {
		return nil
	}
	out := l.ids
	l.ids = nil
	return out
}

// markAnswersConsumedLocked 把这批答案位记进「已兑付」账本（去重）。
// **调用方须已持写锁**——它跟写入提交同属一个临界区，见 [ChannelManager.commitSuperstepPartial]
// 与 [ChannelManager.commitSuperstepAdvance]。
func (cm *ChannelManager) markAnswersConsumedLocked(ids []string) {
	if len(ids) == 0 {
		return
	}
	// 一次性 set 去重，O(n+m)。曾是对累积中的 consumedAnswers 逐条线性扫（O(n·m)），
	// 跑在写锁里、随一次 Run 内 HITL 答案总数平方增长（独立审计指出）。不另存一份
	// 持久 set——那是第二份要跟 Snapshot/Restore/rollback 同步的状态，这里按需重建更省心。
	seen := make(map[string]struct{}, len(cm.consumedAnswers)+len(ids))
	for _, s := range cm.consumedAnswers {
		seen[s] = struct{}{}
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		cm.consumedAnswers = append(cm.consumedAnswers, id)
	}
}

// consumedAnswersSnapshot 返已提交兑付集的快照（拷贝，调用方可自由持有）。
func (cm *ChannelManager) consumedAnswersSnapshot() []string {
	if cm == nil {
		return nil
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.consumedAnswers) == 0 {
		return nil
	}
	out := make([]string, len(cm.consumedAnswers))
	copy(out, cm.consumedAnswers)
	return out
}

// answerConsumed 报这一位是否已经兑付过。
func (cm *ChannelManager) answerConsumed(id string) bool {
	if cm == nil || id == "" {
		return false
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for _, seen := range cm.consumedAnswers {
		if seen == id {
			return true
		}
	}
	return false
}
