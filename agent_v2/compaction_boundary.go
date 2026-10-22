package agent_v2

// **压缩边界**——AutoCompact 的结果跨 Run 留下来，下一次 Run 从最新的边界往后读。
//
// # 为什么要有它
//
// 单次 Run 内的复用（compaction_memo.go）随 Run 结束消失。关掉轮数截断（WithMaxHistoryPairs(0)）、
// 靠摘要压缩控制上下文的长会话里，下一次 Run 的 prior 又是整场历史：读一遍、再在第一轮把它整个摘要一遍
// ——读的量与摘要的输入都随会话长度线性涨，长到一定程度摘要调用自己就超窗口。
//
// # 形状
//
// 某一轮真摘要了一次（单次 Run 内的结果更新了）、而且 `OnModelStart` 钩子这一轮没动消息列表，模型节点
// 成功时往会话历史**追加**一条边界记录（钩子插入或改写过的消息不该当成历史重放，那种情况不写）：
//
//   - 它带着这一刻的**完整替代历史**（[HistoryCompactionAnnotation]）：受保护的组 + 摘要 + 尾部原文，
//     按写入顺序。读的一方用它就够了，边界之前的记录一条都不用再读——所以它是自包含的；
//   - 盖本 agent 的归属与视图模式（自己看 / 转交改写 / 转交原样）。视图是按 agent 定的，一条边界只对
//     写下它的那个 agent、同一种视图成立；
//   - 去掉每次 Run 都会重新算出来的部分：前导 system 里的长期记忆注入（preamble 每次重新检索注入）、
//     编排拼出来的派生输入（本来就不进历史）。
//
// 原始记录一条不动——历史仍是 append-only，业务回看看到的是完整对话，外加若干条边界记录（带
// HistoryCompactionAnnotation，展示时可以跳过或当作「此前的摘要」）。
//
// # 怎么读
//
// [historyPrior] 从末尾往前翻：遇到本 agent、同视图的边界就停，prior = 边界里的替代历史 + 边界之后的
// 记录（投影成视图）。只认最新的一条——它之前的边界都已经被它覆盖。替代历史解不出来（记录损坏）时当它
// 不存在，接着往前读（更早的边界仍是它那个时刻的完整基底）。

import (
	"encoding/json"
	"strconv"

	"crew-go/schema"
)

// HistoryCompactionAnnotation 标「这条历史记录是一条压缩边界」，值是边界那一刻完整替代历史的 JSON
// （[]*schema.Message）。
//
// 边界记录的正文是那次的摘要，供人回看；框架读回 prior 时用的是这个替代历史，见 compaction_boundary.go。
// 业务回看会话时可以按这个键跳过它，或把它当作「此前的摘要」展示。
const HistoryCompactionAnnotation = "crew_history_compaction"

// historyCompactionModeAnnotation 记边界写下时的视图模式（[HandoffHistoryMode] 的十进制）。
const historyCompactionModeAnnotation = "crew_history_compaction_mode"

// isCompactionBoundary 报这条历史记录是不是压缩边界。
func isCompactionBoundary(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	_, ok := m.Annotations[HistoryCompactionAnnotation]
	return ok
}

// buildCompactionBoundary 用这一轮的压缩结果拼边界记录。msgs 是这一轮交给 modelFn 的消息流（钩子改写
// 之后、套用备忘之前），memo 是这一轮新记下的结果。替代历史 = Head 去掉长期记忆注入 + 消息流第 Upto 条
// 之后（去掉派生输入）。
func buildCompactionBoundary(memo *compactionMemo, msgs []*schema.Message, mode HandoffHistoryMode) *schema.Message {
	if memo == nil || memo.Upto > len(msgs) {
		return nil
	}
	replacement := make([]*schema.Message, 0, len(memo.Head)+len(msgs)-memo.Upto)
	summary := ""
	for _, m := range memo.Head {
		if m == nil || isStoreInjected(m) {
			continue
		}
		if summary == "" && isCompactSummary(m) {
			summary = m.Content
		}
		replacement = append(replacement, m)
	}
	for _, m := range msgs[memo.Upto:] {
		if m == nil || isDerivedInput(m) || isStoreInjected(m) {
			continue
		}
		replacement = append(replacement, m)
	}
	if summary == "" {
		return nil // 没有摘要的「压缩结果」不该成为边界
	}
	data, err := json.Marshal(replacement)
	if err != nil {
		return nil
	}
	b := schema.UserMessage(summary)
	b.Annotations = map[string]any{
		HistoryCompactionAnnotation:     string(data),
		historyCompactionModeAnnotation: strconv.Itoa(int(mode)),
	}
	return b
}

// decodeCompactionBoundary 取边界记录里的替代历史；视图模式对不上或解不出来时 ok=false。
func decodeCompactionBoundary(m *schema.Message, mode HandoffHistoryMode) ([]*schema.Message, bool) {
	if m.Annotations[historyCompactionModeAnnotation] != strconv.Itoa(int(mode)) {
		return nil, false
	}
	raw, _ := m.Annotations[HistoryCompactionAnnotation].(string)
	var replacement []*schema.Message
	if err := json.Unmarshal([]byte(raw), &replacement); err != nil || len(replacement) == 0 {
		return nil, false
	}
	return replacement, true
}

// isCompactSummary 报这条消息是不是 AutoCompact 产出的摘要。
func isCompactSummary(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, ok := m.Annotations[AutoCompactSummaryAnnotation].(bool)
	return ok && v
}

// isStoreInjected 报这条消息是不是长期记忆注入（preamble 每次 Run 重新检索注入）。
func isStoreInjected(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, ok := m.Annotations[StoreInjectedAnnotation].(bool)
	return ok && v
}

// sameMessageSlice 报两份消息列表是不是逐条同一个指针（钩子原样放行时是）。
func sameMessageSlice(a, b []*schema.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
