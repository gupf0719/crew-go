package agent_v2

// walkHistorySubs 的不变量：走过的不再走（可比较的值按值认，值类型包装成环也只看一次）；不可比较的值认不出走过，组合成
// 分叉的环时看满上限就停下，不会走不完。

import "testing"

// cycleWrap 是值类型包装（可比较）：内嵌编排指针，编排的方法都提升上来。
type cycleWrap struct{ *HandoffAgent }

// sliceWrap 是不可比较的值类型包装（带切片字段）。
type sliceWrap struct {
	*HandoffAgent
	tags []string
}

func countWalk(subs []Agent) (visits int, exhausted bool) {
	exhausted = walkHistorySubs(subs, func(Agent) bool {
		visits++
		return true
	})
	return visits, exhausted
}

// 可比较的值类型包装在转交候选里出现两次、每次都指回自己：每个只看一次。
func TestWalkHistorySubs_ComparableCycleVisitedOnce(t *testing.T) {
	h := NewHandoffAgent(WithHandoffName("h"), WithHandoffDescription("h"), WithHandoffInitial("again1"))
	h.AddAgent("again1", cycleWrap{h})
	h.AddAgent("again2", cycleWrap{h})
	visits, exhausted := countWalk([]Agent{h})
	if exhausted || visits != 2 {
		t.Errorf("应看 h 与包装各一次（2 次）、不触上限，got visits=%d exhausted=%v", visits, exhausted)
	}
}

// 不可比较的值类型包装组成分叉的环：认不出走过，看满上限停下（按层数算是指数级，曾经的层数上限挡不住）。
func TestWalkHistorySubs_NonComparableCycleStopsAtLimit(t *testing.T) {
	h := NewHandoffAgent(WithHandoffName("h"), WithHandoffDescription("h"), WithHandoffInitial("again1"))
	h.AddAgent("again1", sliceWrap{HandoffAgent: h, tags: []string{"a"}})
	h.AddAgent("again2", sliceWrap{HandoffAgent: h, tags: []string{"b"}})
	visits, exhausted := countWalk([]Agent{h})
	if !exhausted || visits != historyBorrowMaxVisits {
		t.Errorf("应看满上限 %d 后停下，got visits=%d exhausted=%v", historyBorrowMaxVisits, visits, exhausted)
	}
}
