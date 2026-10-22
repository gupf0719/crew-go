package history_conformance

import (
	"context"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// 分页读（HistoryQuery 的 Before + Limit）——框架每次 Run 只读最近一段，靠的就是这几条。
//
// 它们的失效同样是静默的：Limit 取成了最早的 N 条，模型看到的是会话开头而不是刚才说的；游标随
// 追加漂移，往前翻页时漏掉或重复几条；More 报错了，框架要么提前停（prior 缺一截）、要么一直翻
// 到会话开头（只读尾部白做）。

// seed 往 convA 写 n 条内容为 "m01".."mNN" 的消息，分几批写（批边界不该影响位置）。
func seed(t *testing.T, hs agent_v2.HistoryStore, n int) {
	t.Helper()
	var batch []*schema.Message
	for i := 1; i <= n; i++ {
		batch = append(batch, schema.UserMessage(fmt.Sprintf("m%02d", i)))
		if len(batch) == 3 || i == n {
			mustAppend(t, hs, convA, batch...)
			batch = nil
		}
	}
}

func contents(msgs []*schema.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Content)
	}
	return out
}

func mustPage(t *testing.T, hs agent_v2.HistoryStore, q agent_v2.HistoryQuery) agent_v2.HistoryPage {
	t.Helper()
	p, err := hs.Load(context.Background(), convA, q)
	if err != nil {
		t.Fatalf("Load(%+v): %v", q, err)
	}
	return p
}

// testLoadLimitReturnsLatest：Limit 取的是**最近的** N 条，按写入顺序返回。
func testLoadLimitReturnsLatest(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	seed(t, hs, 10)
	p := mustPage(t, hs, agent_v2.HistoryQuery{Limit: 3})
	if got := fmt.Sprint(contents(p.Messages)); got != "[m08 m09 m10]" {
		t.Fatalf("Load(Limit=3) = %s, want [m08 m09 m10] —— Limit 要取最近的 N 条且旧在前", got)
	}
	if !p.More {
		t.Error("前面还有 7 条，More 应为 true")
	}
	if p.Start == "" {
		t.Error("非空页的 Start 不能为空——框架拿它往前翻页")
	}
}

// testPagingCoversAllOnce：从末尾一页一页往前翻，拼起来恰好是整场会话——不漏、不重、顺序对。
func testPagingCoversAllOnce(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	seed(t, hs, 25)
	var pages [][]*schema.Message
	before := agent_v2.HistoryCursor("")
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("翻了 20 页还没翻到头——More 或 Start 不对")
		}
		p := mustPage(t, hs, agent_v2.HistoryQuery{Before: before, Limit: 4})
		pages = append(pages, p.Messages)
		if !p.More {
			break
		}
		if p.Start == before {
			t.Fatalf("游标原地不动：Start=%q", p.Start)
		}
		before = p.Start
	}
	var all []*schema.Message
	for i := len(pages) - 1; i >= 0; i-- {
		all = append(all, pages[i]...)
	}
	want := make([]string, 0, 25)
	for i := 1; i <= 25; i++ {
		want = append(want, fmt.Sprintf("m%02d", i))
	}
	if got := fmt.Sprint(contents(all)); got != fmt.Sprint(want) {
		t.Fatalf("逐页往前翻拼起来 = %s\nwant %s", got, fmt.Sprint(want))
	}
}

// testPagingStableUnderAppend：往前翻页的过程中别人往末尾追加，不影响已经拿到的游标。
//
// 框架读历史时同一场会话的别的分支可能正在写（异步写入器）。游标若是「从末尾数第几条」这种相对
// 位置，追加之后下一页就错位：漏掉几条或者重复几条。
func testPagingStableUnderAppend(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	seed(t, hs, 20)
	p1 := mustPage(t, hs, agent_v2.HistoryQuery{Limit: 5})
	mustAppend(t, hs, convA, schema.UserMessage("late-1"), schema.UserMessage("late-2"))
	p2 := mustPage(t, hs, agent_v2.HistoryQuery{Before: p1.Start, Limit: 5})
	if got := fmt.Sprint(contents(p2.Messages)); got != "[m11 m12 m13 m14 m15]" {
		t.Fatalf("追加之后接着往前翻 = %s, want [m11 m12 m13 m14 m15] —— 游标随追加漂移了", got)
	}
}

// testLimitBeyondTotal：Limit 比剩下的多时返剩下的全部，More 为 false。
func testLimitBeyondTotal(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	seed(t, hs, 5)
	p := mustPage(t, hs, agent_v2.HistoryQuery{Limit: 100})
	if len(p.Messages) != 5 || p.More {
		t.Fatalf("Load(Limit=100) 返 %d 条、More=%v, want 5 条、More=false", len(p.Messages), p.More)
	}
}

// testBeforeOldestIsEmpty：游标已经是会话第一条时，再往前读是空页、More 为 false。
func testBeforeOldestIsEmpty(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	seed(t, hs, 4)
	p := mustPage(t, hs, agent_v2.HistoryQuery{Limit: 10})
	if p.More || len(p.Messages) != 4 {
		t.Fatalf("Load(Limit=10) 返 %d 条、More=%v, want 4 条、More=false", len(p.Messages), p.More)
	}
	empty := mustPage(t, hs, agent_v2.HistoryQuery{Before: p.Start, Limit: 10})
	if len(empty.Messages) != 0 || empty.More {
		t.Fatalf("越过第一条再往前读返 %d 条、More=%v, want 空页", len(empty.Messages), empty.More)
	}
}
