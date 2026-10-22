package history_conformance

import (
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// 按条件读（HistoryQuery 的 MessageID / Task / Agent）——框架的去重（「这个任务里记过 X 没有」「X 是不是已经在
// 别的归属下记过」）只靠这几个条件问后端，「只看自己」的视图按归属读。
//
// 失效同样是静默的：条件没生效（返了不该返的）时去重误判「记过」，该记的那份没记，模型下一轮少一句话；
// 条件之间按「或」处理时同理；More 把不满足条件的更早记录也算进去时，框架往前再读一页拿到空页、判后端违约，
// 这次去重按「没记过」处理，同一句话记两份。

// rec 造一条带消息 ID、任务章与归属的记录（三者为空时不设）。
func rec(content, id, task, agent string) *schema.Message {
	m := schema.UserMessage(content)
	if id != "" {
		m.ID = id
	}
	m.Annotations = map[string]any{}
	if task != "" {
		m.Annotations[agent_v2.HistoryTaskAnnotation] = task
	}
	if agent != "" {
		m.Annotations[agent_v2.HistoryAgentAnnotation] = agent
	}
	return m
}

// testFilterByMessageID：按消息 ID 读返回这个 ID 的全部记录（同一个 ID 可以记多份），按写入顺序。
func testFilterByMessageID(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	mustAppend(t, hs, convA,
		rec("x@a", "id-x", "t1", "a"),
		rec("y", "id-y", "t1", "a"),
	)
	mustAppend(t, hs, convA,
		rec("x@b", "id-x", "t2", "b"),
		rec("z", "id-z", "t2", "b"),
	)
	p := mustPage(t, hs, agent_v2.HistoryQuery{MessageID: "id-x"})
	if got := fmt.Sprint(contents(p.Messages)); got != "[x@a x@b]" {
		t.Fatalf("Load(MessageID=id-x) = %s, want [x@a x@b]——按 ID 读要返这个 ID 的每一份、旧在前", got)
	}
	if p.More {
		t.Error("这个 ID 前面没有更早的记录了，More 应为 false")
	}
	if got := fmt.Sprint(contents(mustPage(t, hs, agent_v2.HistoryQuery{MessageID: "id-z", Limit: 5}).Messages)); got != "[z]" {
		t.Fatalf("Load(MessageID=id-z, Limit=5) = %s, want [z]", got)
	}
}

// testFilterConditionsAreANDed：几个条件同时给时都要满足；Limit 作用在满足条件的记录上。
func testFilterConditionsAreANDed(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	mustAppend(t, hs, convA,
		rec("t1/a#1", "id-1", "t1", "a"),
		rec("t1/b", "id-2", "t1", "b"),
		rec("t2/a", "id-1", "t2", "a"),
		rec("t1/a#2", "id-3", "t1", "a"),
		rec("t2/b", "id-4", "t2", "b"),
	)
	cases := []struct {
		q    agent_v2.HistoryQuery
		want string
	}{
		{agent_v2.HistoryQuery{Task: "t1"}, "[t1/a#1 t1/b t1/a#2]"},
		{agent_v2.HistoryQuery{Agent: "b"}, "[t1/b t2/b]"},
		{agent_v2.HistoryQuery{Task: "t1", Agent: "a"}, "[t1/a#1 t1/a#2]"},
		{agent_v2.HistoryQuery{MessageID: "id-1", Task: "t2"}, "[t2/a]"},
		{agent_v2.HistoryQuery{MessageID: "id-1", Agent: "b"}, "[]"},
		{agent_v2.HistoryQuery{Task: "t1", Agent: "a", Limit: 1}, "[t1/a#2]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(contents(mustPage(t, hs, c.q).Messages)); got != c.want {
			t.Errorf("Load(%+v) = %s, want %s", c.q, got, c.want)
		}
	}
}

// testFilterPagingCoversMatchesOnce：带条件往前一页一页翻，拼起来恰好是满足条件的全部记录——不漏、不重；翻到
// 最早一条满足条件的记录时 More 为 false，哪怕它前面还有不满足条件的记录；中途别人追加（满足与不满足条件的
// 都有），已经拿到的游标照样接得上。
func testFilterPagingCoversMatchesOnce(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	mustAppend(t, hs, convA, rec("other-0", "", "t2", "a")) // 最早一条不满足条件
	var want []string
	for i := 1; i <= 24; i++ {
		task := "t2"
		if i%2 == 1 {
			task = "t1"
			want = append(want, fmt.Sprintf("m%02d", i))
		}
		mustAppend(t, hs, convA, rec(fmt.Sprintf("m%02d", i), "", task, "a"))
	}
	var pages [][]*schema.Message
	before := agent_v2.HistoryCursor("")
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("翻了 20 页还没翻到头——More 或 Start 不对")
		}
		p := mustPage(t, hs, agent_v2.HistoryQuery{Before: before, Limit: 4, Task: "t1"})
		if len(p.Messages) == 0 && p.More {
			t.Fatal("空页却报 More=true——框架会判后端违约")
		}
		pages = append(pages, p.Messages)
		if i == 0 {
			mustAppend(t, hs, convA, rec("late-t1", "", "t1", "a"), rec("late-t2", "", "t2", "a"))
		}
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
	if got := fmt.Sprint(contents(all)); got != fmt.Sprint(want) {
		t.Fatalf("带条件逐页往前翻拼起来 = %s\nwant %s", got, fmt.Sprint(want))
	}
	if len(pages[len(pages)-1]) == 0 {
		t.Fatal("最早一条满足条件的记录所在那页报了 More=true（它前面只剩不满足条件的记录）——More 要按满足" +
			"条件的算，否则框架每次查不到都要多读一页空页")
	}
}

// testFilterNoMatchIsEmpty：没有满足条件的记录时返空页、More 为 false、不报错——「没记过」不是错误；会话不存在时
// 同样。
func testFilterNoMatchIsEmpty(t *testing.T, factory HistoryStoreFactory) {
	hs := factory()
	mustAppend(t, hs, convA, rec("a", "id-a", "t1", "a"), rec("b", "id-b", "t1", "a"))
	for _, q := range []agent_v2.HistoryQuery{
		{MessageID: "nope"},
		{Task: "nope", Limit: 3},
		{Agent: "nope"},
	} {
		p := mustPage(t, hs, q)
		if len(p.Messages) != 0 || p.More {
			t.Errorf("Load(%+v) 返 %d 条、More=%v, want 空页", q, len(p.Messages), p.More)
		}
	}
	empty := factory()
	p := mustPage(t, empty, agent_v2.HistoryQuery{MessageID: "id-a"})
	if len(p.Messages) != 0 || p.More {
		t.Errorf("会话不存在时按条件读返 %d 条、More=%v, want 空页", len(p.Messages), p.More)
	}
}
