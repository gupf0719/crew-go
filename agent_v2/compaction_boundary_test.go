package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// boundaryAgent：开了会话历史、关掉轮数截断、靠 AutoCompact 控制上下文——压缩边界服务的正是这种配置。
func boundaryAgent(name string, hs agent_v2.HistoryStore, m *compactLoopModel, s *countingSummarizer) *agent_v2.BasicAgent {
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(400),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0),
		agent_v2.WithAutoCompact(smallWindow(s)))
}

func countBoundaries(msgs []*schema.Message) int {
	n := 0
	for _, m := range msgs {
		if _, ok := m.Annotations[agent_v2.HistoryCompactionAnnotation]; ok {
			n++
		}
	}
	return n
}

// TestCompactionBoundary_NextRunStartsFromBoundary：上一次 Run 压缩过，下一次 Run 从最新的压缩边界往后
// 读——读回的条数是「边界之后」的量级，第一次模型调用看到的是上次的摘要而不是最早的原文，也不用在第一轮
// 把整场历史重新摘要一遍。
//
// 断言选在三个量上：Run 2 读回了多少条历史（没有边界时是整场）；Run 2 第一次模型调用里有没有上次的摘要；
// Run 2 要摘要时摘的是哪一段（没有边界时 prior 是整场原文、必然超阈值，摘要输入里是整场的工具结果）。Run 1 收尾时
// 离阈值多近随轮数而定，Run 2 摘不摘要不是断言的量。
func TestCompactionBoundary_NextRunStartsFromBoundary(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-boundary")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-boundary"}
	st := &pagedStore{inner: agent_v2.NewInMemoryHistoryStore()}

	m1 := &compactLoopModel{rounds: 300}
	s1 := &countingSummarizer{}
	if _, err := boundaryAgent("coder", st, m1, s1).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第一件事")); err != nil {
		t.Fatal(err)
	}
	all, _ := agent_v2.LoadAllHistory(ctx, st, key)
	if countBoundaries(all) == 0 {
		t.Fatalf("fixture 没搭对：Run 1 摘要了 %d 次，历史里却没有压缩边界", s1.count())
	}
	lastSummary := fmt.Sprintf("SUMMARY-S%d", s1.count())

	m2 := &compactLoopModel{rounds: 1}
	s2 := &countingSummarizer{}
	st.msgs.Store(0)
	if _, err := boundaryAgent("coder", st, m2, s2).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第二件事")); err != nil {
		t.Fatal(err)
	}
	first := inputText(m2.inputs[0])
	if !strings.Contains(first, lastSummary) {
		t.Fatalf("Run 2 第一次模型调用没看到 Run 1 最后那份摘要 %s：\n%.300s", lastSummary, first)
	}
	if strings.Contains(first, "第一件事") && !strings.Contains(first, "SUMMARY") {
		t.Fatalf("Run 2 看到的是 Run 1 的原文而不是摘要")
	}
	if s2.count() > 0 {
		total := strings.Count(inputText(all), "src/a.go:12")
		if got := strings.Count(inputText(s2.inputs[0]), "src/a.go:12"); got*3 > total {
			t.Fatalf("Run 2 在整场原文上重新摘要了一遍（摘要输入里 %d 行旧工具结果，整场 %d 行）", got, total)
		}
	}
	if n := st.msgs.Load(); n*3 > int64(len(all)) {
		t.Fatalf("会话 %d 条，Run 2 读回 %d 条——没从压缩边界开始读", len(all), n)
	} else {
		t.Logf("会话 %d 条（含 %d 条边界），Run 2 读回 %d 条", len(all), countBoundaries(all), n)
	}
}

// TestCompactionBoundary_OtherAgentsBoundaryIgnored：一条边界只对写下它的那个 agent 成立。别的 agent
// 读自己的视图时既不在别人的边界处停、也看不到它（它不是谁说的话）。
func TestCompactionBoundary_OtherAgentsBoundaryIgnored(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-two")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-two"}
	hs := agent_v2.NewInMemoryHistoryStore()

	// b 先说几句，a 再跑一个会压缩的长任务
	mb := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("b", hs, mb, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("B-ONLY-QUESTION")); err != nil {
		t.Fatal(err)
	}
	sa := &countingSummarizer{}
	if _, err := boundaryAgent("a", hs, &compactLoopModel{rounds: 60}, sa).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("A 的长任务")); err != nil {
		t.Fatal(err)
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	if countBoundaries(all) == 0 {
		t.Fatal("fixture 没搭对：a 没留下压缩边界")
	}
	// b 再跑一次：它的 prior 里要有自己上次的问题，不能有 a 的摘要
	mb2 := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("b", hs, mb2, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("b 的第二问")); err != nil {
		t.Fatal(err)
	}
	in := inputText(mb2.inputs[0])
	if !strings.Contains(in, "B-ONLY-QUESTION") {
		t.Fatalf("b 在 a 的边界处停下了，丢了自己上次的问题：\n%.300s", in)
	}
	if strings.Contains(in, "SUMMARY-S") {
		t.Fatalf("b 的视图里出现了 a 的摘要：\n%.300s", in)
	}
}

// TestCompactionBoundary_CorruptBoundaryIgnored：替代历史解不出来的边界当它不存在，接着往前读——
// 不能因为一条坏记录让 prior 变空。
func TestCompactionBoundary_CorruptBoundaryIgnored(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-bad")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-bad"}
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(m *schema.Message) *schema.Message {
		m.Annotations = map[string]any{agent_v2.HistoryAgentAnnotation: "coder"}
		return m
	}
	bad := mk(schema.UserMessage("坏掉的边界"))
	bad.Annotations[agent_v2.HistoryCompactionAnnotation] = "{not json"
	bad.Annotations["crew_history_compaction_mode"] = "0"
	_ = hs.Append(ctx, key, []*schema.Message{
		mk(schema.UserMessage("EARLY-QUESTION")), mk(schema.AssistantMessage("EARLY-ANSWER")), bad,
	})
	m := &compactLoopModel{rounds: 0}
	if _, err := boundaryAgent("coder", hs, m, &countingSummarizer{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatal(err)
	}
	if in := inputText(m.inputs[0]); !strings.Contains(in, "EARLY-QUESTION") || strings.Contains(in, "坏掉的边界") {
		t.Fatalf("坏边界之前的记录应照常读回、坏边界本身不进 prompt：\n%.300s", in)
	}
}

// TestCompactionBoundary_NotWrittenWhenHookAltersMessages：OnModelStart 钩子改了这一轮的消息列表时不写
// 压缩边界——压缩是在钩子改写之后的那份上做的，写进去的替代历史会带着钩子插入 / 改写的内容，下一次
// Run 当成历史重放。
func TestCompactionBoundary_NotWrittenWhenHookAltersMessages(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-hook")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-hook"}
	hs := agent_v2.NewInMemoryHistoryStore()
	s := &countingSummarizer{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(&compactLoopModel{rounds: 60}),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(100),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0), agent_v2.WithHooks(&prefixRewriteHook{}),
		agent_v2.WithAutoCompact(smallWindow(s)))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("改代码 原始")); err != nil {
		t.Fatal(err)
	}
	if s.count() == 0 {
		t.Fatal("fixture 没搭对：一次摘要都没发生")
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	if n := countBoundaries(all); n != 0 {
		t.Fatalf("钩子每轮改写消息，却写了 %d 条压缩边界——钩子改写的内容会被当成历史重放", n)
	}
}
