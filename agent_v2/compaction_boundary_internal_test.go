package agent_v2

import (
	"strings"
	"testing"

	"crew-go/schema"
)

// TestCompactionBoundary_ReplacementExcludesRecomputedParts：替代历史里不带每次 Run 都会重新算出来的
// 东西——长期记忆注入（preamble 每次重新检索注入）与编排拼出来的派生输入（本来就不进历史）。带上的话
// 下一次 Run 的 prompt 里长期记忆出现两份、派生输入从此留在历史里。
func TestCompactionBoundary_ReplacementExcludesRecomputedParts(t *testing.T) {
	mem := schema.SystemMessage("用户偏好：中文")
	mem.Annotations = map[string]any{StoreInjectedAnnotation: true}
	summary := schema.UserMessage("# Previous Conversation Summary\n\nSUM")
	summary.Annotations = map[string]any{AutoCompactSummaryAnnotation: true}
	derived := MarkDerivedInput(schema.UserMessage("原始目标 + 上一步产出"))
	keep := schema.AssistantMessage("最近一条")
	stream := []*schema.Message{mem, schema.UserMessage("早"), schema.AssistantMessage("早答"), derived, keep}
	memo := &compactionMemo{Upto: 3, Head: []*schema.Message{mem, summary}}

	b := buildCompactionBoundary(memo, stream, HandoffHistorySelfOnly)
	if b == nil {
		t.Fatal("有摘要的压缩结果应产出边界")
	}
	if b.Content != summary.Content {
		t.Errorf("边界正文应是摘要，got %q", b.Content)
	}
	r, ok := decodeCompactionBoundary(b, HandoffHistorySelfOnly)
	if !ok {
		t.Fatal("同视图应解得出替代历史")
	}
	var got []string
	for _, m := range r {
		got = append(got, m.Content)
	}
	joined := strings.Join(got, "|")
	if strings.Contains(joined, "用户偏好") || strings.Contains(joined, "原始目标") {
		t.Fatalf("替代历史带上了长期记忆注入或派生输入：%s", joined)
	}
	if !strings.Contains(joined, "SUM") || !strings.Contains(joined, "最近一条") {
		t.Fatalf("替代历史缺了摘要或尾部：%s", joined)
	}
}

// TestCompactionBoundary_OtherViewNotUsed：边界只对写下它时的那种视图成立——同一个 agent 换了视图
// （转交改写 vs 只看自己）读，不能用它。
func TestCompactionBoundary_OtherViewNotUsed(t *testing.T) {
	summary := schema.UserMessage("SUM")
	summary.Annotations = map[string]any{AutoCompactSummaryAnnotation: true}
	b := buildCompactionBoundary(&compactionMemo{Upto: 0, Head: []*schema.Message{summary}}, nil, HandoffHistoryRewrite)
	if b == nil {
		t.Fatal("应产出边界")
	}
	if _, ok := decodeCompactionBoundary(b, HandoffHistorySelfOnly); ok {
		t.Fatal("改写视图下写的边界，被只看自己的视图用上了")
	}
	if _, ok := decodeCompactionBoundary(b, HandoffHistoryRewrite); !ok {
		t.Fatal("同视图应能用")
	}
}

// TestCompactionBoundary_NoSummaryNoBoundary：没有摘要的「压缩结果」（只是重排了受保护的组）不成为边界。
func TestCompactionBoundary_NoSummaryNoBoundary(t *testing.T) {
	if b := buildCompactionBoundary(&compactionMemo{Upto: 0, Head: []*schema.Message{schema.UserMessage("x")}}, nil, HandoffHistorySelfOnly); b != nil {
		t.Fatal("没有摘要时不该写边界")
	}
}
