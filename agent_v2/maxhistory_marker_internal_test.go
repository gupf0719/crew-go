package agent_v2

import (
	"testing"

	"crew-go/schema"
)

// TestTruncatePriorPreservingMarkers 白盒钉死第 0 层简单截断的 **marker 保护**逻辑——
// 带 SkillBodyAnnotation 的消息不被计数截断（对齐 4 层压缩）。
//
// **sanity-revert**：把 truncatePriorPreservingMarkers 函数体换回裸 `prior[len(prior)-limit:]`
// 后，头部带 marker 的那条（第 0 条）会被切掉 → 本测试 FAIL 精确指向。
func TestTruncatePriorPreservingMarkers(t *testing.T) {
	listing := schema.SystemMessage("skill listing")
	listing.Annotations = map[string]any{SkillBodyAnnotation: true}

	// prior = listing（marker，最旧头部）+ 50 条普通消息
	prior := []*schema.Message{listing}
	for i := 0; i < 50; i++ {
		prior = append(prior, schema.UserMessage("m"))
	}

	out := truncatePriorPreservingMarkers(prior, 40)

	// 1) listing 必被保护（在结果里）
	foundListing := false
	for _, m := range out {
		if v, ok := m.Annotations[SkillBodyAnnotation].(bool); ok && v {
			foundListing = true
		}
	}
	if !foundListing {
		t.Fatal("truncatePriorPreservingMarkers 应保护 SkillBodyAnnotation 消息不被截断，实际被截掉")
	}
	// 2) protected(1 listing) + 最后 40 条 rest = 41 条
	if len(out) != 41 {
		t.Errorf("期望 41 条（1 protected + 40 rest），实际 %d", len(out))
	}
	// 3) listing 拼在最前（保其最旧位置）
	if v, ok := out[0].Annotations[SkillBodyAnnotation].(bool); !ok || !v {
		t.Error("protected listing 应拼在结果最前（最旧位置）")
	}
	// 4) 边界：len <= limit 原样返回不截
	short := []*schema.Message{listing, schema.UserMessage("a")}
	if got := truncatePriorPreservingMarkers(short, 40); len(got) != 2 {
		t.Errorf("len<=limit 应原样返回，实际 %d", len(got))
	}
	// 5) 无 marker 时等价裸尾截（不影响既有行为）
	plain := make([]*schema.Message, 0, 50)
	for i := 0; i < 50; i++ {
		plain = append(plain, schema.UserMessage("m"))
	}
	if got := truncatePriorPreservingMarkers(plain, 40); len(got) != 40 {
		t.Errorf("无 marker 应等价裸尾截到 40 条，实际 %d", len(got))
	}
}
