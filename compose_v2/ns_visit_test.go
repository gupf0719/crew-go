package compose_v2_test

// ns 重访段（"@N"）的解析/拼接/族谱哨兵——配套 JoinVisitNS（ns.go）。

import (
	"errors"
	"testing"

	"crew-go/compose_v2"
)

func TestJoinVisitNS(t *testing.T) {
	cases := []struct {
		parent, node string
		visit        int
		want         string
	}{
		{"", "sub", 0, "sub"},      // 首轮不加缀 = JoinNS
		{"a", "sub", 0, "a/sub"},   // 同上
		{"", "sub", 1, "sub@1"},    // 第 2 次访问
		{"a", "sub", 1, "a/sub@1"}, //
		{"a/b", "sub", 2, "a/b/sub@2"},
		{"a", "sub", -3, "a/sub"}, // 负 visit 按 0 处理（防御）
	}
	for _, c := range cases {
		if got := compose_v2.JoinVisitNS(c.parent, c.node, c.visit); got != c.want {
			t.Errorf("JoinVisitNS(%q,%q,%d) = %q, want %q", c.parent, c.node, c.visit, got, c.want)
		}
	}
}

func TestSplitNS_VisitSegment(t *testing.T) {
	segs, err := compose_v2.SplitNS("a/sub@1/inner")
	if err != nil {
		t.Fatalf("SplitNS: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("want 3 segs, got %d", len(segs))
	}
	if segs[1].NodeID != "sub" || segs[1].Kind != "visit" || segs[1].Index != 1 {
		t.Fatalf("visit 段解析错: %+v", segs[1])
	}
	if segs[2].Kind != "node" || segs[2].Index != -1 {
		t.Fatalf("后续 node 段解析错: %+v", segs[2])
	}

	// 非法形态逐条拒
	for _, bad := range []string{"a@0", "a@x", "a@-1", "a@", "@1", "a#1@2", "a@2#1"} {
		if _, err := compose_v2.SplitNS(bad); !errors.Is(err, compose_v2.ErrInvalidNS) {
			t.Errorf("SplitNS(%q) 该报 ErrInvalidNS, got %v", bad, err)
		}
	}
}

func TestVisitNS_SubtreeAndParent(t *testing.T) {
	// 重访实例按节点归组（ParentNS 挂在 a 下面），但不属于 a 那次执行的子树
	if compose_v2.IsNSInSubtree("a", "a@1") {
		t.Errorf("a@1 是 a 的兄弟执行，不该在 a 的子树里")
	}
	if !compose_v2.IsNSInSubtree("a@1", "a@1/inner") {
		t.Errorf("a@1/inner 该在 a@1 的子树里")
	}
	if compose_v2.IsNSInSubtree("a@1", "a@2") {
		t.Errorf("两个重访实例互不在对方子树里")
	}
	if got := compose_v2.ParentNS("a/sub@1"); got != "a/sub" {
		t.Errorf("ParentNS(a/sub@1) = %q, want a/sub", got)
	}
	if got := compose_v2.ParentNS("a/sub@1/inner"); got != "a/sub@1" {
		t.Errorf("ParentNS(a/sub@1/inner) = %q, want a/sub@1", got)
	}
}

func TestValidateNodeIDForNS_RejectsVisitMarker(t *testing.T) {
	if err := compose_v2.ValidateNodeIDForNS("sub@1"); !errors.Is(err, compose_v2.ErrInvalidNodeIDForNS) {
		t.Fatalf("含 @ 的 nodeID 该被拒（会与重访派生 ns 撞槽）, got %v", err)
	}
}
