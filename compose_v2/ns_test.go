package compose_v2_test

// ISSUE-70 Phase 2: ns.go + ns_ctx.go 单元测试。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

func TestJoinNS(t *testing.T) {
	cases := []struct {
		parent, child, want string
	}{
		{"", "a", "a"},
		{"a", "b", "a/b"},
		{"a/b", "c", "a/b/c"},
		{"a", "", "a"},
		{"", "", ""},
		{"a#0", "inner", "a#0/inner"}, // fan-out 分支内子图
	}
	for _, c := range cases {
		if got := compose_v2.JoinNS(c.parent, c.child); got != c.want {
			t.Errorf("JoinNS(%q, %q) = %q, want %q", c.parent, c.child, got, c.want)
		}
	}
}

func TestJoinFanoutNS(t *testing.T) {
	cases := []struct {
		parent, branch string
		idx            int64
		want           string
	}{
		{"", "worker", 0, "worker#0"},
		{"a", "worker", 2, "a/worker#2"},
		{"a/b", "worker", 5, "a/b/worker#5"},
		// 63 位下标（agent_v2 嵌套调用的身份哈希）照原样写出、SplitNS 读得回，与平台字长无关
		{"a", "worker", 1<<62 + 7, "a/worker#4611686018427387911"},
	}
	for _, c := range cases {
		if got := compose_v2.JoinFanoutNS(c.parent, c.branch, c.idx); got != c.want {
			t.Errorf("JoinFanoutNS(%q, %q, %d) = %q, want %q", c.parent, c.branch, c.idx, got, c.want)
		}
		segs, err := compose_v2.SplitNS(c.want)
		if err != nil || len(segs) == 0 || segs[len(segs)-1].Index != c.idx {
			t.Errorf("SplitNS(%q) = %+v, %v；末段下标应读回 %d", c.want, segs, err, c.idx)
		}
	}
}

func TestSplitNS_Valid(t *testing.T) {
	cases := []struct {
		ns   string
		want []compose_v2.NSSegment
	}{
		{"", nil},
		{"a", []compose_v2.NSSegment{{NodeID: "a", Kind: "node", Index: -1}}},
		{"a/b", []compose_v2.NSSegment{
			{NodeID: "a", Kind: "node", Index: -1},
			{NodeID: "b", Kind: "node", Index: -1},
		}},
		{"a#0", []compose_v2.NSSegment{{NodeID: "a", Kind: "fanout", Index: 0}}},
		{"a/b#1/c", []compose_v2.NSSegment{
			{NodeID: "a", Kind: "node", Index: -1},
			{NodeID: "b", Kind: "fanout", Index: 1},
			{NodeID: "c", Kind: "node", Index: -1},
		}},
		{"worker#0/inner", []compose_v2.NSSegment{
			{NodeID: "worker", Kind: "fanout", Index: 0},
			{NodeID: "inner", Kind: "node", Index: -1},
		}},
	}
	for _, c := range cases {
		got, err := compose_v2.SplitNS(c.ns)
		if err != nil {
			t.Errorf("SplitNS(%q) err: %v", c.ns, err)
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("SplitNS(%q) len=%d, want %d", c.ns, len(got), len(c.want))
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("SplitNS(%q)[%d] = %+v, want %+v", c.ns, i, got[i], c.want[i])
			}
		}
	}
}

func TestSplitNS_Invalid(t *testing.T) {
	cases := []string{
		"/a",   // 前导分隔符
		"a/",   // 尾部分隔符
		"a//b", // 连续分隔符
		"a#x",  // fanout idx 非数字
		"a#",   // fanout idx 空
		"#0",   // fanout 缺 nodeID
		"a#-1", // fanout idx 负
	}
	for _, ns := range cases {
		_, err := compose_v2.SplitNS(ns)
		if err == nil {
			t.Errorf("SplitNS(%q) 应返回 error", ns)
			continue
		}
		if !errors.Is(err, compose_v2.ErrInvalidNS) {
			t.Errorf("SplitNS(%q) err 应是 ErrInvalidNS，got %v", ns, err)
		}
	}
}

func TestValidateNodeIDForNS(t *testing.T) {
	valid := []string{
		"a", "ab", "node_1", "工作节点", "a-b", "Worker.V2",
		"worker:translator", // ":" 不是 ns 分隔符，允许
		"a:b:c",             // 多个 ":"
	}
	invalid := []string{
		"",       // 空
		"/",      // 单个 /
		"#",      // 单个 #
		"a/b",    // 含 /
		"a#b",    // 含 #
		"a/b/c",  // 多个 /
		"/a",     // / 开头
		"a/",     // / 结尾
		"node#0", // 末尾 #
		"#0",     // # 开头
		"a#",     // # 末尾
		"//",     // 全分隔符
		"#/a",    // 混合
	}

	for _, id := range valid {
		if err := compose_v2.ValidateNodeIDForNS(id); err != nil {
			t.Errorf("ValidateNodeIDForNS(%q) 应通过，got err: %v", id, err)
		}
	}
	for _, id := range invalid {
		if err := compose_v2.ValidateNodeIDForNS(id); err == nil {
			t.Errorf("ValidateNodeIDForNS(%q) 应失败", id)
		}
	}
}

func TestIsNSInSubtree(t *testing.T) {
	cases := []struct {
		root, ns string
		want     bool
	}{
		{"", "", true},
		{"", "a", true}, // 空 root = 整个 session
		{"", "a#0/b", true},
		{"a", "a", true}, // 自己在自己的子树里
		{"a", "a/b", true},
		{"a", "a/b/c", true},
		{"a", "a/b#1/c", true},
		{"a/b", "a/b/c", true},
		{"a#0", "a#0/inner", true},
		{"a@1", "a@1/inner", true},
		{"a", "a#0", false}, // 同一个节点的另一次执行
		{"a", "a#0/inner", false},
		{"a", "a@1", false},
		{"a", "a@1/inner", false},
		{"a", "ab", false}, // 字符串前缀不算
		{"a", "abc", false},
		{"a/b", "a", false},
		{"a", "b", false},
		{"a/b", "a/c", false},
		{"a#0", "a#1", false},
		{"a@1", "a@11", false},
	}
	for _, c := range cases {
		if got := compose_v2.IsNSInSubtree(c.root, c.ns); got != c.want {
			t.Errorf("IsNSInSubtree(%q, %q) = %v, want %v", c.root, c.ns, got, c.want)
		}
	}
}

func TestParentNS(t *testing.T) {
	// ParentNS 语义：fan-out 分支 parent 是 fan-out 节点本身，普通子图 parent
	// 是上层 graph ns。
	cases := []struct {
		ns, want string
	}{
		{"", ""},
		{"a", ""},            // 根级节点 → 根
		{"a/b", "a"},         // 普通子图，剥 /b
		{"a#0", "a"},         // 根级 fan-out 分支，剥 #0 → fan-out 节点 a
		{"a/b#1", "a/b"},     // 嵌套 fan-out 分支，剥 #1 → fan-out 节点 a/b（不剥 /b）
		{"a/b#1/c", "a/b#1"}, // fan-out 分支内子图，剥 /c
		{"a/b/c", "a/b"},     // 三层子图，剥 /c
		{"a#0/inner", "a#0"}, // 根级 fan-out 分支内子图，剥 /inner
	}
	for _, c := range cases {
		if got := compose_v2.ParentNS(c.ns); got != c.want {
			t.Errorf("ParentNS(%q) = %q, want %q", c.ns, got, c.want)
		}
	}
}

func TestNSCtx_GraphNS(t *testing.T) {
	ctx := context.Background()

	// 默认空
	if compose_v2.GetGraphNS(ctx) != "" {
		t.Error("默认 graph ns 应为 \"\"")
	}

	// 注入 graph ns
	ctx = compose_v2.WithGraphNS(ctx, "outer")
	if compose_v2.GetGraphNS(ctx) != "outer" {
		t.Errorf("graph ns 应是 outer，got %q", compose_v2.GetGraphNS(ctx))
	}

	// 嵌套子 ns push
	ctx = compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "child"))
	if compose_v2.GetGraphNS(ctx) != "outer/child" {
		t.Errorf("嵌套 push 应推进到 outer/child，got %q", compose_v2.GetGraphNS(ctx))
	}
}

// **历史删除** (audit follow-up #6):
//   - TestNSCtx_GraphAndCurrent + TestNSCtx_EnterChildNS: 测的 currentNS 维度 +
//     EnterChildNS helper 已删 (生产 0 caller, godoc 谎报"业务 tool 算 interrupt_id
//     用 currentNS" 实际不用, ask_user_tool 等业务 tool 走 GetGraphNS).
//   - TestNSCtx_EnterFanoutBranchNS (R32+ 视角 4): EnterFanoutBranchNS 已删
//     (生产 0 caller, ParallelAgent JoinNS+AddBranch panic 防重复名 +
//     AgentAsTool/SkillTool 直传 hash(tc.ID) ISSUE-78 替代)。
