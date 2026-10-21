package agent_v2

// 白盒测试 checkForkRecursion——「fork 深度误算业务 skill_ 前缀节点」根因修复的回归。
// 用 package agent_v2（非 _test 黑盒）以引用包内未导出的 checkForkRecursion / forkNSPrefix /
// maxSkillForkDepth（项目规约：未导出函数走白盒 _internal_test.go）。

import (
	"strconv"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestCheckForkRecursion_BizSkillPrefixSegsNotCountedAsForkDepth 是根因修复的核心
// sanity-revert 测试。
//
// **bug 历史**：runForkMode 旧用 HasPrefix(seg.NodeID, "skill_") 数 ns 段估 fork 深度。
// 但业务 SequentialAgent step / ParallelAgent branch 名直接成 ns 段（JoinNS(parentNS,
// step.Name)）——业务用自然的 "skill_xxx" 命名（如 "skill_setup"）会被误算进 fork
// 深度，maxSkillForkDepth 个就触顶提前 fail-fast 误杀真 fork。
//
// **修复**：fork 段改用 forkNSPrefix("__skillfork_") sentinel 前缀，深度计数只数真 fork 段。
//
// **sanity-revert**：把 checkForkRecursion 里 HasPrefix(forkNSPrefix) 改回 HasPrefix("skill_")
// → 下面 5 个业务 skill_ 段被误算 forkSegs=5 >= maxSkillForkDepth → 返 "fork depth exceeded"
// → 本测试 fail，精确指向 bug。
func TestCheckForkRecursion_BizSkillPrefixSegsNotCountedAsForkDepth(t *testing.T) {
	// 模拟业务嵌套编排恰好用 "skill_" 前缀命名 maxSkillForkDepth 个 step（合法业务命名，非 fork）
	bizNames := []string{"skill_setup", "skill_load", "skill_parse", "skill_run", "skill_emit"}
	if len(bizNames) < maxSkillForkDepth {
		t.Fatalf("fixture 业务段数 %d 应 >= maxSkillForkDepth %d 才能触发旧 bug", len(bizNames), maxSkillForkDepth)
	}
	bizSegs := make([]compose_v2.NSSegment, len(bizNames))
	for i, n := range bizNames {
		bizSegs[i] = compose_v2.NSSegment{NodeID: n, Kind: "node", Index: -1}
	}
	// 真要 fork 的 sub-agent（forkNSPrefix 前缀，不在 bizSegs 里——非自指）
	err := checkForkRecursion(bizSegs, forkNSPrefix+"translate", "translate", strings.Join(bizNames, "/"))
	if err != nil {
		t.Fatalf("业务 skill_ 前缀段（%d 个）不应被算进 fork 深度（应只识别 %q sentinel），实际拒绝：%v",
			len(bizSegs), forkNSPrefix, err)
	}
}

// TestCheckForkRecursion_RealForkSegsCountedToDepth 验证真 fork 段（forkNSPrefix 前缀）
// 仍正常计入深度——确保修复没把深度限制彻底关掉。**测边界两侧**（下界不触发 + 临界触发）
// 抓 off-by-one：若 `>=` 误写 `>` 或 maxSkillForkDepth 调值，下界断言能抓到。
func TestCheckForkRecursion_RealForkSegsCountedToDepth(t *testing.T) {
	mkForkSegs := func(n int) []compose_v2.NSSegment {
		segs := make([]compose_v2.NSSegment, n)
		for i := range segs {
			segs[i] = compose_v2.NSSegment{
				NodeID: forkNSPrefix + "s" + strconv.Itoa(i), Kind: "fanout", Index: int64(i),
			}
		}
		return segs
	}
	// 下界：maxSkillForkDepth-1 个真 fork 段**不**触发（第 maxSkillForkDepth 层 fork 仍放行）
	if err := checkForkRecursion(mkForkSegs(maxSkillForkDepth-1), forkNSPrefix+"another", "another", "shallow/chain"); err != nil {
		t.Errorf("%d 个 fork 段（< maxSkillForkDepth）不应触发深度限制，实际：%v", maxSkillForkDepth-1, err)
	}
	// 临界：maxSkillForkDepth 个触发
	err := checkForkRecursion(mkForkSegs(maxSkillForkDepth), forkNSPrefix+"another", "another", "deep/fork/chain")
	if err == nil {
		t.Fatalf("%d 个真 fork 段应触发深度限制", maxSkillForkDepth)
	}
	if !strings.Contains(err.Error(), "fork depth exceeded") {
		t.Errorf("应返 'fork depth exceeded'，实际：%v", err)
	}
}

// TestCheckForkRecursion_SelfReferenceDetected 验证自指检测（segment 精确比对）。
func TestCheckForkRecursion_SelfReferenceDetected(t *testing.T) {
	segs := []compose_v2.NSSegment{
		{NodeID: forkNSPrefix + "research", Kind: "fanout", Index: 0},
	}
	err := checkForkRecursion(segs, forkNSPrefix+"research", "research", forkNSPrefix+"research#0")
	if err == nil || !strings.Contains(err.Error(), "recursive fork detected") {
		t.Errorf("自指（段 == subAgentNSName）应被检测返 'recursive fork detected'，实际：%v", err)
	}
}

// TestCheckForkRecursion_PrefixSubstringNotSelfReference 验证 R21：前缀子串不误判自指
// （"__skillfork_r" 不是 "__skillfork_research" 的自指）。
func TestCheckForkRecursion_PrefixSubstringNotSelfReference(t *testing.T) {
	segs := []compose_v2.NSSegment{
		{NodeID: forkNSPrefix + "research", Kind: "fanout", Index: 0},
	}
	// 要 fork 的是 "r"（前缀是 research 的子串）——segment 精确比对应区分
	err := checkForkRecursion(segs, forkNSPrefix+"r", "r", forkNSPrefix+"research#0")
	if err != nil {
		t.Errorf("R21：%q 不是 %q 的自指（segment 精确比对应区分），实际拒绝：%v",
			forkNSPrefix+"r", forkNSPrefix+"research", err)
	}
}
