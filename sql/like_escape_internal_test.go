package sql

// LIKE 转义白盒哨兵（escapeLikePattern 未导出，按规约走 package sql + _internal_test.go）。
//
// 钉的缺陷形态：ns / namespace 前缀被原样拼进 LIKE 模式，而 `_` 是单字符通配符、
// `%` 是任意长通配符——校验层只守分隔符（ValidateNodeIDForNS 只拒 `/ # @`，
// Namespace.Validate 只拒 `.` 与 NUL），两者都放行 `_` 和 `%`。
//
// 为什么不用真库跑：sql 模块无 sqlite 驱动，为测试引 CGO 依赖不值。LIKE 的
// `_`/`%`/ESCAPE 语义是 SQL 标准、PG 与 MySQL 一致，故这里钉**转义后的模式串**，
// 由 sqlmock 那两条钉「模式串确实被送进了 SQL」。

import "testing"

func TestEscapeLikePattern(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// 触发 bug 的真实形态：框架自己生成的 ns 段
		{"LoopAgent 的 round_N", "round_1", "round!_1"},
		{"PlanAgent 的 step", "worker_step_2", "worker!_step!_2"},
		{"HandoffAgent 的 hop", "agent_x_hop1", "agent!_x!_hop1"},
		// store 层：namespace 段
		{"namespace 含下划线", "a_b", "a!_b"},
		{"namespace 含百分号", "a%b", "a!%b"},
		// 转义符自身必须先转，否则二次转义
		{"前缀含转义符", "a!b", "a!!b"},
		{"转义符与通配符同现", "a!_b", "a!!!_b"},
		// 无元字符时原样
		{"干净前缀", "plain/seg", "plain/seg"},
		{"空串", "", ""},
	}
	for _, c := range cases {
		if got := escapeLikePattern(c.in); got != c.want {
			t.Errorf("%s: escapeLikePattern(%q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

// TestEscapeLikePattern_NeutralizesWildcards：转义后的模式只能匹配字面量自己。
// 这里用一个最小 LIKE 解释器复刻 SQL 语义（`_` 单字符 / `%` 任意长 / ESCAPE 取消
// 元字符含义），把「转义确实消除了通配能力」这件事变成可执行断言，而不是靠人读。
func TestEscapeLikePattern_NeutralizesWildcards(t *testing.T) {
	cases := []struct {
		prefix   string // 调用方给的字面量前缀
		other    string // 不该被它匹配到的邻居
		ownChild string // 该被匹配到的自己的后代
	}{
		{"round_1", "roundX1/child", "round_1/child"},
		{"a_b", "a.b.secret", "a_b.own"}, // store 层：`_` 曾匹配命名空间分隔符 `.`
		{"a%b", "aXXb/inner", "a%b/inner"},
	}
	for _, c := range cases {
		pat := escapeLikePattern(c.prefix) + "%"
		if likeMatch(pat, c.other) {
			t.Errorf("prefix %q 的模式 %q 不该匹配邻居 %q（通配符没被消除）", c.prefix, pat, c.other)
		}
		if !likeMatch(pat, c.ownChild) {
			t.Errorf("prefix %q 的模式 %q 该匹配自己的后代 %q（转义过头）", c.prefix, pat, c.ownChild)
		}
	}
}

// likeMatch 是 SQL LIKE 的最小复刻（含 ESCAPE '!'），仅供本测试断言语义用。
func likeMatch(pattern, s string) bool {
	var m func(p, v int) bool
	m = func(p, v int) bool {
		for p < len(pattern) {
			switch pattern[p] {
			case likeEscapeChar[0]:
				// 转义符：下一个字符按字面量比
				if p+1 >= len(pattern) {
					return false
				}
				if v >= len(s) || s[v] != pattern[p+1] {
					return false
				}
				p, v = p+2, v+1
			case '%':
				for skip := v; skip <= len(s); skip++ {
					if m(p+1, skip) {
						return true
					}
				}
				return false
			case '_':
				if v >= len(s) {
					return false
				}
				p, v = p+1, v+1
			default:
				if v >= len(s) || s[v] != pattern[p] {
					return false
				}
				p, v = p+1, v+1
			}
		}
		return v == len(s)
	}
	return m(0, 0)
}
