package compose_v2

import (
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// 允许直接调 MergeInterrupts 的地方。**只有判据本体那一处**。
var mergeInterruptsAllowedSites = map[string]string{
	"compose_v2/interrupt.go": "判据本体 pickFanoutError 就在这儿",
}

// TestFanoutArbitration_HasExactlyOneImplementation 钉住「扇出仲裁只有一份实现」。
//
// ⚠️ 为什么需要机械对账而不是靠注释提醒：这条规矩**已经失效过一次**。判据抽出来、godoc
// 写上「不是两份拷贝，是同一份」之后，仓里其实还躺着**第三份**逐字手写的同款三分支仲裁
// （BasicAgent 的多 tool_call 扇出）。它行为上完全冗余（接之前做过等价性对拍，全量套件
// 498 次调用两边逐字相同），却跟着漏掉了「终局失败严格支配中断」那一档——`AgentAsTool`
// 包的子 agent 返 abort、兄弟工具同时中断时，人会被问一个注定答不成的问题，答完 Resume
// 才 abort 并把答案一起清掉。
//
// 本仓规约：「必须与 X 保持一致」是设计债的自白，注释拦不住，因为改代码的人看的往往是
// 另一处。所以改成机械的：**手写扇出仲裁的标志就是直接调 MergeInterrupts**（要把 N 个
// 中断合成一批，绕不开它）。多出一个调用点就红。
//
// 真要新开一个仲裁点（而不是复用 PickFanoutError），把它加进 allowlist 并写明为什么它的
// 语义跟 PickFanoutError 不同。
func TestFanoutArbitration_HasExactlyOneImplementation(t *testing.T) {
	// 用 git grep 而不是 grep -r：只扫**已跟踪**的文件。裸 grep 会连
	// `.claude/worktrees/` 下遗留的 agent 工作树一起扫进来，那些是别的 checkout 的副本，
	// 报出来纯噪声（实测一次报了 23 条，全是副本）。
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("不在 git 仓库里：%v", err)
	}
	// `-C <root>` + `--full-name`：不加的话 git grep 只扫当前包、路径还是相对的
	// （实测：从 compose_v2/ 跑只报 interrupt.go，agent_v2 里新写的一份完全看不见）。
	found, _ := exec.Command("git", "-C", strings.TrimSpace(string(root)),
		"grep", "-n", "--full-name", "MergeInterrupts(", "--", "*.go").Output()

	call := regexp.MustCompile(`MergeInterrupts\(`)
	var offenders []string
	for _, line := range strings.Split(string(found), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}
		rel, body := parts[0], parts[2]
		if strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "doc/") {
			continue
		}
		// 声明行本身不算调用
		if strings.Contains(body, "func MergeInterrupts(") {
			continue
		}
		// 注释里提到不算
		if trimmed := strings.TrimSpace(body); strings.HasPrefix(trimmed, "//") {
			continue
		}
		if !call.MatchString(body) {
			continue
		}
		if _, ok := mergeInterruptsAllowedSites[rel]; ok {
			continue
		}
		offenders = append(offenders, rel+":"+parts[1])
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("这些地方在手写扇出仲裁（直接调 MergeInterrupts）：%v\n"+
			"  → 扇出挑错请走 compose_v2.PickFanoutError，判据只该有一份。\n"+
			"  → 真的需要不同语义，加进 mergeInterruptsAllowedSites 并写明哪里不同。",
			offenders)
	}
}
