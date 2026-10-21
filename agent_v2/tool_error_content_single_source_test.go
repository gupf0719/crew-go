package agent_v2

// 机械对账：失败 tool_result 的 `[code] message` 拼法只能有一处。
//
// 钉的形态：这个格式有三个参与者（两个生产者 + 一个必须逐字复现它们的消费者），
// 三处手写时漂移的症状是**静默的**——`OnToolEnd` 的回写会按 (ErrorCode, Error)
// 重算 content，拼不回原样就是「业务 hook 什么都不改，tool_result 也被改掉」。
// 双前缀那个 bug 正是这么来的。
//
// 这条测试挡的是「有人又手写了第四份」。

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// 手写「`[` + 某个 code + `] ` + 某段文本」的两种形状。
//
// **两种都要认**：只认 `fmt.Sprintf` 那一种时，手写字符串拼接（`"["+code+"] "+msg`）
// 绕得过去——而那恰恰是 helper 自己用的写法，也就是最容易被照抄的那种。自查时实测过
// 这个盲区。
var handRolledPrefixRes = []*regexp.Regexp{
	regexp.MustCompile(`fmt\.Sprintf\("\[%[sv]\] %[sv]"`),
	regexp.MustCompile(`"\["\s*\+.*\+\s*"\] "`),
}

// codeIdentRe 区分「拼的是 error code」和「拼的是别的东西」。
//
// 同一个 `[x] y` 形状在本包里有**合法的另一种用途**——`parallel_builder.go` 把分支
// 名拼进聚合输出（`"[" + r.Name + "] " + ...`）。那不是失败文本，不该被这条对账拦。
// 判据取「这一行提到了 code 吗」：ErrorCode / Error.Code / 裸 code 都命中，r.Name 不命中。
var codeIdentRe = regexp.MustCompile(`\bcode\b|Code\b`)

func TestToolErrorContent_IsSingleSource(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		inHelper := false
		for i, line := range strings.Split(string(body), "\n") {
			// helper 自己那一行是**唯一**允许出现这个形状的地方
			if strings.HasPrefix(line, "func toolErrorContent(") {
				inHelper = true
			} else if inHelper && strings.HasPrefix(line, "}") {
				inHelper = false
			}
			if inHelper {
				continue
			}
			if !codeIdentRe.MatchString(line) {
				continue // 拼的不是 error code（如 parallel 的分支名），不归这条管
			}
			for _, re := range handRolledPrefixRes {
				if re.MatchString(line) {
					offenders = append(offenders, name+":"+strconv.Itoa(i+1))
					break
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Errorf("失败 tool_result 的 `[code] message` 拼法出现了手写副本：%v\n"+
			"  → 改调 toolErrorContent(code, message)。三处手写时漂移是静默的："+
			"OnToolEnd 的回写按 (ErrorCode, Error) 重算 content，拼不回原样就是"+
			"「hook 什么都不改，tool_result 也被改掉」。", offenders)
	}
}
