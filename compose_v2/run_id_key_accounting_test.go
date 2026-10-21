package compose_v2_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunIDKey_LiteralCopiesAreAccounted 机械对账：字面量 `"run_id"` 的拷贝数不得增长。
//
// ⚠️ 为什么要这条：`run_id` 是 [compose_v2.RunIDAnnotationKey]，跨 3 个包被消费（agent_v2
// 盖戳、agui 分流、记忆指纹排除）。它一度是**未导出** const，注释写着「与 agent_v2 的 …
// 保持一致」——按本仓规约那是**设计债的自白**：判据在多处出现、靠人记着同步。
//
// 而漂了的后果**完全静默**：记忆指纹那处若漏掉排除，`run_id` 进指纹 ⟹ 每次 Run 指纹都不同
// ⟹ 等于随机键 ⟹ Resume 重跑每次多写一条记忆，`err` 全程 nil。
//
// ⚠️ 这条哨兵写完当场就发现我数错了——我 grep 数出 11 处，它跑出 12。**手数不可靠**，
// 这正是它存在的理由。
// 新写代码请用 `compose_v2.RunIDAnnotationKey`。
func TestRunIDKey_LiteralCopiesAreAccounted(t *testing.T) {
	// **按文件锁定，不是只锁总数。**
	//
	// ⚠️ 只锁总数有个固有弱点，审计实测确认在这个场景成立：收敛掉一处之后总数变小、
	// 哨兵**静默通过**（`t.Logf` 不带 `-v` 看不见），于是腾出一格——下次新增一处字面量
	// 照样绿。**棘轮不会自动收紧。** 按文件记就没这个问题：任何文件多一处都红。
	//
	// ⚠️ **数之前先分清这是两件事**（我自己就先合并错了一次，靠逐处读宿主才发现；
	// 而 agui 那处更绕——我把它归成 wire，审计用变异测试证明它是 annotation 侧）：
	//
	//   - **annotation key**：`schema.Message.Annotations["run_id"]`，框架给消息盖的戳。
	//     这些该用 [RunIDAnnotationKey]。**现已全部收敛**（生产代码里 0 处字面量）。
	//   - **事件 wire 字段名**：`Event.Metadata["run_id"]`（Go-to-Go）和 agui 发给浏览器的
	//     JSON 字段。跟前者**恰好同名，但是另一个契约**——后者跟前端的读取点配对，改了要
	//     一起改。用 annotation 的常量表达它，等于把两个能独立演进的东西焊死。
	//
	// 所以下面这些**不是待收敛的债**，是正当的 wire 字面量。它们自己没有共用常量（散落
	// 两个包），但有测试网兜着（改生产侧的键，agui 与 agent_v2 共 5 条测试立刻红），
	// 按「真问题修完就停」不再为它造第二个常量。
	baseline := map[string]int{
		"agent_v2/basic_agent.go":     3, // Event.Metadata：compact retry / stream 错误 / chunk
		"agent_v2/basic_builder.go":   1, // Event.Metadata：reset_stream
		"agui/adapter.go":             3, // 发给浏览器的 JSON 字段（index.html 读 d.run_id）
		"compose_v2/message_group.go": 1, // 常量自己的定义
	}

	root := ".."
	got := map[string]int{}
	err := filepath.Walk(root, func(p string, fi os.FileInfo, e error) error {
		if e != nil || fi.IsDir() || !strings.HasSuffix(p, ".go") {
			return nil
		}
		if strings.HasSuffix(p, "_test.go") || strings.Contains(p, "/example/") ||
			strings.Contains(p, "/.claude/") || strings.Contains(p, "/modules/") {
			return nil
		}
		b, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil
		}
		n := 0
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			if strings.Contains(line, `"run_id"`) {
				n++
			}
		}
		if n > 0 {
			got[strings.TrimPrefix(p, "../")] = n
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	for f, n := range got {
		want, ok := baseline[f]
		if !ok {
			t.Errorf("%s 新出现了 %d 处字面量 \"run_id\"——先判它属哪一类："+
				"annotation 侧请用 compose_v2.RunIDAnnotationKey；wire 侧才留字面量", f, n)
			continue
		}
		if n > want {
			t.Errorf("%s 的字面量从 %d 涨到 %d 处——新写的请先判属哪一类", f, want, n)
		}
		if n < want {
			t.Logf("%s 已降到 %d 处（基线 %d），把 baseline 调下来锁住成果", f, n, want)
		}
	}
	for f, want := range baseline {
		if _, ok := got[f]; !ok && want > 0 {
			t.Logf("%s 已清零（基线 %d），可以从 baseline 里删掉", f, want)
		}
	}
}
