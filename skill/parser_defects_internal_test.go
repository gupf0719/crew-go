package skill

import (
	"strings"
	"testing"
)

// TestParser_SevenDefectsFromHandRolledYAML 一条哨兵钉住手写 YAML 子集解析器留下的
// **7 条同源缺陷**，每条一个子测试。
//
// ⚠️ 它们全部**静默**——旧实现下这 7 种写法都是 `err == nil` 加一个悄悄错掉的值。
// 其中第 5 条尤其说明根因：那道「非列表键收到列表」的闸只挂在两条产列表分支的一条上，
// 而**那两条分支是手写造出来的**——YAML 里 flow `[a]` 和 block `- a` 是同一种 sequence
// 节点。换成真解析器之后「两条路」根本不存在，判一次覆盖两种写法。
//
// 第 7 条是**换解析器不解决**的那一条：frontmatter 的切分在解析之前、是我们自己做的，
// 而 `strings.TrimSpace` 不吃 U+FEFF（Cf 类，不是 White_Space）。得单独剥。
func TestParser_SevenDefectsFromHandRolledYAML(t *testing.T) {
	const base = "name: d\ndescription: x\n"

	t.Run("1_嵌套metadata不泄漏到顶层", func(t *testing.T) {
		// 标准规定厂商扩展放 metadata 底下 ⟹ 照标准写就踩。旧实现对每行先 TrimSpace，
		// 缩进完全不可见：子键 name 顶掉 skill 真实身份、子键 context 的非法值让整份被拒。
		d, err := parseFrontmatter(base + "metadata:\n  name: vendor-thing\n  context: bogus\n")
		if err != nil {
			t.Fatalf("合规文件被拒：%v", err)
		}
		if d.Name != "d" {
			t.Errorf("skill 真实身份被厂商块顶掉：Name=%q", d.Name)
		}
		if d.Context != "" {
			t.Errorf("嵌套子键泄漏到顶层：Context=%q", d.Context)
		}
	})

	t.Run("2_多行标量保住正文", func(t *testing.T) {
		// 旧实现：WhenToUse == "|"，正文全丢，无 error 无 warn。
		d, err := parseFrontmatter(base + "when_to_use: |\n  第一行\n  第二行\n")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(d.WhenToUse, "第一行") || !strings.Contains(d.WhenToUse, "第二行") {
			t.Errorf("多行正文丢了：%q", d.WhenToUse)
		}
	})

	t.Run("3_行尾注释不吞值", func(t *testing.T) {
		d, err := parseFrontmatter(base + "tags: [a, b] # 这是注释\n")
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Tags) != 2 {
			t.Errorf("整条丢空了：Tags=%v", d.Tags)
		}
	})

	t.Run("4_引号里的逗号不被切碎", func(t *testing.T) {
		d, err := parseFrontmatter(base + `examples: ["先读文件, 再改写", "只读"]` + "\n")
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Examples) != 2 || !strings.Contains(d.Examples[0], "，再改写") &&
			!strings.Contains(d.Examples[0], ", 再改写") {
			t.Errorf("被切碎了：%v", d.Examples)
		}
	})

	t.Run("5_标量键收到列表两种写法都报错", func(t *testing.T) {
		// 旧实现：flow 那条经值校验报错，block 那条**静默走默认 inline**。
		for _, fm := range []string{"context: [inline]\n", "context:\n  - inline\n"} {
			if _, err := parseFrontmatter(base + fm); err == nil {
				t.Errorf("%q 该报错，实际静默通过", fm)
			}
		}
	})

	t.Run("6_键别名走通用归一化", func(t *testing.T) {
		// 旧实现是逐键列别名，而那份清单跟另一处判「这个键要不要收列表」的清单必须同步，
		// 实测漂过一次（大小写没对齐 ⟹ flow 形式下 Tags/allowedTools 静默丢空）。
		for _, k := range []string{"whenToUse", "when_to_use", "when-to-use", "WHEN-TO-USE"} {
			d, err := parseFrontmatter(base + k + ": abc\n")
			if err != nil || d.WhenToUse != "abc" {
				t.Errorf("别名 %s 没命中：%q err=%v", k, d.WhenToUse, err)
			}
		}
	})

	t.Run("7_BOM不让整份frontmatter失踪", func(t *testing.T) {
		// 旧实现：整份不识别 ⟹ skillbox 报的却是「缺 name，跳过」，误导到另一个方向。
		fm, _ := splitFrontmatter("\ufeff---\nname: d\ndescription: x\n---\nbody\n")
		if strings.TrimSpace(fm) == "" {
			t.Error("BOM 让 frontmatter 整份失踪")
		}
	})
}
