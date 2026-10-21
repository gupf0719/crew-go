package skill

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestSkillDescriptor_EveryFieldDeclaresDisposition 机械对账：[SkillDescriptor] 的**每个**
// 导出字段都必须在自己的 godoc 里声明处置（`[框架消费]` 或 `[业务自负]`）。
//
// ⚠️ 为什么锁**字段名单**而不是锁数量：这条曾经是一句「每个导出字段除写入点和清洗点外
// 至少还有一个引用」的提案，配的验收是「今天跑一遍红 3 个」。一年后复跑**仍然是 3 个，
// 但集合换了人**——修好一个就腾出一个名额给下一个，静默通过。只看总数的闸等于没有闸。
//
// ⚠️ 也是为什么标记必须**逐字段各带各的**、不能共享一条组注释：新字段紧贴在一条
// `[框架消费]` 后面加进来就会静默继承那个标记，闸照样废。
//
// 曾经有四个字段「解析了、清洗了、进了公开结构体，而框架一次都没读过」，在结构体里长得
// 跟真正生效的字段一模一样。业务判断「我写了这个到底有没有用」的唯一办法是 grep 消费点。
//
// ⚠️ **这套标记是我们独有的，不是通行做法**（2026-08-28 补查）：同类产品里没有字段级的
// 「解析了但不强制」标记，也没有对配置结构做 AST 级对账的测试——因为它们的正解是
// **把 schema 收到只剩真的兑现的字段**（那边的 skill frontmatter 只有 3 个，我们曾 15 个），
// 不解析的字段不可能变成「解析了但没兑现」，也就没什么可标的。
//
// 所以这道闸的定位是**防回归的护栏**，不是「表达处置」的推荐姿势：正解仍是收窄 schema，
// 闸只负责挡住下一个未兑现字段混进来。真要新增 [业务自负] 字段，先问一句「能不能干脆不解析它」。
func TestSkillDescriptor_EveryFieldDeclaresDisposition(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "skill.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("解析 skill.go: %v", err)
	}

	var st *ast.StructType
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "SkillDescriptor" {
			return true
		}
		st, _ = ts.Type.(*ast.StructType)
		return false
	})
	if st == nil {
		t.Fatal("找不到 SkillDescriptor —— 类型被改名了？这条闸得跟着改")
	}

	seen := 0
	for _, fld := range st.Fields.List {
		for _, nm := range fld.Names {
			if !nm.IsExported() {
				continue
			}
			seen++
			doc := ""
			if fld.Doc != nil {
				doc = fld.Doc.Text()
			}
			if !strings.Contains(doc, "[框架消费]") && !strings.Contains(doc, "[业务自负]") {
				t.Errorf("字段 %s 没有声明处置。\n"+
					"在它自己的 godoc 里写 [框架消费]（框架读它并据此行动）或 "+
					"[业务自负]（框架解析但不据此行动，并写明理由）。\n"+
					"⚠️ 别蹭上一个字段的注释——标记必须逐字段各带各的，否则新字段会静默继承。",
					nm.Name)
			}
		}
	}
	if seen == 0 {
		t.Fatal("一个导出字段都没数到——AST 遍历写错了，这条闸是哑的")
	}
	t.Logf("%d 个导出字段全部声明了处置", seen)
}
