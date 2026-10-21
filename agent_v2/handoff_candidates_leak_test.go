package agent_v2_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// 候选名单不许有任何通往业务代码的出口。
//
// 曾经的形态：名单整个 Run 只造一张 map，既进每一份 grant 又经 ctx 暴露给业务
// （导出的 `GetHandoffCandidates` **原样返回它**）。业务工具里一行
// `GetHandoffCandidates(ctx)["ghost"] = true` 就把名单撑宽了，且因为是同一张 map，
// **对本 Run 全部候选的 grant 同时生效**，`-race` 还有 WARNING。
//
// 后果比「授权变宽」更糟（实测）：`transfer_to_agent("ghost")` 通过了候选校验，
// 而 HandoffAgent 解析不出 ghost ⟹ 整个 Run 以 **`err == nil` + 空 final** 收尾
// ——LLM 的答复被丢掉，且没有任何人被告知。
//
// 修法是**删掉整条 ctx 通道**而不是返拷贝：它的读取点（工具里 `granted == nil` 的
// 回落）生产不可达（HandoffAgent 恒给非 nil 名单、regrant 恒继承），于是那次 push
// 唯一的作用就是泄漏。
//
// 本用例守的是「出口不存在」而不是「出口返的是拷贝」——**后者挡不住业务改自己那份
// 拷贝之外的东西，也挡不住下一个人把拷贝改回引用**。加回任何一个导出的候选名单
// 访问器，这条就红。
func TestHandoffCandidates_NoExportedAccessor(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	scanned := 0
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Recv != nil || !fd.Name.IsExported() {
					continue
				}
				scanned++
				n := fd.Name.Name
				if strings.Contains(n, "HandoffCandidates") {
					t.Errorf("%s 是候选名单通往业务的出口（%s）——名单只该跟着授予走。"+
						"业务够得着它就能撑宽授权范围，而撑宽的下游是 err=nil + 空 final",
						n, fset.Position(fd.Pos()))
				}
			}
		}
	}
	if scanned == 0 {
		t.Fatal("fixture 没搭对：一个导出函数都没扫到，下面的断言恒真")
	}
	t.Logf("扫了 %d 个导出函数，无候选名单访问器", scanned)
}
