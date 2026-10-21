package compose_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// 「名字以 Locked 结尾的方法，调用方必须已持写锁」这条约定，由本用例每次 go test
// 核验一遍——而不是靠调用点上方那句注释。
//
// # 为什么值得一条 AST 哨兵
//
// 这类约定编译器帮不上：漏持锁是数据竞争（只在真并发下、且只有 -race 看得见），
// 重复持锁是永久死锁（Go 的 RWMutex 不可重入）。两种 go build / go vet 都不报，
// 而且**调用点数量在长**——一次改动就从 1 个 helper 变成 3 个。靠人读注释守着的
// 东西，本仓已经栽过：一条「这两句之间不许有代码」的注释守着的窗口，独立审计往
// 中间插一句就复现了双计。
//
// # 判据
//
// 保守但正好贴合本仓的约定：**拿锁的那个函数直接调 Locked helper**。所以只要
// 「调用它的那个函数体里出现过 mu.Lock()」即可。跨两层传递的持锁不在约定内，
// 真出现了本用例会报——那时该改的是代码不是判据。
func TestLockedHelpers_CallersHoldWriteLock(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	type site struct{ fn, callee, pos string }
	var sites []site
	var lockedDefs []string

	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				if strings.HasSuffix(fd.Name.Name, "Locked") {
					lockedDefs = append(lockedDefs, fd.Name.Name)
				}
				// 函数体里有没有拿写锁
				holds := false
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if se, ok := n.(*ast.SelectorExpr); ok && se.Sel.Name == "Lock" {
						if inner, ok := se.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mu" {
							holds = true
						}
					}
					return true
				})
				// 调了哪些 Locked helper
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					ce, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					se, ok := ce.Fun.(*ast.SelectorExpr)
					if !ok || !strings.HasSuffix(se.Sel.Name, "Locked") {
						return true
					}
					if !holds {
						sites = append(sites, site{fd.Name.Name, se.Sel.Name, fset.Position(ce.Pos()).String()})
					}
					return true
				})
			}
		}
	}

	// 正向：确实扫到了 Locked helper 定义与调用，否则本用例在「什么都没扫到」时恒真
	if len(lockedDefs) == 0 {
		t.Fatal("fixture 没搭对：一个 *Locked 方法都没扫到，下面的断言恒真")
	}
	t.Logf("扫到 %d 个 Locked helper: %v", len(lockedDefs), lockedDefs)

	for _, s := range sites {
		t.Errorf("%s 调了 %s 却没在自己函数体里拿写锁（%s）——"+
			"漏持锁是数据竞争、重复持锁是永久死锁，两种编译器都不报",
			s.fn, s.callee, s.pos)
	}
}
