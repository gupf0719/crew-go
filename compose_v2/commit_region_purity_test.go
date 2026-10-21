package compose_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// 提交区不许调用任何可能跑业务代码的东西——由本用例每次 go test 核验。
//
// # 这条守的是什么
//
// `runtime.Goexit` 只能从**被调函数**里冒出来（业务在 reducer 里写一句 logger.Fatal
// 就是）。所以「提交区里一个可能跑业务代码的调用都没有」这件事本身，就是它掐不进来
// 的**结构性**理由——比持锁强，因为锁挡的是并发读者、挡不住自己这条 goroutine 展开。
//
// 掐进去的后果实测过：channel 值已落地、代际没推进、产出这批值的节点还留在 frontier
// 里 ⟹ 续跑重跑它 ⟹ 写入再算一次（Sum 从 5 变 10，err 全程 nil）。
//
// # 为什么要一条哨兵而不是一句注释
//
// 「这儿别加调用」是最会漂的那种约定：往提交区加一句 slog / 一次 hook / 一个 metric
// 都很自然，而它们随时可能间接跑到业务代码。本仓已经栽过一次——一条「这两句之间不许
// 有代码」的注释守着的窗口，独立审计往中间插一句就复现了双计。
//
// # 判据
//
// 提交区 = 两个 commitSuperstep* 本体 + 它们调的五个纯 helper（commitSignalsLocked 再调
// putSignal / sameSlot 两个纯函数）+ applyAllLocked 的提交循环。白名单只放框架自有的、不可能跑业务代码的东西。**归并段不在管辖内**——
// 那里本来就要跑 reducer，被掐断也只是「什么都没发生」。
//
// ⚠️ **临界区扩了就得同步扩这张表**。提交区从「四样」扩到全员成功路径的六样
// （多了「待办清空」与「frontier 前移 + payload 折叠」）时，如果只改代码不改这张表，
// 新纳进来的那两段就成了无人看管的地带——而它们正是原先那四个窗口的所在地。
func TestCommitRegion_CallsNothingBusinessReachable(t *testing.T) {
	allowed := map[string]bool{
		"Lock": true, "Unlock": true, // sync.RWMutex
		"applyAllLocked": true, "updateFrontierLocked": true, "markAnswersConsumedLocked": true,
		"advanceVisitsLocked": true, "appendCompletedSiblingsLocked": true, "commitSignalsLocked": true,
		"putSignal": true, "sameSlot": true,
		"len": true, "append": true, "make": true,
	}
	guarded := map[string]bool{
		"commitSuperstepPartial": true, "commitSuperstepAdvance": true,
		"updateFrontierLocked": true, "markAnswersConsumedLocked": true,
		"advanceVisitsLocked": true, "appendCompletedSiblingsLocked": true, "commitSignalsLocked": true,
		"putSignal": true, "sameSlot": true,
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	seen := map[string]bool{}
	commitLoopFound := false

	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				if guarded[fd.Name.Name] {
					seen[fd.Name.Name] = true
					ast.Inspect(fd.Body, func(n ast.Node) bool {
						ce, ok := n.(*ast.CallExpr)
						if !ok {
							return true
						}
						if isTypeConversion(ce) {
							// 类型转换（`[]T(nil)` / `map[K]V(x)` 之类）语法上也是 CallExpr，
							// 但 Go 的转换**不跑任何用户代码**——没有转换算子可重载，
							// 所以它进不来 Goexit，不该算一道门。
							return true
						}
						name := calleeName(ce)
						if !allowed[name] {
							t.Errorf("%s 调了 %q——提交区不许有可能跑业务代码的调用（%s）。"+
								"Goexit 只能从被调函数进来，加一个调用就是给它开一道门",
								fd.Name.Name, name, fset.Position(ce.Pos()))
						}
						return true
					})
				}
				if fd.Name.Name == "applyAllLocked" {
					ast.Inspect(fd.Body, func(n ast.Node) bool {
						rs, ok := n.(*ast.RangeStmt)
						if !ok {
							return true
						}
						if id, ok := rs.X.(*ast.Ident); !ok || id.Name != "staged" {
							return true
						}
						commitLoopFound = true
						ast.Inspect(rs.Body, func(m ast.Node) bool {
							if ce, ok := m.(*ast.CallExpr); ok {
								t.Errorf("applyAllLocked 的提交循环里出现了调用 %q（%s）——"+
									"那个循环必须是纯赋值，它是「写入落地」这一步不可失败的全部理由",
									calleeName(ce), fset.Position(ce.Pos()))
							}
							return true
						})
						return true
					})
				}
			}
		}
	}

	// 正向：三个受管函数和提交循环都真的扫到了，否则本用例在「什么都没扫到」时恒真
	for name := range guarded {
		if !seen[name] {
			t.Fatalf("fixture 没搭对：受管函数 %s 没扫到（改名了？）——下面的断言恒真", name)
		}
	}
	if !commitLoopFound {
		t.Fatal("fixture 没搭对：applyAllLocked 里没找到 `range staged` 提交循环（重构了？）")
	}
}

func calleeName(ce *ast.CallExpr) string {
	switch fn := ce.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return "?"
}

// isTypeConversion 判 CallExpr 是不是**语法上明确**的类型转换。
//
// 只认那些 Fun 位置写着类型字面量的形态（`[]T(...)` / `map[K]V(...)` / `chan T(...)`
// / `func(...)(...)`  / `interface{...}(...)` / `struct{...}(...)`，含外面裹括号的）。
// `MyType(x)` 与 `myFunc(x)` 在纯 go/ast 下不可分——那两种都是 Ident，仍走名字白名单，
// 所以这个放宽**不会**给真调用开口子。
func isTypeConversion(ce *ast.CallExpr) bool {
	fun := ce.Fun
	for {
		p, ok := fun.(*ast.ParenExpr)
		if !ok {
			break
		}
		fun = p.X
	}
	switch fun.(type) {
	case *ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.FuncType,
		*ast.InterfaceType, *ast.StructType:
		return true
	}
	return false
}
