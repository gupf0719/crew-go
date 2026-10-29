package agent_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// TestTaskEntry_EveryEntryDecidesBeforeOnAgentStart：每个编排的 Run / Resume 入口都要在调
// `OnAgentStart` **之前**占槽并判定任务（Run 调 enterGraphRunTask、Resume 调 enterGraphResumeTask）。钩子之后经
// compose_v2.CallHook 采纳、判定结果随执行状态取回——那一条由 TestHookCtx_AdoptedHooksOnlyThroughCallHook
// 对账（它覆盖全部采纳点，不只这一处）。
//
// 判定排在钩子之后的后果实测过：钩子里经 RunSubAgent 发起的调用用上一个任务的 approve_always 静默
// 执行工具、续上被放弃任务留下的存档；不占槽的话钩子里裸调的子层把宿主的挂起档读进来接着跑、成功时删掉。
// 这条规矩长在九个入口各自的代码里，第 10 个编排漏写不会报错，所以机械对账。Run 入口另有放弃任务（WithAbandonedTasks）
// 的两步：校验（checkAbandonedTasks）在建历史写入器与盖章之前，收尾（abandonTasksAtEntry）在钩子之前。
func TestTaskEntry_EveryEntryDecidesBeforeOnAgentStart(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	checked := map[string]int{}
	for _, pkg := range pkgs {
		for path, f := range pkg.Files {
			file := path[strings.LastIndex(path, "/")+1:]
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Recv == nil || fd.Body == nil || (fd.Name.Name != "Run" && fd.Name.Name != "Resume") {
					continue
				}
				want := "enterGraphRunTask"
				if fd.Name.Name == "Resume" {
					want = "enterGraphResumeTask"
				}
				var hookPos, entryPos, abandonPos, checkPos, historyPos token.Pos
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					ce, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fn := ce.Fun.(type) {
					case *ast.SelectorExpr:
						if fn.Sel.Name == "OnAgentStart" && hookPos == token.NoPos {
							hookPos = ce.Pos()
						}
					case *ast.Ident:
						if fn.Name == want && entryPos == token.NoPos {
							entryPos = ce.Pos()
						}
						if fn.Name == "abandonTasksAtEntry" && abandonPos == token.NoPos {
							abandonPos = ce.Pos()
						}
						if fn.Name == "checkAbandonedTasks" && checkPos == token.NoPos {
							checkPos = ce.Pos()
						}
						if strings.HasPrefix(fn.Name, "startHistory") && historyPos == token.NoPos {
							historyPos = ce.Pos()
						}
					}
					return true
				})
				if hookPos == token.NoPos {
					continue // 这个 Run/Resume 不调 agent 级钩子（不是编排入口）
				}
				where := file + ":" + fd.Name.Name
				if entryPos == token.NoPos || entryPos > hookPos {
					t.Errorf("%s 在 OnAgentStart（%v）之前没有调 %s", where, fset.Position(hookPos), want)
				}
				// Run 入口还要在钩子之前处理调用方声明放弃的任务（WithAbandonedTasks）——漏了的编排静默忽略这个选项
				if fd.Name.Name == "Run" && (abandonPos == token.NoPos || abandonPos > hookPos) {
					t.Errorf("%s 在 OnAgentStart（%v）之前没有调 abandonTasksAtEntry", where, fset.Position(hookPos))
				}
				// 放弃的声明在建历史写入器、盖任务章之前校验：排在后面的话嵌套位置认不出（章已经盖上）、拒绝前调用方前文已经写进历史
				if fd.Name.Name == "Run" && (checkPos == token.NoPos || historyPos == token.NoPos || checkPos > historyPos || checkPos > entryPos) {
					t.Errorf("%s 没有在建历史写入器与 %s 之前调 checkAbandonedTasks", where, want)
				}
				checked[fd.Name.Name]++
			}
		}
	}
	if checked["Run"] < 9 || checked["Resume"] < 9 {
		t.Fatalf("应扫到九个编排的 Run 与 Resume 入口，got %v——这条对账可能在空转", checked)
	}
}
