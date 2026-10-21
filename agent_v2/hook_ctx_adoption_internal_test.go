package agent_v2

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// hookCtxMethods 是返回 ctx 的全部钩子方法（hook.Hooks 的 8 个 + OrchestrationHooks 的 5 个）。方法名在这里面的
// 方法实现（链式转发给下一个钩子）不是采纳点。
var hookCtxMethods = map[string]bool{
	"OnAgentStart": true, "OnAgentEnd": true, "OnModelStart": true, "OnModelEnd": true, "OnModelEndStream": true,
	"OnToolStart": true, "OnToolEnd": true, "OnError": true,
	"OnGraphStart": true, "OnGraphEnd": true, "OnNodeStart": true, "OnNodeEnd": true, "OnNodeEvent": true,
}

// adoptedHookMethods 是框架接住返回值接着用的那几个（其余几个的返回值被丢掉）：调它们只能经 compose_v2.CallHook。
// 曾经这里漏了 OnModelEndStream——那一处直接换成钩子的 ctx，哨兵照绿（审计实测：流式片段发到了另一次 Run 的连接上）。
var adoptedHookMethods = map[string]bool{
	"OnAgentStart": true, "OnModelStart": true, "OnModelEnd": true, "OnModelEndStream": true,
	"OnToolStart": true, "OnToolEnd": true, "OnGraphStart": true, "OnNodeStart": true,
}

// callHookEntries 是放标记、调、采纳收在一处的入口：被采纳的钩子只能在交给它们的闭包里调。
var callHookEntries = map[string]bool{"CallHook": true, "callHookOnto": true}

// isCallHookEntry 认真正的入口：compose_v2 包外只认 `compose_v2.CallHook`，包内才认裸名字（同名的本地函数
// 不算——那样会把它的闭包当成入口，整个跳过采纳）。
func isCallHookEntry(fun ast.Expr, pkg string) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return pkg == "compose_v2" && callHookEntries[f.Name]
	case *ast.SelectorExpr:
		x, ok := f.X.(*ast.Ident)
		return ok && x.Name == "compose_v2" && callHookEntries[f.Sel.Name]
	}
	return false
}

// callHookClosureAt 返回语法树栈上最内层那个直接交给 CallHook / callHookOnto 的闭包的下标，没有就是 -1。
func callHookClosureAt(stack []ast.Node, pkg string) int {
	for i := len(stack) - 1; i > 0; i-- {
		lit, ok := stack[i].(*ast.FuncLit)
		if !ok {
			continue
		}
		if call, ok := stack[i-1].(*ast.CallExpr); ok && isCallHookEntry(call.Fun, pkg) && isArgOf(call, lit) {
			return i
		}
	}
	return -1
}

func isArgOf(call *ast.CallExpr, e ast.Expr) bool {
	for _, a := range call.Args {
		if a == e {
			return true
		}
	}
	return false
}

// paramNames 按顺序返回函数字面量全部参数的名字（没名字的记成 `_`）。
func paramNames(lit *ast.FuncLit) []string {
	var out []string
	if lit.Type.Params == nil {
		return nil
	}
	for _, f := range lit.Type.Params.List {
		if len(f.Names) == 0 {
			out = append(out, "_")
			continue
		}
		for _, n := range f.Names {
			out = append(out, n.Name)
		}
	}
	return out
}

// derivedExpr 判表达式是不是从 ok 里那几个 ctx 派生的：就是其中一个，或者是第一个实参从它们派生的调用
// （`toolHookCtx(c, ...)`、`hookPhase(c, ...)`、`context.WithValue(c, ...)` 这类）。
func derivedExpr(e ast.Expr, ok map[string]bool) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return ok[x.Name]
	case *ast.ParenExpr:
		return derivedExpr(x.X, ok)
	case *ast.CallExpr:
		return len(x.Args) > 0 && derivedExpr(x.Args[0], ok)
	}
	return false
}

// hookCtxNames 算出从 CallHook 闭包（path[0]）到钩子调用之间，哪些名字是从闭包参数派生的 ctx：
//   - 闭包的第一个参数；
//   - 路上嵌套的函数字面量的第一个参数——当它被交给一个第一个实参是派生 ctx 的调用时（`CaptureSignals(toolHookCtx(c,
//     ...), func(cc) ...)`）；别的函数字面量的参数遮住外层的同名名字；
//   - 闭包里用派生的值定义或赋值的局部变量（`hc := context.WithValue(c, ...)`、`var hc = c`、
//     `c, cancel := context.WithTimeout(c, d)` 的第一个）。
//
// 一个名字只要在闭包里被赋成过别的东西（含 range 的迭代变量），就一律不算（不分先后、不分作用域，宁严勿松）。
// 按语法判仍然查不出的：返回值换成了别的 ctx 的辅助函数（`pick(c)` 实际返回存下的）、方法表达式
// （`H.OnToolStart(h, c, ...)` 第一个实参是接收者，会误报）。
func hookCtxNames(path []ast.Node) map[string]bool {
	ok := map[string]bool{}
	entry := path[0].(*ast.FuncLit)
	if ps := paramNames(entry); len(ps) > 0 && ps[0] != "_" {
		ok[ps[0]] = true
	}
	for j := 1; j < len(path); j++ {
		lit, isLit := path[j].(*ast.FuncLit)
		if !isLit {
			continue
		}
		call, isCall := path[j-1].(*ast.CallExpr)
		qualifies := isCall && isArgOf(call, lit) && len(call.Args) > 0 && derivedExpr(call.Args[0], ok)
		for i, n := range paramNames(lit) {
			if i == 0 && qualifies && n != "_" {
				ok[n] = true
			} else {
				delete(ok, n)
			}
		}
	}
	tainted := map[string]bool{}
	mark := func(name string, derived bool) bool {
		switch {
		case name == "_" || tainted[name]:
			return false
		case !derived:
			tainted[name] = true
			delete(ok, name)
			return true
		case !ok[name]:
			ok[name] = true
			return true
		}
		return false
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(entry.Body, func(n ast.Node) bool {
			switch s := n.(type) {
			case *ast.AssignStmt:
				for i, lhs := range s.Lhs {
					id, isID := lhs.(*ast.Ident)
					if !isID {
						continue
					}
					derived := false
					switch {
					case len(s.Rhs) == len(s.Lhs):
						derived = derivedExpr(s.Rhs[i], ok)
					case len(s.Rhs) == 1 && i == 0: // `c, cancel := context.WithTimeout(c, d)`：ctx 在第一个
						derived = derivedExpr(s.Rhs[0], ok)
					}
					changed = mark(id.Name, derived) || changed
				}
			case *ast.ValueSpec:
				for i, id := range s.Names {
					derived := i < len(s.Values) && derivedExpr(s.Values[i], ok)
					changed = mark(id.Name, derived) || changed
				}
			case *ast.RangeStmt:
				for _, e := range []ast.Expr{s.Key, s.Value} {
					if id, isID := e.(*ast.Ident); isID {
						changed = mark(id.Name, false) || changed
					}
				}
			}
			return true
		})
	}
	return ok
}

// repoGoDirs 列出仓库里要扫的目录：跳过业务示例（example/，不是框架的采纳点）、测试数据与隐藏目录。
func repoGoDirs(t *testing.T, root string) []string {
	t.Helper()
	var dirs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if p != root && (strings.HasPrefix(name, ".") || name == "example" || name == "testdata" || name == "vendor") {
			return filepath.SkipDir
		}
		dirs = append(dirs, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

// TestHookCtx_AdoptedHooksOnlyThroughCallHook：框架代码里被采纳的钩子（返回的 ctx 框架接着用的那几个）只能在
// compose_v2.CallHook（引擎包内的 callHookOnto）的闭包里调，交给钩子的 ctx 要从闭包的参数派生——放标记、调、采纳
// 三步由那一处保证。也不许取它们的方法值（换个名字调就绕过了这里的检查）。
//
// 曾经各采纳点各写一遍三步，靠一个按源码位置数「每处钩子调用之前有一次放标记」的哨兵对账：标记放在循环外、
// 只放在一个分支上、放了标记交给钩子的却是另一个变量这几种写法都查不出（审计实测）。收成一个入口之后，第一版
// 这里只查「调用在不在闭包里」：闭包里交给钩子的是外面那个没放标记的 ctx、取方法值再调、写在包级变量的函数
// 字面量里、只扫了三个目录，都照样绿（审计实测）。
//
// 钩子方法自己的实现（链式转发给下一个钩子）不算采纳点，跳过。
func TestHookCtx_AdoptedHooksOnlyThroughCallHook(t *testing.T) {
	calls := 0
	for _, dir := range repoGoDirs(t, "..") {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, pkg := range pkgs {
			for path, f := range pkg.Files {
				where := func(n ast.Node) string {
					return fmt.Sprintf("%s:%d", filepath.Join(dir, filepath.Base(path)), fset.Position(n.Pos()).Line)
				}
				var stack []ast.Node
				ast.Inspect(f, func(n ast.Node) bool {
					if n == nil {
						stack = stack[:len(stack)-1]
						return true
					}
					if fd, ok := n.(*ast.FuncDecl); ok && fd.Recv != nil && hookCtxMethods[fd.Name.Name] {
						return false // 钩子方法自己的实现；返回 false 时不会再收到这个节点的 nil
					}
					stack = append(stack, n)
					sel, ok := n.(*ast.SelectorExpr)
					if !ok || !adoptedHookMethods[sel.Sel.Name] {
						return true
					}
					call, isCall := stack[len(stack)-2].(*ast.CallExpr)
					if !isCall || call.Fun != ast.Expr(sel) {
						t.Errorf("%s 取了 %s 的方法值：被采纳的钩子只能在 compose_v2.CallHook 的闭包里直接调", where(sel), sel.Sel.Name)
						return true
					}
					calls++
					at := callHookClosureAt(stack, pkg.Name)
					switch {
					case at < 0:
						t.Errorf("%s 直接调了 %s：被采纳的钩子要经 compose_v2.CallHook 调（放标记、调、采纳收在那一处）",
							where(call), sel.Sel.Name)
					case len(call.Args) == 0 || !derivedExpr(call.Args[0], hookCtxNames(stack[at:])):
						t.Errorf("%s 交给 %s 的 ctx 不是从 CallHook 闭包的参数派生的：没放这次调用的标记，钩子返回的 ctx 会被当成外来的",
							where(call), sel.Sel.Name)
					}
					return true
				})
			}
		}
	}
	if calls < 25 {
		t.Fatalf("只找到 %d 处被采纳的钩子调用，扫描范围不对（应有近三十处）", calls)
	}
}

// TestHookCtx_LowLevelAdoptionOnlyInPhase：放标记与采纳这两个底层函数在 compose_v2 包里只出现在 phase.go（统一
// 入口所在的文件）——包内别处要采纳钩子返回的 ctx 也经 CallHook / callHookOnto。包外看不见它们，编译器管。
func TestHookCtx_LowLevelAdoptionOnlyInPhase(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../compose_v2", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, pkg := range pkgs {
		for path, f := range pkg.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok || (id.Name != "enterHookCall" && id.Name != "adoptHookCtx") {
					return true
				}
				seen++
				if filepath.Base(path) != "phase.go" {
					t.Errorf("%s:%d 直接用了 %s：要经 CallHook / callHookOnto", filepath.Base(path), fset.Position(id.Pos()).Line, id.Name)
				}
				return true
			})
		}
	}
	if seen == 0 {
		t.Fatal("一处 enterHookCall / adoptHookCtx 都没找到，扫描范围不对")
	}
}
