package agent_v2_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// 编排调用别的 agent 的每一处，都必须走 runMember 或 runDecisionAgent。
//
// # 这条为什么必须是机械的
//
// 收敛之前，每个编排在自己的节点里手写同一段派发代码，全仓 12 处（9 个成员 + 3 个
// 内部决策 LLM）。那段代码承载 8 条协议：子 ns 派生 / 历史归属 / sub-agent store
// scope / 重试 / 转交能力转授 / 中断时落输入 / 产出提取 / 边界补记。**8 × 12 = 96
// 个必须逐个写对的地方，而漏掉任何一条都不报错**，只是各自静默退化：
//
//   - 漏子 ns   ⟹ 子 cp 与父共用命名空间互相覆盖，进度静默蒸发
//   - 漏归属    ⟹ settleHistoryAt 见 slot=="" 直接早退，这一步在历史里什么都不留
//   - 漏 scope  ⟹ 同一份产出被子和父各持久化一次
//   - 漏转授    ⟹ 成员静默失去转交能力（模型硬发 transfer 拿到软失败回灌，改成自己作答）
//   - 漏补记    ⟹ 非 BasicAgent 的执行体（远程 / 自研 / 别的框架的壳）一条不留
//
// 这不是假想：`5afb8eb` 给 6 个编排加 store scope 时漏了 ParallelAgent 和 Routing
// 的 classifier，靠事后审计才发现；此后那些 push 点一直由注释里一句人肉数出来的
// 「协议覆盖 15/15」维持。**人肉计数守不住一个还会长的清单。**
//
// # 加第 10 个编排时会发生什么
//
// 它一定要调它的成员。只要它写 `sub.Run(...)`，这条就红，报错会告诉它选哪个 helper。
// 想加进白名单也可以——但那需要在这里写一行，而那一行是评审的锚点。
func TestMemberDispatch_AllAgentRunsGoThroughHelper(t *testing.T) {
	// 白名单的 key 是**(文件, 所在函数)**，value 是**允许出现的次数**。
	//
	// 首版拿「文件 + 接收者字面量」当 key，独立审计当场打穿三次：在收敛文件自己里
	// 加第三个 helper（参数也叫 `ag`）免检、在 handoff_agent 同一个函数里多写一处
	// `ag.Run` 免检、把真正被守护的那行改走别处再留一个 `if false` 的桩免检——
	// 而 `ag` 这个名字在 15 个生产文件里都有。**「加第 10 个编排一定会红」当时是假的。**
	//
	// 按次数登记之后，多一处就是数字对不上，跟它叫什么名字无关。
	allowed := map[string]map[string]struct {
		count  int
		reason string
	}{
		"member_dispatch.go": {
			"runMember":        {1, "编排成员派发的唯一实现"},
			"runDecisionAgent": {1, "内部决策 LLM 派发的唯一实现"},
		},
		"run_subagent.go": {
			"runNestedCall": {1, "嵌套调用执行器（RunSubAgent / AgentAsTool / skill fork 共用）——它建 " +
				"NewSharedChildSession 做 events 隔离，跟 6 个走 runMember 的编排「共享父 session 让协作语义生效」" +
				"是**故意**的语义差；skill fork 在进它之前摘掉全部授予（fail-closed）"},
		},
		"handoff_agent.go": {
			"buildComposeGraph": {1, "转交能力的**铸造**点（建 grant 而非转授），每次尝试收一次候选" +
				"做出的转交决定 + 按历史视图模式分档的条件补记——三样都是这一处独有的"},
		},
	}

	found := map[string]map[string]int{} // file -> func -> count
	where := map[string]string{}         // "file|func" -> 首个出现位置的描述
	scanned := 0

	fset := token.NewFileSet()
	// **递归扫子目录**：`parser.ParseDir` 不递归，首版因此漏掉「把派发搬进 agent_v2/internal/xxx」
	// 这条（独立审计实测能编译且哨兵绿）。
	dirs := []string{"."}
	_ = filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
		if err != nil || !info.IsDir() || path == "." {
			return nil
		}
		if strings.HasPrefix(info.Name(), ".") || info.Name() == "testdata" {
			return filepath.SkipDir
		}
		dirs = append(dirs, path)
		return nil
	})

	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		if err != nil {
			continue // 子目录可能是别的 module / 解析不了，不阻断
		}
		for _, pkg := range pkgs {
			for path, f := range pkg.Files {
				scanned++
				file := path[strings.LastIndex(path, "/")+1:]
				if dir != "." {
					file = dir + "/" + file
				}
				// 本文件 import 的包名——接收者是包名时那不是 agent 派发
				// （`compose_v2.Run`）。**用 import 表而不是硬编码名字**：首版硬编码
				// 排除了 "t"，于是把一个参数命名为 t 就免检（独立审计实测）。
				pkgNames := map[string]bool{}
				isTestHelper := false
				for _, imp := range f.Imports {
					name := strings.Trim(imp.Path.Value, `"`)
					if name == "testing" {
						// 非 _test.go 却 import testing 的只有 conformance 这类**测试脚手架**
						// 包，它们的 `.Run` 是 `t.Run` 子测试不是 agent 派发。按 import 判
						// 而不是按目录名/接收者名——后两者都能被无意撞上。
						isTestHelper = true
					}
					name = name[strings.LastIndex(name, "/")+1:]
					if imp.Name != nil {
						name = imp.Name.Name
					}
					pkgNames[name] = true
				}
				if isTestHelper {
					continue
				}
				for _, decl := range f.Decls {
					fd, ok := decl.(*ast.FuncDecl)
					if !ok {
						continue
					}
					ast.Inspect(fd, func(n ast.Node) bool {
						sel, ok := n.(*ast.SelectorExpr)
						// **扫 SelectorExpr 而不是 CallExpr.Fun**：方法值
						// `run := a.Run` 不是任何 CallExpr 的 Fun，首版看不见（实测能绕）。
						// **Resume 一并扫**：今天没有编排走它，但它是同一个派发面。
						if !ok || (sel.Sel.Name != "Run" && sel.Sel.Name != "Resume") {
							return true
						}
						if id, isIdent := sel.X.(*ast.Ident); isIdent && pkgNames[id.Name] {
							return true
						}
						if found[file] == nil {
							found[file] = map[string]int{}
						}
						found[file][fd.Name.Name]++
						k := file + "|" + fd.Name.Name
						if _, seen := where[k]; !seen {
							where[k] = fmt.Sprintf("%s:%d（%s.%s）",
								file, fset.Position(sel.Pos()).Line, exprText(sel.X), sel.Sel.Name)
						}
						return true
					})
				}
			}
		}
	}
	if scanned == 0 {
		t.Fatal("一个文件都没扫到——解析路径不对，这条哨兵是空转的")
	}

	guide := "\n  编排派发成员请走 runMember（拿转授、进历史、带重试）；内部决策 LLM" +
		"（分类器 / 评估器 / 反馈教练）走 runDecisionAgent（不进历史、永不拿转交能力）。" +
		"\n  这一处形状真的不同的话，在本用例的 allowed 里登记 (文件, 函数, 次数) 并写明理由。"

	var files []string
	for f := range found {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, file := range files {
		var fns []string
		for fn := range found[file] {
			fns = append(fns, fn)
		}
		sort.Strings(fns)
		for _, fn := range fns {
			got := found[file][fn]
			want, ok := allowed[file][fn]
			if !ok {
				t.Errorf("%s 里的 %s 直接调了 Agent.Run/Resume（%d 处，首个在 %s），未登记。%s",
					file, fn, got, where[file+"|"+fn], guide)
				continue
			}
			if got != want.count {
				t.Errorf("%s 的 %s 登记了 %d 处 Agent.Run/Resume，实际 %d 处（首个在 %s）。"+
					"\n  登记理由：%s%s", file, fn, want.count, got, where[file+"|"+fn], want.reason, guide)
			}
		}
	}
	// 反向对账：登记了而代码里没有的——留一条永远不触发的豁免，下一个人会以为它还守着什么。
	for file, fns := range allowed {
		for fn := range fns {
			if found[file][fn] == 0 {
				t.Errorf("白名单登记了 %s 的 %s，代码里已经没有 Agent.Run/Resume 了——删掉这一行", file, fn)
			}
		}
	}
}

// 编排不许走 RunSubAgent —— 它是给**业务**的公开 helper，语义跟编排成员派发不同。
//
// 独立审计实测：第 10 个编排写 `RunSubAgent(ctx, step.Name, step.Agent, input)` 能编译、
// 上面那条哨兵**绿**（语法上没有 `.Run` 选择器），而它跟 runMember 差三样——不转授
// （成员静默失去转交能力）、不重试、把「共享父 session」换成 NewSharedChildSession
// （兄弟成员互相看不到 events）。正好是白名单注释里自己列的那几条静默退化。
//
// CLAUDE.md 正文还写着「推荐走 RunSubAgent helper」——那句是对**业务的 GraphAgent
// NodeFunc** 说的，编排内部照抄就踩坑。所以这条得机械挡住。
func TestMemberDispatch_OrchestrationsDoNotUseRunSubAgent(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return strings.HasSuffix(fi.Name(), "_agent.go") && !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	scanned := 0
	for _, pkg := range pkgs {
		for path, f := range pkg.Files {
			scanned++
			file := path[strings.LastIndex(path, "/")+1:]
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok || (id.Name != "RunSubAgent" && id.Name != "runNestedCall") {
					return true
				}
				t.Errorf("%s:%d 编排内部调了 %s——那是给业务 GraphAgent NodeFunc 的 helper，"+
					"它建 child session 做 events 隔离、不转授、不重试。编排成员请走 runMember。",
					file, fset.Position(call.Pos()).Line, id.Name)
				return true
			})
		}
	}
	if scanned == 0 {
		t.Fatal("一个 *_agent.go 都没扫到——这条哨兵是空转的")
	}
}

// 每个编排都必须自查「这份转交 / 路由授予是不是发给我的」。
//
// 不自查的下场不是「授权变宽一点」，是**身份校验被一层间接完全绕开**：业务壳把收到的
// `opts...` 原样转发给任意一个框架编排，那个编排照单全收、再替它把能力转授给自己的
// 成员——而壳自己从来没有被授予过。`grantFor` 那一步（比对受让方是不是自己）是唯一
// 拦得住这条的地方。
//
// 所以 runConfig 的 handoffGrant / routeGrant 两个字段**只允许**以 `xxx.handoffGrant.grantFor(自己)`
// 的形状被读（现在全收在 runConfig.grantsFor 里）。裸读一次就红。豁免的只有往里写的 setter 与把它
// 原样读出来交给调用者当场自查的那两个函数。
func TestHandoffGrant_EveryReadIsSelfChecked(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	fields := map[string]bool{"handoffGrant": true, "routeGrant": true}
	// setter 往里写；xxxFromOpts 读出来交给唯一的调用者（RegrantHandoffTo / RegrantRouteTo /
	// HandoffTo）当场 grantFor。
	exempt := map[string]bool{"withHandoffGrant": true, "grantFromOpts": true,
		"withRouteGrant": true, "routeGrantFromOpts": true}

	checked := map[string]int{}
	for _, pkg := range pkgs {
		for path, f := range pkg.Files {
			file := path[strings.LastIndex(path, "/")+1:]
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || exempt[fd.Name.Name] {
					continue
				}
				// 先收集所有「被 .grantFor 消费的表达式」的位置。
				okPos := map[token.Pos]bool{}
				ast.Inspect(fd, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if ok && sel.Sel.Name == "grantFor" {
						okPos[sel.X.Pos()] = true
					}
					return true
				})
				ast.Inspect(fd, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if !ok || !fields[sel.Sel.Name] {
						return true
					}
					checked[sel.Sel.Name]++
					if !okPos[sel.Pos()] {
						t.Errorf("%s:%d 裸读了 %s 字段——必须写成 %s.grantFor(自己)。\n"+
							"  不自查的话，业务壳把 opts 原样转发给本编排就能让本编排替它转授，"+
							"身份校验被一层间接绕开。",
							file, fset.Position(sel.Pos()).Line, sel.Sel.Name, exprText(sel))
					}
					return true
				})
			}
		}
	}
	for name := range fields {
		if checked[name] == 0 {
			t.Fatalf("一处 %s 读取都没扫到——这条哨兵是空转的", name)
		}
	}
}

func exprText(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprText(v.X) + "." + v.Sel.Name
	case *ast.CallExpr:
		return exprText(v.Fun) + "()"
	case *ast.IndexExpr:
		return exprText(v.X) + "[...]"
	default:
		return fmt.Sprintf("%T", e)
	}
}
