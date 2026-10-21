package compose_v2_test

// 机械对账：`DeleteSubtree` 的调用点集合**与条数**。
//
// 钉的形态：这个集合此前由散文维护（「四处生产调用点」+「加新调用点前回来重核这张表」）。
// 那句提醒**三版一次都没拦住**。第一版对账用文本 grep + 硬编码目录白名单，独立审计实测
// **12 种写法只抓到 5 种**，而且当时就漏着一个真调用点（`example/` 里那个）。
//
// 这一版按 go/ast 做，逐 `FuncDecl` 遍历（不用滚动的「当前函数名」游标），找**任何**
// `X.DeleteSubtree` 形态的引用——调用、method value、跨行点号、defer、embedding 提升、
// 字段链都是同一个 `*ast.SelectorExpr`，一并落网。
//
// # 为什么 key 带 receiver、value 带条数
//
// 独立审计端到端试出 7 种假绿路子，其中三种说的是同一件事：**这张表登记的判断
// （「这次调用会不会留残留」）是 `nsPrefix` 实参的性质，而 key 的粒度是「函数」，
// 低于语义所要求的那一格**。
//
//   - 同一个已登记函数里加第二个调用点（哪怕语义相反、传的是非空 ns）→ 旧版白蹭第一条
//     的答案。**而这正是文件头上一版声称已经修掉的那种。**（→ value 记条数）
//   - 同文件里另一个类型上的同名方法 → 旧版取裸方法名，两个塌成一个 key。（→ key 带 receiver）
//   - 包级 `var` 里的引用 → 旧版的滚动游标把它记到**源码顺序上前一个函数**名下。
//     （→ 逐 FuncDecl 遍历，包级的给显式 key）
//
// # 覆盖边界（如实写，不假装）
//
// 钉的是「**主 module 非测试代码**的调用点集合与条数」，不是「DeleteSubtree 调用点」。
// 拦不住的：反射（`MethodByName`，任何静态手段都拦不住）、`_test.go`（按设计）、
// 独立 module（`sql/` `redis/` `a2a/zk/` 各有自己的 go.mod；`redis/checkpointer_cleanup.go`
// 今天就有两个未入账的生产调用点，属声明过的范围）。

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// callSiteNote 是一个调用点的登记项：条数 + 它对「残留会不会被更外层清理者扫掉」的回答。
type callSiteNote struct {
	n    int
	note string
}

// knownDeleteSubtreeCallSites 登记到**包 + receiver + 函数**粒度，值带条数。
var knownDeleteSubtreeCallSites = map[string]callSiteNote{
	"compose_v2/durability.go:deleteCpSubtree":               {1, "Run 出口——嵌套 Run 有祖先；根 Run 空前缀多数 backend 原子"},
	"compose_v2/subgraph.go:(*GraphBuilder).AddSubgraphNode": {1, "子图成功清理——父 Run 出口是更外层的清理者"},
	"agent_v2/resume_helpers.go:commonResumePrep":            {1, "Abort——顶层空前缀；嵌套 Abort 无更外层清理者，残留留着"},
	"agui/handler.go:(*handler).dropCP":                      {1, "会话丢弃——函数体里硬编码空前缀"},
	"example/store/redis_smoke/main.go:demoCheckpointer":     {1, "example 演示，空前缀"},
}

func TestDeleteSubtree_CallSitesAccountedFor(t *testing.T) {
	// 根锚在**本文件的编译期路径**上，不用 `".."`：后者是相对 cwd 的，
	// 预编译二进制搬到别处再跑时 `..` 会变成别的地方（审计实测搬到 /tmp 后
	// 去遍历整个文件系统）。
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("拿不到本文件路径，无法锚定仓库根")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))

	found := map[string]int{}
	scannedPkgs := map[string]bool{}
	scannedFiles := 0

	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err // 不吞错——目录改名后静默零覆盖是上一版的毛病之一
		}
		if d.IsDir() {
			if path == repoRoot {
				return nil
			}
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			// 独立 module 按**真边界**判（目录里有 go.mod），不按目录名——
			// 按名字列表时新建一个叫 `redis` 的普通子包就会被静默跳过（审计实测），
			// 而以后加 backend 也不用回来改这张表。
			if _, serr := os.Stat(filepath.Join(path, "go.mod")); serr == nil {
				return filepath.SkipDir
			}
			// conformance 包是测试基建（整包给别人当测试用，文件不带 _test 后缀）。
			// 一条规则两个方向都对：`store/conformance`、`checkpointer_conformance`
			// 都跳；`conformance_helpers` 这种生产包不跳（旧版的 Contains 会吃掉它）。
			if name == "conformance" || strings.HasSuffix(name, "_conformance") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// 不静默跳过：本仓 parse 失败恒为 0，没有正当用例，而「藏在解析失败的
			// 文件里」是审计实测跑通的一条假绿路子。
			t.Errorf("解析失败 %s: %v —— 别让它静默跳过，扫描面出现窟窿", path, perr)
			return nil
		}
		scannedFiles++
		rel, _ := filepath.Rel(repoRoot, path)
		rel = filepath.ToSlash(rel)
		if i := strings.Index(rel, "/"); i > 0 {
			scannedPkgs[rel[:i]] = true
		}

		countIn := func(n ast.Node, key string) {
			ast.Inspect(n, func(nd ast.Node) bool {
				// 只认 SelectorExpr：`X.DeleteSubtree(...)` 的 CallExpr 会继续下降到
				// 它的 Fun（就是这个 SelectorExpr），两个分支都写就是双计。
				if sel, isSel := nd.(*ast.SelectorExpr); isSel && sel.Sel.Name == "DeleteSubtree" {
					found[key]++
				}
				return true
			})
		}

		// 逐 Decl 遍历，不用滚动游标——包级 var 里的引用不会串到前一个函数名下。
		for _, decl := range f.Decls {
			switch dd := decl.(type) {
			case *ast.FuncDecl:
				countIn(dd, rel+":"+renderFuncKey(dd))
			default:
				countIn(dd, rel+":<pkg-level>")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("扫描失败（别让它静默零覆盖）: %v", err)
	}

	// 三道扫描面自检——「扫到 0」「扫少了」「根指错了」是三种失效，都得防。
	//
	// 「扫到 0」真实发生过：`WalkDir` 的根曾是 `..`，`d.Name()` 返 ".." 撞上隐藏目录
	// 规则把整棵树 SkipDir。「扫少了」同样静默：本测试若被挪到更深的包，能看到的
	// 文件从 250+ 掉到几十，而只查「零命中」的守卫根本不响。
	if scannedFiles < 200 {
		t.Fatalf("只扫到 %d 个文件——扫描面坏了，不是真的这么少（主 module 应有 250+）", scannedFiles)
	}
	for _, anchor := range []string{"compose_v2", "agent_v2", "agui", "example"} {
		if !scannedPkgs[anchor] {
			t.Fatalf("扫描面没覆盖到 %s/ —— repoRoot（%q）指错了地方。", anchor, repoRoot)
		}
	}

	for site, n := range found {
		known, ok := knownDeleteSubtreeCallSites[site]
		if !ok {
			t.Errorf("新的 DeleteSubtree 调用点：%s\n"+
				"  → 回去看 compose_v2.Checkpointer 类型 godoc 的「残留」一节：判据问的是"+
				"「这次调用会不会留残留」+「之后还有没有更外层的清理者」。对这个新调用点"+
				"回答一遍，然后连答案加进 knownDeleteSubtreeCallSites。", site)
			continue
		}
		if known.n != n {
			t.Errorf("%s 的调用条数从 %d 变成 %d\n"+
				"  → 这张表登记的判断是 **nsPrefix 实参的性质**，同一个函数里的两次调用"+
				"可以一个传空前缀、一个传非空 ns，答案相反。逐条回答「会不会留残留」，"+
				"确认现有那句仍然覆盖得住，再改条数。现有登记：%s",
				site, known.n, n, known.note)
		}
	}
	for site := range knownDeleteSubtreeCallSites {
		if _, ok := found[site]; !ok {
			t.Errorf("已登记的调用点 %s 不见了——删了就摘掉，别让这张表变成考古记录", site)
		}
	}
}

// renderFuncKey 把 FuncDecl 渲染成带 receiver 的 key：`(*handler).dropCP` / `plainFn`。
// 不带 receiver 时同名方法会塌成一个 key（审计实测：同文件里另一个类型上的 dropCP
// 直接白蹭已登记那条的答案）。
func renderFuncKey(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	var recv string
	switch rt := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := rt.X.(*ast.Ident); ok {
			recv = "(*" + id.Name + ")"
		}
	case *ast.Ident:
		recv = "(" + rt.Name + ")"
	}
	if recv == "" {
		recv = fmt.Sprintf("(%T)", fd.Recv.List[0].Type)
	}
	return recv + "." + fd.Name.Name
}
