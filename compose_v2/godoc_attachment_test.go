package compose_v2_test

// 机械哨兵：注释块与它要文档化的声明之间不得有空行。
//
// # 为什么需要它
//
// Go 的 doc 注释是「**紧邻**声明的连续注释块」——中间插一个空行，整块就与声明脱钩：
// 前一个符号的 godoc 被吞给后一个，或者干脆谁都没有。这个形状在本仓**四次复发**
// （`memoryItemFingerprint` / `toolErrorContent` / `sql.DeleteSubtree` /
// `Checkpointer.DeleteSubtree`），最后一次是在**修上一次的那个 commit 里造出来的**。
//
// 四次里 `go build` / `go vet` / 全量测试 / commentaudit **全部沉默**——只有 `go doc`
// 看得见，而没人会为每次改注释去跑 `go doc`。所以它不该靠人记，该有机械闸。
//
// # 扫描面
//
// 主 module 全部非测试 .go 的**导出符号**（func / type / var / const / 接口方法 /
// 结构体字段）。未导出的不查——它们的 godoc 不进公开文档面，漏了代价小得多，
// 而全查会把大量「注释块在函数体内、后面隔行写代码」的正常形态卷进来。

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode"
)

func TestGodoc_NoBlankLineBetweenCommentAndDecl(t *testing.T) {
	// 根锚在**本文件的编译期路径**上，不用相对 cwd 的 `".."`——同批的
	// `delete_subtree_callsite_test.go` 已经为此栽过两次并写下了教训：`..` 是相对 cwd 的，
	// 预编译的测试二进制搬到别处再跑时它会指向别的地方（实测搬到 /tmp 后去遍历整个
	// 文件系统）；而 `WalkDir(".." )` 的根 `d.Name()` 返 ".." 还会撞上隐藏目录规则、
	// 把整棵树 SkipDir 掉（实测扫到 0 个）。那条教训写下来了，这条哨兵晚几个 commit
	// 写的时候没继承——所以补上。
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("拿不到本文件路径，无法锚定仓库根")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	scannedPkgs := map[string]bool{}
	var offenders []string
	scanned := 0

	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if path == repoRoot {
				return nil
			}
			// ⚠️ 独立 module（sql / redis / a2a/zk）**照扫**——godoc 脱钩是纯语法的，
			// 跨 module 解析不需要它们在同一个 module 里，而**第三次复发就发生在
			// sql/**。跳过它们等于把哨兵挡在事发地之外。
			//
			// ⚠️ **别跟 `delete_subtree_callsite_test.go` 的跳过表"统一"**：那条按
			// `go.mod` 真边界跳过独立 module，因为它对账的是「**主 module** 非测试代码的
			// 调用点集合」——跳过是它的语义的一部分。两条回答的不是同一个问题，相反的
			// 判断各自成立。
			switch n := d.Name(); {
			case n == "vendor" || n == "node_modules" || n == "testdata":
				return filepath.SkipDir
			case strings.HasPrefix(n, "."):
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			t.Errorf("解析失败 %s: %v（别让它静默跳过——本仓 parse 失败恒为 0，"+
				"出现即说明扫描面坏了）", path, perr)
			return nil
		}
		scanned++
		rel, _ := filepath.Rel(repoRoot, path)
		rel = filepath.ToSlash(rel)
		if i := strings.Index(rel, "/"); i > 0 {
			scannedPkgs[rel[:i]] = true
		}

		// 检法：找「Doc 为 nil、但正上方浮着一个只隔空行的注释块」的导出声明。
		//
		// ⚠️ 这里**不能**检查「Doc 块尾行与声明首行的距离」——一旦中间有空行，
		// Go 的 parser 就不把它当 Doc 了，`Doc` 直接是 nil，那个检法恒不命中
		// （第一版就是这么写的，sanity-revert 当场证明它是假绿）。脱钩的signature
		// 是「该有 doc 的地方 Doc==nil，而注释块掉到了 f.Comments 里」。
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Errorf("读文件失败 %s: %v", path, rerr)
			return nil
		}
		srcLines := strings.Split(string(src), "\n")
		blankBetween := func(from, to int) bool { // (from, to) 开区间，1-based 行号
			for i := from + 1; i < to; i++ {
				if i-1 < 0 || i-1 >= len(srcLines) {
					return false
				}
				if strings.TrimSpace(srcLines[i-1]) != "" {
					return false
				}
			}
			return true
		}

		check := func(doc *ast.CommentGroup, declPos token.Pos, name string) {
			if doc != nil || !isExported(name) {
				return
			}
			declLine := fset.Position(declPos).Line
			// 找结束于声明之前、离得最近的那个注释块
			var best *ast.CommentGroup
			for _, cg := range f.Comments {
				if cg.End() < declPos && (best == nil || cg.End() > best.End()) {
					best = cg
				}
			}
			if best == nil {
				return
			}
			cgEnd := fset.Position(best.End()).Line
			// 必须真隔了空行（>1 行），且中间除空行外没有别的东西
			if declLine-cgEnd <= 1 || !blankBetween(cgEnd, declLine) {
				return
			}
			offenders = append(offenders,
				rel+":"+itoaLocal(declLine)+" "+name+
					"（注释块结束于第 "+itoaLocal(cgEnd)+" 行，中间隔了空行 ⟹ 已脱钩，Doc 为空）")
		}

		for _, decl := range f.Decls {
			switch dd := decl.(type) {
			case *ast.FuncDecl:
				check(dd.Doc, dd.Pos(), dd.Name.Name)
			case *ast.GenDecl:
				for _, spec := range dd.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						check(sp.Doc, sp.Pos(), sp.Name.Name)
						switch tt := sp.Type.(type) {
						case *ast.InterfaceType:
							if tt.Methods != nil {
								for _, m := range tt.Methods.List {
									if len(m.Names) > 0 {
										check(m.Doc, m.Pos(), m.Names[0].Name)
									}
								}
							}
						case *ast.StructType:
							if tt.Fields != nil {
								for _, fld := range tt.Fields.List {
									if len(fld.Names) > 0 {
										check(fld.Doc, fld.Pos(), fld.Names[0].Name)
									}
								}
							}
						}
					case *ast.ValueSpec:
						if len(sp.Names) > 0 {
							check(sp.Doc, sp.Pos(), sp.Names[0].Name)
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("扫描失败: %v", err)
	}
	if scanned < 200 {
		t.Fatalf("只扫到 %d 个文件——扫描面坏了，不是真的这么少（主 module 应有 250+）", scanned)
	}
	// 锚点：**「扫到 0」和「扫少了」是两种失效**，文件数下限只拦得住前者。这几个目录
	// 分布在仓库不同层级且**含独立 module**（第三次复发就在 sql/），任一缺失说明根锚
	// 指错了地方或跳过表把 module 挡在外面——那正是这条哨兵最该覆盖的地方。
	for _, anchor := range []string{"compose_v2", "agent_v2", "sql", "redis"} {
		if !scannedPkgs[anchor] {
			t.Fatalf("扫描面没覆盖到 %s/ —— 根锚（%q）指错了地方，或跳过表把它挡住了。"+
				"本哨兵**有意**扫独立 module：godoc 脱钩是纯语法的，而第三次复发就发生在 sql/。",
				anchor, repoRoot)
		}
	}
	for _, o := range offenders {
		t.Errorf("godoc 与声明之间有空行，整块会脱钩：%s\n"+
			"  → 删掉那个空行。Go 的 doc 注释必须**紧邻**声明；脱钩后 go build / go vet /"+
			"测试全都不报，只有 go doc 看得见。", o)
	}
}

func isExported(name string) bool {
	if name == "" {
		return false
	}
	return unicode.IsUpper([]rune(name)[0])
}

func itoaLocal(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestGodoc_NoAbsorbedDocBlock 是上一条的**姊妹检法**：注释块被并进了上一个符号的 doc。
//
// # 它跟上一条抓的不是同一种脱钩
//
// 上一条抓「注释块与声明之间隔了空行 ⟹ Doc 为 nil」。但还有一种：两个注释块**中间
// 没有空行**，于是 Go 把它们当成**一整块**，整块归给下面那个声明——**上面那个符号的
// godoc 就此蒸发**，而下面那个符号的文档第一句变成了别人的。
//
// 实例（第五次复发，`agent_v2/config.go`）：`WithHooksChained` 的 11 行 godoc 与
// `WithRepeatToolReminder` 的 godoc 之间漏了空行 ⟹ `go doc WithRepeatToolReminder`
// 的第一句是「WithHooksChained 把 h 链到已配的 hook 之前」，而
// `go doc WithHooksChained` 一行都没有。`go build` / `go vet` / 全量测试 / commentaudit
// 全部沉默，**上一条哨兵也过**（它只查空行那种）。
//
// # 检法
//
// 「某个导出符号 Doc 为 nil」**且**「它的名字作为首词出现在别人的 doc 块里」。
// Go 的 doc 约定是首词即符号名，所以这两条同时成立基本只有一个解释：它的 doc 被吞了。
func TestGodoc_NoAbsorbedDocBlock(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("拿不到本文件路径，无法锚定仓库根")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	var offenders []string
	scanned := 0

	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if path == repoRoot {
				return nil
			}
			switch n := d.Name(); {
			case n == "vendor" || n == "node_modules" || n == "testdata":
				return filepath.SkipDir
			case strings.HasPrefix(n, "."):
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			t.Errorf("解析失败 %s: %v", path, perr)
			return nil
		}
		scanned++
		rel, _ := filepath.Rel(repoRoot, path)
		rel = filepath.ToSlash(rel)

		// 第一趟：收集本文件里「导出且 Doc 为 nil」的符号，以及所有非空 doc 块。
		undocumented := map[string]int{} // name -> 声明行
		type docBlock struct {
			owner string
			lines []string
			start int
		}
		var blocks []docBlock
		collect := func(doc *ast.CommentGroup, pos token.Pos, name string) {
			if !isExported(name) {
				return
			}
			if doc == nil {
				undocumented[name] = fset.Position(pos).Line
				return
			}
			var ls []string
			for _, c := range doc.List {
				ls = append(ls, c.Text)
			}
			blocks = append(blocks, docBlock{owner: name, lines: ls, start: fset.Position(doc.Pos()).Line})
		}
		for _, decl := range f.Decls {
			switch dd := decl.(type) {
			case *ast.FuncDecl:
				collect(dd.Doc, dd.Pos(), dd.Name.Name)
			case *ast.GenDecl:
				for _, spec := range dd.Specs {
					switch sp := spec.(type) {
					case *ast.TypeSpec:
						collect(sp.Doc, sp.Pos(), sp.Name.Name)
					case *ast.ValueSpec:
						if len(sp.Names) > 0 {
							collect(sp.Doc, sp.Pos(), sp.Names[0].Name)
						}
					}
				}
			}
		}
		if len(undocumented) == 0 {
			return nil
		}
		// 第二趟：**只看每个 doc 块的首行**。
		//
		// ⚠️ 方向很容易写反（第一版就反了，当场 10 条假阳）：两块粘连时，合并块归给
		// **后面**那个声明，而合并块的**首行**是**前面**那个符号的名字。所以签名是
		// 「块主人是 B，块的第一句却在讲 A，而 A 没有自己的 doc」。
		//
		// 查中间行会把大量正常行文卷进来——doc 里另起一段讲别的符号是常态
		// （`// PlanAgent 的 ...`），那不是脱钩。
		for _, b := range blocks {
			for i, raw := range b.lines {
				if i != 0 {
					break
				}
				txt := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "//"))
				if txt == "" {
					continue
				}
				first := txt
				if sp := strings.IndexAny(txt, " \t"); sp > 0 {
					first = txt[:sp]
				}
				declLine, isUndoc := undocumented[first]
				if !isUndoc || first == b.owner {
					continue
				}
				offenders = append(offenders, rel+":"+itoaLocal(b.start+i)+
					" —— "+first+"（声明在第 "+itoaLocal(declLine)+" 行）的 godoc"+
					"被并进了 "+b.owner+" 的 doc 块，两块之间漏了空行")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("扫描失败: %v", err)
	}
	if scanned < 200 {
		t.Fatalf("只扫到 %d 个文件——扫描面坏了", scanned)
	}
	for _, o := range offenders {
		t.Errorf("godoc 被上一个符号吞了：%s\n"+
			"  → 在两个注释块之间加一个空行。没有它 Go 把两块当成一块，"+
			"上面那个符号的 godoc 会**整个消失**，而下面那个的文档第一句是别人的。", o)
	}
}
