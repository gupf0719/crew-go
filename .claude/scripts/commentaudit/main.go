// Command commentaudit 是 comment-audit skill 的第 1 层:注释/godoc 的机械对账.
//
// 三类检查全部基于 go/ast(符号检查比 grep 准——能区分定义点 vs 引用点):
//
//   - 失效符号引用:注释里反引号包裹的单标识符是否在全仓符号表中.
//     失效 = 重命名/删除 API 没同步注释(ERROR 级,误报靠白名单 + 过滤压低).
//   - 数字声明冲突:同一量词出现多个不同数字(如 "8 编排"/"9 编排"),
//     提示人工确认(WARN 级,不自动判错——同量词可能指代不同集合).
//   - 失效路径/行号:注释里 pkg/xxx.go:行号 与 doc 链接是否存在、行号是否越界
//     (ERROR 级,只查含目录的相对路径,误报极低).
//
// 范围:符号表恒按 -root 全仓构建;-files 非空时只检查这些文件(供 Stop
// hook 增量校验改动文件),为空时检查全仓.
//
// **.md 文档支持**(-files 含 .md 或全仓扫时自动启用):对文档逐行跑同款路径检查——
// doc 里 `pkg/xxx.go:行号` 代码引用是否存在 / 行号越界 + markdown 链接(含相对路径)
// 是否解析得到 + 数字声明冲突.根治 doc 里 file:line drift.**机械层抓不到语义型
// stale**(如"仅 InMemoryCheckpointer"/"未实现 X"——值/符号都存在,只是声明过时),
// 那类仍需人工 / agent 按"status/缺失声明 → grep 代码"审.
//
// 退出码:发现 ERROR 级 finding 返 1(供 Stop hook gate),仅 WARN 或干净返 0.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// finding 是一条机械对账发现(ERROR 级).
type finding struct {
	file string
	line int
	msg  string
}

// countHit 是一处数字声明命中(WARN 级,report 阶段按量词聚合找冲突).
type countHit struct {
	unit string
	num  string
	file string
	line int
}

// 注释里反引号包裹内容:`WithModel` / `Type.Method` / `pkg.Foo`.
var reBacktick = regexp.MustCompile("`([^`]+)`")

// 数字声明:阿拉伯数字紧跟量词白名单.量词前若是"第"则为序数,collectCounts 排除.
var reCountDecl = regexp.MustCompile(`(\d{1,3})\s*(编排|层|维|method|方法|metric|节点|回调|钩子|步|档|种|个|条)`)

// 含目录的 .go 相对路径(至少一级目录,可带 :行号);裸文件名无法可靠定位故排除.
var reGoPath = regexp.MustCompile(`([A-Za-z0-9_]+(?:/[A-Za-z0-9_]+)+\.go)(?::(\d+))?`)

// doc/ 或 .claude/ 下的 markdown 链接.
var reDocPath = regexp.MustCompile(`((?:doc|\.claude)/[A-Za-z0-9_./-]+\.md)`)

// markdown 链接 `](path.md)`(可带 #anchor);用于 .md 文档里**相对路径**链接校验
// (root-relative 的 doc/ 链接走 reDocPath,这里只补相对路径 ../ ./ 与裸文件名).
var reMdLink = regexp.MustCompile(`\]\(([^)]+\.md)(?:#[^)]*)?\)`)

// 合法标识符 base name.
var reIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// 白名单:Go 内置 + 高频标准库类型/标识符,避免反引号写它们时误报.
var symbolAllowlist = map[string]bool{
	"error": true, "string": true, "int": true, "int32": true, "int64": true,
	"uint": true, "uint32": true, "uint64": true, "byte": true, "rune": true,
	"bool": true, "float32": true, "float64": true, "any": true, "nil": true,
	"true": true, "false": true, "iota": true, "Context": true, "Reader": true,
	"Writer": true, "Time": true, "Duration": true, "Error": true, "Stringer": true,
	"Mutex": true, "WaitGroup": true, "Once": true, "Map": true,
}

func main() {
	root := flag.String("root", ".", "扫描根目录(符号表恒按此全仓构建)")
	files := flag.String("files", "", "逗号分隔的文件列表;非空时只检查这些文件的注释")
	showWarn := flag.Bool("warn", true, "是否输出 WARN 级(数字声明冲突)")
	includeTests := flag.Bool("include-tests", false, "是否检查 _test.go 注释(默认跳过,测试注释多字符串字面量噪声)")
	flag.Parse()

	goFiles, err := collectGoFiles(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collect go files: %v\n", err)
		os.Exit(2)
	}

	fset := token.NewFileSet()
	parsed := make(map[string]*ast.File, len(goFiles))
	symbols := make(map[string]bool, 4096)
	for _, f := range goFiles {
		af, perr := parser.ParseFile(fset, f, nil, parser.ParseComments|parser.SkipObjectResolution)
		if perr != nil {
			// 解析失败的文件跳过即可,不阻断整体对账.
			continue
		}
		parsed[f] = af
		collectSymbols(af, symbols)
	}

	// 检查范围:-files 指定则只检查交集,否则全部已解析文件.
	checkSet := make(map[string]*ast.File, len(parsed))
	if *files != "" {
		// -files 模式(hook 增量):传什么查什么,不自动跳 test.
		for _, raw := range strings.Split(*files, ",") {
			p := filepath.Clean(strings.TrimSpace(raw))
			if af, ok := parsed[p]; ok {
				checkSet[p] = af
			}
		}
	} else {
		for p, af := range parsed {
			if !*includeTests && strings.HasSuffix(p, "_test.go") {
				continue
			}
			checkSet[p] = af
		}
	}

	// .md 文档检查集:-files 里的 .md 项(Stop hook 增量),或全仓扫时所有 .md.
	var mdFiles []string
	if *files != "" {
		for _, raw := range strings.Split(*files, ",") {
			p := filepath.Clean(strings.TrimSpace(raw))
			if strings.HasSuffix(p, ".md") {
				if full := filepath.Join(*root, p); fileLineCount(full) >= 0 {
					mdFiles = append(mdFiles, full)
				}
			}
		}
	} else {
		mdFiles = collectMdFiles(*root)
	}

	var errSym, errPath []finding
	var counts []countHit
	lineCache := make(map[string]int) // 文件→行数(-1=不存在),行号越界检查复用
	topDirs := topLevelDirs(*root)    // 本仓顶层目录,路径检查只认本仓相对路径

	for path, af := range checkSet {
		rel := relPath(path, *root)
		for _, cg := range af.Comments {
			// CommentGroup 级豁免仅限"外部框架名"(业务几乎不出现,跨行安全,抓信号在
			// 上一行的 eino 范式引用);历史/proto 信号走 checkSymbols 内窄当行豁免.
			// 路径/数字不受任何语境豁免——本仓文档移动 / 数字漂移仍是真 drift.
			groupExternal := containsAny(strings.ToLower(cg.Text()), externalFrameworks)
			// 代码块状态跨整个 CommentGroup——`//` 注释每条只有一行，判据要的空行上下文在这一层
			inCodeBlock, prevBlank := false, true
			for _, c := range cg.List {
				pos := fset.Position(c.Pos())
				for _, ln := range commentLines(c.Text, &inCodeBlock, &prevBlank) {
					if !groupExternal {
						errSym = append(errSym, checkSymbols(ln, rel, pos.Line, symbols)...)
					}
					errPath = append(errPath, checkPaths(ln, rel, pos.Line, *root, lineCache, topDirs)...)
					if *showWarn {
						counts = append(counts, collectCounts(ln, rel, pos.Line)...)
					}
				}
			}
		}
	}

	// .md 文档逐行检查:复用 checkPaths(代码 `pkg/x.go:行号` + 越界 + root-relative
	// doc 链接)+ collectCounts(数字冲突);另查相对 markdown 链接.符号检查不跑——prose
	// 反引号误报高,且语义型 stale(仅 X / 未实现)机械层本就抓不到(见包注释).
	for _, mdPath := range mdFiles {
		rel := relPath(mdPath, *root)
		for _, dl := range docLines(mdPath) {
			// .md 路径检查跳三类语境(数字冲突不受影响仍查):
			//   1. 外部框架行(eino/langgraph 的 `schema/`/`model/` 路径碰撞 crew-go 顶层名)
			//   2. 历史/删除语境(文档正确记录"已删/整文件移除"的文件 ≠ drift)+ ~~删除线~~
			//   3. 行内 lineSignals(改名/废弃等)
			low := strings.ToLower(dl.text)
			if !containsAny(low, externalFrameworks) && !containsAny(low, lineSignals) &&
				!containsAny(dl.text, docHistorySignals) && !strings.Contains(dl.text, "~~") {
				errPath = append(errPath, checkPaths(dl.text, rel, dl.no, *root, lineCache, topDirs)...)
				errPath = append(errPath, checkDocLinks(dl.text, mdPath, rel, dl.no, *root)...)
			}
			if *showWarn {
				counts = append(counts, collectCounts(dl.text, rel, dl.no)...)
			}
		}
	}

	report(len(parsed), len(checkSet)+len(mdFiles), errSym, errPath, counts, *showWarn)

	if len(errSym) > 0 || len(errPath) > 0 {
		os.Exit(1)
	}
}

// collectGoFiles 遍历 root 收集 .go 文件,跳过 vendor / .git / 自身脚本目录等.
func collectGoFiles(root string) ([]string, error) {
	var out []string
	skipDir := map[string]bool{
		"vendor": true, ".git": true, "node_modules": true,
		"testdata": true, ".claude": true,
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if skipDir[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// collectMdFiles 遍历 root 收集 .md 文档(全仓扫模式用),跳过 vendor/.git 等.
func collectMdFiles(root string) []string {
	var out []string
	skipDir := map[string]bool{
		"vendor": true, ".git": true, "node_modules": true, "testdata": true,
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if skipDir[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			out = append(out, path)
		}
		return nil
	})
	return out
}

// docLine 是 .md 文档的一行(行号 + 文本).
type docLine struct {
	no   int
	text string
}

// docLines 读 .md 返回正文行,跳过 ``` 围栏代码块(块内示例代码的引用可能是假设
// 符号/路径,查它们会假阳)与空行.
func docLines(path string) []docLine {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	var out []docLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	no, inFence := 0, false
	for sc.Scan() {
		no++
		line := sc.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence || strings.TrimSpace(line) == "" {
			continue
		}
		out = append(out, docLine{no, line})
	}
	return out
}

// checkDocLinks 校验 .md 里**相对路径** markdown 链接 `](../x.md)` 是否解析得到
// (root-relative 的 doc/ 链接走 checkPaths 的 reDocPath,这里只补相对路径避免双报).
func checkDocLinks(line, mdPath, rel string, lineNo int, root string) []finding {
	var out []finding
	dir := filepath.Dir(mdPath)
	for _, m := range reMdLink.FindAllStringSubmatch(line, -1) {
		link := m[1]
		if strings.HasPrefix(link, "http") || strings.HasPrefix(link, "file:") || strings.HasPrefix(link, "/") ||
			strings.HasPrefix(link, "doc/") || strings.HasPrefix(link, ".claude/") {
			continue // URL / file:// / 绝对路径 / root-relative(reDocPath 已查)
		}
		if _, serr := os.Stat(filepath.Join(dir, link)); serr != nil {
			out = append(out, finding{rel, lineNo, fmt.Sprintf("markdown 链接 %s 解析后不存在", link)})
		}
	}
	return out
}

// collectSymbols 把一个文件里定义的所有标识符名收进 set——不仅函数/类型/常量/变量/
// 字段/接口方法/import 包名,还含**函数参数 / 命名返回值 / 局部 := 变量 / range 变量**.
//
// 后者是泛化教训(eino 实测):英文 godoc 习惯用反引号引用参数/局部变量解释逻辑
// (如 "when `wasInterrupted` is true"),不收会把它们全报成失效符号(精确率崩到 0).
// 中文注释少这么写所以 crew-go 上没暴露——故用 ast.Inspect 递归进函数体宽收.
func collectSymbols(af *ast.File, set map[string]bool) {
	ast.Inspect(af, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			set[x.Name.Name] = true
		case *ast.TypeSpec:
			set[x.Name.Name] = true
		case *ast.ValueSpec:
			for _, nm := range x.Names {
				set[nm.Name] = true
			}
		case *ast.Field:
			// 覆盖结构体字段 / 接口方法 / 函数参数 / 命名返回值.
			for _, nm := range x.Names {
				set[nm.Name] = true
			}
		case *ast.AssignStmt:
			if x.Tok == token.DEFINE { // := 局部变量定义
				for _, lhs := range x.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						set[id.Name] = true
					}
				}
			}
		case *ast.RangeStmt:
			if id, ok := x.Key.(*ast.Ident); ok {
				set[id.Name] = true
			}
			if id, ok := x.Value.(*ast.Ident); ok {
				set[id.Name] = true
			}
		case *ast.ImportSpec:
			if x.Name != nil {
				set[x.Name.Name] = true
			} else {
				p := strings.Trim(x.Path.Value, `"`)
				set[p[strings.LastIndex(p, "/")+1:]] = true
			}
		case *ast.BasicLit:
			// 标识符形态的字符串常量值(如 `GraphName = "ReActAgent"`):注释反引号引用
			// 这类值是预期的(值存在,非失效符号),收进 set 防假阳(英文项目 PascalCase
			// 常量值躲过下划线规则——eino 实测).
			if x.Kind == token.STRING {
				if v, uerr := strconv.Unquote(x.Value); uerr == nil && reIdent.MatchString(v) {
					set[v] = true
				}
			}
		}
		return true
	})
}

// commentLines 把注释体拆成行,去掉 // 与 /* */ 标记,并跳过两类降噪行:
// 缩进的代码示例块(godoc 惯例以 tab 开头)与编译指令(//go: 等).
// commentLines 把一条注释切成「叙述行」。
//
// ⚠️ **状态必须由调用方跨行持有**：`//` 注释的 `c.Text` 只有**一行**，而 godoc 代码块的
// 判据是「**空行之后**的缩进块」——空行上下文在 CommentGroup 层，不在单行里。早先把状态
// 放在函数内，于是每行都从「组首」重新起算 ⟹ 任何缩进行都被当成代码块跳过，而本仓 bullet
// 折行的续行正好缩进 4-5 空格。一条描述已删方案的 drift 就是这么过闸的。
func commentLines(raw string, inCodeBlock, prevBlank *bool) []string {
	body := raw
	switch {
	case strings.HasPrefix(body, "//"):
		body = strings.TrimPrefix(body, "//")
	case strings.HasPrefix(body, "/*"):
		body = strings.TrimSuffix(strings.TrimPrefix(body, "/*"), "*/")
	}
	var out []string
	// godoc 代码块的真实判据是「**空行之后**的缩进块」(gofmt 就按这个渲染),不是「凡缩进即代码」。
	//
	// ⚠️ 原先只看缩进,于是把 bullet 折行的**续行**一并跳过了——而本仓 bullet 续行惯例正好是
	// 缩进 4-5 空格。双向对照实测:同一注释组里,缩进续行插假符号**不报**、顶格行插**报**。
	// 一条描述已删方案的 drift 就是这么过闸的,而 commit 里还写着「机械层 0 ERROR」。
	for _, ln := range strings.Split(body, "\n") {
		indented := strings.HasPrefix(ln, "\t") || strings.HasPrefix(ln, "    ")
		s := strings.TrimSpace(ln)
		if s == "" {
			*prevBlank, *inCodeBlock = true, false
			continue
		}
		if indented {
			if *prevBlank {
				*inCodeBlock = true // 空行后的缩进 = 真代码块
			}
			*prevBlank = false
			if *inCodeBlock {
				continue
			}
			// 否则是 bullet 折行续行——是叙述,要检
		} else {
			*inCodeBlock, *prevBlank = false, false
		}
		if strings.HasPrefix(s, "go:") || strings.HasPrefix(s, "nolint") || strings.HasPrefix(s, "+build") {
			continue
		}
		out = append(out, s)
	}
	return out
}

// 信号词分两档作用域(实测教训:CommentGroup 级 + 宽信号词会误豁免真 drift——redis
// Cleanup 块含业务"删除 sid"会吞掉同块真缺失常量).
//
// externalFrameworks 外部框架/产品名:业务逻辑里几乎不出现,可安全用 CommentGroup
// 级豁免(跨行抓"eino 范式 `WithX`"这种信号在上一行的情况).
var externalFrameworks = []string{"eino", "langgraph", "langchain", "claude code"}

// lineSignals 历史/改名/外部 proto:可能与业务词共块,只在出现信号的**当行**豁免,
// 不放大整块.**故意不含泛化的"对照/vs"**——"vs 低阶 Router"是对照本项目 API,符号
// 应存在,误豁免会放过真 drift(低阶 API 名漂移正是 drift 富矿);也不含业务高频词
// "删除/移除"与项目高频词"spec"(modules/ 类型注释遍地提到 spec 来源).
var lineSignals = []string{
	"v1", "已删", "废弃", "deprecated", "曾", "不再", "退化",
	"原名", "rename", "改名", "重命名", "otlp", "proto",
}

// docHistorySignals 是 .md 文档**额外**的历史/删除信号(仅作用于 .md 路径检查).
// **.go 侧故意不跳删除**——注释引用删文件是 drift;但文档在"已删/整文件移除/不存在"
// 语境引用删文件是**正确历史记录**(store.md "agent_v2/memory.go 整文件移除"),跳过.
var docHistorySignals = []string{"整文件", "移除", "不存在", "未建", "全删"}

// rePathPlaceholder 匹配占位文件名(如 `observability/xxx.go:NNN` 的 meta 行号 drift
// 警告)——非真实文件,跳过路径检查.
var rePathPlaceholder = regexp.MustCompile(`(?i)^(xxx+|yyy+|zzz+|nnn+|foo|bar|baz|pkg|abc)$`)

// placeholderPrefixes 是 godoc 示例里惯用的占位符类型名前缀(如 `*MyTool`).
var placeholderPrefixes = []string{"My", "Foo", "Bar", "Baz", "Example", "Xxx"}

// isPlaceholder 判断标识符是否是文档示例占位符(惯用前缀).
func isPlaceholder(name string) bool {
	for _, p := range placeholderPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// containsAny 判断 low(调用方已 ToLower)是否含任一信号词.
func containsAny(low string, sigs []string) bool {
	for _, sig := range sigs {
		if strings.Contains(low, sig) {
			return true
		}
	}
	return false
}

// checkSymbols 抽注释里反引号单标识符,报不在全仓符号表的引用.
func checkSymbols(line, file string, lineNo int, symbols map[string]bool) []finding {
	// 当行历史/改名/外部 proto 信号 → 该行符号缺失属预期,跳过(窄作用域只豁免本行,
	// 避免吞掉同块其他行的真 drift).外部框架名走 CommentGroup 级豁免(在 main).
	if containsAny(strings.ToLower(line), lineSignals) {
		return nil
	}
	var out []finding
	for _, m := range reBacktick.FindAllStringSubmatch(line, -1) {
		tok := strings.TrimSpace(m[1])
		// 含空格 = 代码片段/短语,非单符号引用,跳过.
		if strings.ContainsAny(tok, " \t") {
			continue
		}
		// 含 '/' = 路径,交给 checkPaths.
		if strings.Contains(tok, "/") {
			continue
		}
		// 去前导指针/切片/取址与尾部调用/复合字面量括号.
		tok = strings.TrimLeft(tok, "*&[]")
		tok = strings.TrimRight(tok, "(){}")
		// 省略 receiver 的方法引用写法(如 `.Float64`)→ 跳过.
		if strings.HasPrefix(tok, ".") {
			continue
		}
		// 含下划线 = 字符串值 / JSON 字段 / env / metric name / 测试子名,非 Go
		// 驼峰符号引用,跳过(项目命名规范驼峰,符号几乎不含下划线)——主要降噪刀.
		if strings.Contains(tok, "_") {
			continue
		}
		// 全大写 = SQL/Redis 命令关键字或常量字面量(如 `LIKE` `BGSAVE` `TIMESTAMPTZ`),
		// 非 Go 符号引用(项目标识符驼峰),跳过.
		if tok == strings.ToUpper(tok) && tok != strings.ToLower(tok) {
			continue
		}
		segs := strings.Split(tok, ".")
		// 外部包 qualified(首段全小写像包名且不在符号表)→ 外部库引用,跳过.
		if len(segs) > 1 {
			if first := segs[0]; first != "" && first == strings.ToLower(first) && !symbols[first] {
				continue
			}
		}
		// qualified(Type.Method):任一段在符号表/白名单即放过.
		known := false
		for _, seg := range segs {
			if symbols[seg] || symbolAllowlist[seg] {
				known = true
				break
			}
		}
		if known {
			continue
		}
		base := segs[len(segs)-1]
		if !reIdent.MatchString(base) {
			continue
		}
		// 纯小写无下划线(通用词/局部变量名如 ctx/err/done)不查——降误报.
		if base == strings.ToLower(base) && !strings.Contains(base, "_") {
			continue
		}
		// godoc 示例占位符(`*MyTool` / `FooConfig` 等惯用前缀)→ 跳过.
		if isPlaceholder(base) {
			continue
		}
		out = append(out, finding{file, lineNo, fmt.Sprintf("`%s` 不在全仓符号表(疑似重命名/删除未同步注释)", tok)})
	}
	return out
}

// checkPaths 校验注释里含目录的 .go 路径(含行号越界)与 doc 链接是否存在.
func checkPaths(line, file string, lineNo int, root string, lineCache map[string]int, topDirs map[string]bool) []finding {
	var out []finding
	for _, m := range reGoPath.FindAllStringSubmatch(line, -1) {
		p := m[1]
		// 首段不是本仓顶层目录 → 外部框架源码路径引用(eino/adk/compose 等),跳过.
		if i := strings.IndexByte(p, '/'); i < 0 || !topDirs[p[:i]] {
			continue
		}
		// 占位文件名(`observability/xxx.go` 等 meta 示例 / 行号 drift 警告)跳过.
		if rePathPlaceholder.MatchString(strings.TrimSuffix(p[strings.LastIndexByte(p, '/')+1:], ".go")) {
			continue
		}
		full := filepath.Join(root, p)
		cnt, ok := lineCache[full]
		if !ok {
			cnt = fileLineCount(full)
			lineCache[full] = cnt
		}
		if cnt < 0 {
			out = append(out, finding{file, lineNo, fmt.Sprintf("引用路径 %s 不存在", p)})
			continue
		}
		if m[2] != "" {
			n, _ := strconv.Atoi(m[2])
			if n > cnt {
				out = append(out, finding{file, lineNo, fmt.Sprintf("%s:%d 行号越界(文件仅 %d 行)", p, n, cnt)})
			}
		}
	}
	for _, m := range reDocPath.FindAllStringSubmatch(line, -1) {
		p := m[1]
		if _, serr := os.Stat(filepath.Join(root, p)); serr != nil {
			out = append(out, finding{file, lineNo, fmt.Sprintf("引用文档 %s 不存在", p)})
		}
	}
	return out
}

// collectCounts 抽注释里"数字+量词"声明(排除"第N"序数),供 report 聚合找冲突.
func collectCounts(line, file string, lineNo int) []countHit {
	var out []countHit
	for _, loc := range reCountDecl.FindAllStringSubmatchIndex(line, -1) {
		numStart := loc[2]
		prefix := strings.TrimRight(line[:numStart], " ")
		if strings.HasSuffix(prefix, "第") {
			continue
		}
		out = append(out, countHit{
			unit: line[loc[4]:loc[5]],
			num:  line[loc[2]:loc[3]],
			file: file,
			line: lineNo,
		})
	}
	return out
}

// fileLineCount 返回文件行数,文件不存在返 -1.
func fileLineCount(path string) int {
	f, ferr := os.Open(path)
	if ferr != nil {
		return -1
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024) // 防超长行截断
	n := 0
	for sc.Scan() {
		n++
	}
	return n
}

// topLevelDirs 返回 root 下顶层目录名集合,用于区分本仓相对路径与外部源码引用.
func topLevelDirs(root string) map[string]bool {
	dirs := map[string]bool{}
	entries, err := os.ReadDir(root)
	if err != nil {
		return dirs
	}
	for _, e := range entries {
		if e.IsDir() {
			dirs[e.Name()] = true
		}
	}
	return dirs
}

// relPath 把绝对/含 root 前缀的路径转成相对 root 显示,失败回退原值.
func relPath(path, root string) string {
	if r, rerr := filepath.Rel(root, path); rerr == nil {
		return r
	}
	return path
}

// report 打印分类 finding 与摘要.
func report(parsedN, checkedN int, errSym, errPath []finding, counts []countHit, showWarn bool) {
	fmt.Printf("=== comment-audit 第1层机械对账 ===\n")
	fmt.Printf("符号表来源 %d 文件;检查 %d 文件(.go 注释 + .md 文档)\n\n", parsedN, checkedN)

	printFindings("[ERROR] 失效符号引用", errSym)
	printFindings("[ERROR] 失效路径/行号", errPath)

	warnUnits := 0
	if showWarn {
		warnUnits = reportCounts(counts)
	}

	if len(errSym) == 0 && len(errPath) == 0 && warnUnits == 0 {
		fmt.Println("✓ 干净:无失效符号引用 / 失效路径 / 数字声明冲突")
	}
	fmt.Printf("\n摘要:ERROR=%d(符号 %d + 路径 %d) WARN=%d(冲突量词)\n",
		len(errSym)+len(errPath), len(errSym), len(errPath), warnUnits)
}

func printFindings(title string, fs []finding) {
	if len(fs) == 0 {
		return
	}
	sort.Slice(fs, func(i, j int) bool {
		if fs[i].file != fs[j].file {
			return fs[i].file < fs[j].file
		}
		return fs[i].line < fs[j].line
	})
	fmt.Printf("%s (%d):\n", title, len(fs))
	for _, f := range fs {
		fmt.Printf("  %s:%d  %s\n", f.file, f.line, f.msg)
	}
	fmt.Println()
}

// reportCounts 按量词聚合数字声明,打印同量词多数字的冲突,返回冲突量词数.
func reportCounts(counts []countHit) int {
	byUnit := map[string]map[string][]countHit{}
	for _, c := range counts {
		if byUnit[c.unit] == nil {
			byUnit[c.unit] = map[string][]countHit{}
		}
		byUnit[c.unit][c.num] = append(byUnit[c.unit][c.num], c)
	}

	var conflictUnits []string
	for unit, nums := range byUnit {
		if len(nums) > 1 {
			conflictUnits = append(conflictUnits, unit)
		}
	}
	if len(conflictUnits) == 0 {
		return 0
	}
	sort.Strings(conflictUnits)

	fmt.Printf("[WARN] 数字声明冲突 — 需人工确认(同量词多数字,可能指代不同集合):\n")
	for _, unit := range conflictUnits {
		nums := byUnit[unit]
		numKeys := make([]string, 0, len(nums))
		for n := range nums {
			numKeys = append(numKeys, n)
		}
		sort.Slice(numKeys, func(i, j int) bool {
			a, _ := strconv.Atoi(numKeys[i])
			b, _ := strconv.Atoi(numKeys[j])
			return a < b
		})
		fmt.Printf("  量词「%s」出现 %d 个不同数字:\n", unit, len(numKeys))
		for _, n := range numKeys {
			locs := nums[n]
			var sb strings.Builder
			for i, l := range locs {
				if i >= 4 {
					fmt.Fprintf(&sb, " …等共 %d 处", len(locs))
					break
				}
				if i > 0 {
					sb.WriteString(", ")
				}
				fmt.Fprintf(&sb, "%s:%d", l.file, l.line)
			}
			fmt.Printf("    %s%s  ← %s\n", n, unit, sb.String())
		}
	}
	return len(conflictUnits)
}
