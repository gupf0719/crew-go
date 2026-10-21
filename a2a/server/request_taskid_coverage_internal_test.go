package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestRequestTaskID_CoversEveryInboundPayload 是一道**机械对账**：协议入站负载的**全集**里，
// 每一个都必须在 requestTaskID 的 switch 里有一个 case——指向 task 的返 taskIDFound、
// 不指向的返 taskIDNone 并把理由写在 case 上。**没有第二张豁免表**：那个 switch 就是表。
//
// ⚠️ 为什么需要它：requestTaskID 是 `switch v := payload.(type)`，**default 返 ("", false)
// 就是放行**。SDK 将来加一个指向 task 的负载（协议在演进——push-config 那组就是后加的），
// 这里认不出来 ⟹ 归属检查静默不跑 ⟹ 跨租户读写照常发生，**没有任何东西会报错**。本仓栽过
// 同款：守卫长在 A 路径上，业务从 B 路径进来照样把要拒的形态送进存档。
//
// **全集怎么定的**：`req.Payload` 的类型由 SDK 的 `a2asrv.RequestHandler` 接口决定——
// `doCall[Req]` 对每个协议方法泛型化，负载恒是那个方法的入参类型。所以扫这个接口的方法
// 入参，比扫「叫不叫 *Request」（漏掉 `a2atypes.PushConfig` 这种不叫 Request 的负载）或者扫「有没有
// TaskID 字段」（把 `a2atypes.TaskStatusUpdateEvent` 这类**出站**事件也扫进来）都准。
//
// 清单取自 SDK 源码而不是我们手抄——手抄的那份就是下一个会漂的拷贝。
func TestRequestTaskID_CoversEveryInboundPayload(t *testing.T) {
	declared := scanHandlerPayloadTypes(t, sdkModuleDir(t)+"/a2asrv")
	if len(declared) < 8 {
		t.Fatalf("fixture 没搭对：只从 RequestHandler 扫出 %d 个入站负载类型（%v），扫描逻辑失效了",
			len(declared), keysOf(declared))
	}

	handled := map[string]bool{}
	src, err := os.ReadFile("interceptor.go")
	if err != nil {
		t.Fatalf("读 interceptor.go: %v", err)
	}
	for _, m := range regexp.MustCompile(`case \*a2atypes\.([A-Za-z0-9_]+):`).FindAllStringSubmatch(string(src), -1) {
		handled[m[1]] = true
	}
	if len(handled) < 5 {
		t.Fatalf("fixture 没搭对：只从 interceptor.go 扫出 %d 个 case", len(handled))
	}

	var missing []string
	for name := range declared {
		if !handled[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("这些协议入站负载 requestTaskID 的 switch 里没有：%v\n"+
			"  → 落到 default = taskIDUnknown = 归属检查对它没跑（只有一条运行期 ERROR 日志）。\n"+
			"  → 指向已有 task 的加 case 返 found(...)；确实跟 task 无关的也要加 case、"+
			"返 taskIDNone 并把理由写在 case 上。**不要另开一张豁免表**——那就是第二份会漂的清单。",
			missing)
	}

	// 反向：switch 里有、SDK 已经没有的类型 ⟹ 那个 case 是死代码，删掉。
	var stale []string
	for name := range handled {
		if !declared[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("这些 case 对应的类型已经不是入站负载了，是死代码：%v", stale)
	}
}

// scanHandlerPayloadTypes 扫出 a2asrv.RequestHandler 接口每个方法的**第二个入参**类型名
// （第一个恒是 ctx）——那就是 req.Payload 的全集。
func scanHandlerPayloadTypes(t *testing.T, dir string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	found := map[string]bool{}
	for _, pkg := range pkgs {
		ast.Inspect(pkg, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "RequestHandler" {
				return true
			}
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok || it.Methods == nil {
				return true
			}
			for _, m := range it.Methods.List {
				ft, isFunc := m.Type.(*ast.FuncType)
				if !isFunc || ft.Params == nil || len(ft.Params.List) < 2 {
					continue
				}
				// 形如 *a2a.GetTaskRequest
				star, isStar := ft.Params.List[1].Type.(*ast.StarExpr)
				if !isStar {
					continue
				}
				if sel, isSel := star.X.(*ast.SelectorExpr); isSel {
					found[sel.Sel.Name] = true
				}
			}
			return true
		})
	}
	return found
}

// sdkModuleDir 问 go 工具链要 SDK 在模块缓存里的路径，不写死版本号。
func sdkModuleDir(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/a2aproject/a2a-go/v2").Output()
	dir := ""
	if err == nil {
		dir = strings.TrimSpace(string(out))
	}
	if dir == "" {
		// **vendor 模式走的是这条**（实测 `GOFLAGS=-mod=vendor` 下 go list 返 err=nil +
		// 空串，不是报错），SDK 源码这时在 vendor/ 下——回落过去，让这道防线在那些环境里
		// 继续工作，而不是从「能跑」变成「硬失败」。
		if root, rerr := exec.Command("git", "rev-parse", "--show-toplevel").Output(); rerr == nil {
			cand := filepath.Join(strings.TrimSpace(string(root)),
				"vendor", "github.com", "a2aproject", "a2a-go", "v2")
			if _, serr := os.Stat(cand); serr == nil {
				return cand
			}
		}
	}
	if dir == "" {
		// **不能 Skip**：跳过的测试是绿的，而这是这条防线唯一的机械保障。既不在模块缓存
		// 也不在 vendor ⟹ 环境有问题，静默放过等于把防线关掉。
		t.Fatalf("拿不到 SDK 模块路径（go list err=%v，vendor 也没有）", err)
	}
	return dir
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
