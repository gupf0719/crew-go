package agent_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestToolBoundaries_StripBothControlChannels 机械对账：**每一处「把 agent 降格成一件
// 能力」的边界，必须把全部控制信号通道都摘掉。**
//
// # 为什么要机械对账，而不是写进注释靠人记
//
// 这两道摘除是同一条规矩的两半，而它们此前**只落实了一半**：转交那半在两个边界都摘了，
// 路由那半一处都没摘，于是被当工具用的孙辈调一次 route_to 就能劫持 classifier 的分类
// 结果（实测 err 全程 nil，见 TestRouteDest_NotInheritedByAgentAsTool）。
//
// 缺口不是因为谁不小心，是因为「加一个新的控制信号通道」和「在每个边界摘它」是两处
// 独立的改动，中间只有注释相连——项目规约把这种形态叫「设计债的自白」。这条哨兵把它
// 变成机械约束：任一边界漏掉任一通道，或将来新增第三个通道却没在边界摘，都会红。
//
// 判据是**成对出现**而非绝对数量：新增边界时两个都写就自然通过，不需要回来改期望值。
func TestToolBoundaries_StripBothControlChannels(t *testing.T) {
	// 控制信号通道的摘除函数——**新增通道时在这里登记**，哨兵会要求每个边界都摘它。
	strippers := []string{
		"withoutHandoffDest", // 转交：HandoffAgent 放在 ctx 上的收件人
		"withoutRouteDest",   // 路由：RoutingAgent 放在 ctx 上的收件人
	}

	fset := token.NewFileSet()
	// key = "文件:函数"，value = 这个函数里出现过哪些摘除调用
	seen := map[string]map[string]bool{}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("读目录: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			t.Fatalf("解析 %s: %v", name, perr)
		}
		// 摘除函数**自己的定义**所在文件不算调用点。
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			for _, s := range strippers {
				if fn.Name.Name == s {
					return false // 跳过定义本身
				}
			}
			ast.Inspect(fn, func(m ast.Node) bool {
				call, ok := m.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				for _, s := range strippers {
					if id.Name == s {
						key := name + ":" + fn.Name.Name
						if seen[key] == nil {
							seen[key] = map[string]bool{}
						}
						seen[key][s] = true
					}
				}
				return true
			})
			return false
		})
	}

	if len(seen) == 0 {
		t.Fatal("一个摘除调用点都没扫到——哨兵失去意义（是不是符号被改名了？）")
	}
	for site, got := range seen {
		var missing []string
		for _, s := range strippers {
			if !got[s] {
				missing = append(missing, s)
			}
		}
		if len(missing) > 0 {
			t.Errorf("边界 %s 只摘了一部分控制信号，漏掉 %v\n"+
				"——「把 agent 降格成一件能力」的边界必须把全部通道都摘掉，"+
				"漏掉的那条会让内层静默劫持调用方的控制流（err 全程 nil）", site, missing)
		}
	}
	t.Logf("已对账 %d 处边界 × %d 条通道", len(seen), len(strippers))
}
