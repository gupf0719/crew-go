package compose_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestNodeStateAccessor_ScopeOnlyViaScopedOf：nodeStateAccessor 的字面量不许直接填 scope / chain /
// handover——分域只能经 scopedOf 叠上去（它同时把这一层接进分域链、摘掉交接信息）。
//
// 曾经节点级钩子相位的访问器是 `&nodeStateAccessor{cm, key, scope: sc}`：分域链是空的，这个分域被推进
// 的重做代际谁也读不到——钩子里嵌套图的入口否决之后，按 Rerunnable 记下的草稿原样拿回、续跑永不
// 收敛；草稿不记时嵌套图里的提问不换 ID，对 v1 的批准兑付给了 v3（均实测）。
func TestNodeStateAccessor_ScopeOnlyViaScopedOf(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	literals := 0
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(node ast.Node) bool {
			cl, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if id, ok := cl.Type.(*ast.Ident); !ok || id.Name != "nodeStateAccessor" {
				return true
			}
			literals++
			for _, elt := range cl.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					t.Errorf("%s: nodeStateAccessor 字面量用了位置式字段，看不出填了哪几个——写成 key: value", fset.Position(cl.Pos()))
					continue
				}
				if k, ok := kv.Key.(*ast.Ident); ok && (k.Name == "scope" || k.Name == "chain" || k.Name == "handover") {
					t.Errorf("%s: nodeStateAccessor 字面量直接填了 %s——分域经 scopedOf 叠（它把这一层接进分域链），"+
						"交接信息经 HandOver 挂", fset.Position(kv.Pos()), k.Name)
				}
			}
			return true
		})
	}
	if literals == 0 {
		t.Fatal("一个 nodeStateAccessor 字面量都没扫到，扫描器坏了")
	}
}
