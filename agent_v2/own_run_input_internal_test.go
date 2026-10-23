package agent_v2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// 每个 agent 的 Run 入口都经 ownRunInput 与 adoptHookInput：拿自己那份输入（调用方那条不被本 agent 的钩子、编排的
// channel、成员原地改），没有消息 ID 的补一个，钩子换上的输入同样拷一份、补 ID（共享历史视图按 ID 认同一条原始输入）。
// 新加一个编排时漏了它们不报错，只是那个编排的钩子改到调用方的消息、没有 ID 的输入在转交里记几份——这里按形状查。
func TestOwnRunInput_EveryAgentRunEntryUsesBoth(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "Run" || fn.Body == nil || !isAgentRunSignature(fn) {
				continue
			}
			found++
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok {
						calls[id.Name] = true
					}
				}
				return true
			})
			for _, want := range []string{"ownRunInput", "adoptHookInput"} {
				if !calls[want] {
					t.Errorf("%s: %s.Run 没有经 %s", fset.Position(fn.Pos()), recvName(fn), want)
				}
			}
		}
	}
	if found < 9 {
		t.Fatalf("只找到 %d 个 agent Run 入口，want ≥ 9——判据没认出它们", found)
	}
}

// isAgentRunSignature 认 Agent.Run 的形状：(ctx, session, input *schema.Message, opts ...RunOption)。
func isAgentRunSignature(fn *ast.FuncDecl) bool {
	ps := fn.Type.Params.List
	if len(ps) != 4 {
		return false
	}
	star, ok := ps[2].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Message"
}

func recvName(fn *ast.FuncDecl) string {
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.Ident:
		return t.Name
	}
	return "?"
}
