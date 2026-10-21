package compose_v2

// 机械对账：`frontierResult.FirstErr` 一旦被置，同一组语句里必须 append `FailedBranches`。
//
// # 这条不变量守着什么
//
// `commitSuperstepPartial` 无条件把 `narrowTo` 交给 `updateFrontierLocked`，而后者对
// **空**入参的语义是「清空 frontier」不是「保持」。调用点传的是 `res.FailedBranches`，
// 所以一旦出现「置了 FirstErr 但没有任何 FailedBranches」的路径，后果是 frontier 被
// **抹成空**——存档说「没东西要重跑」，整个超步静默丢失，而 `err` 那侧照常报错，
// 所以从外面完全看不出来。
//
// 旧结构里有一句 `if narrowTo != nil` 兜着（因为那时全员成功路径也走同一个方法、传 nil）。
// 拆成两个方法后那句没有意义了，于是**兜底没了、不变量却没人写下来**——两个独立审计
// 各自撞上这一点。这条测试就是把它写下来。
//
// # 判据
//
// 扫 `executeFrontier` 的函数体，找每一处给 `FirstErr` 赋值的地方，要求它所在的那个
// **语句块**里也有一处 append 到 `FailedBranches`。将来加「不归属任何成员的超步级失败」
// 时会当场红——那时该做的是给它一个归属，或者给 `commitSuperstepPartial` 想清楚
// 空 narrowTo 的语义，而不是让它悄悄清空 frontier。

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestFrontierResult_FirstErrAlwaysPairsWithFailedBranches(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "graph_loop.go", nil, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeFrontier" {
			fn = fd
			break
		}
	}
	if fn == nil {
		t.Fatal("fixture 没搭对：找不到 executeFrontier（改名了？）——下面的断言恒真")
	}

	// 两种形态都要认：
	//   (a) 结构体字面量 `frontierResult{FailedBranches: ..., FirstErr: ...}`——同一个
	//       字面量里配对，天然成对；
	//   (b) 赋值 `out.FirstErr = ...`——配对的 append 在**祖先块**里（FirstErr 那句
	//       常常裹在 `if out.FirstErr == nil {}` 内层块），所以按位置包含找全部祖先。
	type errSite struct {
		line   int
		paired bool
	}
	var sites []errSite

	// 先收集所有块及其位置区间，和每个块里有没有 FailedBranches 的赋值/append
	type blockInfo struct {
		start, end token.Pos
		hasFailed  bool
	}
	var blocks []blockInfo
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		b, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		bi := blockInfo{start: b.Pos(), end: b.End()}
		for _, st := range b.List {
			if as, isAssign := st.(*ast.AssignStmt); isAssign {
				for _, lhs := range as.Lhs {
					if sel, isSel := lhs.(*ast.SelectorExpr); isSel && sel.Sel.Name == "FailedBranches" {
						bi.hasFailed = true
					}
				}
			}
		}
		blocks = append(blocks, bi)
		return true
	})

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CompositeLit:
			var hasErr, hasFailed bool
			for _, el := range node.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				id, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				switch id.Name {
				case "FirstErr":
					hasErr = true
				case "FailedBranches":
					hasFailed = true
				}
			}
			if hasErr {
				sites = append(sites, errSite{fset.Position(node.Pos()).Line, hasFailed})
			}
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "FirstErr" {
					continue
				}
				paired := false
				for _, b := range blocks {
					if b.start <= node.Pos() && node.End() <= b.end && b.hasFailed {
						paired = true
						break
					}
				}
				sites = append(sites, errSite{fset.Position(node.Pos()).Line, paired})
			}
		}
		return true
	})

	if len(sites) == 0 {
		t.Fatal("fixture 没搭对：一处 FirstErr 赋值都没扫到——下面的断言恒真")
	}
	for _, s := range sites {
		if s.paired {
			continue
		}
		t.Errorf("graph_loop.go:%d 置了 FirstErr，但同一处（或它的祖先块）里没有给 "+
			"FailedBranches 记上这个失败成员。\n"+
			"  → commitSuperstepPartial 把 res.FailedBranches 无条件交给 "+
			"updateFrontierLocked，而后者对空入参是**清空** frontier 不是保持。\n"+
			"  → 「置了错但没有失败成员」这条路会让整个超步的进度静默消失（存档里 "+
			"frontier 为空 = 没东西要重跑），而 err 那侧照常报错，外面看不出来。\n"+
			"  → 要么给这个失败一个成员归属，要么先想清楚 commitSuperstepPartial "+
			"在 narrowTo 为空时该做什么。", s.line)
	}
	t.Logf("扫到 %d 处 FirstErr 置位，全部配对", len(sites))
}
