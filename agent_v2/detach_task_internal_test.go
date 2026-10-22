package agent_v2

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// taskExecutionCtxKeys 是 agent_v2 的执行状态：每个都实现 compose_v2.ExecutionStateKey（DetachExecution /
// DetachTask 剥掉、CallHook 采纳时取框架那份）。
var taskExecutionCtxKeys = []any{
	sessionKey{}, taskScopeKey{}, callApprovalKey{}, finalRoundKey{}, compactionMemoKey{}, toolsUsedKey{}, foAttemptsKey{}, revealedChKey{},
	revealTurnKey{}, repeatTurnKey{},
	runOverridesCtxKey{}, offloadDedupKey{}, subAgentStoreScopeKey{},
	historyCtxKey{}, historySlotCtxKey{}, historyReceiptCtxKey{},
	handoffHopsCtxKey{}, handoffDestKey{}, handoffHistoryModeKey{}, routeDestKey{},
	endErrCtxKey{}, resumePhaseCtxKey{}, toolResultPosKey{}, toolBatchKey{}, nodeGrantsKey{},
	backgroundScopeKey{},
	inboxCtxKey{},
	interjectionLedgerCtxKey{},
	callerHistoryKey{},
}

// taskEnvironmentCtxKeys 是 agent_v2 的环境：每个都实现 compose_v2.EnvironmentKey（DetachTask 留下；采纳
// 钩子返回的 ctx 时派生的听钩子的、外来的取框架这次的）。
var taskEnvironmentCtxKeys = []any{historyCfgCtxKey{}, orchHooksCtxKey{}}

// usedCtxKeyTypes 扫 dir 下非测试 .go 文件，收集当 ctx key 用的类型名：`context.WithValue(_, T{}, _)`
// 的 T 与 `x.Value(T{})` 的 T。
func usedCtxKeyTypes(t *testing.T, dir string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	keyName := func(e ast.Expr) string {
		if cl, ok := e.(*ast.CompositeLit); ok && len(cl.Elts) == 0 {
			if id, ok := cl.Type.(*ast.Ident); ok {
				return id.Name
			}
		}
		return ""
	}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, dir+"/"+n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch {
			case sel.Sel.Name == "WithValue" && len(call.Args) == 3:
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" {
					if k := keyName(call.Args[1]); k != "" {
						used[k] = true
					}
				}
			case sel.Sel.Name == "Value" && len(call.Args) == 1:
				if k := keyName(call.Args[0]); k != "" {
					used[k] = true
				}
			}
			return true
		})
	}
	return used
}

// TestDetachTask_EveryCtxKeyIsClassified：agent_v2 里每个 ctx key 都必须归类——要么是执行状态（DetachTask
// 剥掉），要么是调用方配的环境（留下）。compose_v2 那一半由它自己的同名测试管。新加 key 时两边都没登记就红：剥不剥是一个要想清楚的决定，
// 不能默认（漏剥的执行状态会把独立运行跟当前执行缠在一起，误剥的环境会让独立运行丢了 checkpointer）。
func TestDetachTask_EveryCtxKeyIsClassified(t *testing.T) {
	used := usedCtxKeyTypes(t, ".")
	if len(used) < 5 {
		t.Fatalf("扫到的 ctx key 太少（%d 个），扫描器坏了", len(used))
	}
	class := map[string]string{}
	for _, k := range taskExecutionCtxKeys {
		n := reflect.TypeOf(k).Name()
		if !isExecutionStateKey(k) || isEnvironmentKey(k) {
			t.Errorf("%s 在执行状态清单里，应只实现 compose_v2.ExecutionStateKey", n)
		}
		class[n] = "execution"
	}
	for _, k := range taskEnvironmentCtxKeys {
		n := reflect.TypeOf(k).Name()
		if isExecutionStateKey(k) || !isEnvironmentKey(k) {
			t.Errorf("%s 在环境清单里，应只实现 compose_v2.EnvironmentKey（实现执行状态会被剥掉；不实现的话采纳外来 ctx 时取到别的请求的）", n)
		}
		if _, dup := class[n]; dup {
			t.Errorf("%s 两边都登记了", n)
		}
		class[n] = "environment"
	}
	var missing, stale []string
	for n := range used {
		if _, ok := class[n]; !ok {
			missing = append(missing, n)
		}
	}
	for n := range class {
		if !used[n] {
			stale = append(stale, n)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("这些 ctx key 没归类（实现 compose_v2.ExecutionStateKey 或 compose_v2.EnvironmentKey，二选一，再登记进本文件对应的清单）：%v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("这些登记了的 key 包里已经不用了（删掉登记）：%v", stale)
	}
}

func isExecutionStateKey(k any) bool {
	_, ok := k.(compose_v2.ExecutionStateKey)
	return ok
}

func isEnvironmentKey(k any) bool {
	_, ok := k.(compose_v2.EnvironmentKey)
	return ok
}

// TestExecutionStateKey_EngineDetachAndAdoptRecognizeAgentV2Keys：agent_v2 的 key 由引擎那层的
// compose_v2.DetachExecution 与 compose_v2.CallHook 的采纳认——两处都只看 key 类型上的标记。
//   - 执行状态：脱离时剥掉；采纳时不论钩子返回的 ctx 是哪来的都取框架的；
//   - 环境：脱离时留下；采纳派生的 ctx 时听钩子的，外来的取框架这次的。
//
// 曾经 agent_v2 另有一份清单与一个采纳函数，引擎的采纳点（OnGraphStart / OnNodeStart）不认得
// agent_v2 的 key，钩子在那两处返回别处缓存的 ctx 时会话历史与转交静默丢；环境曾经一律跟钩子返回的
// ctx，外来 ctx 上别的请求的编排钩子照收，流式片段送到别人的连接上（均实测）。
func TestExecutionStateKey_EngineDetachAndAdoptRecognizeAgentV2Keys(t *testing.T) {
	type tag string
	push := func(ctx context.Context, exec, env tag) context.Context {
		for _, k := range taskExecutionCtxKeys {
			ctx = context.WithValue(ctx, k, exec)
		}
		for _, k := range taskEnvironmentCtxKeys {
			ctx = context.WithValue(ctx, k, env)
		}
		return ctx
	}
	base := push(context.Background(), "from", "from-env")

	detached := compose_v2.DetachExecution(push(context.Background(), "hook", "hook-env"))
	for _, k := range taskExecutionCtxKeys {
		if v := detached.Value(k); v != nil {
			t.Errorf("DetachExecution 之后还看得见执行状态 %T：%v", k, v)
		}
	}
	for _, k := range taskEnvironmentCtxKeys {
		if v := detached.Value(k); v != tag("hook-env") {
			t.Errorf("DetachExecution 不该剥环境 %T，实际 %v", k, v)
		}
	}

	for _, c := range []struct {
		name    string
		hook    func(context.Context) context.Context // 由交给钩子的 ctx 得出钩子交回的那个
		wantEnv tag
	}{
		{"派生的", func(in context.Context) context.Context { return push(in, "hook", "hook-env") }, "hook-env"},
		{"外来的", func(context.Context) context.Context { return push(context.Background(), "hook", "hook-env") }, "from-env"},
	} {
		adopted, _ := compose_v2.CallHook(base, func(in context.Context) (context.Context, error) { return c.hook(in), nil })
		for _, k := range taskExecutionCtxKeys {
			if v := adopted.Value(k); v != tag("from") {
				t.Errorf("%s：执行状态 %T 应取框架那份，实际 %v", c.name, k, v)
			}
		}
		for _, k := range taskEnvironmentCtxKeys {
			if v := adopted.Value(k); v != c.wantEnv {
				t.Errorf("%s：环境 %T 应为 %v，实际 %v", c.name, k, c.wantEnv, v)
			}
		}
	}
}
