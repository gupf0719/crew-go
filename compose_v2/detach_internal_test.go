package compose_v2

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// executionCtxKeys 与 isExecutionKey 是同一份清单：DetachExecution 剥掉的「当前这次执行」的状态。
var executionCtxKeys = []any{
	sessionIDKey{}, runTaskIDKey{}, currentTaskIDKey{}, freshRunCtxKey{}, resumeCtxKey{}, answerLedgerKey{},
	graphNSKey{}, nodeIDKey{}, nodeVisitKey{}, nodeInstanceIDKey{}, nodeInterruptCounterKey{},
	toolCallIdxKey{}, currentToolCallIDKey{}, inToolCallKey{}, withinNodeScopeKey{},
	nodeStateAccessorKey{}, subCallCounterKey{}, subCallScopeKey{}, abnormalNodeExitKey{},
	componentKindKey{}, eventSinkKey{}, sendSinkKey{}, toolCategoryLookupKey{}, outerRerunGensKey{}, outerRegensKey{},
	threadIDKey{}, softCancelCtxKey{}, detachedKey{}, detachedFromKey{}, runAliveKey{}, signalScopeKey{}, taskRecipientKeyCtxKey{}, hookCallKey{}, exitGraceKey{}, streamRecordKey{},
}

// configurableExecutionCtxKeys 是执行状态里调用方可配的那一样（脱离时剥；采纳派生的 ctx 时听钩子的），
// 其余执行状态是框架的身份与位置（采纳时一律取框架的，会话 ID 也在其中）。
var configurableExecutionCtxKeys = []any{softCancelCtxKey{}}

// environmentCtxKeys 是 DetachExecution 留下的 key：调用方给整个请求配的环境（isConfigurableKey）。
var environmentCtxKeys = []any{
	checkpointerKey{}, orchHooksKey{}, ownerKey{},
	durabilityCtxKey{}, staleGuardCtxKey{}, emitCpEventCtxKey{},
}

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

// TestDetach_EveryCtxKeyIsClassified：包里每个 ctx key 都必须归类——要么是执行状态（DetachExecution
// 剥掉），要么是调用方配的环境（留下）。新加 key 时两边都没登记就红：剥不剥是一个要想清楚的决定，
// 不能默认（漏剥的执行状态会把独立运行跟当前执行缠在一起，误剥的环境会让独立运行丢了 checkpointer）。
// 另一维同样钉住：钩子能不能改它（isConfigurableKey）——环境与那两样可配的执行状态能，框架的身份与
// 位置不能。
func TestDetach_EveryCtxKeyIsClassified(t *testing.T) {
	used := usedCtxKeyTypes(t, ".")
	if len(used) < 10 {
		t.Fatalf("扫到的 ctx key 太少（%d 个），扫描器坏了", len(used))
	}
	class := map[string]string{}
	configurable := map[string]bool{}
	for _, k := range configurableExecutionCtxKeys {
		configurable[reflect.TypeOf(k).Name()] = true
	}
	for _, k := range executionCtxKeys {
		n := reflect.TypeOf(k).Name()
		if !isExecutionKey(k) {
			t.Errorf("%s 在执行状态清单里，isExecutionKey 却不剥它", n)
		}
		if got := isConfigurableKey(k); got != configurable[n] {
			t.Errorf("%s：isConfigurableKey=%v，清单里登记的是 %v（可配的执行状态只有 configurableExecutionCtxKeys 那几样）", n, got, configurable[n])
		}
		class[n] = "execution"
	}
	for _, k := range environmentCtxKeys {
		n := reflect.TypeOf(k).Name()
		if isExecutionKey(k) {
			t.Errorf("%s 在环境清单里，isExecutionKey 却剥它", n)
		}
		if !isConfigurableKey(k) {
			t.Errorf("%s 在环境清单里，isConfigurableKey 却不认它（采纳外来 ctx 时会取到别的请求的）", n)
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
		t.Errorf("这些 ctx key 没归类（执行状态登记进 isExecutionKey，环境登记进 isConfigurableKey，再登记进本文件对应的清单）：%v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("这些登记了的 key 包里已经不用了（删掉登记）：%v", stale)
	}
}

// singleNodeGraph 编译一张只有一个节点 n 的图，节点体跑 body。
func singleNodeGraph(t *testing.T, body NodeFunc) *Graph {
	t.Helper()
	b := NewBuilder()
	b.AddNode("n", body)
	b.AddEdge(StartNode, "n")
	b.AddEdge("n", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestExecutingTasks_RegisteredWhileRunning_ReleasedOnEveryExit：不在节点执行范围里、配了 sid 的 Run 执行期间登记
// 「这个任务有执行在跑」（DetachedFrom 按任务判「仍在执行」靠它），不论怎么退出——跑完、报错、挂起、节点 panic——
// 之后都注销。漏注销的话这个任务从此一直算在执行：挂起之后用存下的 ctx 脱离、续跑它会被拒。
func TestExecutingTasks_RegisteredWhileRunning_ReleasedOnEveryExit(t *testing.T) {
	cases := map[string]struct {
		body    func(ctx context.Context) ([]ChannelWrite, error)
		wantErr func(err error) bool
	}{
		"跑完": {
			body:    func(context.Context) ([]ChannelWrite, error) { return nil, nil },
			wantErr: func(err error) bool { return err == nil },
		},
		"报错": {
			body:    func(context.Context) ([]ChannelWrite, error) { return nil, errors.New("boom") },
			wantErr: func(err error) bool { return err != nil && !errors.Is(err, ErrRunAborted) },
		},
		"挂起": {
			body: func(ctx context.Context) ([]ChannelWrite, error) {
				_, err := Ask(ctx, &AskQuery{Question: "要人确认"})
				return nil, err
			},
			wantErr: func(err error) bool { _, ok := IsInterrupt(err); return ok },
		},
		"节点 panic": {
			body:    func(context.Context) ([]ChannelWrite, error) { panic("boom") },
			wantErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "boom") },
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sid := "executing-" + name
			var during bool
			g := singleNodeGraph(t, func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
				during = taskExecuting(sid)
				return c.body(ctx)
			})
			ctx := WithCheckpointerCtx(context.Background(), NewInMemoryCheckpointer())
			err := Run(ctx, g, NewChannelManager(), WithSessionID(sid))
			if !c.wantErr(err) {
				t.Fatalf("fixture 没搭对：退出方式不是「%s」，实际 err=%v", name, err)
			}
			if !during {
				t.Error("执行期间这个任务应登记为在跑")
			}
			if taskExecuting(sid) {
				t.Errorf("「%s」之后应注销，实际仍登记为在跑", name)
			}
		})
	}
}

// TestExecutingTasks_NodeGoexit_Released：节点里 runtime.Goexit（比如日志库的 Fatal 钩子）。单节点超步内联跑节点，
// Run 所在的 goroutine 直接终止、Run 不返回，defer 照跑——登记照样注销。注销不在 defer 里的话，这个任务从此一直算在执行。
func TestExecutingTasks_NodeGoexit_Released(t *testing.T) {
	sid := "executing-goexit"
	var during, returned bool
	var runErr error
	g := singleNodeGraph(t, func(context.Context, Channels) ([]ChannelWrite, error) {
		during = taskExecuting(sid)
		runtime.Goexit()
		return nil, nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runErr = Run(context.Background(), g, NewChannelManager(), WithSessionID(sid))
		returned = true
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("跑节点的 goroutine 没结束")
	}
	if returned {
		t.Logf("Run 返回了（节点不是内联跑的）：%v", runErr)
	}
	if !during {
		t.Error("执行期间这个任务应登记为在跑")
	}
	if taskExecuting(sid) {
		t.Error("节点 Goexit 之后应注销，实际仍登记为在跑")
	}
}

// TestExecutingTasks_ConcurrentRunsSameSID_BalancedCount：同一个 sid 同时有几次执行（违反契约，但计数不能因此乱）：
// 都在跑时算在跑，全部结束之后不算。
func TestExecutingTasks_ConcurrentRunsSameSID_BalancedCount(t *testing.T) {
	const n = 8
	sid := "executing-concurrent"
	var entered sync.WaitGroup
	entered.Add(n)
	release := make(chan struct{})
	var sawRunning atomic.Int32
	g := singleNodeGraph(t, func(context.Context, Channels) ([]ChannelWrite, error) {
		entered.Done()
		<-release
		if taskExecuting(sid) {
			sawRunning.Add(1)
		}
		return nil, nil
	})
	errs := make([]error, n)
	var done sync.WaitGroup
	for i := range n {
		done.Add(1)
		go func() {
			defer done.Done()
			errs[i] = Run(context.Background(), g, NewChannelManager(), WithSessionID(sid))
		}()
	}
	allIn := make(chan struct{})
	go func() { entered.Wait(); close(allIn) }()
	select {
	case <-allIn:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("fixture 没搭对：几次执行没能同时进到节点里")
	}
	if !taskExecuting(sid) {
		t.Error("几次执行都在跑时应登记为在跑")
	}
	close(release)
	done.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 次执行：%v", i, err)
		}
	}
	if got := sawRunning.Load(); got != n {
		t.Errorf("每次执行在自己的节点里都应看到这个任务在跑，实际 %d/%d", got, n)
	}
	if taskExecuting(sid) {
		t.Error("全部结束之后应注销，实际仍登记为在跑")
	}
}
