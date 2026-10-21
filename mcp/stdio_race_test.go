package mcp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/mcp"
)

// TestMain：MCP_MOCK_MODE 环境变量非空时本测试 binary re-exec 自己作 mock MCP stdio server
// （避免 go run 每次编译开销），否则正常跑测试。
func TestMain(m *testing.M) {
	if mode := os.Getenv("MCP_MOCK_MODE"); mode != "" {
		runMockMCPServer(mode)
		return
	}
	os.Exit(m.Run())
}

// runMockMCPServer 读 stdin JSON-RPC：响应 initialize 与 tools/list（一个工具 t）；tools/call 按 mode 行为
// （hang=永久阻塞让 rpc 挂 / exit=子进程崩溃 / 其他=正常响应）。
func runMockMCPServer(mode string) {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 10*1024*1024)
	w := bufio.NewWriter(os.Stdout)
	for sc.Scan() {
		var req map[string]any
		if json.Unmarshal(sc.Bytes(), &req) != nil {
			continue
		}
		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{}})
			w.Write(b)
			w.WriteByte('\n')
			w.Flush()
		case "notifications/initialized":
			// notify 无响应
		case "tools/list":
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"tools": []any{map[string]any{"name": "t"}}}})
			w.Write(b)
			w.WriteByte('\n')
			w.Flush()
		case "tools/call":
			switch mode {
			case "hang":
				time.Sleep(60 * time.Second) // 阻塞让 rpc 真挂在等响应（不用 select{}——纯 Go 子进程触发 runtime deadlock panic 会提前崩溃）；测试 Close kill 子进程
			case "exit":
				os.Exit(1) // 子进程崩溃（readLoop 退出）
			default:
				b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"content": []any{}}})
				w.Write(b)
				w.WriteByte('\n')
				w.Flush()
			}
		}
	}
}

func newMockStdioClient(t *testing.T, mode string) *mcp.StdioClient {
	t.Helper()
	c, err := mcp.NewStdioClient("mock", os.Args[0], nil, mcp.WithStdioEnv("MCP_MOCK_MODE="+mode))
	if err != nil {
		t.Fatalf("NewStdioClient: %v", err)
	}
	return c
}

// TestStdio_StdinRace_CallToolVsClose 验证 F1 stdin 分裂锁修复——CallTool（rpc 读 stdin，
// writeMu）与 Close（写 stdin=nil，原 mu、修复后 writeMu）并发不 data race。
//
// sanity：revert F1（stdin 写回 mu）后 -race 报 DATA RACE（Close 写 stdin vs rpc 读 stdin）。
func TestStdio_StdinRace_CallToolVsClose(t *testing.T) {
	// 8 iter 足够——race 的 happens-before 检测单次并发即触发，多 iter 仅防调度串行化；
	// 瓶颈是每 iter re-exec 测试 binary 启动子进程（-race binary 启动 ~1s），故不宜多。
	for i := 0; i < 8; i++ {
		c := newMockStdioClient(t, "hang") // tools/call hang 让 rpc 写完 stdin req 后阻塞在等响应
		if err := c.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize iter %d: %v", i, err)
		}
		// CallTool ctx 短超时——rpc 写 stdin req（race 访问点）已在挂起前发生，无需等子进程被 kill
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = c.CallTool(ctx, "t", nil) }() // rpc 写/读 stdin
		go func() { defer wg.Done(); _ = c.Close() }()                    // 写 stdin=nil
		wg.Wait()
		cancel()
	}
}

// TestStdio_ReadLoopDrain_ChildCrash 验证 F2——子进程崩溃时 readLoop drain pending 唤醒
// in-flight CallTool（不永久挂到有人 Close）。
//
// sanity：revert F2（readLoop 不 drain pending）后 CallTool(Background) 永久挂 → 测试超时 FAIL。
func TestStdio_ReadLoopDrain_ChildCrash(t *testing.T) {
	c := newMockStdioClient(t, "exit") // tools/call → 子进程 os.Exit(1)
	ctx := context.Background()
	if err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() {
		_, err := c.CallTool(ctx, "t", nil) // 子进程 exit → readLoop 退出 drain → 唤醒 rpc
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("子进程崩溃应返 error（connection closed）")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("F2 未修：子进程崩溃后 CallTool 永久挂（readLoop 未 drain pending 唤醒 waiter）")
	}
}

// slowClient：Initialize 带 sleep 的 Client，用于 F3 并发测试——放慢 doStart 让并发 Start
// 窗口真正打开（否则 fakeClient 瞬间完成，第一个 Start 极快 set started=true，Go scheduler
// 串行化掉 thunder-herd，check/set 分离 bug 不显现，见 CLAUDE.md "并发场景需 WaitGroup 屏障
// + 短超时强制真正并发"）。
type slowClient struct {
	initCount atomic.Int32
}

func (s *slowClient) Name() string { return "slow" }
func (s *slowClient) Initialize(_ context.Context) error {
	s.initCount.Add(1)
	time.Sleep(20 * time.Millisecond) // 停留让并发 Start 都到达 check 门
	return nil
}
func (s *slowClient) ListTools(_ context.Context) ([]mcp.ToolSpec, error) {
	return []mcp.ToolSpec{{Name: "t1"}}, nil
}
func (s *slowClient) CallTool(_ context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
	return &mcp.CallResult{}, nil
}
func (s *slowClient) Close() error { return nil }

// TestHub_Start_ConcurrentIdempotent 验证 F3——并发 Start 幂等只执行一次（不重复 Initialize/
// ListTools/register）。slowClient.Initialize sleep 让并发窗口真正打开。
//
// sanity：revert F3（全程持 mu → check 后立即释放）后并发 Start 都过 started==false 门 →
// slowClient 并发 sleep → initCount>1。
func TestHub_Start_ConcurrentIdempotent(t *testing.T) {
	sc := &slowClient{}
	h := mcp.NewHub(mcp.WithClient(sc))
	const N = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _ = h.Start(context.Background()) }()
	}
	close(start)
	wg.Wait()
	if got := sc.initCount.Load(); got != 1 {
		t.Fatalf("并发 Start 应幂等只执行一次（initCount==1），got %d（重复 ListTools/register）", got)
	}
}
