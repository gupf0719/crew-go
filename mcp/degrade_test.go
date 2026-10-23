package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"crew-go/mcp"
	"crew-go/tool"
)

// --- 测试替身 ---

// memClient 是测试用的内存 Client：工具清单固定；CallTool 交给 onCall（nil 时返回 "ok:<原始名>"），
// 并记下收到的每次调用，供「这次调用有没有真发给 server」的断言用。
type memClient struct {
	name    string
	tools   []mcp.ToolSpec
	initErr error
	onCall  func(ctx context.Context, name string, args map[string]any) (*mcp.CallResult, error)
	onClose func() // Close 时调用（在计数之后）：模拟关连接要等一会儿、或杀掉 server 进程

	mu       sync.Mutex
	received []string
	closed   atomic.Int32
}

func (c *memClient) Name() string                     { return c.name }
func (c *memClient) Initialize(context.Context) error { return c.initErr }
func (c *memClient) ListTools(context.Context) ([]mcp.ToolSpec, error) {
	return c.tools, nil
}
func (c *memClient) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallResult, error) {
	c.mu.Lock()
	c.received = append(c.received, name)
	c.mu.Unlock()
	if c.onCall != nil {
		return c.onCall(ctx, name, args)
	}
	return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok:" + name}}}, nil
}
func (c *memClient) Close() error {
	c.closed.Add(1)
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func (c *memClient) calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.received...)
}

// seqFactory 是按次数造 Client 的工厂：第 n 次（从 1 起）建之前先等 gates[n]（有的话），再交出 build(n)。
type seqFactory struct {
	mu    sync.Mutex
	n     int
	build func(n int) (mcp.Client, error)
	gates map[int]chan struct{}
}

func (f *seqFactory) New(ctx context.Context) (mcp.Client, error) {
	f.mu.Lock()
	f.n++
	n := f.n
	gate := f.gates[n]
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.build(n)
}

func (f *seqFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

var errDown = errors.New("connection refused")

func specs(names ...string) []mcp.ToolSpec {
	out := make([]mcp.ToolSpec, 0, len(names))
	for _, n := range names {
		out = append(out, mcp.ToolSpec{
			Name:        n,
			Description: "tool " + n,
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
		})
	}
	return out
}

// toolsJSON 是发给模型的那份工具定义（名字、描述、schema、顺序）序列化后的样子——「工具集不变」比的就是它。
func toolsJSON(t *testing.T, h *mcp.Hub) string {
	t.Helper()
	raw, err := json.Marshal(h.Toolkit().ToolInfos())
	if err != nil {
		t.Fatalf("marshal tool infos: %v", err)
	}
	return string(raw)
}

func names(h *mcp.Hub) []string {
	out := []string{}
	for _, ti := range h.Toolkit().ToolInfos() {
		out = append(out, ti.Name)
	}
	return out
}

func statusOf(t *testing.T, h *mcp.Hub, server string) mcp.ServerStatus {
	t.Helper()
	for _, st := range h.Status() {
		if st.Name == server {
			return st
		}
	}
	t.Fatalf("没有 server %q 的状态：%+v", server, h.Status())
	return mcp.ServerStatus{}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等不到：%s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func call(t *testing.T, h *mcp.Hub, name string) *tool.Result {
	t.Helper()
	res, err := h.Toolkit().Call(context.Background(), name, map[string]any{"q": "x"})
	if err != nil {
		t.Fatalf("%s：工具调用失败只该是失败结果、不该是 error（error 会让 BasicAgent 整轮 Run 失败）：%v", name, err)
	}
	if res == nil {
		t.Fatalf("%s：结果为 nil", name)
	}
	return res
}

func wantFailure(t *testing.T, res *tool.Result, code, substr string) {
	t.Helper()
	if res.Success || res.Error == nil {
		t.Fatalf("该失败，实际：%+v", res)
	}
	if res.Error.Code != code {
		t.Errorf("失败代码 = %q，要 %q（消息：%s）", res.Error.Code, code, res.Error.Message)
	}
	if !strings.Contains(res.Error.Message, substr) {
		t.Errorf("失败消息该含 %q，实际：%s", substr, res.Error.Message)
	}
}

func fastBackoff() mcp.HubOption {
	return mcp.WithReconnectBackoff(5*time.Millisecond, 20*time.Millisecond)
}

// --- 按 server 降级：Start ---

// 连不上的 server 按快照注册工具：工具集跟它在线时一样，调用返回「该能力暂时不可用」、不发给 server，
// Start 不失败。默认（不开 WithPartialStart）照旧全有或全无。
func TestHub_PartialStart_RegistersSnapshotToolsForOfflineServer(t *testing.T) {
	up := &memClient{name: "up", tools: specs("ping")}
	newHub := func(extra ...mcp.HubOption) *mcp.Hub {
		f := &seqFactory{build: func(int) (mcp.Client, error) { return nil, errDown }}
		opts := append([]mcp.HubOption{
			mcp.WithClient(up),
			mcp.WithClientFactory("down", f.New, mcp.WithToolSnapshot(specs("lookup", "write"))),
			fastBackoff(),
		}, extra...)
		return mcp.NewHub(opts...)
	}

	strict := newHub()
	if err := strict.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("默认全有或全无：down 连不上 Start 该失败并带上原因，实际 %v", err)
	}

	h := newHub(mcp.WithPartialStart())
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("WithPartialStart 下个别 server 连不上不该让 Start 失败：%v", err)
	}
	defer h.Close()

	if got, want := names(h), []string{"mcp_up_ping", "mcp_down_lookup", "mcp_down_write"}; !slices.Equal(got, want) {
		t.Fatalf("工具集 = %v，要 %v（离线的 down 按快照注册，顺序按 server 注册顺序）", got, want)
	}
	wantFailure(t, call(t, h, "mcp_down_lookup"), mcp.ErrCodeServerUnavailable, "该能力暂时不可用（down offline）")
	if res := call(t, h, "mcp_up_ping"); !res.Success || res.Output != "ok:ping" {
		t.Errorf("在线的 up 照常可用，实际 %+v", res)
	}

	st := statusOf(t, h, "down")
	if st.State != mcp.ServerOffline || st.Err == nil || !st.FromSnapshot {
		t.Errorf("down 状态该是离线、带原因、来自快照，实际 %+v", st)
	}
	if !slices.Equal(st.Registered, []string{"lookup", "write"}) || st.Tools != nil {
		t.Errorf("down 注册了快照里的两个工具、从没连上过（Tools 为 nil），实际 %+v", st)
	}
	if st := statusOf(t, h, "up"); st.State != mcp.ServerOnline || len(st.Tools) != 1 {
		t.Errorf("up 该在线并报出清单，实际 %+v", st)
	}
}

// 没有快照的离线 server 本会话不注册工具（不知道它有什么），Start 照样成功。
func TestHub_PartialStart_NoSnapshotRegistersNothing(t *testing.T) {
	f := &seqFactory{build: func(int) (mcp.Client, error) { return nil, errDown }}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(),
		mcp.WithClient(&memClient{name: "up", tools: specs("ping")}),
		mcp.WithClientFactory("down", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	if got := names(h); !slices.Equal(got, []string{"mcp_up_ping"}) {
		t.Errorf("没快照的离线 server 不注册工具，实际 %v", got)
	}
	if st := statusOf(t, h, "down"); st.State != mcp.ServerOffline || st.FromSnapshot || st.Registered != nil {
		t.Errorf("down 状态不对：%+v", st)
	}
}

// 调用方 ctx 结束（取消 / 到期）时 Start 失败，哪怕开了 WithPartialStart——那是调用方放弃启动，不是 server 连不上。
func TestHub_PartialStart_CallerCtxDoneFailsStart(t *testing.T) {
	f := &seqFactory{
		build: func(int) (mcp.Client, error) { return &memClient{name: "slow", tools: specs("a")}, nil },
		gates: map[int]chan struct{}{1: make(chan struct{})}, // 永远不放行：只有 ctx 能解开
	}
	h := mcp.NewHub(mcp.WithPartialStart(), mcp.WithClientFactory("slow", f.New))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := h.Start(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("调用方 ctx 到期，Start 该失败并带上 ctx 错，实际 %v", err)
	}
	if h.Status() != nil || len(names(h)) != 0 {
		t.Errorf("Start 失败不该留下工具或状态")
	}
}

// 连接尝试受 WithConnectTimeout 约束：一个卡住的 server 只让 Start 等那么久，然后按离线处理。
func TestHub_PartialStart_ConnectTimeoutBoundsSlowServer(t *testing.T) {
	f := &seqFactory{
		build: func(int) (mcp.Client, error) { return &memClient{name: "slow"}, nil },
		gates: map[int]chan struct{}{1: make(chan struct{})},
	}
	h := mcp.NewHub(mcp.WithPartialStart(), mcp.WithConnectTimeout(30*time.Millisecond), fastBackoff(),
		mcp.WithClientFactory("slow", f.New, mcp.WithToolSnapshot(specs("a"))))
	begin := time.Now()
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	if d := time.Since(begin); d > 2*time.Second {
		t.Errorf("Start 等了 %s，没被 WithConnectTimeout 约束", d)
	}
	if st := statusOf(t, h, "slow"); st.State != mcp.ServerOffline || !errors.Is(st.Err, context.DeadlineExceeded) {
		t.Errorf("卡住的 server 该按超时离线，实际 %+v", st)
	}
}

// --- 重连：工具集不变 ---

// Start 时离线的 server 后台重连上之后，按快照注册的工具可以正常调用；server 现在报的清单跟快照不同
// （多了一个、改了一个的描述）也不改工具集——发给模型的工具定义逐字节不变。
func TestHub_ReconnectAfterStartFailure_KeepsToolset(t *testing.T) {
	live := specs("lookup", "write", "extra")
	live[1].Description = "write v2" // 定义变了
	var srv *memClient
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n <= 2 {
			return nil, errDown
		}
		srv = &memClient{name: "down", tools: live}
		return srv, nil
	}}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(),
		mcp.WithClientFactory("down", f.New, mcp.WithToolSnapshot(specs("lookup", "write"))))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	before := toolsJSON(t, h)

	waitFor(t, "down 重连上", func() bool { return statusOf(t, h, "down").State == mcp.ServerOnline })
	if res := call(t, h, "mcp_down_lookup"); !res.Success || res.Output != "ok:lookup" {
		t.Fatalf("重连后快照注册的工具该能调通，实际 %+v", res)
	}
	if got := srv.calls(); !slices.Equal(got, []string{"lookup"}) {
		t.Errorf("server 该收到一次 lookup（按原始名），实际 %v", got)
	}
	if after := toolsJSON(t, h); after != before {
		t.Errorf("重连后工具集变了：\n前 %s\n后 %s", before, after)
	}
	st := statusOf(t, h, "down")
	if len(st.Tools) != 3 || st.Tools[2].Name != "extra" || !slices.Equal(st.Registered, []string{"lookup", "write"}) {
		t.Errorf("Status.Tools 该是 server 现在的清单（存成下次的快照），Registered 仍是本会话的，实际 %+v", st)
	}
	if f.count() != 3 {
		t.Errorf("工厂该被调 3 次（两次失败 + 一次成功），实际 %d", f.count())
	}
}

// 重连后 server 上已经没有的工具：调用返回「已经没有」，不发给 server。
func TestHub_ToolRemovedAfterReconnect_ReturnsNotFound(t *testing.T) {
	srv := &memClient{name: "down", tools: specs("a")}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return nil, errDown
		}
		return srv, nil
	}}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(),
		mcp.WithClientFactory("down", f.New, mcp.WithToolSnapshot(specs("a", "b"))))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	waitFor(t, "down 重连上", func() bool { return statusOf(t, h, "down").State == mcp.ServerOnline })
	wantFailure(t, call(t, h, "mcp_down_b"), tool.ErrCodeNotFound, "已经没有工具")
	if got := srv.calls(); len(got) != 0 {
		t.Errorf("已经没有的工具不该发给 server，实际收到 %v", got)
	}
}

// --- 中途掉线 ---

// 一次调用撞上连接错误：返回「该能力暂时不可用」（不是 error），server 标成掉线；掉线期间的调用直接失败、
// 不发给任何连接；后台新建实例重连上后恢复，旧实例被关掉。工具集全程不变。
func TestHub_MidSessionDrop_FastFailsThenReconnects(t *testing.T) {
	var broken atomic.Bool
	first := &memClient{name: "dev", tools: specs("run"), onCall: func(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
		if broken.Load() {
			return nil, errDown
		}
		return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok:" + name}}}, nil
	}}
	second := &memClient{name: "dev", tools: specs("run")}
	release := make(chan struct{})
	f := &seqFactory{
		build: func(n int) (mcp.Client, error) {
			if n == 1 {
				return first, nil
			}
			return second, nil
		},
		gates: map[int]chan struct{}{2: release}, // 重连那一次先卡住，留出「掉线期间」的窗口
	}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("dev", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	before := toolsJSON(t, h)

	if res := call(t, h, "mcp_dev_run"); !res.Success {
		t.Fatalf("第一次调用该成功：%+v", res)
	}
	broken.Store(true)
	wantFailure(t, call(t, h, "mcp_dev_run"), mcp.ErrCodeServerUnavailable, "该能力暂时不可用（dev 连接出错）")
	if st := statusOf(t, h, "dev"); st.State != mcp.ServerOffline || !errors.Is(st.Err, errDown) {
		t.Fatalf("撞上连接错误后该标成掉线并记下原因，实际 %+v", st)
	}

	wantFailure(t, call(t, h, "mcp_dev_run"), mcp.ErrCodeServerUnavailable, "该能力暂时不可用（dev offline）")
	if got := len(first.calls()); got != 2 {
		t.Errorf("掉线期间的调用不该发给旧连接：旧连接收到 %d 次（要 2）", got)
	}
	waitFor(t, "旧连接被关掉", func() bool { return first.closed.Load() == 1 })

	close(release)
	waitFor(t, "dev 重连上", func() bool { return statusOf(t, h, "dev").State == mcp.ServerOnline })
	if res := call(t, h, "mcp_dev_run"); !res.Success || res.Output != "ok:run" {
		t.Fatalf("重连后该恢复：%+v", res)
	}
	if got := second.calls(); len(got) != 1 {
		t.Errorf("重连后的调用该发给新连接，新连接收到 %v", got)
	}
	if after := toolsJSON(t, h); after != before {
		t.Errorf("掉线、重连之后工具集变了")
	}
}

// WithClient 给的固定实例：连接出错同样是失败结果而不是 error；它没法新建连接，所以不标掉线，下一次调用照常发给它。
//
// 这条替换了原来的 TestRemoteToolCallTransportErrorPropagates（「传输错误原样返回 error」）：BasicAgent 把工具
// 返回的 error 当成整轮失败，一个 server 出错就拖垮整个 Run，所以契约改成了交给模型。
func TestHub_FixedClient_TransportErrorIsFailure(t *testing.T) {
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
		return nil, errors.New("conn reset")
	}}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_srv_t"), mcp.ErrCodeServerUnavailable, "连接出错")
	call(t, h, "mcp_srv_t")
	if got := len(fc.calls()); got != 2 {
		t.Errorf("固定实例不标掉线，两次调用都该发给它，实际 %d 次", got)
	}
	if st := statusOf(t, h, "srv"); st.State != mcp.ServerOnline {
		t.Errorf("固定实例状态该仍是在线，实际 %+v", st)
	}
}

// 吞点哨兵：调用方 ctx 结束（取消）时必须返回 error、不能转成失败结果——那是控制流，转成工具失败会让 agent
// 当成普通失败继续跑。server 也不因此算掉线。
func TestRemoteTool_CallerCanceled_PropagatesError(t *testing.T) {
	started := make(chan struct{})
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return fc, nil }}
	h := mcp.NewHub(mcp.WithClientFactory("srv", f.New, mcp.WithCallTimeout(time.Minute)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()
	res, err := h.Toolkit().Call(ctx, "mcp_srv_t", nil)
	if !errors.Is(err, context.Canceled) || res != nil {
		t.Fatalf("调用方取消时该返回带 context.Canceled 的 error、结果为 nil，实际 res=%+v err=%v", res, err)
	}
	if st := statusOf(t, h, "srv"); st.State != mcp.ServerOnline {
		t.Errorf("调用方取消不是 server 的问题，不该标掉线，实际 %+v", st)
	}
}

// 超过 WithCallTimeout：超时失败（告诉模型 server 那边可能仍在执行），server 不算掉线、下一次照常发给它。
func TestHub_CallTimeout_FailsWithoutMarkingOffline(t *testing.T) {
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return fc, nil }}
	h := mcp.NewHub(mcp.WithClientFactory("srv", f.New, mcp.WithCallTimeout(20*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_srv_t"), tool.ErrCodeTimeout, "可能仍在执行")
	wantFailure(t, call(t, h, "mcp_srv_t"), tool.ErrCodeTimeout, "超过 20ms")
	if got := len(fc.calls()); got != 2 {
		t.Errorf("超时不算掉线，两次都该发给 server，实际 %d 次", got)
	}
	if f.count() != 1 {
		t.Errorf("超时不该触发重连，工厂被调 %d 次", f.count())
	}
}

// Client 明确说「没发出」（ErrNotSent）、哪怕是被时限打断的，也算连不上：标掉线，之后的调用直接失败，不再每次
// 干等满时限；文案说「这次调用没有发出」（不然模型会把没执行的写操作当成「可能已执行」）。
func TestHub_NotSentWithinTimeout_MarksOffline(t *testing.T) {
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
		<-ctx.Done()
		return nil, fmt.Errorf("post: %w: %w", mcp.ErrNotSent, ctx.Err())
	}}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return fc, nil
		}
		return nil, errDown
	}}
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("srv", f.New, mcp.WithCallTimeout(20*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_srv_t"), mcp.ErrCodeServerUnavailable, "该能力暂时不可用（srv 连不上），这次调用没有发出")
	if st := statusOf(t, h, "srv"); st.State != mcp.ServerOffline {
		t.Errorf("没发出的超时该标掉线，实际 %+v", st)
	}
}

// 「没有发出」只认 ErrNotSent，不从错误形状推断：一个包着 dial 错误、却没说没发出的错误（比如写操作已经执行、
// 取结果的第二个连接 dial 失败）必须按「不确定是否已执行」说——说成「没有发出」模型会再做一遍。
func TestHub_DialShapedErrorWithoutNotSent_IsUncertain(t *testing.T) {
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
		return nil, fmt.Errorf("fetch result: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errDown})
	}}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return fc, nil }}
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("srv", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_srv_t"), mcp.ErrCodeServerUnavailable, "不确定是否已执行")
}

// 迟到的旧连接错误不能把新连接标掉：调用 A 卡在旧连接上，期间旧连接坏了（调用 B 撞上、标掉线）、重连上了新连接；
// 之后 A 才带着连接错误返回——这时 server 必须仍在线（掉线判定只认当前这一代连接）。
func TestHub_LateErrorFromOldConnection_DoesNotKnockNewOne(t *testing.T) {
	releaseA := make(chan struct{})
	enteredA := make(chan struct{})
	old := &memClient{name: "s", tools: specs("slow", "fast"), onCall: func(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
		if name == "slow" {
			close(enteredA)
			<-releaseA
		}
		return nil, errDown // 旧连接上的调用最终都是连接错误
	}}
	fresh := &memClient{name: "s", tools: specs("slow", "fast")}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return old, nil
		}
		return fresh, nil
	}}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("s", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	doneA := make(chan *tool.Result, 1)
	go func() { doneA <- call(t, h, "mcp_s_slow") }()
	<-enteredA
	wantFailure(t, call(t, h, "mcp_s_fast"), mcp.ErrCodeServerUnavailable, "连接出错")
	waitFor(t, "重连上新连接", func() bool { return statusOf(t, h, "s").State == mcp.ServerOnline })
	close(releaseA)
	wantFailure(t, <-doneA, mcp.ErrCodeServerUnavailable, "连接出错")
	if st := statusOf(t, h, "s"); st.State != mcp.ServerOnline {
		t.Fatalf("旧连接迟到的错误把新连接标掉了：%+v", st)
	}
	if res := call(t, h, "mcp_s_fast"); !res.Success {
		t.Errorf("新连接该照常可用：%+v", res)
	}
}

// stdio 子进程退了（崩溃）：调用返回「该能力暂时不可用」，后台新起一个子进程重连上，调用恢复。
// （Client 关了不能再用、stdio 的子进程只能重起——这就是 WithClientFactory 存在的理由。）
func TestHub_StdioFactory_RespawnsAfterChildExit(t *testing.T) {
	var spawned atomic.Int32
	f := func(context.Context) (mcp.Client, error) {
		mode := "ok"
		if spawned.Add(1) == 1 {
			mode = "exit" // 第一个子进程收到 tools/call 就退出
		}
		return mcp.NewStdioClient("mock", os.Args[0], nil, mcp.WithStdioEnv("MCP_MOCK_MODE="+mode))
	}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("mock", f))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_mock_t"), mcp.ErrCodeServerUnavailable, "连接出错")
	waitFor(t, "新起的子进程连上", func() bool { return statusOf(t, h, "mock").State == mcp.ServerOnline })
	if res := call(t, h, "mcp_mock_t"); !res.Success {
		t.Fatalf("重起子进程后该恢复：%+v", res)
	}
	if got := spawned.Load(); got != 2 {
		t.Errorf("该起过 2 个子进程，实际 %d", got)
	}
}

// server 答了 JSON-RPC 错误：它活着，只是这次调用报错——普通失败、带上 server 的错误信息，不标掉线。
// 走真的 StreamableHTTPClient，顺带守「HTTP 客户端交出的 JSON-RPC 错误认得出来」。
func TestHub_RPCError_KeepsServerOnline(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "t"}}})
		case "tools/call":
			callCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]any{"code": -32602, "message": "invalid params: q is required"}})
		}
	}))
	defer server.Close()
	f := func(context.Context) (mcp.Client, error) { return mcp.NewStreamableHTTPClient("rpc", server.URL) }
	h := mcp.NewHub(mcp.WithClientFactory("rpc", f))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_rpc_t"), tool.ErrCodeInternal, "invalid params: q is required")
	call(t, h, "mcp_rpc_t")
	if got := callCount.Load(); got != 2 {
		t.Errorf("JSON-RPC 错误不算掉线，两次都该发给 server，实际 %d 次", got)
	}
}

// server 返回 (nil, nil)：失败结果，不 panic。
func TestRemoteTool_NilResultIsFailure(t *testing.T) {
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
		return nil, nil
	}}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	wantFailure(t, call(t, h, "mcp_srv_t"), tool.ErrCodeInternal, "空结果")
}

// --- 硬件互斥 ---

// WithSerialCalls：同一 server 的并发调用串行执行；对照组（不开）确实会重叠——证明这个用例分得出两种行为。
func TestHub_SerialCalls_NeverOverlap(t *testing.T) {
	maxInFlight := func(serial bool) int32 {
		var inFlight, peak atomic.Int32
		fc := &memClient{name: "cam", tools: specs("shoot"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			inFlight.Add(-1)
			return &mcp.CallResult{}, nil
		}}
		var opts []mcp.ServerOption
		if serial {
			opts = append(opts, mcp.WithSerialCalls())
		}
		h := mcp.NewHub(mcp.WithClient(fc, opts...))
		if err := h.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer h.Close()
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-gate
				call(t, h, "mcp_cam_shoot")
			}()
		}
		close(gate)
		wg.Wait()
		return peak.Load()
	}
	if got := maxInFlight(true); got != 1 {
		t.Errorf("WithSerialCalls 下同时在执行的调用至多 1 个，实际峰值 %d", got)
	}
	if got := maxInFlight(false); got < 2 {
		t.Errorf("对照组（不串行）该出现重叠，实际峰值 %d——用例分不出两种行为", got)
	}
}

// 排队有自己的时限（WithCallTimeout）：前一个调用一直占着时，排不上的那个按超时失败，没有发出。
func TestHub_SerialCalls_QueueWaitHasTimeout(t *testing.T) {
	hold := make(chan struct{})
	entered := make(chan struct{}, 1)
	fc := &memClient{name: "cam", tools: specs("shoot"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
		entered <- struct{}{}
		<-hold
		return &mcp.CallResult{}, nil
	}}
	h := mcp.NewHub(mcp.WithClient(fc, mcp.WithSerialCalls(), mcp.WithCallTimeout(30*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = h.Toolkit().Call(context.Background(), "mcp_cam_shoot", nil) }()
	<-entered
	wantFailure(t, call(t, h, "mcp_cam_shoot"), tool.ErrCodeTimeout, "没排上")
	close(hold)
	<-done
	if got := len(fc.calls()); got != 1 {
		t.Errorf("排不上的调用不该发出，server 收到 %d 次", got)
	}
}

// --- 命名与确定性错误 ---

// WithToolNamer 决定框架内的工具名；撞名（会让一个工具静默顶掉另一个）与 server 名重复都让 Start 失败。
func TestHub_ToolNamer_AndDuplicatesFailStart(t *testing.T) {
	join := func(server, tool string) string { return server + "__" + tool }
	h := mcp.NewHub(mcp.WithToolNamer(join),
		mcp.WithClient(&memClient{name: "devbox", tools: specs("git_diff")}),
		mcp.WithClient(&memClient{name: "cloud", tools: specs("list_tasks")}))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := names(h); !slices.Equal(got, []string{"devbox__git_diff", "cloud__list_tasks"}) {
		t.Errorf("工具名该按 namer 生成，实际 %v", got)
	}
	if res := call(t, h, "devbox__git_diff"); !res.Success || res.Output != "ok:git_diff" {
		t.Errorf("调用按原始名发给 server，实际 %+v", res)
	}
	_ = h.Close()

	clash := mcp.NewHub(mcp.WithPartialStart(), mcp.WithToolNamer(func(string, string) string { return "same" }),
		mcp.WithClient(&memClient{name: "a", tools: specs("x")}),
		mcp.WithClient(&memClient{name: "b", tools: specs("y")}))
	if err := clash.Start(context.Background()); err == nil || !strings.Contains(err.Error(), `"same"`) {
		t.Errorf("两个工具撞名该让 Start 失败（哪怕开了 WithPartialStart），实际 %v", err)
	}

	dupServer := mcp.NewHub(mcp.WithClient(&memClient{name: "a", tools: specs("x")}),
		mcp.WithClient(&memClient{name: "a", tools: specs("y")}))
	if err := dupServer.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate server name") {
		t.Errorf("server 名重复该让 Start 失败，实际 %v", err)
	}

	dupTool := mcp.NewHub(mcp.WithClient(&memClient{name: "a", tools: specs("x", "x")}))
	if err := dupTool.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicate tool name") {
		t.Errorf("server 报了重名工具该让 Start 失败，实际 %v", err)
	}
}

// 快照与状态都是深拷贝：调用方改自己手里的那份，不影响已注册工具发给模型的定义。
func TestHub_SnapshotAndStatusAreCopies(t *testing.T) {
	snap := specs("a")
	f := &seqFactory{build: func(int) (mcp.Client, error) { return nil, errDown }}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(), mcp.WithClientFactory("s", f.New, mcp.WithToolSnapshot(snap)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	before := toolsJSON(t, h)
	snap[0].InputSchema["properties"].(map[string]any)["q"] = "mutated"
	snap[0].Description = "mutated"
	if after := toolsJSON(t, h); after != before {
		t.Errorf("改调用方的快照改到了已注册工具：%s", after)
	}
}

// --- 状态回调 ---

// WithStatusHook 按状态变化的先后顺序回调：启动时离线 → 重连上 → 掉线 → 再重连上；上线的那几次带着清单。
func TestHub_StatusHook_OrderedTransitions(t *testing.T) {
	var broken atomic.Bool
	mk := func() mcp.Client {
		return &memClient{name: "s", tools: specs("a"), onCall: func(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
			if broken.Load() {
				return nil, errDown
			}
			return &mcp.CallResult{}, nil
		}}
	}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return nil, errDown
		}
		return mk(), nil
	}}
	var mu sync.Mutex
	var seen []mcp.ServerStatus
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(),
		mcp.WithClientFactory("s", f.New, mcp.WithToolSnapshot(specs("a"))),
		mcp.WithStatusHook(func(st mcp.ServerStatus) {
			mu.Lock()
			seen = append(seen, st)
			mu.Unlock()
		}))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(seen) }
	waitFor(t, "重连上（第 2 个事件）", func() bool { return count() >= 2 })
	broken.Store(true)
	call(t, h, "mcp_s_a")
	broken.Store(false)
	waitFor(t, "再次重连上（第 4 个事件）", func() bool { return count() >= 4 })

	mu.Lock()
	defer mu.Unlock()
	var states []mcp.ServerState
	for _, st := range seen {
		states = append(states, st.State)
	}
	want := []mcp.ServerState{mcp.ServerOffline, mcp.ServerOnline, mcp.ServerOffline, mcp.ServerOnline}
	if !slices.Equal(states, want) {
		t.Fatalf("回调顺序 = %v，要 %v", states, want)
	}
	if seen[1].Tools == nil || seen[3].Tools == nil || seen[0].Tools != nil {
		t.Errorf("上线事件该带清单、启动时离线的事件没有清单：%+v", seen)
	}
}

// --- 关闭 ---

// Close 打断正在进行的重连尝试、停掉重连循环；之后的调用返回「已关闭」，Start 报错。
func TestHub_Close_StopsReconnecting(t *testing.T) {
	block := make(chan struct{}) // 永不放行：第 2 次尝试会一直卡在工厂里，只有 ctx 能解开
	f := &seqFactory{
		build: func(int) (mcp.Client, error) { return nil, errDown },
		gates: map[int]chan struct{}{2: block},
	}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(), mcp.WithConnectTimeout(time.Hour),
		mcp.WithClientFactory("s", f.New, mcp.WithToolSnapshot(specs("a"))))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, "第 2 次尝试开始", func() bool { return f.count() >= 2 })
	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 没有打断卡住的重连尝试")
	}
	n := f.count()
	time.Sleep(50 * time.Millisecond)
	if f.count() != n {
		t.Errorf("Close 之后还在重连：工厂调用 %d → %d", n, f.count())
	}
	wantFailure(t, call(t, h, "mcp_s_a"), mcp.ErrCodeServerUnavailable, "已关闭")
	if err := h.Start(context.Background()); err == nil {
		t.Error("Close 之后 Start 该报错")
	}
	if err := h.Close(); err != nil {
		t.Errorf("重复 Close 该安全：%v", err)
	}
	if st := statusOf(t, h, "s"); st.State != mcp.ServerOffline || st.Err == nil {
		t.Errorf("Close 之后状态该是离线并带原因：%+v", st)
	}
}

// 并发调用撞上掉线与重连（-race 下跑）：每次调用都只是成功或失败结果，从不返回 error；工具集不变。
func TestHub_ConcurrentCallsDuringFlapping(t *testing.T) {
	var flaky atomic.Int32
	f := &seqFactory{build: func(int) (mcp.Client, error) {
		return &memClient{name: "s", tools: specs("a"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			if flaky.Add(1)%3 == 0 {
				return nil, errDown
			}
			return &mcp.CallResult{}, nil
		}}, nil
	}}
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Millisecond, 2*time.Millisecond), mcp.WithClientFactory("s", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	before := toolsJSON(t, h)
	// 跑到发生过几轮「掉线 → 重连」为止：离线期间的调用直接失败、极快，按次数跑的话一轮重连都等不到。
	deadline := time.Now().Add(5 * time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f.count() < 5 && time.Now().Before(deadline) {
				res, err := h.Toolkit().Call(context.Background(), "mcp_s_a", nil)
				if err != nil || res == nil {
					t.Errorf("掉线 / 重连期间的调用只该是结果，实际 res=%v err=%v", res, err)
					return
				}
				_ = h.Status()
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if f.count() < 5 {
		t.Errorf("该发生过几轮重连，工厂只被调了 %d 次", f.count())
	}
	if after := toolsJSON(t, h); after != before {
		t.Errorf("工具集变了")
	}
}

// --- 审计补的用例（并发与生命周期）---

// 状态回调里调 Close（「掉线就整个关掉」这种写法）不死锁：Close 返回、连接被关掉；外面再 Close 拿到同一个结果。
func TestHub_StatusHookMayCallClose(t *testing.T) {
	c := &memClient{name: "s", tools: specs("a")}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return c, nil }}
	var h *mcp.Hub
	var once sync.Once
	inner := make(chan error, 1)
	h = mcp.NewHub(mcp.WithClientFactory("s", f.New), mcp.WithStatusHook(func(mcp.ServerStatus) {
		once.Do(func() { inner <- h.Close() })
	}))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case err := <-inner:
		if err != nil {
			t.Errorf("回调里的 Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("回调里调 Close 没返回（Close 在等正在执行这个回调的 goroutine——自己等自己）")
	}
	if got := c.closed.Load(); got != 1 {
		t.Errorf("连接该被关掉一次，实际 %d 次", got)
	}
	if err := h.Close(); err != nil {
		t.Errorf("再 Close 该拿到同一个结果（nil），实际 %v", err)
	}
}

// 串行 + 超时：超时的那个调用 server 那边可能还在执行（stdio 的 rpc 到期就返回、不通知 server 取消），
// 排队的调用不能发给同一个连接——那样硬件上就同时有两个操作。连接按不可信处理：标掉线、关旧连接
// （这里模拟 stdio 旧子进程被杀、它手上的操作随之结束）、重连后才再发。
func TestHub_SerialTimeout_NoOverlapOnServer(t *testing.T) {
	var device, peak atomic.Int32 // 「硬件」上同时在执行的操作数（各连接共用一台设备）
	enter := func() {
		n := device.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				return
			}
		}
	}
	var first *memClient
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		c := &memClient{name: "cam", tools: specs("shoot")}
		if n == 1 {
			killed := make(chan struct{})
			var once sync.Once
			c.onCall = func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
				enter()
				go func() { <-killed; device.Add(-1) }() // server 那边一直执行到连接被关（子进程被杀）
				<-ctx.Done()
				return nil, ctx.Err() // 客户端到期就返回，同 StdioClient.rpc
			}
			c.onClose = func() { once.Do(func() { close(killed) }) }
			first = c
			return c, nil
		}
		c.onCall = func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			enter()
			device.Add(-1)
			return &mcp.CallResult{}, nil
		}
		return c, nil
	}}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("cam", f.New, mcp.WithSerialCalls(), mcp.WithCallTimeout(40*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	resA := make(chan *tool.Result, 1)
	go func() { resA <- call(t, h, "mcp_cam_shoot") }()
	waitFor(t, "A 在 server 上执行", func() bool { return device.Load() == 1 })
	time.Sleep(10 * time.Millisecond) // B 晚一点开始：它的时限要晚于 A 的到期，才排得上
	call(t, h, "mcp_cam_shoot")       // B：排队，A 超时之后才轮到
	wantFailure(t, <-resA, tool.ErrCodeTimeout, "已断开 cam 重连")
	if got := len(first.calls()); got != 1 {
		t.Errorf("排队的调用发给了还在执行的旧连接（旧连接收到 %d 次）", got)
	}
	waitFor(t, "重连上", func() bool { return statusOf(t, h, "cam").State == mcp.ServerOnline })
	if res := call(t, h, "mcp_cam_shoot"); !res.Success {
		t.Fatalf("重连后该恢复：%+v", res)
	}
	if got := peak.Load(); got != 1 {
		t.Errorf("硬件上同时有 %d 个操作在执行（WithSerialCalls 要至多 1 个）", got)
	}
}

// 排队中的串行调用在拿到信号量后按现在的状态再准入：前一个调用撞上连接错误、server 已判掉线时，它不再发给旧连接。
func TestHub_QueuedSerialCall_NotSentToStaleConnection(t *testing.T) {
	releaseA := make(chan struct{})
	old := &memClient{name: "s", tools: specs("a"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
		<-releaseA
		return nil, errDown
	}}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return old, nil
		}
		return &memClient{name: "s", tools: specs("a")}, nil
	}}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("s", f.New, mcp.WithSerialCalls()))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	doneA := make(chan struct{})
	go func() { defer close(doneA); call(t, h, "mcp_s_a") }()
	waitFor(t, "A 发给了旧连接", func() bool { return len(old.calls()) == 1 })
	resB := make(chan *tool.Result, 1)
	go func() { resB <- call(t, h, "mcp_s_a") }()
	time.Sleep(20 * time.Millisecond) // B 在信号量上排队
	close(releaseA)                   // A 撞上连接错误 → 掉线
	<-doneA
	res := <-resB
	if got := len(old.calls()); got != 1 {
		t.Fatalf("排队的 B 发给了已判掉线的旧连接（旧连接收到 %d 次），B=%+v", got, res)
	}
}

// Close 打断进行中的 Start（不设连接时限、Start 的 ctx 也不结束时，原来 Close 会永远等下去）。
func TestHub_Close_InterruptsStart(t *testing.T) {
	f := &seqFactory{
		build: func(int) (mcp.Client, error) { return &memClient{name: "s"}, nil },
		gates: map[int]chan struct{}{1: make(chan struct{})}, // 永不放行
	}
	h := mcp.NewHub(mcp.WithPartialStart(), mcp.WithConnectTimeout(0), mcp.WithClientFactory("s", f.New))
	startErr := make(chan error, 1)
	go func() { startErr <- h.Start(context.Background()) }()
	waitFor(t, "Start 卡在连接上", func() bool { return f.count() == 1 })
	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close 没有打断进行中的 Start")
	}
	if err := <-startErr; err == nil {
		t.Error("被 Close 打断的 Start 该返回错误")
	}
	if h.Status() != nil {
		t.Error("没启动成功的 Hub 不该有状态")
	}
}

// 并发的 Close：后来的等第一个把连接关完再返回（原来看到「已关闭」就立刻返回 nil，调用方以为资源已释放）。
func TestHub_ConcurrentClose_WaitsForFirst(t *testing.T) {
	release := make(chan struct{})
	c := &memClient{name: "s", tools: specs("a"), onClose: func() { <-release }}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() { _ = h.Close() }()
	waitFor(t, "第一个 Close 开始关连接", func() bool { return c.closed.Load() == 1 })
	second := make(chan error, 1)
	go func() { second <- h.Close() }()
	select {
	case <-second:
		t.Fatal("第二个 Close 在第一个关完连接之前就返回了")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("第一个关完之后，第二个 Close 该返回")
	}
	if got := c.closed.Load(); got != 1 {
		t.Errorf("连接该只关一次，实际 %d 次", got)
	}
}

// --- 审计补的用例（错误映射）---

// fullBacklogAddr 起一个 backlog 塞满、从不 accept 的监听：之后的 dial 收不到 SYN-ACK、真挂起（模拟黑洞网络，
// 比如笔记本离家后对它的 TCP 连接发不出去）。塞不满的环境跳过。
func fullBacklogAddr(t *testing.T) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 1); err != nil {
		t.Fatal(err)
	}
	sa, _ := syscall.Getsockname(fd)
	addr := fmt.Sprintf("127.0.0.1:%d", sa.(*syscall.SockaddrInet4).Port)
	for i := 0; i < 64; i++ {
		c, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
		if err != nil {
			return addr
		}
		t.Cleanup(func() { _ = c.Close() })
	}
	t.Skip("backlog 塞不满，跳过")
	return ""
}

// 真 net/http：server 变成黑洞（dial 挂起）后，调用被时限打断时请求还没写出——要按「连不上、没有发出」标掉线，
// 之后的调用直接失败。请求 ctx 结束时 net/http 交出的是 ctx 的错（链上没有 dial 错误），按错误形状判会判成
// 「超时、server 那边可能仍在执行」、server 一直算在线、每次都干等满时限。
func TestStreamableHTTP_BlackholeDial_NotSentAndOffline(t *testing.T) {
	var executed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "t"}}})
		case "tools/call":
			executed.Add(1)
			writeResult(w, req.ID, map[string]any{"content": []any{}})
		}
	}))
	defer server.Close()
	hole := fullBacklogAddr(t)
	var blackhole atomic.Bool
	d := &net.Dialer{}
	tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if blackhole.Load() {
			addr = hole
		}
		return d.DialContext(ctx, network, addr)
	}}
	f := func(context.Context) (mcp.Client, error) {
		return mcp.NewStreamableHTTPClient("bh", server.URL, mcp.WithHTTPClient(&http.Client{Transport: tr}))
	}
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("bh", f, mcp.WithCallTimeout(200*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	blackhole.Store(true)
	wantFailure(t, call(t, h, "mcp_bh_t"), mcp.ErrCodeServerUnavailable, "该能力暂时不可用（bh 连不上），这次调用没有发出")
	if st := statusOf(t, h, "bh"); st.State != mcp.ServerOffline {
		t.Fatalf("黑洞里的调用该把 server 标掉线，实际 %+v", st)
	}
	begin := time.Now()
	wantFailure(t, call(t, h, "mcp_bh_t"), mcp.ErrCodeServerUnavailable, "offline")
	if d := time.Since(begin); d > 100*time.Millisecond {
		t.Errorf("掉线后的调用该直接失败，实际等了 %s", d)
	}
	if n := executed.Load(); n != 0 {
		t.Errorf("黑洞期间 server 不该执行任何调用，实际 %d 次", n)
	}
}

// HTTP 的错误响应按 server 的意思分档：JSON-RPC 错误（哪怕在非 2xx 响应体里）、只拒绝这一个请求的 4xx、2xx 但结果
// 解析不了 → 普通失败、server 仍在线；401 / 403 / 404 → 标掉线（每次都会这样）、但说「没有执行」；5xx → 标掉线、
// 「不确定是否已执行」。错误信息里的响应体截短并抹掉自己发出的 token。
func TestStreamableHTTP_ErrorResponsesClassified(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantCode   string
		wantMsg    string
		wantOnline bool
		notMsg     string // 说明里不该出现的断言
	}{
		{"非 2xx 里的 JSON-RPC 错误", 400, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params: q is required"}}`,
			tool.ErrCodeInternal, "invalid params: q is required", true, ""},
		{"400 纯文本", 400, "bad request", tool.ErrCodeInternal, "拒绝了这次调用（HTTP 400），没有执行", true, ""},
		{"429", 429, "slow down", tool.ErrCodeInternal, "HTTP 429", true, ""},
		{"408（代理空闲超时也回它，请求可能已转给 server）", 408, "timeout", tool.ErrCodeTimeout, "不确定是否已执行", true, "没有执行"},
		{"409 不断言没执行", 409, "conflict", tool.ErrCodeInternal, "HTTP 409", true, "没有执行"},
		{"401 带 JSON-RPC 错误体仍按状态码分档", 401, `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"invalid credentials: Bearer s3cret-token"}}`,
			mcp.ErrCodeServerUnavailable, "HTTP 401", false, "s3cret-token"},
		{"非 2xx 的 JSON-RPC 错误交给模型前抹掉 token", 400, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"bad token Bearer s3cret-token"}}`,
			tool.ErrCodeInternal, "bad token <redacted>", true, "s3cret-token"},
		{"200 但 image 的 base64 坏了", 200, `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"image","data":"!!!","mimeType":"image/png"}]}}`,
			tool.ErrCodeInternal, "解析不了", true, ""},
		{"200 但不是 JSON", 200, "event: message\ndata: {}\n\n", tool.ErrCodeInternal, "解析不了", true, ""},
		{"200 的 JSON-RPC 错误交给模型前抹掉 token", 200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"denied for s3cret-token"}}`,
			tool.ErrCodeInternal, "denied for <redacted>", true, "s3cret-token"},
		{"401", 401, "unauthorized: Bearer s3cret-token", mcp.ErrCodeServerUnavailable, "拒绝了请求：HTTP 401），这次调用没有执行", false, ""},
		{"404", 404, "", mcp.ErrCodeServerUnavailable, "HTTP 404", false, ""},
		{"500", 500, "boom", mcp.ErrCodeServerUnavailable, "不确定是否已执行", false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req rpcReq
				_ = json.NewDecoder(r.Body).Decode(&req)
				switch req.Method {
				case "initialize":
					writeResult(w, req.ID, map[string]any{})
				case "tools/list":
					writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "t"}}})
				default:
					w.WriteHeader(c.status)
					_, _ = w.Write([]byte(c.body))
				}
			}))
			defer server.Close()
			f := func(context.Context) (mcp.Client, error) {
				return mcp.NewStreamableHTTPClient("x", server.URL, mcp.WithHTTPHeader("Authorization", "Bearer s3cret-token"))
			}
			h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("x", f))
			if err := h.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer h.Close()
			res := call(t, h, "mcp_x_t")
			wantFailure(t, res, c.wantCode, c.wantMsg)
			if c.notMsg != "" && strings.Contains(res.Error.Message, c.notMsg) {
				t.Errorf("说明里不该有 %q：%s", c.notMsg, res.Error.Message)
			}
			st := statusOf(t, h, "x")
			if got := st.State == mcp.ServerOnline; got != c.wantOnline {
				t.Errorf("server 在线 = %v，要 %v（%+v）", got, c.wantOnline, st)
			}
			if st.Err != nil && strings.Contains(st.Err.Error(), "s3cret-token") {
				t.Errorf("Status.Err 里带出了 token：%v", st.Err)
			}
		})
	}
}

// 调用方取消时返回的 error 链上要有 ctx 的错——哪怕 Client 交出的是别的错（连接被关之类）：下游按 errors.Is 判「用户取消」。
func TestRemoteTool_CallerCanceled_ChainHasCtxError(t *testing.T) {
	started := make(chan struct{})
	fc := &memClient{name: "srv", tools: specs("t"), onCall: func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
		close(started)
		<-ctx.Done()
		return nil, errors.New("mcp stdio: connection closed")
	}}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()
	_, err := h.Toolkit().Call(ctx, "mcp_srv_t", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error 链上该有 context.Canceled，实际 %v", err)
	}
}

// 默认（全有或全无）的 Start 不给连接尝试加时限——首次运行要下载包的 stdio server 起得慢，原来能起来；开了
// WithPartialStart（或显式 WithConnectTimeout）才有时限，免得一个卡住的 server 挡住启动。
func TestHub_StrictStart_NoDefaultConnectTimeout(t *testing.T) {
	deadlineOf := func(opts ...mcp.HubOption) bool {
		var has bool
		f := func(ctx context.Context) (mcp.Client, error) {
			_, has = ctx.Deadline()
			return &memClient{name: "s", tools: specs("a")}, nil
		}
		h := mcp.NewHub(append(opts, mcp.WithClientFactory("s", f))...)
		if err := h.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		_ = h.Close()
		return has
	}
	if deadlineOf() {
		t.Error("默认 Start 的连接尝试不该有时限")
	}
	if !deadlineOf(mcp.WithPartialStart()) {
		t.Error("WithPartialStart 的连接尝试该有默认时限")
	}
	if !deadlineOf(mcp.WithConnectTimeout(time.Minute)) {
		t.Error("显式 WithConnectTimeout 该生效")
	}
}

// tools/list 里的 annotations 解析出来、随 Status 交出（深拷贝）。
func TestStreamableHTTP_ToolAnnotations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{
				{"name": "read", "annotations": map[string]any{"readOnlyHint": true, "title": "读"}},
				{"name": "write"},
			}})
		}
	}))
	defer server.Close()
	f := func(context.Context) (mcp.Client, error) { return mcp.NewStreamableHTTPClient("a", server.URL) }
	h := mcp.NewHub(mcp.WithClientFactory("a", f))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	tools := statusOf(t, h, "a").Tools
	if len(tools) != 2 || tools[0].Annotations == nil || tools[0].Annotations.ReadOnlyHint == nil ||
		!*tools[0].Annotations.ReadOnlyHint || tools[0].Annotations.Title != "读" || tools[1].Annotations != nil {
		t.Fatalf("annotations 没解析对：%+v", tools)
	}
	*tools[0].Annotations.ReadOnlyHint = false
	if again := statusOf(t, h, "a").Tools; !*again[0].Annotations.ReadOnlyHint {
		t.Error("Status 交出的 annotations 该是深拷贝")
	}
}

// stdio 子进程已经退了，请求写不进管道：肯定没发出——告诉模型「没有发出」，标掉线、重起子进程。
func TestHub_StdioWriteToDeadChild_IsNotSent(t *testing.T) {
	var spawned atomic.Int32
	f := func(context.Context) (mcp.Client, error) {
		mode := "ok"
		if spawned.Add(1) == 1 {
			mode = "die" // 第一个子进程报完清单就退出
		}
		return mcp.NewStdioClient("mock", os.Args[0], nil, mcp.WithStdioEnv("MCP_MOCK_MODE="+mode))
	}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("mock", f))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	time.Sleep(200 * time.Millisecond) // 等第一个子进程退出
	wantFailure(t, call(t, h, "mcp_mock_t"), mcp.ErrCodeServerUnavailable, "这次调用没有发出")
	waitFor(t, "新起的子进程连上", func() bool { return statusOf(t, h, "mock").State == mcp.ServerOnline })
	if res := call(t, h, "mcp_mock_t"); !res.Success {
		t.Fatalf("重起子进程后该恢复：%+v", res)
	}
}

// --- 第二轮审计补的用例 ---

// 串行 + 调用方取消：请求已经发出、调用方喊停（stdio 不通知 server 取消），server 那边可能还在执行——跟超时一样按连接
// 不可信处理，排队的调用不能发给同一个连接（第一轮修超时时漏了这条路）。
func TestHub_SerialCallerCancel_NoOverlapOnServer(t *testing.T) {
	var device, peak atomic.Int32
	enter := func() {
		n := device.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				return
			}
		}
	}
	var first *memClient
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		c := &memClient{name: "cam", tools: specs("shoot")}
		if n == 1 {
			killed := make(chan struct{})
			var once sync.Once
			c.onCall = func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
				enter()
				go func() { <-killed; device.Add(-1) }() // server 那边一直执行到连接被关
				<-ctx.Done()
				return nil, ctx.Err()
			}
			c.onClose = func() { once.Do(func() { close(killed) }) }
			first = c
			return c, nil
		}
		c.onCall = func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			enter()
			device.Add(-1)
			return &mcp.CallResult{}, nil
		}
		return c, nil
	}}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("cam", f.New, mcp.WithSerialCalls()))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() { _, err := h.Toolkit().Call(ctxA, "mcp_cam_shoot", nil); errA <- err }()
	waitFor(t, "A 在 server 上执行", func() bool { return device.Load() == 1 })
	doneB := make(chan struct{})
	go func() { // B：另一个 ctx（带时限：发给旧连接的话它会一直卡到时限），排队
		defer close(doneB)
		ctxB, cancelB := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelB()
		_, _ = h.Toolkit().Call(ctxB, "mcp_cam_shoot", nil)
	}()
	time.Sleep(20 * time.Millisecond)
	cancelA()
	if err := <-errA; !errors.Is(err, context.Canceled) {
		t.Fatalf("A 被取消该返回 ctx 错，实际 %v", err)
	}
	<-doneB
	if got := len(first.calls()); got != 1 {
		t.Errorf("排队的 B 发给了还在执行 A 的旧连接（旧连接收到 %d 次）", got)
	}
	if got := peak.Load(); got != 1 {
		t.Errorf("硬件上同时有 %d 个操作在执行", got)
	}
}

// 排队与执行各自有一份时限：两个并行调用各执行 60ms、时限 100ms，排在后面的那个不该因为排队把执行时间吃掉而超时、
// 把 server 断开重连。
func TestHub_SerialQueueWait_DoesNotEatExecutionBudget(t *testing.T) {
	c := &memClient{name: "cam", tools: specs("shoot"), onCall: func(ctx context.Context, _ string, _ map[string]any) (*mcp.CallResult, error) {
		select {
		case <-time.After(60 * time.Millisecond):
			return &mcp.CallResult{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return c, nil }}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("cam", f.New, mcp.WithSerialCalls(), mcp.WithCallTimeout(100*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	var wg sync.WaitGroup
	results := make([]*tool.Result, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) { defer wg.Done(); results[i] = call(t, h, "mcp_cam_shoot") }(i)
	}
	wg.Wait()
	for i, res := range results {
		if !res.Success {
			t.Errorf("第 %d 个调用该成功（各自执行 60ms < 时限 100ms），实际 %+v", i+1, res)
		}
	}
	if got := c.closed.Load(); got != 0 || f.count() != 1 {
		t.Errorf("不该断开重连：连接被关 %d 次、工厂被调 %d 次", got, f.count())
	}
}

// Close 打断 Start 时，WithClient 的实例只关一次（Start 的回滚已经关了，Close 不再关第二次）。
func TestHub_CloseInterruptsStart_ClosesFixedOnce(t *testing.T) {
	entered := make(chan struct{})
	c := &blockingInitClient{memClient: memClient{name: "s", tools: specs("a")}, entered: entered}
	h := mcp.NewHub(mcp.WithPartialStart(), mcp.WithConnectTimeout(0), mcp.WithClient(c))
	startErr := make(chan error, 1)
	go func() { startErr <- h.Start(context.Background()) }()
	<-entered
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-startErr; err == nil {
		t.Fatal("被打断的 Start 该返回错误")
	}
	if got := c.closed.Load(); got != 1 {
		t.Errorf("固定实例该只关一次，实际 %d 次", got)
	}
}

// blockingInitClient 的 Initialize 一直卡到 ctx 结束。
type blockingInitClient struct {
	memClient
	entered chan struct{}
	once    sync.Once
}

func (c *blockingInitClient) Initialize(ctx context.Context) error {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	return ctx.Err()
}

// HTTP/2：请求体（这里 400KB 的参数）还在往外写时调用时限到了——h2 的 RoundTrip 在 ctx 结束时立刻返回、不等写请求的
// goroutine，WroteRequest 可能在 Do 返回之后才到，而请求其实已经完整发出、server 执行了。这时绝不能告诉模型「没有发出」
// （模型会再做一遍写操作）。用真的内核阻塞：TCP 中继在请求头写出之后停读 600ms，两端的 socket 缓冲都设成 4KB。
func TestStreamableHTTP_H2_SlowBodyWrite_NeverClaimsNotSent(t *testing.T) {
	var executed atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return // 请求体没读全：没执行
		}
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "write"}}})
		case "tools/call":
			executed.Add(1)
			writeResult(w, req.ID, map[string]any{"content": []any{}})
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	var stall atomic.Bool
	ln, err := (&net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		_ = c.Control(func(fd uintptr) { serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096) })
		return serr
	}}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	target := strings.TrimPrefix(srv.URL, "https://")
	go func() { // 中继：收到 stall 信号就停读一会儿，让客户端的写在内核里堵住
		for {
			cc, err := ln.Accept()
			if err != nil {
				return
			}
			sc, err := net.Dial("tcp", target)
			if err != nil {
				_ = cc.Close()
				continue
			}
			go func() { _, _ = io.Copy(cc, sc); _ = cc.Close() }()
			go func() {
				buf := make([]byte, 4096)
				for {
					if stall.Swap(false) {
						time.Sleep(600 * time.Millisecond)
					}
					n, err := cc.Read(buf)
					if n > 0 {
						_, _ = sc.Write(buf[:n])
					}
					if err != nil {
						_ = sc.Close()
						return
					}
				}
			}()
		}
	}()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	d := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		c, err := d.DialContext(ctx, network, ln.Addr().String())
		if err != nil {
			return nil, err
		}
		_ = c.(*net.TCPConn).SetWriteBuffer(4096)
		return c, nil
	}
	f := func(context.Context) (mcp.Client, error) {
		return mcp.NewStreamableHTTPClient("srv", srv.URL, mcp.WithHTTPClient(&http.Client{Transport: tr}))
	}
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("srv", f, mcp.WithCallTimeout(100*time.Millisecond)))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{WroteHeaders: func() { stall.Store(true) }})
	res, err := h.Toolkit().Call(ctx, "mcp_srv_write", map[string]any{"content": strings.Repeat("v", 400<<10)})
	if err != nil || res == nil || res.Error == nil {
		t.Fatalf("该是失败结果（超时），实际 res=%+v err=%v", res, err)
	}
	time.Sleep(1500 * time.Millisecond) // 等中继放行、server 处理完
	if strings.Contains(res.Error.Message, "没有发出") && executed.Load() > 0 {
		t.Fatalf("告诉模型「%s」，server 实际执行了 %d 次", res.Error.Message, executed.Load())
	}
	t.Logf("模型看到：[%s] %s；server 执行 %d 次", res.Error.Code, res.Error.Message, executed.Load())
}

// redact 的边界：截短不切断多字节字符（Status.Err 是合法 UTF-8）；endpoint 路径里的长段（有的服务把 token 放进 URL）也抹掉。
func TestStreamableHTTP_RedactEdges(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "t"}}})
		default:
			w.WriteHeader(500)
			_, _ = w.Write([]byte(strings.Repeat("a", 511) + "中文错误 Cannot POST " + r.URL.Path))
		}
	}))
	defer server.Close()
	secretURL := server.URL + "/mcp/sk-PATH-SECRET-1234567"
	f := func(context.Context) (mcp.Client, error) { return mcp.NewStreamableHTTPClient("x", secretURL) }
	h := mcp.NewHub(mcp.WithReconnectBackoff(time.Hour, time.Hour), mcp.WithClientFactory("x", f))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	call(t, h, "mcp_x_t")
	st := statusOf(t, h, "x")
	if st.Err == nil || !utf8.ValidString(st.Err.Error()) {
		t.Errorf("Status.Err 该是合法 UTF-8：%q", st.Err)
	}
	if strings.Contains(st.Err.Error(), "sk-PATH-SECRET-1234567") && strings.Contains(st.Err.Error(), "Cannot POST") {
		t.Errorf("响应体里回显的 URL 路径秘密没抹：%v", st.Err)
	}
}

// TestHub_ClientCallToolPanic_IsFailureNotPanic 钉住 [mcp.RemoteTool.Call] 的契约在业务 Client panic 时
// 也成立：只在调用方 ctx 结束时返回 error，其余失败（panic 在内）一律以 tool.Failure 交回模型。
//
// ⚠️ 这条**必须单独存在**：panic 不兜的话它穿过 RemoteTool.Call 与 tool.Toolkit.Call（那一层不 recover），
// 直到 agent_v2 外层兜底转成 error ⟹ 一个 server 的 bug 拖垮整个 Run，而「按 server 降级」的全部意义就是
// 不让这件事发生。实测过：把 server.callClient 的 recover 去掉，全套 mcp 测试照样全绿。
//
// 同时钉「panic 不标掉线」：panic 是调用方代码的 bug，连接可能好着，标掉线会把一个工具的 bug 升级成整个
// server 的降级（后续调用全部快速失败）。
func TestHub_ClientCallToolPanic_IsFailureNotPanic(t *testing.T) {
	c := &memClient{
		name:  "s",
		tools: specs("boom", "fine"),
		onCall: func(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
			if name == "boom" {
				panic("业务 Client 炸了")
			}
			return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok"}}}, nil
		},
	}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()

	byName := map[string]*mcp.RemoteTool{}
	for _, tl := range h.Tools() {
		byName[tl.Name()] = tl
	}
	boom, fine := byName["mcp_s_boom"], byName["mcp_s_fine"]
	if boom == nil || fine == nil {
		t.Fatalf("期望注册 mcp_s_boom 与 mcp_s_fine，实际 %v", slices.Sorted(maps.Keys(byName)))
	}

	res, err := func() (r *tool.Result, e error) {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("panic 冲出了 RemoteTool.Call：%v（契约要求转成 tool.Failure）", rec)
			}
		}()
		return boom.Call(context.Background(), map[string]any{"q": "x"})
	}()
	if err != nil {
		t.Errorf("panic 应转成 tool.Failure 而不是 error，got err=%v", err)
	}
	if res == nil || res.Error == nil || res.Error.Code != tool.ErrCodeInternal {
		t.Errorf("期望 tool.Failure(%s)，got %+v", tool.ErrCodeInternal, res)
	}

	// panic 不做健康降级：同一个 server 的另一件工具照常调得通。
	//
	// ⚠️ 这两条断言在**固定实例**（WithClient）上**恒真**，钉不住「不标掉线」那个决定——markOffline
	// 第一行就是 `if s.factory == nil { return false }`，对固定实例是结构性 no-op。真正钉它的是下面那条
	// 用 WithClientFactory 的姊妹用例（审计实测：panic 分支插入 markOffline，本用例照样 PASS）。
	if res, err := fine.Call(context.Background(), map[string]any{"q": "x"}); err != nil || res == nil || res.Error != nil {
		t.Errorf("panic 把 server 标成掉线了：后续调用 res=%+v err=%v", res, err)
	}
	if st := h.Status(); len(st) != 1 || st[0].State != mcp.ServerOnline {
		t.Errorf("panic 之后 server 状态应仍是 online，got %+v", st)
	}
}

// TestHub_ClientCallToolPanic_NonSerialKeepsConnection：**非串行**的 factory server 上 panic 只毁掉这一次
// 调用——不做健康降级、也不换连接（没有重叠风险，panic 是代码 bug 而不是连接坏了）。
//
// 这条与下一条是一对，分开钉 connectionUntrusted 的两种答案。用 factory 而不是固定实例：markOffline 对
// 固定实例是 no-op，拿它验不出「标没标掉线」。
func TestHub_ClientCallToolPanic_NonSerialKeepsConnection(t *testing.T) {
	var built atomic.Int32
	newC := func(context.Context) (mcp.Client, error) {
		built.Add(1)
		var boom atomic.Bool
		return &memClient{name: "s", tools: specs("t"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			if boom.CompareAndSwap(false, true) {
				panic("炸了")
			}
			return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok"}}}, nil
		}}, nil
	}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("s", newC))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	tl := h.Tools()[0]
	if res, _ := tl.Call(context.Background(), map[string]any{}); res == nil || res.Error == nil {
		t.Fatalf("fixture 没搭对：第一次应被兜成失败结果")
	}
	time.Sleep(120 * time.Millisecond) // 给重连（若发生）时间
	if st := h.Status(); len(st) != 1 || st[0].State != mcp.ServerOnline {
		t.Errorf("非串行 server 上 panic 不该标掉线，got %+v", st)
	}
	if n := built.Load(); n != 1 {
		t.Errorf("非串行 server 上 panic 不该换连接，工厂建了 %d 个", n)
	}
}

// TestHub_ClientCallToolPanic_SerialResetsConnection：**串行** factory server 上 panic 之后必须换连接。
//
// panic 不带 ErrNotSent ⟹ 按既有规则「请求可能已发出、结果不明」⟹ 旧连接那边可能还在执行，下一个调用
// 发给它就让硬件上同时有两个操作，而 WithSerialCalls 的全部意义就是至多一个。
//
// ⚠️ 这条是 af7c45a 的回归哨兵：那次只跳过了健康降级，连带把这一维也跳过了。实测后果（本用例在修复前红）：
// 工厂只建 1 个连接、硬件并发峰值 2，而 err == nil。既有哨兵一条都钉不住它。
func TestHub_ClientCallToolPanic_SerialResetsConnection(t *testing.T) {
	var built atomic.Int32
	var mu sync.Mutex
	var clients []*memClient
	newC := func(context.Context) (mcp.Client, error) {
		built.Add(1)
		var boom atomic.Bool
		c := &memClient{name: "cam", tools: specs("shoot"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			if boom.CompareAndSwap(false, true) {
				// 请求已写出、硬件已开始执行，解码响应时炸了——那边还在动，而我们不知道结果
				panic("解码响应炸了")
			}
			return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok"}}}, nil
		}}
		mu.Lock()
		clients = append(clients, c)
		mu.Unlock()
		return c, nil
	}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("cam", newC, mcp.WithSerialCalls()))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	tl := h.Tools()[0]
	if res, _ := tl.Call(context.Background(), map[string]any{}); res == nil || res.Error == nil {
		t.Fatalf("fixture 没搭对：第一次应被兜成失败结果")
	}
	time.Sleep(150 * time.Millisecond) // 等后台重连
	_, _ = tl.Call(context.Background(), map[string]any{})

	if n := built.Load(); n < 2 {
		t.Fatalf("串行 server 上 panic 后应换连接（旧连接那边可能还在执行），工厂只建了 %d 个", n)
	}
	// 承重的断言是「第二次调用没有发给那条状态不明的旧连接」。
	//
	// ⚠️ 不要断言「硬件并发峰值 <= 1」：模拟「硬件还在动」要让 panic 那次永不收尾，于是它永久占着
	// 一个槽，换连接之后峰值必然是 2——那个量在 bug 发生与修复后都是 2，不承重（第一版就写错了，实测
	// 修复后照样红）。旧连接收到几次调用才会随这个 bug 变。
	mu.Lock()
	first := clients[0]
	mu.Unlock()
	if got := len(first.calls()); got != 1 {
		t.Errorf("第二次调用被发给了那条状态不明的旧连接：它累计收到 %d 次（应只有 panic 那一次）", got)
	}
}

// nilMethodErr 的 Error() 是**值接收者** ⟹ *nilMethodErr 也有 Error()，而对 typed-nil 指针调它会解引用
// nil。这是 CLAUDE.md 点名的 typed-nil 陷阱：`err != nil` 为真，调它的方法却 panic。业务自定义 Client
// 返回这样的 error 完全可能（`var e *myErr; return nil, e`）。
type nilMethodErr struct{ msg string }

func (e nilMethodErr) Error() string { return e.msg }

func typedNilError() error {
	var e *nilMethodErr
	return e // 作为 error 非 nil
}

// TestHub_BusinessErrorMethodPanic_ContainedAndCloseNotStuck：业务 Client 返回的 **error 的方法** panic
// 时，① 不冲出 RemoteTool.Call（契约：只在调用方 ctx 结束时返回 error，其余一律 tool.Failure），
// ② 不把 Hub.Close 挂死。
//
// 真入口只有一个：markOffline 里原先的 `slog.Warn(…, cause.Error())`。它夹在 bg.Add(1) 与
// go reconnectLoop 之间，panic 留下一个「Add 了却没有 Done」的 WaitGroup ⟹ Close 的 bg.Wait() 永久挂住。
// 两层后果这条用例各有一个断言。
//
// ⚠️ 断言必须同时有「panic 没冲出」**和**「Close 在看门狗内返回」：panic 之前失败结果就已经算好了，
// 只看返回值看不出 Close 已经废了。
//
// 顺带钉 ctx 路径的错误链：那条路按契约**可以**返回 error（调用方 ctx 结束了），但 ctx 的原因与业务 error
// 都得留在链上，而且读它的文本不许 panic（`fmt.Errorf("%w")` 本来就安全——fmt 自己兜住 Error() 的 panic；
// 这条断言防的是有人把它改成手工拼接或 `%s`，那会断链）。
func TestHub_BusinessErrorMethodPanic_ContainedAndCloseNotStuck(t *testing.T) {
	// cancelDuringCall 在 CallTool **执行期间**取消调用方 ctx。⚠️ 提前取消的话 call 在排队那一步
	// （select on waitCtx.Done()）就返回了，根本走不到 c.CallTool、业务 error 从未产生——第一版就是这么
	// 搭的，两个版本都绿（变异验证当场暴露）。
	newHub := func(cancelDuringCall func()) (*mcp.Hub, *mcp.RemoteTool) {
		t.Helper()
		c := &memClient{name: "s", tools: specs("t"), onCall: func(context.Context, string, map[string]any) (*mcp.CallResult, error) {
			if cancelDuringCall != nil {
				cancelDuringCall()
			}
			return nil, typedNilError()
		}}
		h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("s",
			func(context.Context) (mcp.Client, error) { return c, nil }, mcp.WithSerialCalls()))
		if err := h.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return h, h.Tools()[0]
	}
	assertCloseNotStuck := func(t *testing.T, h *mcp.Hub) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- h.Close() }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Hub.Close 挂死了：bg.Add(1) 已加而 reconnectLoop 没起，bg.Done() 永不调")
		}
	}

	t.Run("markOffline_的日志不许直接调Error", func(t *testing.T) {
		h, tl := newHub(nil)
		escaped := func() (esc any) {
			defer func() { esc = recover() }()
			res, err := tl.Call(context.Background(), map[string]any{})
			if err != nil || res == nil || res.Error == nil {
				t.Errorf("该以 tool.Failure 交回，got res=%+v err=%v", res, err)
			}
			return nil
		}()
		if escaped != nil {
			t.Errorf("panic 冲出了 RemoteTool.Call：%v", escaped)
		}
		assertCloseNotStuck(t, h)
	})

	t.Run("ctx路径的错误链完整且读文本不panic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		h, tl := newHub(cancel)
		escaped := func() (esc any) {
			defer func() { esc = recover() }()
			_, err := tl.Call(ctx, map[string]any{})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("调用方 ctx 取消应留在错误链上，got %v", err)
			}
			if err != nil {
				_ = err.Error() // 读一次文本：不许 panic
			}
			return nil
		}()
		if escaped != nil {
			t.Errorf("panic 冲出了 RemoteTool.Call：%v", escaped)
		}
		assertCloseNotStuck(t, h)
	})
}

// TestMCP_NoDirectErrorMethodCallInLogs：机械对账 —— **本包不许把 error 的 Error() 直接调来喂日志**。
//
// 为什么这是一条机械哨兵而不是一句注释：error 的 Error() 可能是**业务代码**（业务自定义 Client 返回的
// error），CLAUDE.md 点名的 typed-nil 陷阱下调它就 panic。而框架里「哪些 error 是业务的」会随重构漂移
// ——`hub.go` 那两处与 reconnectLoop 那处当前安全，只因为它们的 err 恰好先被 connect 的
// `fmt.Errorf("%s: initialize: %w", …)` 包装过；有人改了 connect 不包装，它们立刻变成新入口。
//
// 实测的边界（go1.26）：
//   - `slog.Warn(…, "err", err)`（传**值**）、`fmt.Errorf("…%w", err)`、`fmt.Sprintf("%v", err)` 都**安全**
//     ——内部经 fmt 格式化，而 fmt 对 Error() 的 panic 自己兜住、渲染成 `%!v(PANIC=Error method: …)`；
//     对 typed-nil 指针更是直接打 `<nil>`、连 Error() 都不调。`errors.Is` / `errors.As` 也照常成立。
//   - 只有**直接调** `err.Error()` 会让 panic 冲出去。
//
// 所以规矩很简单：传值，别调。
func TestMCP_NoDirectErrorMethodCallInLogs(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	// 形如 `logKeyErr, <任意>.Error()` —— 把 error 的文本直接取出来喂日志。
	bad := regexp.MustCompile(`logKeyErr,\s*[^,)]*\.Error\(\)`)
	var hits []string
	scanned := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		scanned++
		for i, line := range strings.Split(string(b), "\n") {
			if bad.MatchString(line) {
				hits = append(hits, fmt.Sprintf("%s:%d: %s", f, i+1, strings.TrimSpace(line)))
			}
		}
	}
	if scanned < 5 {
		t.Fatalf("只扫到 %d 个生产文件——扫描面坏了，不是真的这么少", scanned)
	}
	if len(hits) > 0 {
		t.Errorf("这些地方把 error 的 Error() 直接调出来喂日志了，改成传 error 本身（slog 内部经 fmt，"+
			"对 Error() 的 panic 自己兜住）：\n  %s", strings.Join(hits, "\n  "))
	}
}

// ignoreCtxFactory 是**建连接时不看 ctx** 的工厂：第 1 次直接给 first，第 2 次先等 gate（故意只等 gate、
// 不 select ctx.Done()）再给 second。
//
// 为什么不能用 seqFactory：它的 gate 是 `select { gate; ctx.Done() }`，而 Hub.Close 会取消重连尝试的
// ctx ⟹ 放行 gate 与取消是竞态，多数时候走 ctx 分支、连接根本建不出来，于是**根本到不了 goOnline**，
// 下面那条不变量就测不到（第 6 类炸弹：fixture 没搭对 ⟹ 测出「缺陷不存在」）。业务工厂建连接时不检查
// ctx 是真实世界的常态，这个形状不是人为的。
type ignoreCtxFactory struct {
	mu            sync.Mutex
	n             int
	gate          chan struct{}
	first, second mcp.Client
}

func (f *ignoreCtxFactory) New(context.Context) (mcp.Client, error) {
	f.mu.Lock()
	f.n++
	n := f.n
	f.mu.Unlock()
	if n == 1 {
		return f.first, nil
	}
	<-f.gate
	return f.second, nil
}

func (f *ignoreCtxFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// TestHub_ReconnectFinishingAfterClose_DiscardsNewClient：重连在 Hub.Close 之后才连上时，那个新连接
// 必须被关掉、不能挂到已关闭的 server 上。
//
// 守的是 `server.goOnline` 开头的 `if s.closed { discard }` 早退。缺它的话：Close 已经把 server 标死、
// 已有连接也关了，而重连循环随后把一个**活着的**连接写进 `s.client` 并标 online ⟹ 那个连接**永远没人
// 关**（Hub 已经关完了，不会再有第二次 Close 去捡它），stdio 型就是一个留在系统里的子进程；而 Hub 的
// 状态还会报 online，调用方照 `Status` 判断会以为这个 server 可用。
//
// ⚠️ 这条不变量**有时间窗**，所以 fixture 要把窗口撑开：工厂卡在第 2 次建连接上 → Close 进到
// `bg.Wait()`（它在等重连循环退出）→ 这时才放行工厂 ⟹ goOnline 必然发生在 s.closed 之后。
// 断言选在「新连接被关了没」上——那是只有早退才给的那个量（Close 关的是**掉线前**那个连接，
// 数它的次数抓不到这个 bug）。
func TestHub_ReconnectFinishingAfterClose_DiscardsNewClient(t *testing.T) {
	var broken atomic.Bool
	first := &memClient{name: "dev", tools: specs("run"), onCall: func(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
		if broken.Load() {
			return nil, errDown
		}
		return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "ok"}}}, nil
	}}
	second := &memClient{name: "dev", tools: specs("run")}
	f := &ignoreCtxFactory{gate: make(chan struct{}), first: first, second: second}
	h := mcp.NewHub(fastBackoff(), mcp.WithClientFactory("dev", f.New))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// 打掉连接 → 标离线 → 重连循环起来，卡在工厂第 2 次建连接上
	broken.Store(true)
	wantFailure(t, call(t, h, "mcp_dev_run"), mcp.ErrCodeServerUnavailable, "")
	waitFor(t, "重连尝试开始", func() bool { return f.count() >= 2 })

	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()

	// 等 Close 真的把 server 标死（调用返「已关闭」即 s.closed 已置）——**必须等到这一步再放行工厂**，
	// 否则连接可能在 closed 之前就挂上去了，那条早退根本不参与，用例恒真。
	waitFor(t, "Close 已把 server 标死", func() bool {
		out, err := h.Toolkit().Call(context.Background(), "mcp_dev_run", map[string]any{"q": "x"})
		return err == nil && out != nil && out.Error != nil && strings.Contains(out.Error.Message, "已关闭")
	})
	close(f.gate) // 现在才放行：goOnline 必然在 s.closed 之后发生

	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 挂住了——重连循环的 bg.Done 没被等到")
	}

	waitFor(t, "Close 之后连上的那个连接被关掉", func() bool { return second.closed.Load() >= 1 })
	if n := second.closed.Load(); n != 1 {
		t.Errorf("Close 之后才连上的连接应被关掉恰好 1 次，实际 %d 次——0 次 = 它挂在了已关闭的 server 上、"+
			"永远没人关（stdio 型是一个留在系统里的子进程）", n)
	}
	if st := statusOf(t, h, "dev"); st.State != mcp.ServerOffline {
		t.Errorf("Hub 已关闭，server 状态应是离线，实际 %v——重连把它标回 online 了，"+
			"调用方照 Status 判断会以为这个 server 还能用", st.State)
	}
}
