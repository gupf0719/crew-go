package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/mcp"
	"crew-go/tool"
)

// --- fakeClient：测试用内存 MCP 客户端实现 ---

type fakeClient struct {
	name       string
	tools      []mcp.ToolSpec
	results    map[string]*mcp.CallResult
	initErr    error
	listErr    error
	callErr    error
	initCount  atomic.Int32
	closeCount atomic.Int32
}

func (f *fakeClient) Name() string                       { return f.name }
func (f *fakeClient) Initialize(_ context.Context) error { f.initCount.Add(1); return f.initErr }
func (f *fakeClient) ListTools(_ context.Context) ([]mcp.ToolSpec, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.tools, nil
}
func (f *fakeClient) CallTool(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	if r, ok := f.results[name]; ok {
		return r, nil
	}
	return &mcp.CallResult{IsError: true, Content: []mcp.Block{{Type: "text", Text: "not found"}}}, nil
}
func (f *fakeClient) Close() error { f.closeCount.Add(1); return nil }

// --- helpers ---

func toolNames(tk *tool.Toolkit) []string {
	tools := tk.List()
	out := make([]string, 0, len(tools))
	for _, t := range tools {
		out = append(out, t.Name())
	}
	sort.Strings(out)
	return out
}

// --- 单测 ---

func TestCanonicalName(t *testing.T) {
	cases := []struct {
		clientName string
		source     string
		want       string
	}{
		{"fs", "read_file", "mcp_fs_read_file"},
		{"my-server", "do-thing", "mcp_my_server_do_thing"},
		{"server with space", "tool", "mcp_server_with_space_tool"},
		{"", "x", "mcp_tool_x"},
	}
	for _, c := range cases {
		got := mcp.CanonicalName(c.clientName, c.source)
		if got != c.want {
			t.Errorf("CanonicalName(%q,%q) = %q, want %q", c.clientName, c.source, got, c.want)
		}
	}
}

func TestHubStartAggregatesTools(t *testing.T) {
	a := &fakeClient{
		name:  "a",
		tools: []mcp.ToolSpec{{Name: "ping", Description: "ping a"}},
	}
	b := &fakeClient{
		name:  "b",
		tools: []mcp.ToolSpec{{Name: "ping", Description: "ping b"}, {Name: "echo", Description: "echo b"}},
	}
	h := mcp.NewHub(mcp.WithClient(a), mcp.WithClient(b))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	want := []string{"mcp_a_ping", "mcp_b_echo", "mcp_b_ping"}
	got := toolNames(h.Toolkit())
	if !slices.Equal(got, want) {
		t.Errorf("tools = %v, want %v", got, want)
	}

	// Start 幂等。
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
}

func TestHubStartFailureRollsBackClose(t *testing.T) {
	good := &fakeClient{name: "good", tools: []mcp.ToolSpec{{Name: "ok"}}}
	bad := &fakeClient{name: "bad", initErr: errors.New("boom")}
	h := mcp.NewHub(mcp.WithClient(good), mcp.WithClient(bad))

	err := h.Start(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// 两个 client 都应被 Close（回滚）。
	if good.closeCount.Load() < 1 {
		t.Error("good client should have been closed on rollback")
	}
	if bad.closeCount.Load() < 1 {
		t.Error("bad client should have been closed on rollback")
	}
}

func TestRemoteToolCallSuccessStructured(t *testing.T) {
	fc := &fakeClient{
		name:  "srv",
		tools: []mcp.ToolSpec{{Name: "add", InputSchema: map[string]any{"type": "object"}}},
		results: map[string]*mcp.CallResult{
			"add": {Structured: map[string]any{"sum": 3}},
		},
	}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	res, err := h.Toolkit().Call(context.Background(), "mcp_srv_add", map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got %+v", res)
	}
	m, ok := res.Output.(map[string]any)
	if !ok || m["sum"] != 3 {
		t.Errorf("unexpected output: %v", res.Output)
	}
}

func TestRemoteToolCallIsErrorMapsFailure(t *testing.T) {
	fc := &fakeClient{
		name:  "srv",
		tools: []mcp.ToolSpec{{Name: "boom"}},
		results: map[string]*mcp.CallResult{
			"boom": {IsError: true, Content: []mcp.Block{{Type: "text", Text: "server side error"}}},
		},
	}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	res, err := h.Toolkit().Call(context.Background(), "mcp_srv_boom", nil)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if res.Success {
		t.Fatal("expected failure result")
	}
	if res.Error == nil {
		t.Fatal("expected error field")
	}
	if res.Error.Code != tool.ErrCodeInternal {
		t.Errorf("code = %q, want %q", res.Error.Code, tool.ErrCodeInternal)
	}
	if res.Error.Message != "server side error" {
		t.Errorf("message = %q", res.Error.Message)
	}
}

func TestRemoteToolCallTransportErrorPropagates(t *testing.T) {
	fc := &fakeClient{
		name:    "srv",
		tools:   []mcp.ToolSpec{{Name: "t"}},
		callErr: errors.New("conn reset"),
	}
	h := mcp.NewHub(mcp.WithClient(fc))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, err := h.Toolkit().Call(context.Background(), "mcp_srv_t", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- HTTP 集成测：httptest.Server 模拟 MCP server ---

type rpcReq struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int64          `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

func writeResult(w http.ResponseWriter, id int64, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func TestStreamableHTTPClientEndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-03-26"})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{
				"tools": []map[string]any{
					{"name": "echo", "description": "echo back", "inputSchema": map[string]any{"type": "object"}},
				},
			})
		case "tools/call":
			args, _ := req.Params["arguments"].(map[string]any)
			who, _ := args["who"].(string)
			writeResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "hello " + who}},
			})
		default:
			http.Error(w, "unknown method", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	c, err := mcp.NewStreamableHTTPClient("http", server.URL)
	if err != nil {
		t.Fatalf("NewStreamableHTTPClient: %v", err)
	}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()

	res, err := h.Toolkit().Call(context.Background(), "mcp_http_echo", map[string]any{"who": "world"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success: %+v", res)
	}
	if res.Output != "hello world" {
		t.Errorf("output = %v, want 'hello world'", res.Output)
	}
}

func TestStreamableHTTPClientHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	c, err := mcp.NewStreamableHTTPClient("x", server.URL)
	if err != nil {
		t.Fatalf("NewStreamableHTTPClient: %v", err)
	}
	if err := c.Initialize(context.Background()); err == nil {
		t.Fatal("expected error on HTTP 500, got nil")
	}
}

func TestAllowToolsFilter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{
				"tools": []map[string]any{{"name": "a"}, {"name": "b"}, {"name": "c"}},
			})
		}
	}))
	defer server.Close()

	c, err := mcp.NewStreamableHTTPClient("x", server.URL, mcp.WithHTTPAllowTools("a", "c"))
	if err != nil {
		t.Fatalf("NewStreamableHTTPClient: %v", err)
	}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	want := []string{"mcp_x_a", "mcp_x_c"}
	got := toolNames(h.Toolkit())
	if !slices.Equal(got, want) {
		t.Errorf("filtered tools = %v, want %v", got, want)
	}
}

// TestHubStartFailsWhenListToolsErrors 守「ListTools 报错必须让 Start 失败」。
//
// # 为什么单独一条
//
// `fakeClient` 早就有 `listErr` 字段和读取逻辑，**但全仓从没有任何地方设过它**——
// 于是 `Hub.ListTools` 那条错误路径零覆盖：把它的 `if err != nil` 整个吞掉，
// `go test ./... -count=1` **全仓全绿**（实测）。
//
// 这是「fixture 上摆着开关、从没被拨过」那一类假绿：同一个 fixture 的 `initErr`
// 有用例守（TestHubStartFailureRollsBackClose），`listErr` 一直裸着。
//
// 业务侧的实际后果：某个 MCP server 的 ListTools 失败被静默吞掉 ⟹ Hub 认为它
// 没有工具、Start 照常成功 ⟹ LLM 看不到那批工具，而没有任何错误提示。
//
// sanity-revert：`mcp/hub.go` 的 `ListTools` 错误分支删掉（`tools, _ := ...`）→ 本用例红。
func TestHubStartFailsWhenListToolsErrors(t *testing.T) {
	good := &fakeClient{name: "good", tools: []mcp.ToolSpec{{Name: "ok"}}}
	bad := &fakeClient{name: "bad", listErr: errors.New("list boom")}
	h := mcp.NewHub(mcp.WithClient(good), mcp.WithClient(bad))

	err := h.Start(context.Background())
	if err == nil {
		t.Fatal("ListTools 报错时 Start 该失败——吞掉它会让那批工具静默消失，LLM 看不到且无提示")
	}
	if !strings.Contains(err.Error(), "list boom") {
		t.Errorf("错误该带上原因，实际：%v", err)
	}
	// 回滚语义跟 initErr 那条一致：两个 client 都该被关掉
	if good.closeCount.Load() < 1 {
		t.Error("good client 该在回滚时被关闭")
	}
}
