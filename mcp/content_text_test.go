package mcp_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"crew-go/mcp"
	"crew-go/tool"
)

// callWith 起一个只有一个工具 t 的 HTTP MCP server，tools/call 回 result，返回框架拿到的 Output。
func callWith(t *testing.T, result map[string]any) any {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-06-18"})
		case "tools/list":
			writeResult(w, req.ID, map[string]any{"tools": []map[string]any{{"name": "t", "inputSchema": map[string]any{"type": "object"}}}})
		case "tools/call":
			writeResult(w, req.ID, result)
		default:
			http.Error(w, "unknown method", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	c, err := mcp.NewStreamableHTTPClient("s", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := mcp.NewHub(mcp.WithClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	res, err := h.Toolkit().Call(context.Background(), "mcp_s_t", map[string]any{})
	if err != nil || !res.Success {
		t.Fatalf("Call = %+v, %v", res, err)
	}
	return res.Output
}

// TestCallContent_Text：各类内容块按原顺序整理成文本——嵌入资源取出正文、资源链接写一行、二进制只写说明，
// 不把字节交给模型（以前打包成 map，按 %v 打印出来是一长串数字）。
func TestCallContent_Text(t *testing.T) {
	png := base64.StdEncoding.EncodeToString([]byte("\x89PNG fake image bytes"))
	got := callWith(t, map[string]any{"content": []map[string]any{
		{"type": "text", "text": "截图如下"},
		{"type": "image", "data": png, "mimeType": "image/png"},
		{"type": "resource", "resource": map[string]any{"uri": "file:///repo/README.md", "mimeType": "text/markdown", "text": "# deskbot"}},
		{"type": "resource", "resource": map[string]any{"uri": "file:///a.bin", "mimeType": "application/octet-stream", "blob": png}},
		{"type": "resource_link", "uri": "file:///repo/go.mod", "name": "go.mod"},
		{"type": "audio", "data": png, "mimeType": "audio/wav"},
	}})
	want := "截图如下\n\n" +
		"[image，image/png，21 字节：二进制内容，没有交给模型]\n\n" +
		"[资源 file:///repo/README.md]\n# deskbot\n\n" +
		"[resource，application/octet-stream，21 字节，file:///a.bin：二进制内容，没有交给模型]\n\n" +
		"[资源链接 go.mod file:///repo/go.mod]\n\n" +
		"[audio，audio/wav，21 字节：二进制内容，没有交给模型]"
	if got != want {
		t.Errorf("Output =\n%q\n期望\n%q", got, want)
	}
}

// TestCallContent_Structured：有 structuredContent 时交出解出来的值（BasicAgent 用 tool.OutputText 编成 JSON 给模型）。
func TestCallContent_Structured(t *testing.T) {
	got := callWith(t, map[string]any{
		"content":           []map[string]any{{"type": "text", "text": `{"total":1}`}},
		"structuredContent": map[string]any{"total": 1},
	})
	if m, ok := got.(map[string]any); !ok || m["total"] != json.Number("1") {
		t.Errorf("Output = %#v，期望 structuredContent 解出来的 map（数字是 json.Number）", got)
	}
}

// TestCallContent_StructuredBigIntExact：超过 2^53 的整数原样交给模型，不变成 float64 的近似值。
func TestCallContent_StructuredBigIntExact(t *testing.T) {
	got := callWith(t, map[string]any{"structuredContent": json.RawMessage(`{"id":1234567890123456789,"ratio":0.1}`)})
	if s := tool.OutputText(got); s != `{"id":1234567890123456789,"ratio":0.1}` {
		t.Errorf("交给模型的是 %s", s)
	}
}

// TestCallContent_BadBase64DoesNotFailCall：二进制块的 base64 不规范时不让整次调用失败——同一次结果里的文本照常交出。
func TestCallContent_BadBase64DoesNotFailCall(t *testing.T) {
	got := callWith(t, map[string]any{"content": []map[string]any{
		{"type": "text", "text": "文件在这"},
		{"type": "resource", "resource": map[string]any{"uri": "file:///a.bin", "blob": "AQID_w"}},                         // URL-safe、缺 padding：认
		{"type": "image", "data": "!!!不是base64", "mimeType": "image/png"},                                                  // 解不了：只写说明
		{"type": "resource", "resource": map[string]any{"uri": "file:///empty.txt", "mimeType": "text/plain", "text": ""}}, // 空文件
	}})
	want := "文件在这\n\n" +
		"[resource，4 字节，file:///a.bin：二进制内容，没有交给模型]\n\n" +
		"[image，image/png，base64 解不了：二进制内容，没有交给模型]\n\n" +
		"[资源 file:///empty.txt]\n（空）"
	if got != want {
		t.Errorf("Output =\n%q\n期望\n%q", got, want)
	}
}
