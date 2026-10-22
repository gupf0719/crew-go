package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"crew-go/mcp"
	"crew-go/tool"
)

func boolPtr(b bool) *bool { return &b }

// server 自述只读的工具：只有经 WithTrustedClient 注册时才算只读；经 WithClient 注册时注解照样解析、
// 读得到，但不算只读（MCP 规范：不能凭不受信任 server 的注解做决定）。
func TestRemoteTool_ReadOnlyOnlyWhenTrusted(t *testing.T) {
	spec := mcp.ToolSpec{Name: "lookup", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(true)}}
	for _, tc := range []struct {
		name    string
		trusted bool
		want    bool
	}{
		{"untrusted", false, false},
		{"trusted", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClient{name: "srv_" + tc.name, tools: []mcp.ToolSpec{spec}}
			opt := mcp.WithClient(c)
			if tc.trusted {
				opt = mcp.WithTrustedClient(c)
			}
			h := mcp.NewHub(opt)
			if err := h.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer h.Close()
			rt := h.Tools()[0]
			if got := tool.IsReadOnly(rt); got != tc.want {
				t.Errorf("IsReadOnly = %v, want %v", got, tc.want)
			}
			if a := rt.Annotations(); a == nil || a.ReadOnlyHint == nil || !*a.ReadOnlyHint {
				t.Errorf("注解该照样解析、读得到，实际 %+v", a)
			}
		})
	}
}

// 受信任的 server 也只在明确声明只读、且没自相矛盾时才算只读。
func TestRemoteTool_ReadOnlyHintEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name string
		ann  *mcp.ToolAnnotations
		want bool
	}{
		{"没有注解", nil, false},
		{"没给 readOnlyHint", &mcp.ToolAnnotations{Title: "x"}, false},
		{"readOnlyHint=false", &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(false)}, false},
		{"只读又有破坏性（自相矛盾）", &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(true), DestructiveHint: boolPtr(true)}, false},
		{"只读且明说无破坏性", &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(true), DestructiveHint: boolPtr(false)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClient{name: "srv", tools: []mcp.ToolSpec{{Name: "t", Annotations: tc.ann}}}
			h := mcp.NewHub(mcp.WithTrustedClient(c))
			if err := h.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer h.Close()
			if got := tool.IsReadOnly(h.Tools()[0]); got != tc.want {
				t.Errorf("IsReadOnly = %v, want %v", got, tc.want)
			}
		})
	}
}

// tools/list 里的 annotations 经真实的 JSON-RPC 解析进 ToolSpec。
func TestStreamableHTTPClient_ParsesAnnotations(t *testing.T) {
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
					{"name": "search", "inputSchema": map[string]any{"type": "object"},
						"annotations": map[string]any{"title": "搜索", "readOnlyHint": true, "openWorldHint": false}},
					{"name": "delete", "inputSchema": map[string]any{"type": "object"}},
				},
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
	h := mcp.NewHub(mcp.WithTrustedClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Close()
	got := map[string]bool{}
	for _, rt := range h.Tools() {
		got[rt.SourceName()] = tool.IsReadOnly(rt)
		if rt.SourceName() == "search" {
			a := rt.Annotations()
			if a == nil || a.Title != "搜索" || a.OpenWorldHint == nil || *a.OpenWorldHint {
				t.Errorf("search 的注解没解析全：%+v", a)
			}
		}
	}
	if !got["search"] || got["delete"] {
		t.Errorf("search 该算只读、delete（没注解）不该，实际 %v", got)
	}
}

// 一个工具的注解字段类型不对（readOnlyHint 给了字符串）：丢掉它的注解（按没有注解处理、不算只读），别的工具
// 照常可用——注解只是提示，不能让一个写错的提示把整个 server 的工具列表废掉。
//
// sanity-revert：注解按强类型一起解 ⟹ tools/list 整份解析失败、Hub.Start 报错，本用例红。
func TestStreamableHTTPClient_MalformedAnnotationsDropped(t *testing.T) {
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
					{"name": "good", "inputSchema": map[string]any{"type": "object"},
						"annotations": map[string]any{"readOnlyHint": true}},
					{"name": "sloppy", "inputSchema": map[string]any{"type": "object"},
						"annotations": map[string]any{"readOnlyHint": "true"}},
				},
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
	h := mcp.NewHub(mcp.WithTrustedClient(c))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("一个工具的注解写错不该让整个 server 起不来：%v", err)
	}
	defer h.Close()
	got := map[string]bool{}
	for _, rt := range h.Tools() {
		got[rt.SourceName()] = tool.IsReadOnly(rt)
		if rt.SourceName() == "sloppy" && rt.Annotations() != nil {
			t.Errorf("注解解析失败的工具该按没有注解处理，实际 %+v", rt.Annotations())
		}
	}
	if len(got) != 2 {
		t.Fatalf("两个工具都该可用，实际 %v", got)
	}
	if !got["good"] || got["sloppy"] {
		t.Errorf("good 该算只读、sloppy（注解解析失败）不该，实际 %v", got)
	}
}
