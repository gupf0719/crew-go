package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"crew-go/mcp"
)

// tools/list 分页时按 nextCursor 翻到底：服务端一分页，第二页起的工具原来静默丢失、模型永远看不到。

// pagedServer 按 pages 分页返回工具；cursorOf 决定第 i 页之后给什么 nextCursor（最后一页给 ""）。
func pagedServer(t *testing.T, pages [][]string, cursorOf func(i int) string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Method {
		case "initialize":
			writeResult(w, req.ID, map[string]any{"protocolVersion": "2025-03-26"})
		case "tools/list":
			page := 0
			if c, _ := req.Params["cursor"].(string); c != "" {
				for i := range pages {
					if cursorOf(i) == c {
						page = i + 1
						break
					}
				}
			}
			tools := make([]map[string]any, 0, len(pages[page]))
			for _, n := range pages[page] {
				tools = append(tools, map[string]any{"name": n, "inputSchema": map[string]any{"type": "object"}})
			}
			res := map[string]any{"tools": tools}
			if page < len(pages)-1 {
				res["nextCursor"] = cursorOf(page)
			}
			writeResult(w, req.ID, res)
		default:
			http.Error(w, "unknown method", http.StatusBadRequest)
		}
	}))
}

func TestListTools_FollowsNextCursor(t *testing.T) {
	srv := pagedServer(t, [][]string{{"a", "b"}, {"c"}, {"d", "e"}}, func(i int) string { return "page-" + string(rune('1'+i)) })
	defer srv.Close()
	c, err := mcp.NewStreamableHTTPClient("paged", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	specs, err := c.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	names := make([]string, 0, len(specs))
	for _, s := range specs {
		names = append(names, s.Name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "a,b,c,d,e" {
		t.Errorf("三页的工具都该拉到，got %v", names)
	}
}

func TestListTools_RepeatedCursorIsError(t *testing.T) {
	// 服务端每页都给同一个 cursor：再翻会一直重复，报错而不是死循环或返回半截
	srv := pagedServer(t, [][]string{{"a"}, {"b"}, {"c"}}, func(int) string { return "same" })
	defer srv.Close()
	c, err := mcp.NewStreamableHTTPClient("loop", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListTools(context.Background()); err == nil || !strings.Contains(err.Error(), "已经用过的 cursor") {
		t.Errorf("重复的 cursor 应报错，got %v", err)
	}
}
