package agent_v2

import (
	"context"
	"slices"
	"sync"
	"testing"

	"crew-go/schema"
	"crew-go/tool"
)

func tsTool(name, desc string) tool.Tool {
	return tool.NewFuncTool(name, desc, map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		})
}

// TestFilterVisibleTools：核心强制——非延迟恒可见、已揭示延迟工具可见、未揭示延迟工具隐藏。
func TestFilterVisibleTools(t *testing.T) {
	infos := []*schema.ToolInfo{{Name: "static"}, {Name: "def1"}, {Name: "def2"}}
	deferredNames := map[string]bool{"def1": true, "def2": true}
	rs := newRevealedNames([]string{"def1"}) // 只揭示 def1
	got := map[string]bool{}
	for _, i := range filterVisibleTools(infos, deferredNames, rs) {
		got[i.Name] = true
	}
	if !got["static"] {
		t.Error("非延迟 static 应恒可见")
	}
	if !got["def1"] {
		t.Error("已揭示 def1 应可见")
	}
	if got["def2"] {
		t.Error("未揭示 def2 应隐藏（不进 bind list）")
	}
}

// TestFilterVisibleTools_NoDeferred：无 deferred 名单 → 原样返（关闭效果）。
func TestFilterVisibleTools_NoDeferred(t *testing.T) {
	infos := []*schema.ToolInfo{{Name: "a"}, {Name: "b"}}
	if out := filterVisibleTools(infos, nil, nil); len(out) != 2 {
		t.Errorf("无 deferred 应原样返 2，实际 %d", len(out))
	}
}

// TestRevealTurn_Concurrent：这一批的解锁记录并发记（-race 验 ISSUE-81 fanout 多个 tool_search 同时跑）。
func TestRevealTurn_Concurrent(t *testing.T) {
	turn := &revealTurn{}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			turn.add("t")
			_ = turn.list()
		}()
	}
	wg.Wait()
	if got := turn.list(); len(got) != 16 {
		t.Errorf("并发记 16 次应有 16 条，实际 %d", len(got))
	}
}

// TestNewlyRevealed：写进已解锁集的只有之前没解锁过的名字，去重、保持先后——检索到早已解锁的工具、同一批两次
// 检索命中同一个工具，都不该让已解锁集多一份，也不该让这一批被当成「解锁了新工具」。
func TestNewlyRevealed(t *testing.T) {
	cases := []struct{ names, have, want []string }{
		{nil, nil, nil},
		{[]string{"a", "b"}, nil, []string{"a", "b"}},
		{[]string{"a", "b"}, []string{"a"}, []string{"b"}},
		{[]string{"b", "a", "b"}, nil, []string{"b", "a"}},
		{[]string{"a", "a"}, []string{"a"}, nil},
	}
	for _, c := range cases {
		if got := newlyRevealed(c.names, c.have); !slices.Equal(got, c.want) {
			t.Errorf("newlyRevealed(%v, %v) = %v，应为 %v", c.names, c.have, got, c.want)
		}
	}
}

// TestDefaultToolRetriever：默认关键词检索匹配 + 按分降序 + MaxResults 截断。
func TestDefaultToolRetriever(t *testing.T) {
	tools := []tool.Tool{
		tsTool("get_weather", "查天气预报"),
		tsTool("send_email", "发送邮件"),
		tsTool("read_file", "读取文件"),
	}
	got := defaultToolRetriever(0, 5)(context.Background(), "天气", tools)
	if len(got) != 1 || got[0] != "get_weather" {
		t.Errorf("查「天气」应只匹配 get_weather，实际 %v", got)
	}
}

func TestDefaultToolRetriever_MaxResults(t *testing.T) {
	tools := []tool.Tool{tsTool("file_read", "file"), tsTool("file_write", "file"), tsTool("file_list", "file")}
	if got := defaultToolRetriever(0, 2)(context.Background(), "file", tools); len(got) != 2 {
		t.Errorf("MaxResults=2 应截断到 2，实际 %d（%v）", len(got), got)
	}
}

// TestCollectDeferred：按 Deferred 谓词从 toolkit 挑出延迟工具。
func TestCollectDeferred(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tsTool("keep", "static"))
	tk.Register(tsTool("mcp_x", "deferred"))
	cfg := &ToolSearchConfig{Enabled: true, Deferred: func(tt tool.Tool) bool { return tt.Name() == "mcp_x" }}
	def := cfg.collectDeferred(tk)
	if len(def) != 1 || def[0].Name() != "mcp_x" {
		t.Errorf("应只收 mcp_x，实际 %v", def)
	}
}

// TestCollectDeferred_Disabled：未启用 / Deferred nil → 空（不延迟任何）。
func TestCollectDeferred_Disabled(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tsTool("mcp_x", "x"))
	if def := (&ToolSearchConfig{Enabled: false, Deferred: func(tool.Tool) bool { return true }}).collectDeferred(tk); len(def) != 0 {
		t.Errorf("Enabled=false 应不延迟，实际 %d", len(def))
	}
	if def := (&ToolSearchConfig{Enabled: true, Deferred: nil}).collectDeferred(tk); len(def) != 0 {
		t.Errorf("Deferred=nil 应不延迟，实际 %d", len(def))
	}
}

// TestCollectDeferred_ReservesToolSearchName：业务工具误叫 tool_search 且被 Deferred 命中时，
// collectDeferred 必须排除它（保留名给 meta-tool）——否则 filterVisibleTools 会把 meta-tool 自己
// 过滤出 bind list → LLM 永远调不了 tool_search = 静默死锁。
func TestCollectDeferred_ReservesToolSearchName(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tsTool("tool_search", "业务误用保留名"))
	tk.Register(tsTool("mcp_x", "真 deferred"))
	cfg := &ToolSearchConfig{Enabled: true, Deferred: func(tool.Tool) bool { return true }} // 全延迟
	def := cfg.collectDeferred(tk)
	for _, d := range def {
		if d.Name() == toolSearchToolName {
			t.Fatal("collectDeferred 必须排除 tool_search 保留名（防 meta-tool 被过滤死锁）")
		}
	}
	if len(def) != 1 || def[0].Name() != "mcp_x" {
		t.Errorf("应只剩 mcp_x，实际 %v", def)
	}
}
