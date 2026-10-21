package sql

import (
	"testing"

	"crew-go/store"
)

// TestSplitNS_Valid_Roundtrips 验合法 ns string 正常 round-trip.
//
// **iter-5-I Option A**: 删 iter-4 H4 段验证 (NUL byte / 空段) 双层过度防御 —
// 跟 LangGraph `_decode_ns_bytes` (写入 fail-fast / 读出 0 验证) 设计对齐.
// 写入端 `store.Namespace.Validate()` 已防非法 ns 入库, 读出端信任 DB.
func TestSplitNS_Valid_Roundtrips(t *testing.T) {
	cases := []struct {
		joined string
		want   store.Namespace
	}{
		{"users", store.Namespace{"users"}},
		{"users.u1", store.Namespace{"users", "u1"}},
		{"users.u1.memories", store.Namespace{"users", "u1", "memories"}},
		{"", nil}, // 空 string 返 nil (joined == "" 早退)
	}
	for _, c := range cases {
		ns := splitNS(c.joined)
		if !ns.Equal(c.want) {
			t.Errorf("splitNS(%q) = %v, want %v", c.joined, ns, c.want)
		}
	}
}

// TestJoinNS_Encoding 验合法 namespace 用 `.` 分隔编码 (跟 LangGraph PG 范式
// 对齐, sibling false-positive 防御靠 segment validation).
func TestJoinNS_Encoding(t *testing.T) {
	cases := []struct {
		ns   store.Namespace
		want string
	}{
		{store.Namespace{"users", "u1"}, "users.u1"},
		{store.Namespace{"users", "u1", "memories"}, "users.u1.memories"},
		{store.Namespace{"single"}, "single"},
	}
	for _, c := range cases {
		got := joinNS(c.ns)
		if got != c.want {
			t.Errorf("joinNS(%v) = %q, want %q", c.ns, got, c.want)
		}
	}
}
