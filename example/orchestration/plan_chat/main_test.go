package main

import (
	"encoding/json"
	"testing"

	"crew-go/agent_v2"
)

// refKey 把 RefSpec 转成可比对的字符串（v2 RefSpec.Index 是 *int 不能 DeepEqual）。
func refKey(r agent_v2.RefSpec) string {
	if r.Index != nil {
		return "i:" + itoa(*r.Index)
	}
	return "a:" + r.AgentName
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	out := []byte{}
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// TestParseRawRefs 覆盖 parseRawRefs：把 LLM 输出的 refs（int/string）转成 RefSpec (v2)。
// v2 RefSpec.Index 改为 *int 表达"未设置"语义——比较用 refKey 字符串化。
func TestParseRawRefs(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string // refKey 序列
	}{
		{name: "empty", raw: `[]`, want: nil},
		{name: "all ints", raw: `[0, 2]`, want: []string{"i:0", "i:2"}},
		{name: "string ref", raw: `["coder"]`, want: []string{"a:coder"}},
		{name: "mixed int+string", raw: `[0, "critic"]`, want: []string{"i:0", "a:critic"}},
		{name: "invalid type (object) skipped not errored", raw: `[0, {"agent":"coder"}, 1]`, want: []string{"i:0", "i:1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw []json.RawMessage
			if err := json.Unmarshal([]byte(tc.raw), &raw); err != nil {
				t.Fatalf("unmarshal raw: %v", err)
			}
			got := parseRawRefs(raw)
			gotKeys := make([]string, 0, len(got))
			for _, r := range got {
				gotKeys = append(gotKeys, refKey(r))
			}
			if !slicesEqual(gotKeys, tc.want) {
				t.Errorf("got %+v, want %+v", gotKeys, tc.want)
			}
		})
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
