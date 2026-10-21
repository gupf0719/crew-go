package agent_v2

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

// TestEncodeTree_MatchesJSONMarshal：一万层以内，encodeTree 的输出与 json.Marshal 逐字相同——摘要换编码器
// 不改变任何正常深度输入的身份。
func TestEncodeTree_MatchesJSONMarshal(t *testing.T) {
	trees := []any{
		nil, true, false, 0.0, math.Copysign(0, -1), 0.1, 1e21, 123456789.0, -3.5e-7,
		"", "<b>& 中文\"\\\n", []any{}, map[string]any{},
		map[string]any(nil), []any(nil),
		map[string]any{"b": 1.0, "a": []any{"x", nil, map[string]any{"<k>": false, "é": 2.0}}, "中": "文", "": "empty"},
		[]any{[]any{[]any{1.0}}, map[string]any{"z": map[string]any{"y": []any{}}}},
	}
	for _, tree := range trees {
		want, err := json.Marshal(tree)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := encodeTree(&buf, tree); err != nil {
			t.Fatalf("encodeTree(%v): %v", tree, err)
		}
		if buf.String() != string(want) {
			t.Errorf("encodeTree(%#v)\n got %s\nwant %s", tree, buf.String(), want)
		}
	}
}

// TestEncodeTree_DeeperThanJSONLimit：超过一万层也编得出来，且只差在最深处一个叶子的两棵树编出来不同。
func TestEncodeTree_DeeperThanJSONLimit(t *testing.T) {
	deep := func(leaf string) any {
		var v any = leaf
		for range 12000 {
			v = []any{v}
		}
		return v
	}
	var a, b bytes.Buffer
	if err := encodeTree(&a, deep("工单-1")); err != nil {
		t.Fatal(err)
	}
	if err := encodeTree(&b, deep("工单-2")); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("只在最深处的叶子上不同的两棵树编出了同样的字节")
	}
}
