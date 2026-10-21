package store

import (
	"encoding/json"
	"testing"
)

// TestToFloat_Accepts_JsonNumber 验 iter-5-M round audit 视角 2 HIGH-2 修 —
// InMem toFloat 识别 json.Number 跟 redis backend 对齐.
//
// **业务路径**: 业务 `json.NewDecoder(r).UseNumber().Decode(&filter)` 后 filter
// map 含 json.Number value. InMem store 接到这个 filter 调 Search:
//   - 修前: toFloat(json.Number) → false → 数值 op fail-fast ErrFilterTypeMismatch
//   - 修后: toFloat(json.Number) → Float64() → 数值比较 ✓ 跟 redis backend 一致
//
// **sanity-revert**: 删 toFloat 的 `case json.Number` 分支 → 此测试 fail.
func TestToFloat_Accepts_JsonNumber(t *testing.T) {
	cases := []struct {
		name string
		in   json.Number
		want float64
	}{
		{"int", json.Number("42"), 42.0},
		{"float", json.Number("3.14"), 3.14},
		{"negative", json.Number("-100"), -100.0},
		{"int64_max_as_string", json.Number("9223372036854775807"), 9223372036854775807.0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := toFloat(c.in)
			if !ok {
				t.Errorf("toFloat(json.Number(%q)) should return ok=true (跨 backend 对齐 redis)", c.in)
				return
			}
			if got != c.want {
				t.Errorf("toFloat(json.Number(%q)) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

// TestToFloat_Rejects_Invalid_JsonNumber 验 json.Number 含非法数字字符串时返 false.
func TestToFloat_Rejects_Invalid_JsonNumber(t *testing.T) {
	_, ok := toFloat(json.Number("not_a_number"))
	if ok {
		t.Error("toFloat(json.Number(\"not_a_number\")) should return ok=false")
	}
}
