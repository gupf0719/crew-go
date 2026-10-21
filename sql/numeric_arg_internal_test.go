package sql

import (
	"encoding/json"
	"testing"
)

// TestNumericArg_All_Numeric_Kinds 验全 12 Go numeric kind + json.Number 都能转成
// driver 可绑定的数值——业务 Snowflake ID (int64) / timestamp_ms / counter (uint64) /
// `json.Decoder.UseNumber()` 出来的 json.Number 都走数值路径, 不走 ErrFilterTypeMismatch.
func TestNumericArg_All_Numeric_Kinds(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"int", int(42)}, {"int8", int8(42)}, {"int16", int16(42)}, {"int32", int32(42)},
		{"int64", int64(42)}, {"uint", uint(42)}, {"uint8", uint8(42)}, {"uint16", uint16(42)},
		{"uint32", uint32(42)}, {"uint64", uint64(42)}, {"float32", float32(3.14)}, {"float64", float64(3.14)},
		{"json.Number int", json.Number("42")}, {"json.Number float", json.Number("3.14")},
		{"json.Number big", json.Number("9007199254740993")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := numericArg(c.v); !ok {
				t.Errorf("numericArg(%T) should be ok", c.v)
			}
		})
	}
	// json.Number 整数走 Int64 保精度 (> 2^53 不能经 float64)
	if got, _ := numericArg(json.Number("9007199254740993")); got != int64(9007199254740993) {
		t.Errorf("json.Number big: got %v (%T), want int64 9007199254740993", got, got)
	}
	// uint64 高位置位: database/sql 拒绝绑定, 退 float64
	if got, ok := numericArg(uint64(1 << 63)); !ok || got != float64(1<<63) {
		t.Errorf("uint64 high bit: got %v (%T) ok=%v, want float64", got, got, ok)
	}
}

// TestNumericArg_NonNumeric_False 验非数值 (含数字字符串 / bool / json.Number 非法串)
// 返 false——共享层据此返 ErrFilterTypeMismatch (正常由 store.ValidateFilter 先拦).
func TestNumericArg_NonNumeric_False(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"string", "abc"}, {"numeric string", "10"}, {"bool", true}, {"nil", nil},
		{"slice", []int{1, 2}}, {"map", map[string]int{"a": 1}}, {"struct", struct{ X int }{X: 1}},
		{"json.Number invalid", json.Number("not_a_number")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := numericArg(c.v); ok {
				t.Errorf("numericArg(%T=%v) should not be ok", c.v, c.v)
			}
		})
	}
}
