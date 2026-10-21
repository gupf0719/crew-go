package sql

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
)

// validIdentifierTableName 是 SQL **表名** 校验正则 — 字母/下划线开头, 后续
// 字母/数字/下划线, 最长 63 char (PG NAMEDATALEN 默认 64 减 NUL).
//
// **audit iter-4 视角 2 M3 修**: WithStoreTable / WithCheckpointerTable 接受
// 业务任意 string 直接 fmt.Sprintf 拼 SQL 字面量 — 业务误传含 `;` / `--` /
// 空格的表名直接 SQL injection.
var validIdentifierTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// validateTableName 校验 SQL 表名合法 (限 63 char + SQL identifier 字符集).
func validateTableName(name string) error {
	if !validIdentifierTableName.MatchString(name) {
		return fmt.Errorf("sql: invalid table name %q (must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$, max 63 chars)", name)
	}
	return nil
}

// numericArg 把范围 op 的 filter 值转成 driver 能绑定的 Go 数值.
//
// 覆盖全 Go numeric kind + json.Number (业务 `json.Decoder.UseNumber()` 路径; 先试
// Int64 保精度, 再退 Float64). uint64 高位置位时 database/sql 拒绝绑定, 退成 float64
// (精度丢失, 但 > 2^63 的过滤值本就没有精确比较的路). 非数值返 false——正常情况下
// store.ValidateFilter 已在扫描前拦住 (返 ErrFilterTypeMismatch), 这里是兜底.
func numericArg(v any) (any, bool) {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
		if f, err := n.Float64(); err == nil {
			return f, true
		}
		return nil, false
	case uint64:
		if n > math.MaxInt64 {
			return float64(n), true
		}
		return n, true
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32,
		float32, float64:
		return v, true
	}
	return nil, false
}
