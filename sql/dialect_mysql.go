package sql

import (
	"fmt"
	"strings"
)

// MySQL 是 MySQL 5.7+ (JSON 类型起) dialect.
//
// **业务 driver 推荐**: `go-sql-driver/mysql`. 业务自 import:
//
//	import _ "github.com/go-sql-driver/mysql"
//	db, _ := sql.Open("mysql", "user:pass@tcp(...)/db?parseTime=true")
//
// **关键 DSN 参数**:
//   - `parseTime=true` — `time.Time` 列自动解析 (不开会报 `unsupported Scan`)
//   - `loc=UTC` — 强制 UTC (跟 store conformance `Timestamps_Use_UTC_Location`
//     测对齐, 业务 prod TZ 不一致避免 8h 偏移 bug)
//   - 不必配 `collation`: Setup 建的身份列 (ns / data_key / sid) 是二进制串, 与参数的比较
//     逐字节、区分大小写与尾随空格, 跟连接的 collation 无关 (业务自管 schema 见 Store.Setup)
//
// **MySQL 5.6 不兼容** (无原生 JSON 类型). 业务用 5.6 自降级 LONGTEXT + 业务侧
// JSON marshal/unmarshal (本 module 不支持).
//
// **不内置语义搜索** — MySQL 无原生 vector, 业务需向量搜索接 Milvus/ES/Qdrant
// 而非本 SQL backend.
type MySQL struct{}

// Placeholder 返 MySQL 占位符 `?` (位置无关, 按出现顺序绑定).
func (MySQL) Placeholder(n int) string {
	return "?"
}

// UpsertSuffix 返 MySQL `ON DUPLICATE KEY UPDATE col=VALUES(col), ...`.
//
// **注意**: MySQL UPSERT 按表的 PK / Unique 约束自动触发, **不用** 列显式声明
// (跟 PG ON CONFLICT 需明列冲突列不同). keyCols 参数在 MySQL 路径**忽略**
// (业务 DDL 时把 keyCols 设为 PRIMARY KEY 或 UNIQUE INDEX 即可).
//
// 业务 SQL 示例:
//
//	INSERT INTO store (ns, key, value) VALUES (?, ?, ?)
//	  ON DUPLICATE KEY UPDATE value=VALUES(value)
//	     ↑ 表 PRIMARY KEY (ns, key) 触发 UPSERT
func (MySQL) UpsertSuffix(keyCols, updateCols []string) string {
	if len(updateCols) == 0 {
		return ""
	}
	sets := make([]string, len(updateCols))
	for i, col := range updateCols {
		sets[i] = fmt.Sprintf("%s=VALUES(%s)", col, col)
	}
	return "ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
}

// JSONType 返 "JSON" (MySQL 5.7+).
func (MySQL) JSONType() string { return "JSON" }

// BlobType 返 "LONGBLOB" (4GB, 业务 cp 内 Channels JSON marshal 可能达 MB 级,
// 普通 BLOB 64KB 上限不够).
func (MySQL) BlobType() string { return "LONGBLOB" }

// JSONFieldExists 返 `JSON_CONTAINS_PATH(col, 'one', '$.field')`（值为 JSON null
// 也算存在——它是路径存在性, 不看值）.
func (MySQL) JSONFieldExists(col, field string, b ParamBinder) string {
	return fmt.Sprintf("JSON_CONTAINS_PATH(%s, 'one', %s)", col, b.Bind(jsonFieldPath(field)))
}

// JSONFieldEquals 返 `JSON_EXTRACT(col, '$.field') = CAST(? AS JSON)`——JSON 原生相等.
//
// MySQL JSON 比较: 同类型按值 (INTEGER 与 DOUBLE 互通, 10 = 10.0), 类型不同按优先级
// 排序永不相等 (true ≠ 1, "10" ≠ 10), 数组按序逐元素, 对象键集相等; 字段缺失时
// `JSON_EXTRACT` 是 SQL NULL ⟹ 不命中. kind 用不上.
//
// 参数是 filter 值的 JSON 文本, `CAST(? AS JSON)` 让它以 JSON 值参与比较 (裸字符串
// 参数会被当成 JSON STRING 与数值行永不相等).
//
// ⚠️ 与旧的 `JSON_CONTAINS` 不同: 那是包含语义, `{"f": 1}` 会命中 `{"f": [1, 2]}`
// (标量含于数组); 现在是相等, 与 in-memory 一致.
func (MySQL) JSONFieldEquals(col, field, valueJSON string, _ JSONKind, b ParamBinder) string {
	pf := b.Bind(jsonFieldPath(field))
	pv := b.Bind(valueJSON)
	return fmt.Sprintf("JSON_EXTRACT(%s, %s) = CAST(%s AS JSON)", col, pf, pv)
}

// JSONFieldNumericCompare 返
//
//	(JSON_TYPE(JSON_EXTRACT(col, '$.field')) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') AND JSON_EXTRACT(col, '$.field') <op> ?)
//
// MySQL 的 JSON 比较永不报错——跨类型按优先级排序 (STRING > INTEGER, NULL 最低),
// 所以裸 `JSON_EXTRACT(...) > 10` 会把**每个字符串 / bool / 对象 / 数组行都判成大于
// 10**、把 JSON null 行判成小于 10 (真库实测). `JSON_TYPE` 守卫把非数值行排掉;
// 两个合取项都没有副作用, AND 的求值顺序无关 (与 PG 不同, 不需要 CASE).
//
// **列表必须含 `UNSIGNED INTEGER`**: 手册的 JSON_TYPE 返回值表没列它, 但 MySQL 会把
// 大正整数 (实测 9007199254740993) 标成它, 漏了 Snowflake ID / 纳秒时间戳会被守卫误排.
//
// 参数按 Go 数值绑定 (int64 → 整数, float64 → 浮点); 按字符串绑定会退化成 JSON
// STRING 的字典序比较, 共享层保证不会.
func (MySQL) JSONFieldNumericCompare(col, field, op string, num any, b ParamBinder) string {
	// `?` 是位置绑定——同一条 path 出现两次就得绑两次，顺序即出现顺序。
	path := jsonFieldPath(field)
	p1 := b.Bind(path)
	p2 := b.Bind(path)
	pv := b.Bind(num)
	return fmt.Sprintf("(JSON_TYPE(JSON_EXTRACT(%s, %s)) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') AND JSON_EXTRACT(%s, %s) %s %s)",
		col, p1, col, p2, op, pv)
}

// 编译期 interface 实现检查.
var _ Dialect = MySQL{}
