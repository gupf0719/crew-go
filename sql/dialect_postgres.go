package sql

import (
	"fmt"
	"strings"
)

// Postgres 是 PostgreSQL 9.5+ (ON CONFLICT 起) dialect.
//
// 用到的 jsonb 算子 (`->` / `->>` / `?` / `jsonb_typeof` / `::jsonb`) 都是 9.4 起就有的,
// 所以下限由 ON CONFLICT 决定; 真库 conformance 只在 16.9 上跑过 (2026-09-04).
//
// 表结构迁移 + UPSERT 语义按 PostgreSQL ON CONFLICT 范式实现.
//
// **业务 driver 推荐**: `jackc/pgx/v5/stdlib` (pgx 比 lib/pq 性能 +100%, 支持
// 真 prepared statements + named params). 业务自 import driver:
//
//	import _ "github.com/jackc/pgx/v5/stdlib"
//	db, _ := sql.Open("pgx", "postgres://...")
//
// 不推荐 PG <9.5 (无 ON CONFLICT) — crew-go SQL backend 不兼容老版本.
//
// **不内置 pgvector** — 第一版 conformance 不含语义搜索 (Search WithQuery).
// 业务想接 pgvector / hnsw / ivfflat 自行扩展 dialect impl.
type Postgres struct{}

// Placeholder 返 PG 占位符 `$N` (N 从 1 开始).
func (Postgres) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

// UpsertSuffix 返 PG `ON CONFLICT (...) DO UPDATE SET col=EXCLUDED.col, ...`.
//
// 业务 SQL 示例:
//
//	INSERT INTO store (ns, key, value) VALUES ($1, $2, $3)
//	  ON CONFLICT (ns, key) DO UPDATE SET value=EXCLUDED.value
func (Postgres) UpsertSuffix(keyCols, updateCols []string) string {
	if len(keyCols) == 0 || len(updateCols) == 0 {
		return ""
	}
	sets := make([]string, len(updateCols))
	for i, col := range updateCols {
		sets[i] = fmt.Sprintf("%s=EXCLUDED.%s", col, col)
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s",
		strings.Join(keyCols, ", "),
		strings.Join(sets, ", "))
}

// JSONType 返 "JSONB" (PG 9.4+ 推荐, 二进制存储 + GIN index).
func (Postgres) JSONType() string { return "JSONB" }

// BlobType 返 "BYTEA" (PG 二进制类型, 无大小硬上限受 1GB toast 限制).
func (Postgres) BlobType() string { return "BYTEA" }

// JSONFieldExists 返 `col ? 'field'`（jsonb 顶级键存在, 值为 JSON null 也算）.
func (Postgres) JSONFieldExists(col, field string, b ParamBinder) string {
	return fmt.Sprintf("%s ? %s", col, b.Bind(field))
}

// JSONFieldEquals 返 `col->'field' = $n::jsonb`——jsonb 原生相等.
//
// jsonb 相等已经是 [store.ValueEqual] 的语义: 数值按 numeric 比 (10 = 10.0, 精度
// 不丢), 类型不同即不等 (true ≠ 1, "10" ≠ 10), null 只等于 null, 数组按序逐元素,
// 对象键集相等; 字段缺失时 `col->'field'` 是 SQL NULL ⟹ 不命中. kind 用不上.
//
// 参数是 filter 值的 JSON 文本 (Go string), `::jsonb` 显式 cast 让 text 参数在任何
// driver 下都落成 jsonb.
//
// ⚠️ 与旧的 `@>` containment 不同: `{"tags": ["a"]}` **不**命中 `{"tags": ["a","b"]}`——
// 那是相等不是包含 (in-memory 的语义就是相等). 需要包含语义的业务自扩 dialect.
func (Postgres) JSONFieldEquals(col, field, valueJSON string, _ JSONKind, b ParamBinder) string {
	// 按占位符出现顺序绑：先字段名、再值。
	pf := b.Bind(field)
	pv := b.Bind(valueJSON)
	return fmt.Sprintf("%s->%s = %s::jsonb", col, pf, pv)
}

// JSONFieldNumericCompare 返
//
//	(CASE WHEN jsonb_typeof(col->'field') = 'number' THEN (col->>'field')::numeric END) <op> $n::numeric
//
// **必须是 CASE 不能是 AND**: PG 不保证 WHERE 子句的求值顺序 (文档 4.2.14 "The order
// of evaluation of subexpressions is not defined"), `jsonb_typeof(...) = 'number' AND
// (...)::numeric > $1` 靠的是 planner 按 cost 把守卫排在前面——真库实测把
// `jsonb_typeof` 的 COST 调高后 cast 被排到前面, 字符串行 20/20 报 22P02; CASE 的
// 条件求值是文档保证的, 同一实验下不受影响. 非数值行 CASE 落 NULL ⟹ 比较为 NULL ⟹
// 不命中、不报错; 数字字符串 `"9"` 不会被静默 cast 参与比较.
//
// 参数 `$n::numeric` 显式 cast 让绑定类型不影响结果: 2026-09-04 真库 conformance 跑的是
// pgx/v5 stdlib (按 int8 / float8 绑定); lib/pq 按 text 绑定同样能落成 numeric (2026-09-07
// 真库补测: 三种绑定形态结果一致).
//
// ⚠️ 另注: lib/pq **不受** bindJSON 那条 exec-mode 坑影响 (它对非 bytea 目标原样送出),
// 那条精确地是 pgx 两个 OID 不可知模式独有的——见 sql_helpers.go 的 bindJSON.
func (Postgres) JSONFieldNumericCompare(col, field, op string, num any, b ParamBinder) string {
	// PG 的 $n 可复用——字段名只绑一次、在 SQL 里出现两次。
	pf := b.Bind(field)
	pv := b.Bind(num)
	return fmt.Sprintf("(CASE WHEN jsonb_typeof(%s->%s) = 'number' THEN (%s->>%s)::numeric END) %s %s::numeric",
		col, pf, col, pf, op, pv)
}

// 编译期 interface 实现检查.
var _ Dialect = Postgres{}
