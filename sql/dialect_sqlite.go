package sql

import (
	"fmt"
	"strings"
)

// SQLite 是 SQLite 3.38+ dialect（`->` 操作符起）。
//
// # 它为什么存在：**给这个模块一个能真跑起来的引擎**
//
// 本模块此前唯一的测试手段是 sqlmock——它钉的是**发出去的 SQL 字符串长什么样**，
// 不是**那条 SQL 跑出来对不对**。代价实测过两次：ns 子树匹配先后漏掉「重访实例段」
// 与「前缀里的 LIKE 元字符」，两个 bug 对测试套件全程隐形，都是靠人工审计才发现，
// 而同样的缺陷在跑 conformance 的 in-memory / redis 上会当场变红。
//
// 加这个 dialect 是为了让 `compose_v2/checkpointer_conformance` 与 `store/conformance`
// 能在**真 SQL 引擎**上跑起来（配 `modernc.org/sqlite`，纯 Go、无 cgo、无外部进程，
// 与 redis 那边用 miniredis 是同一个生态位）。
//
// # 它覆盖什么、不覆盖什么（别把它当成 PG 覆盖）
//
// **覆盖**：跨方言共有的语义契约——ns 子树匹配、UPSERT 幂等、round-trip、
// 批量、并发、filter 语义（三个 JSONField* 方法的行为按 conformance 逐格与另两个方言
// 对齐）。SQL 主体在本模块是**同一份字符串**，只有占位符与 JSONField* 片段经 dialect
// 渲染。TTL 只覆盖**拒绝路径**（不支持 → 返 `ErrTTLNotSupported`）——sql 后端不支持
// TTL，过期那条子测试在这里是 SKIP。
//
// **不覆盖**：方言私有部分——PG 的 JSONB 存储与 planner 行为、MySQL 的 JSON 类型
// 优先级、两者各自的锁与并发语义、类型精度。想覆盖那些只能连真库。
//
// # 三处已知差异（都已在本实现里处理或标注）
//
//  1. **LIKE 默认大小写不敏感**。SQLite 的 `LIKE` 对 ASCII 忽略大小写，而本模块的
//     PG/MySQL DDL 都是大小写敏感的（MySQL 的身份列是二进制串）。测试连接
//     必须 `PRAGMA case_sensitive_like=ON`，否则 ns 大小写相关的用例会**误红**。
//     见 [OpenSQLiteForTest]。
//  2. **无 JSONB**：JSON 存 TEXT。（2026-09-07 起 `Put` 绑的是 string 所以存成 TEXT；
//     此前绑 `[]byte` 存成 BLOB——两者在本模块用到的每一处 `json_*` 上**逐格等价**，
//     16 种 JSON 形态 × 5 个表达式共 80 格差异 0 条，新老行混存也不影响 filter，
//     所以那次改动不需要数据迁移。绑 string 的理由见 sql_helpers.go 的 bindJSON。）
//  3. **`json_extract` 会抹掉 JSON 的类型信息**：JSON `true` 出来是 INTEGER 1、JSON
//     null 出来是 SQL NULL（与字段缺失无法区分）、数组 / 对象出来是文本。所以相等与
//     范围比较都要配 `json_type` 守卫或改用 `->`（返 JSON 文本）——见各方法 godoc。
type SQLite struct{}

// Placeholder 返 SQLite 占位符 `?`（位置无关，按出现顺序绑定，同 MySQL）。
func (SQLite) Placeholder(n int) string { return "?" }

// UpsertSuffix 返 SQLite `ON CONFLICT (keyCols) DO UPDATE SET col=excluded.col, ...`
// （SQLite 3.24+，语法与 PG 同源，冲突列**需明列**）。
func (SQLite) UpsertSuffix(keyCols, updateCols []string) string {
	if len(updateCols) == 0 {
		return ""
	}
	sets := make([]string, len(updateCols))
	for i, col := range updateCols {
		sets[i] = fmt.Sprintf("%s=excluded.%s", col, col)
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s",
		strings.Join(keyCols, ", "), strings.Join(sets, ", "))
}

// JSONType 返 "TEXT"——SQLite 没有 JSON 类型，`json_*` 函数直接作用于文本。
func (SQLite) JSONType() string { return "TEXT" }

// BlobType 返 "BLOB"（SQLite 单值上限受 `SQLITE_MAX_LENGTH` 约束，默认 1GB）。
func (SQLite) BlobType() string { return "BLOB" }

// JSONFieldExists 返 `json_type(col, '$.field') IS NOT NULL`——`json_type` 对缺失
// 路径返 SQL NULL，对 JSON null 返文本 `'null'`，正好区分「缺失」与「值是 null」。
func (SQLite) JSONFieldExists(col, field string, b ParamBinder) string {
	return fmt.Sprintf("json_type(%s, %s) IS NOT NULL", col, b.Bind(jsonFieldPath(field)))
}

// JSONFieldEquals 按 filter 值的 JSON 类别选比较形态——SQLite 没有 JSON 原生相等，
// `json_extract` 又会抹掉类型（true → 1、null → SQL NULL、数组 / 对象 → 文本），
// 所以要按 kind 分三路，每路都消费一次占位符（参数是 filter 值的 JSON 文本）：
//
//   - number：`(json_type(col,'$.f') IN ('integer','real') AND json_extract(col,'$.f') = json_extract(?, '$'))`
//     ——守卫挡住 JSON true（会被 json_extract 成 1、与 filter 1 相等）；`json_extract(?, '$')`
//     把 JSON 文本 `10.0` 变成 REAL，INTEGER 10 与 REAL 10.0 按数值相等 ✓
//   - bool / null：`json_type(col,'$.f') = json_type(?, '$')`——两侧都归成 `'true'` /
//     `'false'` / `'null'` 文本比；缺失路径 json_type 是 SQL NULL ⟹ 不命中 ✓
//   - string / array / object：`json(col) -> '$.f' = json(?)`——`->` 返子元素的 JSON
//     **文本**（字符串带引号、数组 / 对象是压缩后的 JSON），两侧都经 `json()` 规范化
//     再比文本；缺失路径 `->` 返 SQL NULL ⟹ 不命中 ✓。对象键序：本模块 `Put` 存的是
//     `store.NormalizeValue` 的结果再经 Go `json.Marshal`（struct 也先变成 map，键排序），
//     filter 值在 `store.WithFilter` 里同样先换成 JSON 形状、再经 `json.Marshal`，所以两侧序一致；
//     业务绕过 `Put` 直写的乱序对象文本比会不等，那一档 PG / MySQL（原生无序相等）不受影响
//
// 语义与 [store.ValueEqual] 逐格一致：10 == 10.0、true != 1、"10" != 10、null 只等
// null、数组逐元素（不是包含）、对象逐键。
func (SQLite) JSONFieldEquals(col, field, valueJSON string, kind JSONKind, b ParamBinder) string {
	// `?` 是位置绑定——按占位符在 SQL 里出现的先后 Bind，同一条 path 出现两次就绑两次。
	path := jsonFieldPath(field)
	switch kind {
	case JSONKindNumber:
		p1 := b.Bind(path)
		p2 := b.Bind(path)
		pv := b.Bind(valueJSON)
		return fmt.Sprintf("(json_type(%s, %s) IN ('integer','real') AND json_extract(%s, %s) = json_extract(%s, '$'))",
			col, p1, col, p2, pv)
	case JSONKindBool, JSONKindNull:
		pf := b.Bind(path)
		pv := b.Bind(valueJSON)
		return fmt.Sprintf("json_type(%s, %s) = json_type(%s, '$')", col, pf, pv)
	default: // string / array / object：JSON 文本相等
		pf := b.Bind(path)
		pv := b.Bind(valueJSON)
		return fmt.Sprintf("json(%s) -> %s = json(%s)", col, pf, pv)
	}
}

// JSONFieldNumericCompare 返
//
//	(json_type(col, '$.field') IN ('integer','real') AND json_extract(col, '$.field') <op> ?)
//
// SQLite 的比较永不报错——跨存储类按 NULL < INTEGER/REAL < TEXT < BLOB 排序，所以裸
// `json_extract(...) > 10` 会把**每个字符串 / 对象 / 数组行都判成大于 10**，JSON true
// 被抹成 1 后 `< 10` 还会命中（真引擎实测）。`json_type` 守卫把非数值行排掉；两个
// 合取项都没有副作用，AND 的求值顺序无关（与 PG 不同，不需要 CASE）。
//
// 参数按 Go 数值绑定；⚠️ 别用 `CAST(json_extract(...) AS REAL)`——非数值文本会被
// CAST 成 0.0，`$lt 10` 反命中。
func (SQLite) JSONFieldNumericCompare(col, field, op string, num any, b ParamBinder) string {
	path := jsonFieldPath(field)
	p1 := b.Bind(path)
	p2 := b.Bind(path)
	pv := b.Bind(num)
	return fmt.Sprintf("(json_type(%s, %s) IN ('integer','real') AND json_extract(%s, %s) %s %s)",
		col, p1, col, p2, op, pv)
}

// 编译期 interface 实现检查.
var _ Dialect = SQLite{}
