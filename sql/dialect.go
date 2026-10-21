// Package sql 提供 store.Store + compose_v2.Checkpointer 的 SQL backend 共享实现 —
// 业务通过 Dialect 注入支持 Postgres / MySQL（SQLite 作为跑 conformance 的真引擎替身）.
//
// **设计哲学** (相比 postgres + sqlite 各自独立子包的做法, crew-go 走"共享
// SQL 层 + dialect 注入"路径减少 90% 重复代码):
//   - 主 SDK module `crew-go` **0 driver 依赖** (业务自己 import 想用的 driver)
//   - 本 module `crew-go/sql` 仅依赖 stdlib `database/sql` + 主 module 接口
//   - 业务 `go get crew-go/sql` opt-in, 配 PostgresDialect / MySQLDialect 实例
//   - 自己提供 `*sql.DB` (业务自管 driver / 连接池 / DSN)
//
// **使用范式**:
//
//	import (
//	    _ "github.com/jackc/pgx/v5/stdlib"  // 业务 import 想用的 driver
//	    sqlstd "database/sql"
//	    agentSQL "crew-go/sql"
//	)
//
//	db, _ := sqlstd.Open("pgx", "postgres://...")
//	store := agentSQL.NewStore(db, agentSQL.Postgres{})
//	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})
//
// **方言注入 7 个方法**——业务每加新 SQL 数据库 (CockroachDB / TiDB / Aurora /
// 等) 只需实现这 7 个方法不需复制全部 SQL CRUD 逻辑.
package sql

import "strings"

import "fmt"

// Dialect 抽象不同 SQL 数据库的语法差异. 业务可自实现给 CockroachDB / TiDB /
// 其他兼容 backend 用.
//
// 7 个方言函数覆盖 SQL backend 实现的所有非共享语法 (audit 阶段 2 落地实证: SQL
// 70-80% CRUD 共享, 20-30% 是 dialect-specific 方言). 其中三个 JSONField* 方法
// 承载 store filter 的语义——**语义以 [store.MatchFilter] 为准**（type bracketing:
// 类型对不上的行不命中、不报错），conformance 套件逐格钉, 业务自实现 dialect 必须
// 让 `store/conformance` 全绿.
//
// 方法语义 contract 见各方法 godoc + Postgres/MySQL 实例参考.
type Dialect interface {
	// Placeholder 返第 n 个参数占位符 (n 从 1 开始).
	//   Postgres: $1, $2, $3...
	//   MySQL:    ?, ?, ?...
	// 业务 SQL 拼接用 `dialect.Placeholder(1)` 替代字面量, 跨 backend 同一份 SQL.
	Placeholder(n int) string

	// UpsertSuffix 返 INSERT 语句的 UPSERT 后缀.
	//   Postgres: "ON CONFLICT (keyCols...) DO UPDATE SET col1=EXCLUDED.col1, ..."
	//   MySQL:    "ON DUPLICATE KEY UPDATE col1=VALUES(col1), ..."
	//
	// keyCols: 冲突检测的唯一列 (PG ON CONFLICT 需明列, MySQL 自动按 PK/Unique).
	// updateCols: 冲突时要更新的列.
	//
	// Checkpointer.SaveNS 拿它在事务里占住一行（updateCols 只有一个键列，冲突时是一次值不变的更新），之后在同一个
	// 事务里读这一行的 graph_id 判冲突。所以冲突时它必须：
	//   - 对已存在的那一行加写锁、持有到事务结束：值不变的 DO UPDATE / ON DUPLICATE KEY UPDATE 满足，DO NOTHING
	//     不满足——不加锁时两个写入者在已有的行上都判过、都写成（真 PG 实测 40 轮 39 轮如此），conformance 的
	//     Concurrent_SaveNS_Conflicting_GraphID_On_Existing_Row_Exactly_One_Wins 会红；
	//   - 在 SaveNS 声明的 READ COMMITTED 事务里，让之后的普通 SELECT 读得到加锁时已经提交的那一版（每条语句一个
	//     快照）。驱动不支持按事务声明隔离级别时 BeginTx 报错，响亮不静默；快照建在事务第一条语句的隔离级别（PG 的
	//     REPEATABLE READ / SERIALIZABLE）不行——已有的行被并发改过时输家报串行化失败（40001）而不是 ErrGraphIDConflict。
	// 返回空串（不支持 UPSERT）时，SaveNS 从同一 (sid, ns) 的第二次写入起报主键冲突。
	UpsertSuffix(keyCols, updateCols []string) string

	// JSONType 返 JSON 列的 DDL 类型名.
	//   Postgres: "JSONB" (推荐, 比 JSON 快 + 支持 GIN index)
	//   MySQL:    "JSON" (5.7+, 5.6 业务自降级 LONGTEXT)
	JSONType() string

	// BlobType 返二进制大字段的 DDL 类型名.
	//   Postgres: "BYTEA"
	//   MySQL:    "LONGBLOB" (普通 BLOB 64KB 上限, 业务 cp 可能爆)
	BlobType() string

	// JSONFieldExists 返「JSON 列 col 的顶级字段 field 存在」的 WHERE 子句——
	// 字段值是 JSON null 也算存在.
	//   Postgres: "col ? $1"                                $1 = 字段名
	//   MySQL:    "JSON_CONTAINS_PATH(col, 'one', ?)"         ? = '$."field"'
	//
	// 共享层用它拼 `$ne`（= 存在 AND NOT 相等）.
	JSONFieldExists(col, field string, b ParamBinder) string

	// JSONFieldEquals 返「JSON 列 col 的顶级字段 field 与参数 JSON 语义相等」的
	// WHERE 子句. **参数绑定的是 filter 值的 JSON 文本**（json.Marshal 产物,
	// 如 `10` / `"abc"` / `true` / `null` / `["a","b"]` / `{"a":1}`）, kind 是它的
	// JSON 类别 (从同一份文本派生), 给分不清 bool 与 1 / JSON null 与缺失的引擎做守卫用
	// (PG / MySQL 有原生 JSON 相等, 可忽略 kind).
	//
	// 语义必须与 [store.ValueEqual] 一致: 数值互通 (10 == 10.0), bool != 1, "10" != 10,
	// null 只等于 null, 数组逐元素相等 (不是包含), 对象逐键相等; 字段缺失不命中.
	//   Postgres: "col->$1 = $2::jsonb"                     $1 = 字段名
	//   MySQL:    "JSON_EXTRACT(col, ?) = CAST(? AS JSON)"    第一个 ? = '$."field"'
	JSONFieldEquals(col, field, valueJSON string, kind JSONKind, b ParamBinder) string

	// JSONFieldNumericCompare 返「JSON 列 col 的顶级字段 field 是 JSON 数值且与参数
	// 比较为真」的 WHERE 子句. **参数绑定的是 Go 数值** (int64 / float64 等).
	//
	// 语义必须与 [store.MatchFilter] 的范围 op 一致 (type bracketing): 字段是字符串 /
	// bool / null / 数组 / 对象或缺失 ⟹ **不命中、不报错**——不允许把字符串 cast 成数
	// (PG `(col->>'f')::numeric` 裸 cast 会在字符串行上报 22P02, 数字字符串还会被
	// 静默 cast), 也不允许按引擎的跨类型全序比 (MySQL / SQLite 会把字符串行判成
	// 「大于任何数」). 引擎不保证 AND 短路时用 CASE 强制求值顺序.
	//   Postgres: "(CASE WHEN jsonb_typeof(col->$1) = 'number' THEN (col->>$1)::numeric END) > $2::numeric"
	//   MySQL:    "(JSON_TYPE(JSON_EXTRACT(col, ?)) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') AND JSON_EXTRACT(col, ?) > ?)"
	//
	// op 是 SQL 比较符 (`>` / `>=` / `<` / `<=`).
	JSONFieldNumericCompare(col, field, op string, num any, b ParamBinder) string
}

// jsonFieldPath 把顶级字段名包成 SQL/JSON path 字面量 `$."field"`——MySQL 与 SQLite 用
// （PG 的 `->` 直接收字段名文本，不走 path 语法，所以不需要）。
//
// **引号包裹是承重的**（真 SQLite 实测）：裸拼 `"$."+field` 时 `*` 是通配符、`.` 是层级
// 分隔符 ⟹ 字段名 `*` 会匹配到整行所有字段、`a.b` 会命中嵌套的 a→b 而不是字面字段 `a.b`。
// 包上引号之后 `$."*"` 返 0 行、`$."a.b"` 只命中字面字段。
//
// 转义顺序不能反：先转 `\` 再转 `"`——反过来的话新插入的反斜杠会被二次转义。
func jsonFieldPath(field string) string {
	esc := strings.ReplaceAll(field, `\`, `\\`)
	esc = strings.ReplaceAll(esc, `"`, `\"`)
	return `$."` + esc + `"`
}

// ParamBinder 把实参绑进本次查询并返回它的占位符——[Dialect] 的 JSONField* 方法用它，
// **字段名与 filter 值一律绑参、不拼进 SQL 字面量**。
//
// # 为什么字段名必须绑参
//
// 字段名来自业务的 filter map（store.WithFilter），拼进 SQL 就是注入面。这里曾靠一道
// `^[a-zA-Z_][a-zA-Z0-9_]*$` 的标识符白名单兜底——它确实承重（放宽成 `^.+$` 后三个引擎
// 实测全部跨 namespace 越权读），但它**拦错了东西**：字段名进 SQL 时是**字符串字面量 /
// JSON path 实参**，不是标识符。于是 `user-id` / `a.b` / `用户` 这些完全合法的 JSON 字段名
// 被一并拒掉，而它们在 in-memory / redis 上照常匹配——同一个 filter 换个后端从「有结果」
// 变成「报错」。
//
// 绑参之后注入面消失、字符集限制随之取消，那道白名单就退休了。
//
// ⚠️ **换来一个新的承重件**：MySQL / SQLite 侧字段名要进 JSON path，必须经 [jsonFieldPath]
// 包成 `$."field"`。裸拼 `"$."+field` 时 `*` 是**通配符**（真 MySQL 上会把整行所有字段值
// 吐出来）、`a.b` 会命中嵌套的 a→b。引号包裹之后两者都失效（真 SQLite 实测：`$."*"` 返 0 行、
// `$."a.b"` 只命中字面字段）。
//
// # 调用约定
//
// **按占位符在 SQL 里出现的先后顺序调用 Bind**——`?` 是位置绑定，顺序错了就是静默读错参数。
// 同一个实参出现两次时：PG 可以复用同一个 `$n`（Bind 一次、用两次），MySQL / SQLite 必须
// Bind 两次（各自一个 `?`）。这个差异由各 dialect 自己处理，共享层不管。
type ParamBinder interface {
	// Bind 绑一个实参，返回它在 SQL 里的占位符。
	Bind(v any) string
}

// JSONKind 是 filter 值的 JSON 类别——[Dialect.JSONFieldEquals] 的守卫提示. 由
// json.Marshal 产物的首字节派生 (jsonKindOf), 与绑定的参数文本恒一致.
type JSONKind int

// JSON 六类. 数值不分 int / float (JSON 只有 number).
const (
	JSONKindNull JSONKind = iota
	JSONKindBool
	JSONKindNumber
	JSONKindString
	JSONKindArray
	JSONKindObject
)

// String 返类别名 (日志 / 错误信息用).
func (k JSONKind) String() string {
	switch k {
	case JSONKindNull:
		return "null"
	case JSONKindBool:
		return "bool"
	case JSONKindNumber:
		return "number"
	case JSONKindString:
		return "string"
	case JSONKindArray:
		return "array"
	case JSONKindObject:
		return "object"
	}
	return fmt.Sprintf("JSONKind(%d)", int(k))
}

// jsonKindOf 从 json.Marshal 产物的首字节派生 JSONKind. 入参必须是合法 JSON 文本
// (json.Marshal 成功即合法); 空文本按 null 处理.
func jsonKindOf(jsonText []byte) JSONKind {
	if len(jsonText) == 0 {
		return JSONKindNull
	}
	switch jsonText[0] {
	case 'n':
		return JSONKindNull
	case 't', 'f':
		return JSONKindBool
	case '"':
		return JSONKindString
	case '[':
		return JSONKindArray
	case '{':
		return JSONKindObject
	}
	return JSONKindNumber
}
