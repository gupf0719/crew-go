package sql

import (
	"bytes"
	dbsql "database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// errNoRows 返 dbsql.ErrNoRows. 仅作 helper 让 checkpointer_query.go 等文件
// `errors.Is(err, errNoRows())` 不重复 import dbsql.
func errNoRows() error { return dbsql.ErrNoRows }

// paramBinder 是 [ParamBinder] 的实现——按 Bind 的调用顺序累积实参并发占位符。
//
// offset 是本次查询里**已经绑掉的参数个数**（filter 前面还有 ns 前缀等），PG 的 `$n`
// 要从它之后接着编号。
type paramBinder struct {
	dialect Dialect
	offset  int
	args    []any
}

// Bind 实现 [ParamBinder]。
func (p *paramBinder) Bind(v any) string {
	p.args = append(p.args, v)
	return p.dialect.Placeholder(p.offset + len(p.args) - 1)
}

// bindJSON 把 json.Marshal 的产物绑成 **string** 而不是 []byte。
//
// **本模块所有把 JSON 文本绑进 SQL 的地方都要走它**（写入侧 store.Put 与 checkpointer 的
// 两个 SaveNS 路径，读取侧 store_search 的 filter 参数，共 5 处）。它的实现就是
// `string(b)`，所以「顺手写成 string(...)」在今天等价——但那样这颗雷的知识只长在调用
// helper 的那几个点上，另几个点上的正确是巧合。`grep -c bindJSON` 应当等于绑定点数。
//
// # 为什么必须是 string
//
// pgx 在两个「OID 不可知」的 exec mode 下（`pgx.QueryExecModeExec` 与
// `pgx.QueryExecModeSimpleProtocol`）拿不到目标列的类型，只能按 Go 类型猜：`[]byte` 被当成
// bytea 编码，PG 收到后无法解析成 json/jsonb ⟹ 每一次写都失败：
//
//	ERROR: invalid input syntax for type json (SQLSTATE 22P02)
//
// pgx 自己的文档就写着这件事，可直接引（`pgx/v5@v5.10.0/conn.go` 的
// `QueryExecModeSimpleProtocol` 那段）：
//
//	[]byte values are encoded as PostgreSQL bytea. string must be used
//	instead for text type values including json and jsonb.
//
// 真 PG 16.9 上 5 个 exec mode × 2 种绑定 × 3 种列类型逐格实测（30 格），只有这两个模式
// 的 []byte 进 json/jsonb 挂。
//
// # 影响面：正是池化代理下会用到的那两个模式
//
// pgx 给「代理 / 连接池服务器」推荐的恰好就是这两个（同一段文档：`pgx.QueryExecModeExec`
// 优先、`pgx.QueryExecModeSimpleProtocol` 用于不支持扩展协议的代理）——也就是说 PgBouncer
// 这类部署无论选哪个都踩。那类部署下 store 写不进去、cp 存不下来，**cp 更要命：它是
// HITL Resume 的落点**，挂起存不下来就没有 Resume 可言。
//
// # 为什么不用 json.RawMessage / driver.Valuer
//
// 不是因为它们今天坏（实测 v5.10.0 下 `json.RawMessage` 编出来的 wire 与 string 一样），
// 是因为**鲁棒性**：string 在 pgx 的 `pgtype.planEncode` 开头就短路、**不进类型注册表**
// （`pgtype/pgtype.go` 里那个 `pgtype.encodePlanStringToAnyTextFormat` 分支），而注册表那条路上有过
// 「exec mode 下自定义 jsonb 类型失效」这类回归。走不经过注册表的那条，回归波及不到。
//
// # 另两个方言：MySQL 也会挂，SQLite 两种都收
//
// ⚠️ **别以为这是 PG 独有的**。MySQL 在 `interpolateParams=true` 下由驱动客户端自己拼 SQL
// 文本，`[]byte` 被 `go-sql-driver/mysql` 拼成二进制字面量（`connection.go` 的
// `mysql.escapeBytesQuotes`），JSON 列当场拒收——真 MySQL 8.0.35 实测：
//
//	Error 3144 (22032): Cannot create a JSON value from a string with CHARACTER SET 'binary'
//
// `interpolateParams=true` 在 MySQL 那边的地位等同 PgBouncer 之于 PG——常见的省一次往返的
// 生产开关，也是某些代理层的要求。**而它默认关闭，所以测试环境全绿、生产才炸**：下一个人
// 若据「MySQL 不受影响」把这里改成 `if _, ok := d.(Postgres); ok` 的条件转换，红不了。
//
// SQLite 两种绑法都收。SQLite 这边
// 值会从 BLOB 变成 TEXT——把本文件生成的**全部**列侧片段逐格比过：
//
//	json_type(col,'$.f') IS NOT NULL          键存在判定
//	json_type(col,'$.f') IN ('integer','real') 数值守卫
//	json_extract(col,'$.f')                    数值比较取值
//	json(col) -> '$.f'                         string/array/object 相等（json() 包装在这里，
//	                                           它没消失，只是从旧的 JSONContains 搬了家）
//
// 加上 `json_valid` / `->>` / `json_quote` 等共 10 个表达式 × 16 种 JSON 形态 = 160 格，
// 差异 0 条（另有 typeof 对照组确认两列真的分别是 BLOB / TEXT，否则整张网格恒真）；
// 新老行混存时走真 Store.Search 的 5 条 filter 路径也各自命中。**不需要数据迁移。**
//
// # 代价
//
// 多一次 []byte→string 拷贝，1 alloc：1KB 约 105ns、64KB 约 12µs、1MB 约 98µs。
// 对一次真 DB 往返（0.5~5ms）是少数派；真正要留意的是 `durability=sync` 下每超步一次
// MB 级分配带来的 GC 压力。
func bindJSON(b []byte) string { return string(b) }

// unmarshalValue 用 json.Decoder + UseNumber() 反序列化 — audit iter-4 视角 3 H5
// 修. 原 `json.Unmarshal(&value any)` 默认所有数字 → float64. 业务 Put int64
// ID (Snowflake / timestamp_ms / 任何 > 2^53 的数) 后 Get 拿到 float64 精度丢失.
//
// json.Decoder + UseNumber() 让数字保留 json.Number string 形态, 业务 type
// switch 后用 `.Int64()` / `.Float64()` / `.String()` 自管.
//
// 业务用法:
//
//	item, _ := store.Get(ctx, ns, key)
//	if num, ok := item.Value.(json.Number); ok {
//	    id, _ := num.Int64()  // 大整数保留精度
//	}
//
// **跨 backend 行为对齐**: 读回的形状就是 store.NormalizeValue 的结果——每个后端存的、读回的都是值的 JSON
// 形状（数字是 json.Number），业务按同一套类型分支读。
func unmarshalValue(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("unmarshal value: %w", err)
	}
	return v, nil
}

// nullableTime 让 *Time 列 Scan 不 nil-panic — PG `TIMESTAMP NULL` / MySQL
// `DATETIME(6) NULL` 业务允许 NULL 字段时用. Search/Get 列假设 NOT NULL DDL, 但
// 业务 backend 可能用 NULL 字段, 加 nullable wrap 健壮.
type nullableTime struct {
	t time.Time
}

func (n *nullableTime) Scan(value any) error {
	var nt dbsql.NullTime
	if err := nt.Scan(value); err != nil {
		return err
	}
	if nt.Valid {
		n.t = nt.Time
	}
	return nil
}
