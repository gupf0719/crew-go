package sql

import (
	"context"
	"errors"
	"fmt"
)

// Setup 创建 Store 表 + 索引.
//
// **业务部署模式 (重要 — audit iter-5 视角 2/4 H1+H7)**:
//
//   - **PG**: 用 `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` —
//     **幂等**, 多 pod 并发跑 OK.
//   - **MySQL**: 用 `CREATE TABLE IF NOT EXISTS` (幂等) + `CREATE INDEX` (5.7
//     无 IF NOT EXISTS, **不幂等**) — **多 pod 并发跑会 fail** (第二 pod 报
//     "Duplicate key name").
//
// **推荐生产部署**:
//   - K8s init container / 独立 migration job 单次跑 Setup() — 业务 pod 启动时
//     不跑
//   - 跨 K8s pod 用 Job + activeDeadlineSeconds, 确保单次执行
//   - 业务自管 schema (`不调 Setup`, 手动 ALTER TABLE 升级) 在多 pod 场景更安全
//
// 业务集成路径:
//
//	store := agentSQL.NewStore(db, agentSQL.Postgres{})
//	if err := store.Setup(ctx); err != nil { ... }
//
// **业务自管**: 表名通过 NewStore option WithStoreTable 配置; column 名固定
// (ns / data_key / value / created_at / updated_at); 业务想自管 schema (e.g.,
// 加自己的索引 / 列) 可以**不调 Setup** 自己 CREATE TABLE — 仅需保证列名 + PK
// 一致 (`PRIMARY KEY (ns, data_key)` 必建, 否则 UPSERT silent INSERT 重复行), 且
// **ns / data_key 按字节比较与排序**: PG 声明 `COLLATE "C"`, MySQL 用 VARBINARY, SQLite
// 默认的 BINARY 即可. 否则 MySQL 上逐字节不同的 ns / key 会落进同一行——utf8mb4_bin 忽略
// 尾随空格, 默认的 utf8mb4_0900_ai_ci 还不区分大小写和重音——PG 上 ListNamespaces 的次序随库
// 的排序规则变 (理由见下面各方言的建表注释).
//
// **业务自定义 dialect** (e.g., CockroachDB / TiDB) 业务自管 schema, Setup() 仅
// 支持 PostgresDialect / MySQLDialect.
func (s *Store) Setup(ctx context.Context) error {
	migrations, err := s.storeMigrations()
	if err != nil {
		return err
	}
	for i, m := range migrations {
		if _, err := s.db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("sql store setup migration %d failed: %w", i+1, err)
		}
	}
	return nil
}

// storeMigrations 返本 Store 的 DDL 列表 (按 dialect 选 PG 或 MySQL 版本).
// 业务自定义 dialect 不支持 Setup, 返 error.
func (s *Store) storeMigrations() ([]string, error) {
	switch s.dialect.(type) {
	case Postgres:
		return postgresStoreMigrations(s.table), nil
	case MySQL:
		return mysqlStoreMigrations(s.table), nil
	case SQLite:
		return sqliteStoreMigrations(s.table), nil
	default:
		return nil, errors.New("sql store Setup: business 自定义 dialect 不支持 Setup, 业务自管 schema")
	}
}

// postgresStoreMigrations 返 PG store 表 DDL.
//
// **Schema 设计**:
//   - ns / data_key 用 TEXT (PG 任意长度无 性能差异), 声明 `COLLATE "C"`: PG 的等值比较
//     本来就逐字节, 但 ORDER BY 跟着排序规则走, 不声明时用库的默认规则——en_US / ICU 这类
//     语言规则大小写在第一层不区分、标点排在字母前面, ListNamespaces 的次序与分页窗口就和
//     其它后端不一样 (真 PG 16.9 的 ICU en-US 库实测: "o.a" 排在 "o.B" 前、"o._c" 排最前).
//   - value 用 JSONB (PG 推荐, 比 JSON 快 + GIN index 支持)
//   - created_at / updated_at 用 TIMESTAMPTZ 强制 UTC (业务 read 跟 store
//     conformance Timestamps_Use_UTC_Location 一致)
//   - PRIMARY KEY (ns, data_key) 触发 ON CONFLICT UPSERT
//   - 加 ns 索引让前缀 Search 快速
func postgresStoreMigrations(table string) []string {
	// audit iter-4 视角 3 H6 修: 原 `DEFAULT (NOW() AT TIME ZONE 'UTC')` 实际返
	// `TIMESTAMP WITHOUT TIME ZONE`, 写 `TIMESTAMPTZ` 列时被 session TZ 解释 →
	// 业务 PG session TZ=Asia/Shanghai 时 DEFAULT 值跟 UTC 差 8h. 改为直接
	// `DEFAULT NOW()` (返 TIMESTAMPTZ, 内部存 UTC, 不受 session TZ 影响). 业务
	// PG TIMESTAMPTZ 列的 NOW() 是 UTC-correct.
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    ns          TEXT COLLATE "C" NOT NULL,
    data_key    TEXT COLLATE "C" NOT NULL,
    value       JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ns, data_key)
);`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_ns_idx ON %s (ns);`, table, table),
	}
}

// mysqlStoreMigrations 返 MySQL store 表 DDL.
//
// **audit iter-5 视角 2 HIGH-1 + 视角 4 H7 修 (回滚 iter-4 stored procedure)**:
// iter-4 用 `SET @var` + `PREPARE/EXECUTE/DEALLOCATE` 5 SQL 模拟 IF NOT EXISTS
// 是 R29 第 5 次教训实证: Go `database/sql` 连接池**不保证**多次 ExecContext
// 用同一 conn, MySQL session 变量是 connection-local → `PREPARE stmt FROM
// @sql` 拿 NULL fail. sqlmock 永远单连接掩盖 (5 类炸弹 #5 mock trap).
//
// **真业务接 MySQL 跑 Setup() 100% fail**.
//
// 修法: 回到简单 `CREATE INDEX` 不带 IF NOT EXISTS (MySQL 5.7 不支持), godoc
// 明示 Setup 不幂等 — 业务必须**单次跑**, 推荐放 K8s init container / 独立
// migration job (不在多 pod 业务启动时跑). godoc 不再撒谎"幂等".
//
// **ns / data_key 用二进制串 (VARBINARY)**: 它们是 store 的身份, 必须逐字节比较. utf8mb4_bin
// 在 MySQL 8.0 是 PAD SPACE——比较时忽略尾随空格, 主键把只差尾随空格的 "p.a " 与 "p.a" 当成
// 同一行 (真 MySQL 8.0.35 实测: 后写的覆盖先写的, Get 拿到另一个 namespace 的值, 删 "p.a"
// 连带删掉 "p.a ", err == nil). 二进制串逐字节比较、不填充, ORDER BY 也是字节序, MySQL 5.7+
// 与各分支上都一样. NO PAD 的 utf8mb4_0900_bin 也能修, 但要 8.0.17+, 且 MariaDB 11.4.5 把
// 这个名字当成 PAD SPACE 的 utf8mb4_bin (MDEV-36361).
//
// **主键列宽**: InnoDB 索引键上限 3072 字节. ns VARBINARY(2048) + data_key VARBINARY(1020)
// = 3068 字节 (与 cp 表 sid 1020 + ns 2048 同形状, 离上限 4 字节). **MySQL 上 key 最长 1020
// 字节、ns 拼接后最长 2048 字节**是本方言的长度契约, 按最宽的 4 字节字符算正好 255 / 512 个
// 字符 (store.Store.Put 说的可移植上限; PG / SQLite 用 TEXT 无此限); 超长写入在 STRICT 模式
// 报 "Data too long", 非 STRICT 模式静默截断——业务别关 STRICT. 两列宽合计超过 3072 字节时
// `CREATE TABLE` 直接报 Error 1071 (2026-09-03 两列都是 VARCHAR(512) 时实测).
//
// **Schema 设计**:
//   - ns VARBINARY(2048) / data_key VARBINARY(1020) (身份列, 长度按字节, 合计 ≤ 3072 字节见上)
//   - value 用 JSON (MySQL 5.7+)
//   - created_at / updated_at 用 TIMESTAMP(6) 含微秒
//   - 业务 DSN 必填 `parseTime=true&loc=UTC`
//   - PRIMARY KEY (ns, data_key) 触发 ON DUPLICATE KEY UPDATE
//   - 表默认 CHARSET=utf8mb4 COLLATE=utf8mb4_bin (现有列都不用它, 留给业务自己加的文本列)
func mysqlStoreMigrations(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    ns          VARBINARY(2048) NOT NULL,
    data_key    VARBINARY(1020) NOT NULL,
    value       JSON,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (ns, data_key)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, table),
		fmt.Sprintf(`CREATE INDEX %s_ns_idx ON %s (ns);`, table, table),
	}
}

// sqliteStoreMigrations 返 SQLite store 表 DDL。
//
// 两处与 PG/MySQL 的类型差异，都不影响它替它们验证语义：
//   - **JSON 存 TEXT**（SQLite 无 JSON 类型，`json_*` 直接作用于文本）。
//   - **时间戳用 TIMESTAMP 亲和性**：本模块的 created_at / updated_at 由 Go 侧
//     `time.Now().UTC()` 产生并绑参（见 store.go），DDL 里的 DEFAULT 只是兜底，
//     所以 UTC 语义由 Go 保证、不依赖方言的 NOW()。
//
// 列名、主键、索引与另两个方言逐字对齐——那是它能当语义替身的前提。ns / data_key 用
// TEXT 默认的 BINARY 排序规则，本来就逐字节比较与排序，与 PG 的 `COLLATE "C"`、MySQL 的
// VARBINARY 是同一条规则（见 Setup）。
func sqliteStoreMigrations(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    ns          TEXT NOT NULL,
    data_key    TEXT NOT NULL,
    value       TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ns, data_key)
);`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_ns_idx ON %s (ns);`, table, table),
	}
}
