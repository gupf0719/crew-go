package sql

import (
	"context"
	"errors"
	"fmt"
)

// Setup 创建 Checkpointer 表 + 索引. 业务一次调一次, `CREATE TABLE IF NOT EXISTS`
// 重复调幂等.
//
// **schema 设计**:
//   - PRIMARY KEY (sid, ns) — UPSERT 触发
//   - (sid, status) 复合索引 — ListPending 高效
//   - sid / ns / parent_ns / graph_id 按字节比较与排序 (PG `COLLATE "C"`、MySQL VARBINARY、
//     SQLite 默认的 BINARY)——(sid, ns) 是存档的身份, MySQL 文本列的排序规则会让逐字节不同的
//     两个会话 / 两个 ns 落进同一行 (utf8mb4_bin 忽略尾随空格, 默认的 utf8mb4_0900_ai_ci 还不
//     区分大小写和重音; 理由见各方言的建表注释)
//
// 业务自定义 dialect (CockroachDB / TiDB) 不支持 Setup, 业务自管 schema, 上面这条照做.
func (c *Checkpointer) Setup(ctx context.Context) error {
	migrations, err := c.cpMigrations()
	if err != nil {
		return err
	}
	for i, m := range migrations {
		if _, err := c.db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("sql cp setup migration %d failed: %w", i+1, err)
		}
	}
	return nil
}

func (c *Checkpointer) cpMigrations() ([]string, error) {
	switch c.dialect.(type) {
	case Postgres:
		return postgresCheckpointMigrations(c.table), nil
	case MySQL:
		return mysqlCheckpointMigrations(c.table), nil
	case SQLite:
		return sqliteCheckpointMigrations(c.table), nil
	default:
		return nil, errors.New("sql cp Setup: 业务自定义 dialect 不支持 Setup, 业务自管 schema")
	}
}

// postgresCheckpointMigrations 返 PG cp 表 DDL.
//
// **Schema 关键决策**:
//   - sid / ns / parent_ns / graph_id 用 TEXT (PG 任意长度无性能差), 声明 `COLLATE "C"`:
//     PG 的等值比较本来就逐字节; cp 的枚举 (ListPending / LoadAllInSession) 不承诺次序,
//     声明它是为了与 store 表守同一条规则——身份列按字节比较与排序, 不随库的排序规则变
//   - status SMALLINT (compose_v2.CheckpointStatus 是 int, 但实际值 0/1, smallint 够)
//   - schema_ver INTEGER
//   - created_at BIGINT (Checkpoint.CreatedAt 是 int64 Unix timestamp 不是 time.Time)
//   - payload JSON（**不是 JSONB**）— 完整 cp marshal 含 Channels + CurrentFrontier（成员身份
//     内联）+ PendingInterrupts + NodeVisits。存档只整段存取、从不在库里按字段查，用不上 JSONB
//     的算子；而 JSONB 拒收 `\u0000`（PG 的 text 放不下 NUL，报 22P05），存档里只要有一个值
//     带 NUL（工具读了个二进制文件、嵌套调用的完成记录里带着那段转录）整份就写不进去——实测
//     父节点的中断档因此丢失、续跑报「没有存档」。JSON 类型原样存文本，接受 `\u0000`。
//   - 索引: (sid, status) for ListPending
func postgresCheckpointMigrations(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    sid          TEXT COLLATE "C" NOT NULL,
    ns           TEXT COLLATE "C" NOT NULL,
    parent_ns    TEXT COLLATE "C" NOT NULL DEFAULT '',
    status       SMALLINT NOT NULL DEFAULT 0,
    schema_ver   INTEGER NOT NULL DEFAULT 0,
    graph_id     TEXT COLLATE "C" NOT NULL DEFAULT '',
    created_at   BIGINT NOT NULL DEFAULT 0,
    payload      JSON NOT NULL,
    PRIMARY KEY (sid, ns)
);`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_sid_status_idx ON %s (sid, status);`, table, table),
	}
}

// mysqlCheckpointMigrations 返 MySQL cp 表 DDL.
//
// audit iter-5 视角 2 HIGH-1 修: 同款回滚 stored procedure 到简单 CREATE INDEX.
// 见 store_schema.go::mysqlStoreMigrations 详细说明.
//
// **sid / ns / parent_ns / graph_id 用二进制串 (VARBINARY)**, 理由同 store 表: utf8mb4_bin
// 是 PAD SPACE, 主键把只差尾随空格的 (sid, ns) 当成同一行——真 MySQL 8.0.35 实测, 会话 "s "
// 的存档覆盖了会话 "s" 的, 删会话 "s" 连带删掉 "s " 的, err == nil. 列宽按字节, 与改之前的
// 字节容量相同 (sid 1020 + ns 2048 = 3068 字节, 贴着 InnoDB 索引键 3072 字节的上限).
func mysqlCheckpointMigrations(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    sid          VARBINARY(1020) NOT NULL,
    ns           VARBINARY(2048) NOT NULL,
    parent_ns    VARBINARY(2048) NOT NULL DEFAULT '',
    status       SMALLINT NOT NULL DEFAULT 0,
    schema_ver   INT NOT NULL DEFAULT 0,
    graph_id     VARBINARY(1020) NOT NULL DEFAULT '',
    created_at   BIGINT NOT NULL DEFAULT 0,
    payload      JSON NOT NULL,
    PRIMARY KEY (sid, ns)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, table),
		fmt.Sprintf(`CREATE INDEX %s_sid_status_idx ON %s (sid, status);`, table, table),
	}
}

// sqliteCheckpointMigrations 返 SQLite cp 表 DDL。
//
// 与 PG/MySQL 的差异只在类型名：SQLite 是动态类型，`TEXT` / `INTEGER` 只是亲和性
// 提示；JSON 存 TEXT（`json_*` 函数直接作用于文本，见 [SQLite] 的 godoc）。
// 列名、主键、索引与另两个方言**逐字对齐**——那正是它能替它们验证语义的前提。
// sid / ns / parent_ns / graph_id 用 TEXT 默认的 BINARY 排序规则，本来就逐字节比较与
// 排序，与另两个方言的身份列同一条规则（见 Setup）。
func sqliteCheckpointMigrations(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    sid          TEXT NOT NULL,
    ns           TEXT NOT NULL,
    parent_ns    TEXT NOT NULL DEFAULT '',
    status       INTEGER NOT NULL DEFAULT 0,
    schema_ver   INTEGER NOT NULL DEFAULT 0,
    graph_id     TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL DEFAULT 0,
    payload      TEXT NOT NULL,
    PRIMARY KEY (sid, ns)
);`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_sid_status_idx ON %s (sid, status);`, table, table),
	}
}
