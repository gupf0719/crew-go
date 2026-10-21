package sql_test

import (
	"context"
	databasesql "database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
)

// TestStore_Setup_PG_Generates_Correct_NOW_DDL 验 PG Setup 生成的 DDL 用
// `DEFAULT NOW()` (不是 `DEFAULT (NOW() AT TIME ZONE 'UTC')`).
//
// **audit iter-4 视角 3 H6 修**: 原 `DEFAULT (NOW() AT TIME ZONE 'UTC')` 实际
// 返 `TIMESTAMP WITHOUT TIME ZONE`, 写 `TIMESTAMPTZ` 列被 session TZ 解释 →
// 业务 PG session TZ=Asia/Shanghai 时 DEFAULT 值跟 UTC 差 8h.
//
// **sanity-revert 验证 (iter-5-F audit 视角 2 H1 修死代码)**: 改
// `sql/store_schema.go::postgresStoreMigrations` 回 `DEFAULT (NOW() AT TIME
// ZONE 'UTC')` → 此测试 fail (mock regex 不匹配 SQL 含 `AT TIME ZONE`).
func TestStore_Setup_PG_Generates_Correct_NOW_DDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	// 期望 CREATE TABLE 含 plain `DEFAULT NOW(),` (PG TIMESTAMPTZ native UTC)
	// 不含 `AT TIME ZONE` (会触发 session TZ 解释 bug)
	mock.ExpectExec(`DEFAULT NOW\(\),`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE INDEX").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := st.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		if strings.Contains(err.Error(), "AT TIME ZONE") {
			t.Errorf("PG TZ bug regression: SQL contains AT TIME ZONE — session TZ 解释 bug 复现: %v", err)
		} else {
			t.Errorf("expectations not met: %v", err)
		}
	}
}

// TestStore_Setup_MySQL_DDL_No_PREPARE_Stored_Procedure 验 MySQL Setup 生成
// 的 SQL **不含** stored procedure (PREPARE/EXECUTE/DEALLOCATE).
//
// **audit iter-5 视角 2 HIGH-1 修 (回滚 iter-4 stored procedure)**: iter-4 用
// `SET @var + PREPARE + EXECUTE` 5 SQL 模拟 IF NOT EXISTS — Go database/sql
// 连接池不保证多次 Exec 用同一 conn, MySQL session 变量是 connection-local →
// PREPARE 拿 NULL fail. sqlmock 假绿掩盖.
//
// **sanity-revert 验证**: 改 mysqlStoreMigrations 回 stored procedure pattern
// → mock 期望 2 条简单 SQL 但实际跑 6 SQL → expectations 不匹配 fail.
func TestStore_Setup_MySQL_DDL_No_PREPARE_Stored_Procedure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.MySQL{})

	// 期望: 2 条简单 SQL (CREATE TABLE + CREATE INDEX), 不含 PREPARE/EXECUTE
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE INDEX crew_store_ns_idx").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := st.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		if strings.Contains(err.Error(), "PREPARE") || strings.Contains(err.Error(), "@idx_exists") {
			t.Errorf("MySQL Setup stored procedure regression (iter-4 跨 connection bug 复现): %v", err)
		}
	}
}

// TestCheckpointer_Setup_PG_PayloadIsJSONNotJSONB 钉住 PG 存档表的 payload 列是 JSON、不是 JSONB。
//
// JSONB 拒收 `\u0000`（PG 的 text 放不下 NUL，报 22P05），而存档里任何一个值带 NUL——工具读了个
// 二进制文件、嵌套调用的完成记录里带着那段转录——整份存档就写不进去：真 PG 上实测父节点的中断
// 档因此丢失，续跑报「没有存档」。存档只整段存取、从不在库里按字段查，用不上 JSONB 的算子。
//
// SQLite（TEXT）与 sqlmock 都测不出这件事，只能钉 DDL 本身；行为验证在真 PG / 真 MySQL 上跑过
// （值带 NUL 的存档往返、存档 conformance 全套）。
func TestCheckpointer_Setup_PG_PayloadIsJSONNotJSONB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})
	mock.ExpectExec(`payload\s+JSON NOT NULL`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE INDEX").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := cp.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v（payload 列若是 JSONB，存档里任何带 NUL 的值都会让整份写入失败）", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestSetup_IdentityColumnsCompareBytes 钉住 Setup 建的表里身份列按字节比较与排序：store 的
// ns / data_key、cp 的 sid / ns / parent_ns / graph_id，PG 声明 `COLLATE "C"`、MySQL 用 VARBINARY。
//
// 回退了的后果都是 err == nil 的静默错：MySQL 的 utf8mb4_bin 是 PAD SPACE，只差尾随空格的两个
// namespace / key / 会话落进同一行（互相覆盖、删一个带走另一个）；PG 不声明时 ORDER BY 跟库的
// 排序规则走，ListNamespaces 的次序与分页窗口和其它后端不一样。行为由 conformance 的
// Identity_Is_Byte_Exact / KeyIdentity / ListNamespaces_Sorted_By_String_Bytes 钉，
// 但它们只在真 MySQL 与非 C 排序规则的真 PG 上会红（2026-10-15 实测），SQLite 本来就逐字节、
// sqlmock 不执行 SQL，所以这里钉 DDL 本身。
func TestSetup_IdentityColumnsCompareBytes(t *testing.T) {
	for _, c := range []struct {
		name    string
		setup   func(*databasesql.DB) error
		pattern string
	}{
		{
			"store/PG",
			func(db *databasesql.DB) error {
				return agentSQL.NewStore(db, agentSQL.Postgres{}).Setup(context.Background())
			},
			`ns\s+TEXT COLLATE "C" NOT NULL,\s+data_key\s+TEXT COLLATE "C" NOT NULL,`,
		},
		{
			"store/MySQL",
			func(db *databasesql.DB) error {
				return agentSQL.NewStore(db, agentSQL.MySQL{}).Setup(context.Background())
			},
			`ns\s+VARBINARY\(2048\) NOT NULL,\s+data_key\s+VARBINARY\(1020\) NOT NULL,`,
		},
		{
			"checkpointer/PG",
			func(db *databasesql.DB) error {
				return agentSQL.NewCheckpointer(db, agentSQL.Postgres{}).Setup(context.Background())
			},
			`sid\s+TEXT COLLATE "C" NOT NULL,\s+ns\s+TEXT COLLATE "C" NOT NULL,\s+parent_ns\s+TEXT COLLATE "C" NOT NULL DEFAULT '',(?s).*graph_id\s+TEXT COLLATE "C" NOT NULL DEFAULT '',`,
		},
		{
			"checkpointer/MySQL",
			func(db *databasesql.DB) error {
				return agentSQL.NewCheckpointer(db, agentSQL.MySQL{}).Setup(context.Background())
			},
			`sid\s+VARBINARY\(1020\) NOT NULL,\s+ns\s+VARBINARY\(2048\) NOT NULL,\s+parent_ns\s+VARBINARY\(2048\) NOT NULL DEFAULT '',(?s).*graph_id\s+VARBINARY\(1020\) NOT NULL DEFAULT '',`,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			mock.ExpectExec(c.pattern).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("CREATE INDEX").WillReturnResult(sqlmock.NewResult(0, 0))
			if err := c.setup(db); err != nil {
				t.Errorf("Setup 的建表语句里身份列不是按字节比较的类型 / 排序规则: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("expectations not met: %v", err)
			}
		})
	}
}
