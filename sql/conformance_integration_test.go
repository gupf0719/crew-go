//go:build integration

package sql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"crew-go/agent_v2"
	"crew-go/agent_v2/history_conformance"
	"crew-go/compose_v2"
	"crew-go/compose_v2/checkpointer_conformance"
	crewsql "crew-go/sql"
	"crew-go/store"
	"crew-go/store/conformance"
)

func TestPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("CREW_SQL_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("设置 CREW_SQL_POSTGRES_DSN 以连接 PostgreSQL 测试库")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("CREW_SQL_POSTGRES_DSN 无法解析")
	}

	for _, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeExec,
		pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			runSQLBackendConformance(t, crewsql.Postgres{}, func() *sql.DB {
				conn := config.Copy()
				conn.DefaultQueryExecMode = mode
				return stdlib.OpenDB(*conn)
			})
		})
	}
	t.Run("SharedApproval", func(t *testing.T) {
		runSQLSharedApproval(t, "pgx", dsn)
	})
	t.Run("AGUISharedSessions", func(t *testing.T) {
		runAGUISessionScenarios(t, crewsql.Postgres{}, func() *sql.DB {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
	t.Run("BackgroundTasks", func(t *testing.T) {
		runBackgroundTaskScenarios(t, crewsql.Postgres{}, func() *sql.DB {
			return stdlib.OpenDB(*config)
		})
	})
	t.Run("StoreTTLSweepWaitsForRenewal", func(t *testing.T) {
		runPostgresTTLSweepWaitsForRenewal(t, stdlib.OpenDB(*config))
	})
	t.Run("StoreTTLMigratesVisibleLegacyTable", func(t *testing.T) {
		runPostgresTTLMigratesVisibleLegacyTable(t, stdlib.OpenDB(*config))
	})
}

func TestMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("CREW_SQL_MYSQL_DSN")
	if dsn == "" {
		t.Skip("设置 CREW_SQL_MYSQL_DSN 以连接 MySQL 测试库")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("CREW_SQL_MYSQL_DSN 无法解析")
	}
	config.ParseTime = true

	for _, interpolate := range []bool{false, true} {
		t.Run(fmt.Sprintf("interpolateParams=%t", interpolate), func(t *testing.T) {
			conn := config.Clone()
			conn.InterpolateParams = interpolate
			connector, err := mysql.NewConnector(conn)
			if err != nil {
				t.Fatal(err)
			}
			runSQLBackendConformance(t, crewsql.MySQL{}, func() *sql.DB {
				return sql.OpenDB(connector)
			})
		})
	}
	t.Run("SharedApproval", func(t *testing.T) {
		runSQLSharedApproval(t, "mysql", config.FormatDSN())
	})
	t.Run("AGUISharedSessions", func(t *testing.T) {
		runAGUISessionScenarios(t, crewsql.MySQL{}, func() *sql.DB {
			db, err := sql.Open("mysql", config.FormatDSN())
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
	t.Run("BackgroundTasks", func(t *testing.T) {
		connector, err := mysql.NewConnector(config)
		if err != nil {
			t.Fatal(err)
		}
		runBackgroundTaskScenarios(t, crewsql.MySQL{}, func() *sql.DB {
			return sql.OpenDB(connector)
		})
	})
}

func newSQLTestTable() string {
	return "crew_test_" + strings.ToLower(rand.Text())
}

func waitForSQLTestDB(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := db.PingContext(ctx)
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("测试数据库未就绪：%v", err)
		case <-ticker.C:
		}
	}
}

func runSQLBackendConformance(t *testing.T, dialect crewsql.Dialect, open func() *sql.DB) {
	t.Helper()
	db := open()
	t.Cleanup(func() { _ = db.Close() })
	waitForSQLTestDB(t, db)

	t.Run("Checkpointer", func(t *testing.T) {
		checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
			table := newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, db, table+"_task_lease", table)
			cp := crewsql.NewCheckpointer(db, dialect, crewsql.WithCheckpointerTable(table))
			if err := cp.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			return cp
		})
	})
	t.Run("Store", func(t *testing.T) {
		conformance.RunStoreTests(t, func() store.Store {
			table := newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, db, table)
			st := crewsql.NewStore(db, dialect, crewsql.WithStoreTable(table))
			if err := st.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			return st
		})
	})
	t.Run("StoreTTL", func(t *testing.T) {
		conformance.RunStoreTests(t, func() store.Store {
			table := newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, db, table)
			st := newTTLTestStore(t, db, dialect, table)
			if err := st.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			return st
		})
		t.Run("VisibilityRetentionAndClock", func(t *testing.T) {
			clockDB := open()
			t.Cleanup(func() { _ = clockDB.Close() })
			clockDB.SetMaxOpenConns(1)
			// 保持单条连接的非 UTC 会话时区，写入与读取仍以数据库 UTC 纪元作过期比较。
			switch dialect.(type) {
			case crewsql.Postgres:
				ttlExec(t, clockDB, "SET TIME ZONE INTERVAL '+10:30' HOUR TO MINUTE")
			case crewsql.MySQL:
				ttlExec(t, clockDB, "SET SESSION time_zone = '+10:30'")
			}
			table, history := newSQLTestTable(), newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, clockDB, history, table)
			checkSQLStoreTTL(t, clockDB, dialect, table, history)
			if _, mysqlClock := dialect.(crewsql.MySQL); mysqlClock {
				checkMySQLTTLPinnedClock(t, clockDB)
			}
		})
	})
	if _, isPostgres := dialect.(crewsql.Postgres); isPostgres {
		t.Run("DecimalFilterPrecision", func(t *testing.T) {
			table := newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, db, table)
			st := crewsql.NewStore(db, dialect, crewsql.WithStoreTable(table))
			if err := st.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertDecimalFilterPrecision(t, st, true)
		})
	}
	t.Run("HistoryStore", func(t *testing.T) {
		history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
			table := newSQLTestTable()
			dropSQLTestTablesOnCleanup(t, db, table)
			history := crewsql.NewHistoryStore(db, dialect, crewsql.WithHistoryTable(table))
			if err := history.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			return history
		})
	})
	t.Run("TaskLease", func(t *testing.T) {
		table := newSQLTestTable()
		leaseTable := table + "_task_lease"
		dropSQLTestTablesOnCleanup(t, db, leaseTable, table)
		cp := crewsql.NewCheckpointer(db, dialect, crewsql.WithCheckpointerTable(table))
		if err := cp.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
		peerDB := open()
		t.Cleanup(func() { _ = peerDB.Close() })
		peer := crewsql.NewCheckpointer(peerDB, dialect, crewsql.WithCheckpointerTable(table))
		checkpointer_conformance.RunTaskLeaseTests(t, cp, peer, func(sid string) {
			q := fmt.Sprintf("UPDATE %s SET expires_at = 0 WHERE sid = %s", leaseTable, dialect.Placeholder(1))
			if _, err := db.ExecContext(t.Context(), q, sid); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func checkMySQLTTLPinnedClock(t *testing.T, db *sql.DB) {
	t.Helper()
	table := newSQLTestTable()
	dropSQLTestTablesOnCleanup(t, db, table)
	st := newTTLTestStore(t, db, &crewsql.MySQL{}, table)
	ctx := t.Context()
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	// 改当前连接的语句时钟，故意让它与测试进程的时间相差数年。
	ttlExec(t, db, "SET SESSION timestamp = 1700000000.125")
	ns := store.NS("fixed")
	if err := st.Put(ctx, ns, "k", "v", store.WithTTL(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var expires int64
	if err := db.QueryRowContext(ctx, "SELECT expires_at_ms FROM "+table).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if expires != 1700000001625 {
		t.Fatalf("写入应从实际语句时钟加上 TTL，得到 %d", expires)
	}
	ttlExec(t, db, "SET SESSION timestamp = 1700000001.624")
	requireTTLItem(t, st, ns, "k")
	ttlExec(t, db, "SET SESSION timestamp = 1700000001.625")
	if got, err := st.Get(ctx, ns, "k"); err != nil || got != nil {
		t.Fatalf("读操作应在数据库截止时刻返回缺失，得到 %+v，err=%v", got, err)
	}
	if n, err := st.SweepExpired(ctx); err != nil || n != 1 {
		t.Fatalf("清扫应使用同一个数据库时钟，得到 %d，err=%v", n, err)
	}
	ttlExec(t, db, "SET SESSION timestamp = DEFAULT")
}

func runPostgresTTLMigratesVisibleLegacyTable(t *testing.T, db *sql.DB) {
	t.Helper()
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	waitForSQLTestDB(t, db)
	ctx := t.Context()
	table := newSQLTestTable()
	base := crewsql.NewStore(db, crewsql.Postgres{}, crewsql.WithStoreTable(table))
	if err := base.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	var schemaName string
	const lookup = `SELECT pg_catalog.quote_ident(n.nspname) FROM pg_catalog.pg_namespace n
		JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace WHERE c.oid = pg_catalog.to_regclass($1)`
	if err := db.QueryRowContext(ctx, lookup, table).Scan(&schemaName); err != nil {
		t.Fatal(err)
	}
	dropSQLTestTablesOnCleanup(t, db, schemaName+"."+table)
	ns := store.NS("legacy")
	if err := base.Put(ctx, ns, "key", "原表"); err != nil {
		t.Fatal(err)
	}
	// 让首个 schema 可建表，但普通 Store 的表名仍解析到后面的既有表。
	ttlExec(t, db, "CREATE TEMP TABLE ttl_schema_marker (id INTEGER)")
	ttlExec(t, db, "SELECT pg_catalog.set_config('search_path', 'pg_temp,' || pg_catalog.current_setting('search_path'), false)")
	if got := requireTTLItem(t, base, ns, "key"); got.Value != "原表" {
		t.Fatalf("旧表应仍在搜索路径上，得到 %+v", got)
	}
	st := newTTLTestStore(t, db, crewsql.Postgres{}, table)
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	var same bool
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.to_regclass($1) = pg_catalog.to_regclass($2)",
		table, schemaName+"."+table).Scan(&same); err != nil || !same {
		t.Fatalf("迁移应继续使用旧表，解析到原表 %v，err=%v", same, err)
	}
	if got := requireTTLItem(t, st, ns, "key"); got.Value != "原表" {
		t.Fatalf("迁移后的原表数据应可用，得到 %+v", got)
	}
}

func runPostgresTTLSweepWaitsForRenewal(t *testing.T, db *sql.DB) {
	t.Helper()
	t.Cleanup(func() { _ = db.Close() })
	waitForSQLTestDB(t, db)
	table := newSQLTestTable()
	dropSQLTestTablesOnCleanup(t, db, table)
	st := newTTLTestStore(t, db, &crewsql.Postgres{}, table)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	ns := store.NS("renewal")
	if err := st.Put(ctx, ns, "k", "续期前", store.WithTTL(time.Hour)); err != nil {
		t.Fatal(err)
	}
	ttlExec(t, db, "UPDATE "+table+" SET expires_at_ms = -1")
	renewal, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = renewal.Rollback() }() // Commit 后回滚无须处理。
	if _, err := renewal.ExecContext(ctx, "UPDATE "+table+" SET expires_at_ms = "+
		"floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + 3600000"); err != nil {
		t.Fatal(err)
	}

	type sweepResult struct {
		n   int64
		err error
	}
	result := make(chan sweepResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		n, err := st.SweepExpired(ctx)
		result <- sweepResult{n, err}
	}()
	defer func() {
		cancel()
		<-finished
	}()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		const probe = "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND strpos(query, $1) = 1)"
		var blocked bool
		if err := db.QueryRowContext(ctx, probe, "DELETE FROM "+table+" WHERE").Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case r := <-result:
			t.Fatalf("清扫应等待在途续期，得到 %+v", r)
		case <-ctx.Done():
			t.Fatal("等待清扫与续期重叠超时")
		case <-ticker.C:
		}
	}
	if err := renewal.Commit(); err != nil {
		t.Fatal(err)
	}
	if r := <-result; r.err != nil || r.n != 0 {
		t.Fatalf("清扫取得锁后应保留已续期的行，得到 %+v", r)
	}
	if item := requireTTLItem(t, st, ns, "k"); item.Value != "续期前" {
		t.Fatalf("续期后的内容应保留，得到 %+v", item)
	}
}

func runSQLSharedApproval(t *testing.T, driver, dsn string) {
	t.Helper()
	newFixture := func(t *testing.T) sharedApproval {
		return newSharedApproval(t, sharedApprovalBackend{driver: driver, dsn: dsn, prefix: newSQLTestTable()})
	}
	t.Run("AbandonDuringResume", func(t *testing.T) {
		runAbandonDuringResume(t, newFixture(t))
	})
	t.Run("ResumeDuringAbandon", func(t *testing.T) {
		runResumeDuringAbandon(t, newFixture(t))
	})
}
