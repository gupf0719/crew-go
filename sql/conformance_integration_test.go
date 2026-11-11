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
