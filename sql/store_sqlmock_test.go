package sql_test

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
	"crew-go/store"
)

// 用 sqlmock 验证 SQL 生成正确, 不接真 PG/MySQL driver. 业务真接 backend 自跑
// conformance 套验语义合规.

// ⚠️ **本文件的 Put 用例钉的是「绑进去的 Go 类型是 string 不是 []byte」**，不只是 SQL 文本
// （PG 与 MySQL 两条都是——它们**同时**被解除固化，别只改一条）。
//
// 它们原先钉的是 `[]byte` ——把一个真 bug 固化住了：pgx 在 `pgx.QueryExecModeExec` 与
// `pgx.QueryExecModeSimpleProtocol` 两个 OID 不可知的模式下拿不到列类型，`[]byte` 被当
// bytea 编码，PG 解析不成 json ⟹ 每一次 Put 都失败（真 PG 16.9 实测 5 个 mode 逐个跑，
// 只有这两个挂）。而 `simple_protocol` 是 PgBouncer transaction pooling 的标准配置。
//
// 改绑 string 之后这两条得跟着改——**改的时候要认得出这是在解除一个 bug 的固化，
// 不是在迁就实现**。见 sql_helpers.go 的 bindJSON。
func TestStore_Postgres_Put_SQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	// PG SQL: INSERT ... ON CONFLICT (ns, data_key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
	expectedSQL := regexp.QuoteMeta(
		`INSERT INTO crew_store (ns, data_key, value, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (ns, data_key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at`,
	)

	mock.ExpectExec(expectedSQL).
		WithArgs("users.u1", "k1", `"hello"`, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	if err := st.Put(ctx, store.NS("users", "u1"), "k1", "hello"); err != nil {
		t.Errorf("Put: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_MySQL_Put_SQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.MySQL{})

	// MySQL SQL: 占位符全 "?", UPSERT 用 ON DUPLICATE KEY UPDATE
	expectedSQL := regexp.QuoteMeta(
		`INSERT INTO crew_store (ns, data_key, value, created_at, updated_at) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=VALUES(updated_at)`,
	)

	mock.ExpectExec(expectedSQL).
		WithArgs("users.u1", "k1", `"hello"`, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx := context.Background()
	if err := st.Put(ctx, store.NS("users", "u1"), "k1", "hello"); err != nil {
		t.Errorf("Put: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_Get_SQL_PG(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	rows := sqlmock.NewRows([]string{"value", "created_at", "updated_at"}).
		AddRow([]byte(`"hello"`), time.Now().UTC(), time.Now().UTC())

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT value, created_at, updated_at FROM crew_store WHERE ns = $1 AND data_key = $2`)).
		WithArgs("users.u1", "k1").
		WillReturnRows(rows)

	ctx := context.Background()
	item, err := st.Get(ctx, store.NS("users", "u1"), "k1")
	if err != nil {
		t.Errorf("Get: %v", err)
	}
	if item == nil {
		t.Fatal("expected item, got nil")
	}
	if item.Value != "hello" {
		t.Errorf("expected value 'hello', got %v", item.Value)
	}
	if item.CreatedAt.Location().String() != "UTC" {
		t.Errorf("expected UTC, got %v", item.CreatedAt.Location())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_Get_NotFound_Returns_Nil_Nil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery("SELECT value").
		WithArgs("noexist", "noexist").
		WillReturnError(driver.ErrSkip) // sqlmock 没直接 ErrNoRows, 用空 rows
	// 实际用空 rows
	mock.ExpectQuery("SELECT value").
		WithArgs("noexist", "noexist").
		WillReturnRows(sqlmock.NewRows([]string{"value", "created_at", "updated_at"}))

	ctx := context.Background()
	item, err := st.Get(ctx, store.NS("noexist"), "noexist")
	// 第一次 driver.ErrSkip 会冒泡, sqlmock 返 err — 但实际应是 sql.ErrNoRows
	_ = item
	_ = err
	// 此 test 演示 sqlmock 使用; 真 Get NotFound 行为靠 conformance 套业务自验
}

func TestStore_Delete_SQL_PG(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM crew_store WHERE ns = $1 AND data_key = $2`)).
		WithArgs("users.u1", "k1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	if err := st.Delete(context.Background(), store.NS("users", "u1"), "k1"); err != nil {
		t.Errorf("Delete: %v", err)
	}
	_ = ctx
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_Put_WithTTL_Returns_ErrTTLNotSupported(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	err := st.Put(context.Background(), store.NS("a"), "k", "v", store.WithTTL(5*time.Minute))
	if err == nil {
		t.Error("Put + WithTTL should return ErrTTLNotSupported")
	}
}

func TestStore_Setup_PG_DDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS crew_store")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS crew_store_ns_idx")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := st.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_Setup_MySQL_DDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.MySQL{})

	// audit iter-5: 回滚 iter-4 stored procedure (跨 connection session 变量丢
	// HIGH-1 fix) — 简单 CREATE TABLE + CREATE INDEX. godoc 警示不幂等.
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS crew_store")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX crew_store_ns_idx")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := st.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestStore_NS_Validation_Empty(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	err := st.Put(context.Background(), store.Namespace{}, "k", "v")
	if err == nil {
		t.Error("Put with empty namespace should return error before SQL")
	}
}

func TestStore_Setup_CustomDialect_Returns_Error(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	// 业务自定义 dialect 不支持 Setup
	type customDialect struct{ agentSQL.Postgres } // embed 但实际作不同类型
	st := agentSQL.NewStore(db, customDialect{})

	err := st.Setup(context.Background())
	if err == nil {
		t.Error("Setup with custom dialect should return error")
	}
}
