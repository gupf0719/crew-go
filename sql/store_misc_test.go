package sql_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
	"crew-go/store"
)

// TestStore_Does_Not_Implement_TTLAware 验 sql.Store 不实现 store.TTLAware
// interface — 业务 type-assert (`(store.TTLAware)`) 返 false 让 conformance
// TTL 子测试自动 skip "expire" 路径走 "ErrTTLNotSupported" 路径.
//
// **audit iter-4 视角 1 H2 修**: 原 sql.Store 实现 TTLAware 但 SupportsTTL()
// 返 false 违反 `store/conformance/ttl.go:22-25` contract "实现了 TTLAware
// 必须返 true".
//
// **sanity-revert 验证**: 加回 `func (s *Store) SupportsTTL() bool { return false }`
// + `var _ store.TTLAware = (*Store)(nil)` → 业务 type-assert ok=true → 此
// 测试 fail.
func TestStore_Does_Not_Implement_TTLAware(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	// type-assert (store.TTLAware) — sql.Store **不**应实现此接口
	_, ok := interface{}(st).(store.TTLAware)
	if ok {
		t.Error("sql.Store should NOT implement store.TTLAware (PoC 不支持 TTL); 跑业务 conformance TTLAware_TypeAssert 必 fail")
	}
}

// TestStore_ListNamespaces_Suffix_Returns_Error 验业务调 WithListSuffix 时
// sql.Store 返 ErrUnsupportedFilterOp (fail-fast) 而非 silent 忽略.
//
// **audit iter-4 视角 2 H1 修**: 原 cfg.Suffix 拿到后**完全没用** silent 忽略,
// 业务 InMem 阶段用 Suffix 过滤跑通, 接 sql backend 返**全部 ns** 跨 backend
// 行为漂移.
//
// **sanity-revert 验证**: 删 `if len(cfg.Suffix) > 0 { return error }` →
// Suffix 静默忽略, 业务 InMem 行为不复现, sql 返全部 namespace silent 漂移.
// 测试 errors.Is 失败 fail.
func TestStore_ListNamespaces_Suffix_Returns_Error(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	_, err := st.ListNamespaces(context.Background(),
		store.WithListSuffix(store.NS("history")),
	)
	if err == nil {
		t.Error("ListNamespaces with WithListSuffix should return error (跨 backend silent 漂移防御)")
	}
	if !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("expected ErrUnsupportedFilterOp, got %v", err)
	}
}

// TestStore_Get_Int64_Preserves_Precision_Via_UseNumber 验业务 Put int64
// Snowflake ID / timestamp_ms 后 Get 拿 json.Number 保留大整数精度.
//
// **audit iter-4 视角 3 H5 + iter-5 unmarshalValue 修**: 原 json.Unmarshal
// 数字 → float64. int64 > 2^53 精度丢失 — 业务 Snowflake ID (e.g.,
// 1234567890123456789) 业务读出来变 1234567890123456800 (低位丢).
//
// **sanity-revert 验证**: 删 sql_helpers.go::unmarshalValue 改回 json.Unmarshal →
// 业务 Get int64 拿 float64 精度丢失 (本测试期望 json.Number string 保大整数,
// 拿 float64 时 String() 不一致 fail).
func TestStore_Get_Int64_Preserves_Precision_Via_UseNumber(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	// 业务真 Snowflake ID 例 (大于 2^53)
	const bigInt64 int64 = 1234567890123456789
	// sql backend JSON 存 number 字面量 "1234567890123456789"
	rows := sqlmock.NewRows([]string{"value", "created_at", "updated_at"}).
		AddRow([]byte(`1234567890123456789`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT value").
		WithArgs("test", "snowflake").
		WillReturnRows(rows)

	item, err := st.Get(context.Background(), store.NS("test"), "snowflake")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if item == nil {
		t.Fatal("expected item, got nil")
	}

	// 业务期望 json.Number 类型 (跨 backend safe 大整数保留)
	num, ok := item.Value.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number to preserve int64 precision, got %T: %v (业务 Snowflake ID 精度丢失)",
			item.Value, item.Value)
	}

	// 验大整数精度 — Int64() 应等于原 bigInt64
	got, err := num.Int64()
	if err != nil {
		t.Fatalf("json.Number.Int64: %v", err)
	}
	if got != bigInt64 {
		t.Errorf("精度丢失: expected %d, got %d (json.Number Int64 应保大整数精度)",
			bigInt64, got)
	}
}
