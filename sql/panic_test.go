package sql_test

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
)

// TestNewStore_Panic_On_Invalid_Table_Name 验业务 `WithStoreTable("foo;DROP")`
// 时 NewStore panic — fail-fast 防 SQL injection (业务 init 时崩, 不让恶意/
// 误传 table name 留到运行时).
//
// **audit iter-4 视角 2 M3 + iter-5-F 视角 1 H3 修**: NewStore / NewCheckpointer
// 加 panic 但 0 测试覆盖 — 业务 init 路径必跑必失败的位置.
//
// **sanity-revert 验证**: 删 NewStore 内 `if err := validateTableName(s.table);
// err != nil { panic(...) }` → 业务恶意 table name 不 panic → 此测试 fail
// (recover 拿 nil).
func TestNewStore_Panic_On_Invalid_Table_Name(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	cases := []struct {
		name      string
		tableName string
	}{
		{"sql_injection_drop", "foo; DROP TABLE users; --"},
		{"empty", ""},
		{"starts_with_digit", "123_table"},
		{"has_space", "my table"},
		{"has_dot", "schema.table"}, // schema-qualified 不支持 (iter-5 LOW M3)
		{"sql_injection_quote", "table' OR '1'='1"},
		{"too_long", strings.Repeat("a", 64)}, // 超过 63 chars
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("NewStore(WithStoreTable(%q)) should panic (SQL injection 防御缺失)", c.tableName)
					return
				}
				// fail 信息含 "invalid table name" 精确指向 validation
				if msg, ok := r.(string); ok {
					if !strings.Contains(msg, "invalid table name") {
						t.Errorf("panic msg should contain 'invalid table name', got: %v", msg)
					}
				}
			}()
			_ = agentSQL.NewStore(db, agentSQL.Postgres{}, agentSQL.WithStoreTable(c.tableName))
		})
	}
}

// TestNewCheckpointer_Panic_On_Invalid_Table_Name 同款 — Checkpointer init
// 路径 panic 防御.
func TestNewCheckpointer_Panic_On_Invalid_Table_Name(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	defer func() {
		r := recover()
		if r == nil {
			t.Error("NewCheckpointer with invalid table name should panic")
			return
		}
		if msg, ok := r.(string); ok {
			if !strings.Contains(msg, "invalid table name") {
				t.Errorf("panic msg should contain 'invalid table name', got: %v", msg)
			}
		}
	}()
	_ = agentSQL.NewCheckpointer(db, agentSQL.Postgres{},
		agentSQL.WithCheckpointerTable("ckpt; DROP TABLE x; --"))
}

// TestNewStore_DefaultTableName_OK 验 default 表名 (DefaultStoreTable) NewStore
// 不 panic — 业务用默认配置正常初始化.
func TestNewStore_DefaultTableName_OK(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewStore with default table name should NOT panic, got: %v", r)
		}
	}()
	_ = agentSQL.NewStore(db, agentSQL.Postgres{})
}
