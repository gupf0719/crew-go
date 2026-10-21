package sql_test

import (
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"crew-go/compose_v2"
	agentSQL "crew-go/sql"
)

// TestCheckpointer_SaveNS_BindsPayloadAsString 钉「cp 的 payload 绑进去的 Go 类型是
// string 不是 []byte」——三个方言各一遍。
//
// # 为什么这条比 store 那两条更要紧
//
// pgx 在 QueryExecModeExec 与 QueryExecModeSimpleProtocol 两个 OID 不可知的模式下拿不到
// 列类型，只能按 Go 类型猜：[]byte 被当 bytea 编码，PG 解析不成 jsonb ⟹ SaveNS 每次都
// 失败（真 PG 16.9 实测 5 个 mode 逐个跑，只有这两个挂）。而 simple_protocol 是
// PgBouncer transaction pooling 的标准配置。
//
// cp 是 HITL Resume 的落点：store 写不进去业务丢的是长期记忆，**cp 写不进去丢的是
// 「用户答了一半的那件事」**——挂起存不下来，Resume 无从谈起。
//
// 这条用例走 sqlmock 而不是真库，因为要钉的正是「绑的是什么 Go 类型」，那是 driver 之前
// 的事；真库反而看不见（本机 SQLite 两种都收）。见 sql_helpers.go 的 bindJSON。
func TestCheckpointer_SaveNS_BindsPayloadAsString(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dialect agentSQL.Dialect
	}{
		{"Postgres", agentSQL.Postgres{}},
		{"MySQL", agentSQL.MySQL{}},
		{"SQLite", agentSQL.SQLite{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			cp := &compose_v2.Checkpoint{GraphID: "g1", SchemaVer: 1}

			// 三个方言走同一条路：占住这一行（payload 是字面量占位，不绑参数）→ 读 graph_id →
			// 整行写入。只钉整行写入那条的第 6 个参数（payload）的 Go 类型：sqlmock 的 WithArgs
			// 按类型 + 值比对，传 string 时 []byte 实参不匹配（反之亦然），正是我们要的判别力。
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta("INSERT INTO")).
				WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT graph_id FROM")).
				WillReturnRows(sqlmock.NewRows([]string{"graph_id"}).AddRow(""))
			mock.ExpectExec(regexp.QuoteMeta("UPDATE")).
				WithArgs(
					sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
					payloadIsString{t},
					sqlmock.AnyArg(), sqlmock.AnyArg(),
				).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			c := agentSQL.NewCheckpointer(db, tc.dialect)
			if err := c.SaveNS(t.Context(), "sid1", "", cp); err != nil {
				t.Fatalf("SaveNS: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sqlmock expectations: %v", err)
			}
		})
	}
}

// payloadIsString 是 sqlmock.Argument 实现——只认 string，收到 []byte 就不匹配。
type payloadIsString struct{ t *testing.T }

func (p payloadIsString) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		p.t.Errorf("payload 绑成了 %T，应为 string（[]byte 会让 pgx 在 exec / simple_protocol 下按 bytea 编码，PG 解析不成 jsonb）", v)
		return false
	}
	if len(s) == 0 {
		p.t.Errorf("payload 是空串")
		return false
	}
	return true
}
