package sql_test

import (
	"strings"
	"testing"

	"crew-go/sql"
)

func TestPostgres_Placeholder(t *testing.T) {
	d := sql.Postgres{}
	cases := []struct {
		n    int
		want string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
	}
	for _, c := range cases {
		got := d.Placeholder(c.n)
		if got != c.want {
			t.Errorf("Placeholder(%d): expected %q, got %q", c.n, c.want, got)
		}
	}
}

func TestMySQL_Placeholder(t *testing.T) {
	d := sql.MySQL{}
	// MySQL 占位符位置无关, 都是 "?"
	for _, n := range []int{1, 2, 5, 100} {
		if got := d.Placeholder(n); got != "?" {
			t.Errorf("MySQL Placeholder(%d) expected ?, got %q", n, got)
		}
	}
}

func TestPostgres_UpsertSuffix(t *testing.T) {
	d := sql.Postgres{}
	got := d.UpsertSuffix([]string{"ns", "key"}, []string{"value", "updated_at"})
	want := "ON CONFLICT (ns, key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at"
	if got != want {
		t.Errorf("UpsertSuffix:\n  expected %q\n  got      %q", want, got)
	}
}

func TestPostgres_UpsertSuffix_EmptyCols_Returns_Empty(t *testing.T) {
	d := sql.Postgres{}
	if got := d.UpsertSuffix(nil, []string{"value"}); got != "" {
		t.Errorf("empty keyCols should return empty, got %q", got)
	}
	if got := d.UpsertSuffix([]string{"ns"}, nil); got != "" {
		t.Errorf("empty updateCols should return empty, got %q", got)
	}
}

func TestMySQL_UpsertSuffix(t *testing.T) {
	d := sql.MySQL{}
	got := d.UpsertSuffix([]string{"ns", "key"}, []string{"value", "updated_at"})
	want := "ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=VALUES(updated_at)"
	if got != want {
		t.Errorf("UpsertSuffix:\n  expected %q\n  got      %q", want, got)
	}
}

func TestMySQL_UpsertSuffix_Ignores_KeyCols(t *testing.T) {
	d := sql.MySQL{}
	// MySQL UPSERT 按 PK/Unique 自动, keyCols 参数被忽略
	withKeys := d.UpsertSuffix([]string{"ns", "key"}, []string{"value"})
	withoutKeys := d.UpsertSuffix(nil, []string{"value"})
	if withKeys != withoutKeys {
		t.Errorf("MySQL UpsertSuffix should ignore keyCols, got differ:\n  with    %q\n  without %q",
			withKeys, withoutKeys)
	}
}

func TestPostgres_Types(t *testing.T) {
	d := sql.Postgres{}
	if d.JSONType() != "JSONB" {
		t.Errorf("PG JSONType expected JSONB, got %q", d.JSONType())
	}
	if d.BlobType() != "BYTEA" {
		t.Errorf("PG BlobType expected BYTEA, got %q", d.BlobType())
	}
}

func TestMySQL_Types(t *testing.T) {
	d := sql.MySQL{}
	if d.JSONType() != "JSON" {
		t.Errorf("MySQL JSONType expected JSON, got %q", d.JSONType())
	}
	if d.BlobType() != "LONGBLOB" {
		t.Errorf("MySQL BlobType expected LONGBLOB, got %q", d.BlobType())
	}
}

// TestDialects_JSONFieldExists_Embeddable 钉三个方言的「字段存在」片段形状 + 能直接
// 嵌进 WHERE。字段名与 path 现在都绑参（逐格 args 断言在 dialect_jsonfield_test.go）。
func TestDialects_JSONFieldExists_Embeddable(t *testing.T) {
	for _, c := range []struct {
		name string
		d    sql.Dialect
		want string
	}{
		{"pg", sql.Postgres{}, "value ? $1"},
		{"mysql", sql.MySQL{}, "JSON_CONTAINS_PATH(value, 'one', ?)"},
		{"sqlite", sql.SQLite{}, "json_type(value, ?) IS NOT NULL"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := c.d.JSONFieldExists("value", "tag", newRec(c.d))
			if got != c.want {
				t.Errorf("JSONFieldExists:\n  expected %q\n  got      %q", c.want, got)
			}
			// 业务真实 SQL 拼接时应能直接嵌进 WHERE 子句
			if frag := "SELECT * FROM store WHERE " + got; !strings.Contains(frag, "WHERE "+c.want) {
				t.Errorf("SQL fragment 不可拼接: %q", frag)
			}
		})
	}
}

// TestCustomDialect_Compiles 验业务可自实现 Dialect (e.g., CockroachDB / TiDB)
// 跟 PG 大部分一致仅个别方法 override.
func TestCustomDialect_Compiles(t *testing.T) {
	// CockroachDB 用 PG dialect 大部分 OK, 仅 UpsertSuffix 用 `ON CONFLICT DO
	// NOTHING / UPDATE` 同款. 业务可 embed PG 复用大部分:
	type cockroachDB struct{ sql.Postgres }

	var d sql.Dialect = cockroachDB{}
	if d.Placeholder(1) != "$1" {
		t.Error("embed Postgres should inherit Placeholder")
	}
}
