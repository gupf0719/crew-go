package sql

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"crew-go/store"
)

func TestMySQLTTLColumnMetadataDistinguishesDefaultsFromGeneratedColumns(t *testing.T) {
	for _, extra := range []string{"DEFAULT_GENERATED", "VIRTUAL GENERATED", "STORED GENERATED"} {
		t.Run(extra, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			st, err := NewTTLStore(db, MySQL{})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectQuery("SHOW COLUMNS FROM crew_store").
				WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
					AddRow("expires_at_ms", "bigint", "YES", "", "NULL", extra))
			exists, err := st.checkTTLColumn(t.Context())
			want := extra == "DEFAULT_GENERATED"
			if exists != want || (err == nil) != want {
				t.Fatalf("列的可写性和默认值判断错误，得到 %v，err=%v", exists, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLiteTTLStoreExactCutoffAndDurationRounding(t *testing.T) {
	ctx := t.Context()
	db, err := OpenSQLiteForTest()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st, err := NewTTLStore(db, SQLite{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE test_clock (millis BIGINT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO test_clock (millis) VALUES (1000)"); err != nil {
		t.Fatal(err)
	}
	st.ttlClock = "(SELECT millis FROM test_clock)"
	ns := store.NS("fixed_clock")
	for _, tc := range []struct {
		key  string
		ttl  time.Duration
		want int64
	}{
		{"tiny", time.Nanosecond, 1001},
		{"fraction", time.Millisecond + time.Nanosecond, 1002},
		{"maximum", time.Duration(1<<63 - 1), 9223372037855},
	} {
		if err := st.Put(ctx, ns, tc.key, tc.key, store.WithTTL(tc.ttl)); err != nil {
			t.Fatal(err)
		}
		var cutoff int64
		if err := db.QueryRowContext(ctx, "SELECT expires_at_ms FROM crew_store WHERE data_key = ?", tc.key).Scan(&cutoff); err != nil {
			t.Fatal(err)
		}
		if cutoff != tc.want {
			t.Fatalf("%s: 截止毫秒 %d，期望 %d", tc.key, cutoff, tc.want)
		}
	}
	if item, err := st.Get(ctx, ns, "tiny"); err != nil || item == nil {
		t.Fatalf("正 TTL 在截止前应可读，得到 %+v，err=%v", item, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE test_clock SET millis = 1001"); err != nil {
		t.Fatal(err)
	}
	if item, err := st.Get(ctx, ns, "tiny"); err != nil || item != nil {
		t.Fatalf("到期时应立即隐藏，得到 %+v，err=%v", item, err)
	}
	if n, err := st.SweepExpired(ctx); err != nil || n != 1 {
		t.Fatalf("相等时应可清扫，得到 %d，err=%v", n, err)
	}
	if item, err := st.Get(ctx, ns, "fraction"); err != nil || item == nil {
		t.Fatalf("清扫后下一毫秒才到期的行应保留，得到 %+v，err=%v", item, err)
	}
}
