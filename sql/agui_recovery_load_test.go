//go:build integration

package sql_test

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"crew-go/agui"
	"crew-go/internal/aguiload"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

func TestAGUICancellationLoad(t *testing.T) {
	dsn := os.Getenv("CREW_SQL_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("需要隔离的 PostgreSQL")
	}
	for _, replicas := range []int{1, 4} {
		t.Run(fmt.Sprintf("replicas_%d", replicas), func(t *testing.T) {
			ctx := t.Context()
			db, err := dbsql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			table := fmt.Sprintf("load_%d", time.Now().UnixNano())
			store := crewsql.NewAGUISessionStore(crewsql.NewCheckpointer(db, crewsql.Postgres{}, crewsql.WithCheckpointerTable(table)))
			if err := store.Setup(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table+", "+table+"_task_lease, "+table+"_agui_session, "+table+"_agui_cancel"); err != nil {
					t.Error(err)
				}
			}()
			payload, err := json.Marshal(&agui.SessionRecord{TaskID: "normal", Prior: []*schema.Message{schema.UserMessage(strings.Repeat("x", 4096))}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO "+table+"_agui_session (sid,payload) SELECT 'agui-thread:' || lpad(to_hex(i),64,'0'), $1::bytea FROM generate_series(1,$2) i", payload, aguiload.Sessions); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" (sid,ns,status,payload) SELECT 'normal-' || i, '', 1, '{}'::json FROM generate_series(1,$1) i", aguiload.Sessions); err != nil {
				t.Fatal(err)
			}
			aguiload.Run(t, store, replicas, func() string {
				var scans, rows int64
				if err := db.QueryRowContext(ctx, "SELECT seq_tup_read+idx_tup_fetch, seq_scan+idx_scan FROM pg_stat_user_tables WHERE relname=$1", table+"_agui_session").Scan(&rows, &scans); err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("session_rows=%d session_scans=%d", rows, scans)
			})
		})
	}
}
