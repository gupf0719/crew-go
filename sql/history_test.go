package sql_test

import (
	"context"
	databasesql "database/sql"
	"encoding/json"
	"math"
	"path/filepath"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agent_v2/history_conformance"
	"crew-go/schema"
	agentSQL "crew-go/sql"
)

func newSQLiteHistory(t *testing.T) (*agentSQL.HistoryStore, *databasesql.DB) {
	t.Helper()
	db := newSQLiteDB(t)
	h := agentSQL.NewHistoryStore(db, agentSQL.SQLite{})
	if err := h.Setup(t.Context()); err != nil {
		t.Fatalf("history Setup: %v", err)
	}
	return h, db
}

func TestSQLiteHistoryStore_Conformance(t *testing.T) {
	history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
		h, _ := newSQLiteHistory(t)
		return h
	})
}

// TestSQLiteHistoryStore_BatchIsAtomic：一批中途插入失败，整批都不留。半批落盘 = tool_call 在、
// tool_result 不在，下次喂给模型就是孤儿调用。
func TestSQLiteHistoryStore_BatchIsAtomic(t *testing.T) {
	h, db := newSQLiteHistory(t)
	if _, err := db.Exec(`CREATE TRIGGER boom BEFORE INSERT ON crew_history
WHEN instr(CAST(NEW.payload AS TEXT), 'boom') > 0 BEGIN SELECT RAISE(ABORT, 'boom'); END;`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := agent_v2.HistoryKey{Owner: "o", ConvID: "c"}
	err := h.Append(ctx, key, []*schema.Message{schema.UserMessage("q1"), schema.UserMessage("boom")})
	if err == nil {
		t.Fatal("第二条插入失败时 Append 应报错")
	}
	if got, err := h.Load(ctx, key); err != nil || len(got) != 0 {
		t.Fatalf("失败的一批应整批不留，Load 返 %d 条（err=%v）", len(got), err)
	}
	if err := h.Append(ctx, key, []*schema.Message{schema.UserMessage("q2")}); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.Load(ctx, key); len(got) != 1 || got[0].Content != "q2" {
		t.Fatalf("回滚之后的写入应正常：%v", got)
	}
}

// TestSQLiteHistoryStore_UnencodableMessageWritesNothing：有一条编不出 JSON，整批不写。
func TestSQLiteHistoryStore_UnencodableMessageWritesNothing(t *testing.T) {
	h, _ := newSQLiteHistory(t)
	ctx := context.Background()
	key := agent_v2.HistoryKey{Owner: "o", ConvID: "c"}
	bad := schema.AssistantMessage("a")
	bad.Annotations = map[string]any{"score": math.NaN()}
	if err := h.Append(ctx, key, []*schema.Message{schema.UserMessage("q"), bad}); err == nil {
		t.Fatal("消息编码失败时 Append 应报错")
	}
	if got, _ := h.Load(ctx, key); len(got) != 0 {
		t.Fatalf("编码失败的一批应整批不写，Load 返 %d 条", len(got))
	}
}

// TestSQLiteHistoryStore_SurvivesReopen：写进文件库、关掉、重开，读回的消息逐字段相同——包括
// model/anthropic 存在 Annotations 里的回放快照（thinking 签名要原样回放，差一个字节就对不上）。
func TestSQLiteHistoryStore_SurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	open := func() (*agentSQL.HistoryStore, *databasesql.DB) {
		db, err := databasesql.Open("sqlite", "file:"+path+"?_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		h := agentSQL.NewHistoryStore(db, agentSQL.SQLite{})
		if err := h.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
		return h, db
	}
	ctx := context.Background()
	key := agent_v2.HistoryKey{Owner: "deskbot", ConvID: "conv-1"}
	assistant := &schema.Message{
		Role:    schema.RoleAssistant,
		Content: "我查一下 <天气> & 时间",
		ContentBlocks: []*schema.ContentBlock{{
			Type:     "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: "toolu_1", Name: "clock__now", Arguments: `{"tz":"Asia/Shanghai","n":12345678901234567890}`},
		}},
		Annotations: map[string]any{
			"anthropic.assistant_content": `{"v":1,"content":[{"type":"thinking","thinking":"嗯","signature":"EqQBCkgIBh+/="}]}`,
			agent_v2.HistoryAgentAnnotation: "deskbot",
		},
	}
	want := []*schema.Message{schema.UserMessage("现在几点"), assistant}

	h1, db1 := open()
	if err := h1.Append(ctx, key, want); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	h2, db2 := open()
	defer db2.Close()
	got, err := h2.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("重开后读回的消息不同：\n want %s\n  got %s", wantJSON, gotJSON)
	}
}

func TestSQLiteHistoryStore_SetupIdempotentAndKeyRequired(t *testing.T) {
	h, _ := newSQLiteHistory(t)
	if err := h.Setup(t.Context()); err != nil {
		t.Fatalf("第二次 Setup 应幂等：%v", err)
	}
	for _, key := range []agent_v2.HistoryKey{{Owner: "o"}, {ConvID: "c"}} {
		if err := h.Append(context.Background(), key, []*schema.Message{schema.UserMessage("q")}); err == nil {
			t.Errorf("key 不完整 %+v 时 Append 应报错", key)
		}
	}
}
