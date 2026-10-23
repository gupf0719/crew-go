package sql

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// DefaultHistoryTable 是 HistoryStore 的默认表名。
const DefaultHistoryTable = "crew_history"

// HistoryStore 是 agent_v2.HistoryStore 的 SQL 实现，给 agent_v2.WithHistory 用：会话历史跨进程
// 重启留存（内存版 agent_v2.NewInMemoryHistoryStore 重启即丢）。
//
// 存法：每条消息一行，整条消息的 JSON 存进二进制列；行序 = 自增主键序 = 写入序，Load 按它回放，游标
// （agent_v2.HistoryCursor）就是这一行的 id。owner 与 conv_id 分两列存、不拼串（拼串会让 {acme/eu, c-1} 与
// {acme, eu/c-1} 撞成同一个会话），按字节比较（PG `COLLATE "C"`、MySQL VARBINARY、SQLite 默认 BINARY）。
// HistoryQuery 的三个过滤条件（消息 ID、任务章、归属）在写入时从消息上抽成三列，Load 在库里按它们过滤，
// 不在进程里逐条解码再筛；标注的值不是字符串时那一列存空串，跟 agent_v2.HistoryQuery.Matches 一样不命中
// （过滤条件非空才生效）。
//
// Append 的一批在一个事务里写：要么整批在、要么整批不在。半批落盘的后果是 assistant 的 tool_call 在、
// 配对的 tool_result 不在——下次 Load 喂给模型就是孤儿调用（OpenAI 422 / Anthropic 400），而且
// append-only 意味着这条坏记录永远留在历史里。
//
// 消息经 JSON 往返：Annotations 里的数字读回是 float64、结构体读回是 map（与 Store / Checkpointer
// 同一约定），读标注按 JSON 类型读。
//
// 用前调一次 Setup 建表。语义验证：SQLite 跑过 agent_v2/history_conformance 全套；PG / MySQL 的
// 建表语句照 Checkpointer 的列类型约定写，**没有连真库跑过**，用前先跑一遍 conformance。业务自研
// dialect 自管 schema（列与索引见各方言的建表语句）。
type HistoryStore struct {
	db      *dbsql.DB
	dialect Dialect
	table   string
}

var _ agent_v2.HistoryStore = (*HistoryStore)(nil)

// HistoryOption 配置 HistoryStore。
type HistoryOption func(*HistoryStore)

// WithHistoryTable 改表名（默认 DefaultHistoryTable）。
func WithHistoryTable(name string) HistoryOption {
	return func(h *HistoryStore) { h.table = name }
}

// NewHistoryStore 建一个 HistoryStore。表名不合法是编程错误，直接 panic。
func NewHistoryStore(db *dbsql.DB, dialect Dialect, opts ...HistoryOption) *HistoryStore {
	h := &HistoryStore{db: db, dialect: dialect, table: DefaultHistoryTable}
	for _, opt := range opts {
		opt(h)
	}
	if err := validateTableName(h.table); err != nil {
		panic("crew-go/sql NewHistoryStore: " + err.Error())
	}
	return h
}

// Append 把 msgs 追加到会话末尾，整批一个事务。先把整批编码完再开事务：有一条编不出来（比如
// Annotations 里有 NaN）就整批不写。
func (h *HistoryStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	if key.Owner == "" || key.ConvID == "" {
		return fmt.Errorf("sql history append: key 不完整 (owner=%q conv=%q)", key.Owner, key.ConvID)
	}
	if len(msgs) == 0 {
		return nil
	}
	type row struct {
		payload            []byte
		msgID, task, agent string
	}
	rowsToWrite := make([]row, len(msgs))
	for i, m := range msgs {
		b, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("sql history append: 第 %d 条消息编码失败: %w", i, err)
		}
		r := row{payload: b}
		if m != nil {
			r.msgID = m.ID
			r.task, _ = m.Annotations[agent_v2.HistoryTaskAnnotation].(string)
			r.agent, _ = m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		}
		rowsToWrite[i] = r
	}

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sql history append: begin: %w", err)
	}
	ph := h.dialect.Placeholder
	q := fmt.Sprintf(`INSERT INTO %s (owner, conv_id, payload, msg_id, task, agent) VALUES (%s, %s, %s, %s, %s, %s)`,
		h.table, ph(1), ph(2), ph(3), ph(4), ph(5), ph(6))
	// 逐行插入而不是一条多行 INSERT：同一事务里先后执行的插入拿到的自增值递增，行序就是消息序；
	// 多行 INSERT 在 MySQL 交错锁模式下不保证同一条语句里的值连续递增。
	for i, r := range rowsToWrite {
		if _, err := tx.ExecContext(ctx, q, key.Owner, key.ConvID, r.payload, r.msgID, r.task, r.agent); err != nil {
			_ = tx.Rollback() // 回滚失败不改变结论：这批没写进去，原样报插入的错
			return fmt.Errorf("sql history append: 第 %d 条: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sql history append: commit: %w", err)
	}
	return nil
}

// Load 按写入顺序返回 q 圈定的那一段（语义见 agent_v2.HistoryStore）：过滤条件与 Before 在库里算，Limit > 0 时按
// id 倒序取 Limit+1 行——多取的那一行只用来判 More。会话不存在、没有满足条件的记录时返空页。每次都从库里解码出
// 新对象，调用方改返回值不影响存储。
func (h *HistoryStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	ph := h.dialect.Placeholder
	var where strings.Builder
	args := []any{key.Owner, key.ConvID}
	where.WriteString("owner = " + ph(1) + " AND conv_id = " + ph(2))
	cond := func(col string, v any) {
		args = append(args, v)
		where.WriteString(" AND " + col + " = " + ph(len(args)))
	}
	if q.Before != "" {
		before, err := strconv.ParseInt(string(q.Before), 10, 64)
		if err != nil || before < 0 {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: invalid cursor %q", q.Before)
		}
		args = append(args, before)
		where.WriteString(" AND id < " + ph(len(args)))
	}
	if q.MessageID != "" {
		cond("msg_id", q.MessageID)
	}
	if q.Task != "" {
		cond("task", q.Task)
	}
	if q.Agent != "" {
		cond("agent", q.Agent)
	}
	stmt := fmt.Sprintf(`SELECT id, payload FROM %s WHERE %s ORDER BY id`, h.table, where.String())
	// Limit = math.MaxInt 时多取一行会溢出成负数（PG / MySQL 拒绝负的 LIMIT），那么大的上限按不限处理。
	limited := q.Limit > 0 && q.Limit < math.MaxInt
	if limited {
		stmt += fmt.Sprintf(" DESC LIMIT %d", q.Limit+1)
	}
	rows, err := h.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: %w", err)
	}
	defer rows.Close()
	var (
		ids []int64
		out []*schema.Message
	)
	for rows.Next() {
		var (
			id int64
			p  []byte
		)
		if err := rows.Scan(&id, &p); err != nil {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: scan: %w", err)
		}
		m := new(schema.Message)
		if err := json.Unmarshal(p, m); err != nil {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: id %d 的消息解码失败: %w", id, err)
		}
		ids = append(ids, id)
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: %w", err)
	}
	more := false
	if limited {
		// 倒序读的：多出来的那一行是更早的一条，只说明还有更多
		if len(out) > q.Limit {
			more = true
			ids, out = ids[:q.Limit], out[:q.Limit]
		}
		slices.Reverse(ids)
		slices.Reverse(out)
	}
	if len(out) == 0 {
		return agent_v2.HistoryPage{}, nil
	}
	return agent_v2.HistoryPage{Messages: out, Start: agent_v2.HistoryCursor(strconv.FormatInt(ids[0], 10)), More: more}, nil
}

// Setup 建表与索引，`IF NOT EXISTS`，重复调用幂等。
func (h *HistoryStore) Setup(ctx context.Context) error {
	ddl, err := h.historyMigrations()
	if err != nil {
		return err
	}
	for i, m := range ddl {
		if _, err := h.db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("sql history setup migration %d failed: %w", i+1, err)
		}
	}
	return nil
}

// historyMigrations 返各方言的建表语句。索引 (owner, conv_id, id) 让 Load 走索引、按 id 有序；三个过滤列不单独建
// 索引——同一场会话的行沿这个索引从新往旧扫、逐行比过滤列，命中凑够 Limit 就停。
func (h *HistoryStore) historyMigrations() ([]string, error) {
	t := h.table
	index := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_owner_conv_idx ON %s (owner, conv_id, id);`, t, t)
	switch h.dialect.(type) {
	case Postgres:
		// payload 用 BYTEA：存的是 JSON 字节，不在库里查字段；JSONB 会拒收 \u0000（见 Checkpointer 的建表注释）。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       BIGSERIAL PRIMARY KEY,
    owner    TEXT COLLATE "C" NOT NULL,
    conv_id  TEXT COLLATE "C" NOT NULL,
    payload  BYTEA NOT NULL,
    msg_id   TEXT COLLATE "C" NOT NULL DEFAULT '',
    task     TEXT COLLATE "C" NOT NULL DEFAULT '',
    agent    TEXT COLLATE "C" NOT NULL DEFAULT ''
);`, t), index}, nil
	case MySQL:
		// owner / conv_id 用 VARBINARY，理由同 Checkpointer 的身份列（utf8mb4_bin 是 PAD SPACE）；
		// 索引键 1020 + 1020 + 8 字节，在 InnoDB 3072 字节上限内。三个过滤列同样按字节比较，各 2048 字节（消息 ID、
		// RunID、编排位置都远短于此）。MySQL 没有 CREATE INDEX IF NOT EXISTS，索引随表一起建。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    owner    VARBINARY(1020) NOT NULL,
    conv_id  VARBINARY(1020) NOT NULL,
    payload  LONGBLOB NOT NULL,
    msg_id   VARBINARY(2048) NOT NULL DEFAULT '',
    task     VARBINARY(2048) NOT NULL DEFAULT '',
    agent    VARBINARY(2048) NOT NULL DEFAULT '',
    INDEX %s_owner_conv_idx (owner, conv_id, id)
) ENGINE=InnoDB;`, t, t)}, nil
	case SQLite:
		// AUTOINCREMENT：删过的 id 不复用，行序永远等于写入序。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    owner    TEXT NOT NULL,
    conv_id  TEXT NOT NULL,
    payload  BLOB NOT NULL,
    msg_id   TEXT NOT NULL DEFAULT '',
    task     TEXT NOT NULL DEFAULT '',
    agent    TEXT NOT NULL DEFAULT ''
);`, t), index}, nil
	default:
		return nil, errors.New("sql history Setup: 业务自定义 dialect 不支持 Setup, 业务自管 schema")
	}
}
