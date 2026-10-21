package sql

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"strings"

	"crew-go/compose_v2"
)

// DefaultCheckpointTable 是 Checkpointer 默认表名.
const DefaultCheckpointTable = "crew_checkpoint"

// Checkpointer 是 compose_v2.Checkpointer 接口的 SQL 共享实现.
//
// **存储设计**:
//   - 列 (sid, ns, parent_ns, status, schema_ver, graph_id, created_at,
//     payload) — metadata 单独列让 SQL 高效查询 (ListPending),
//     payload JSON 列含 Channels + CurrentFrontier（成员身份内联）+ PendingInterrupts +
//     NodeVisits 完整 cp marshal
//   - PRIMARY KEY (sid, ns) — 业务 SaveNS 触发 UPSERT, sid+ns 二级寻址
//
// **业务接入**:
//
//	import (
//	    _ "github.com/jackc/pgx/v5/stdlib"
//	    sqlstd "database/sql"
//	    agentSQL "crew-go/sql"
//	)
//
//	db, _ := sqlstd.Open("pgx", "postgres://...")
//	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})
//	cp.Setup(ctx) // 创建表 + 索引
//
// **conformance 验证**: 本模块自身**没有跑** conformance —— 此前唯一的测试手段是 sqlmock（`b1a9434` 起已接真 SQLite 引擎跑 conformance，见 `conformance_sqlite_test.go`；PG / MySQL 那档仍需业务侧连真库），
// 它对 payload 传 `sqlmock.AnyArg()`、内容从不检查，所以对"某个 cp 字段没进 wire"
// 这类退化结构性失明（实测：给 `Checkpoint.Epoch` 打 `json:"-"`，本模块测试全绿）。
//
// 现有的两层保障：
//   - `payload_roundtrip_test.go` 验 `MarshalCheckpoint` ↔ `UnmarshalCheckpoint` 全字段
//     —— 那正是本模块存取 payload 的唯一路径（1 个 marshal 点 / 3 个 unmarshal 点，
//     没有任何按列重建 Checkpoint 的路径），能挡住字段掉出 wire
//   - 业务接真库时跑 `compose_v2/checkpointer_conformance.RunCheckpointerTests(t, factory)`
//     验 SQL 层语义
//
// 所有 cp 业务字段都在 `payload` 列（JSON 类型；PG 也不用 JSONB——它拒收 `\u0000`，见 Setup 的 DDL 说明），独立列只有元数据（sid/ns/status/
// graph_id/created_at）—— 这是设计如此，单一 marshal 点保证新字段自动带上。
type Checkpointer struct {
	db      *dbsql.DB
	dialect Dialect
	table   string
}

// CheckpointerOption 配置 Checkpointer.
type CheckpointerOption func(*Checkpointer)

// WithCheckpointerTable 自定义表名 (默认 `crew_checkpoint`).
func WithCheckpointerTable(name string) CheckpointerOption {
	return func(c *Checkpointer) { c.table = name }
}

// NewCheckpointer 创建 SQL Checkpointer.
//
// **panic on invalid table name** (audit iter-4 视角 2 M3): WithCheckpointerTable
// 传非法 identifier 时 panic — 业务 init 时 fail-fast 防 SQL injection.
func NewCheckpointer(db *dbsql.DB, dialect Dialect, opts ...CheckpointerOption) *Checkpointer {
	c := &Checkpointer{
		db:      db,
		dialect: dialect,
		table:   DefaultCheckpointTable,
	}
	for _, opt := range opts {
		opt(c)
	}
	if err := validateTableName(c.table); err != nil {
		panic("crew-go/sql NewCheckpointer: " + err.Error())
	}
	return c
}

// ── compose_v2.Checkpointer interface 实现 ──

// SaveNS 实现 compose_v2.Checkpointer.SaveNS.
//
// 填 cp.ID = sessionID、cp.NS = ns（覆盖入参），ParentNS 为空且 ns 非空时补成
// `compose_v2.ParentNS(ns)`，与 InMemoryCheckpointer.SaveNS 同协议。
//
// # GraphID 只有一把尺：存档里那份
//
// 「这份档属于哪张图」以存档里的 GraphID 为准——Run / Resume 入口的 fail-fast 读的就是
// LoadNS 反序列化出来的那份。`graph_id` 列是它的副本，只在这里写、每次都与 payload 一起
// 整行写，所以每一行的列与存档恒相同，写入侧读列判冲突就是按存档判。规则与
// InMemoryCheckpointer 相同：存档的 GraphID 与本次的都非空且不同 ⟹ [compose_v2.ErrGraphIDConflict]，
// 这一行不动；否则整行替换，GraphID 跟着换（换成空也照换）。
//
// 判定与写入在同一个 READ COMMITTED 事务里（不随库的默认隔离级别），先锁住这一行再判：
//  1. 这一行不存在就插一行空档占位，存在就原样不动——两种情况下它都被本事务锁住（用的是方言的 UpsertSuffix，
//     它对冲突的行要加写锁，见 Dialect.UpsertSuffix）。占位行没提交之前，READ COMMITTED 及以上的读者看不到它，
//     提交时它已被第 3 步整行替换；
//  2. 读这一行的 graph_id；
//  3. 冲突就回滚，否则整行写入、提交。
//
// 所以两个写入者带不同 GraphID 同时写同一 (sid, ns) 时恰好一个成功、另一个报冲突，留下的
// 是成功者那份。整个过程只用方言的 UpsertSuffix 与标准的 SELECT / UPDATE，自定义方言走的
// 是同一条路；也不读 RowsAffected——MySQL 对「值没变的更新」报 0 行，拿它判冲突会把一次
// 原样重写当成冲突。
func (c *Checkpointer) SaveNS(ctx context.Context, sessionID, ns string, cp *compose_v2.Checkpoint) error {
	if cp == nil {
		return errors.New("checkpoint is nil")
	}
	if sessionID == "" {
		return errors.New("sessionID is empty")
	}

	cp.ID = sessionID
	cp.NS = ns
	if cp.ParentNS == "" && ns != "" {
		cp.ParentNS = compose_v2.ParentNS(ns)
	}

	payload, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("sql cp SaveNS marshal: %w", err)
	}

	// 事务自己声明 READ COMMITTED，不随库的默认隔离级别：判定靠「锁住这一行之后读到的是最新提交的那一版」，而
	// 快照建在事务第一条语句的隔离级别（PG 的 REPEATABLE READ / SERIALIZABLE）下，已有的行被并发改过时第 1 步就
	// 报串行化失败（40001）——那不是 ErrGraphIDConflict，出口记账会把它当成「档没写成」去清这一层，清掉的是赢家
	// 那份（真 PG 实测：默认隔离级别设成可重复读时，已有行上并发 40 轮 39 轮输家报 40001）。
	tx, err := c.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("sql cp SaveNS begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// 回滚出错不改变结论：要返回的错误已经定了，没提交的事务在连接归还或断开时由数据库回滚。
			_ = tx.Rollback()
		}
	}()

	ph := c.dialect.Placeholder
	claim := fmt.Sprintf(
		`INSERT INTO %s (sid, ns, parent_ns, status, schema_ver, graph_id, created_at, payload)
		 VALUES (%s, %s, '', 0, 0, '', 0, '{}') %s`,
		c.table, ph(1), ph(2),
		c.dialect.UpsertSuffix([]string{"sid", "ns"}, []string{"sid"}),
	)
	if _, err := tx.ExecContext(ctx, claim, sessionID, ns); err != nil {
		return fmt.Errorf("sql cp SaveNS claim row: %w", err)
	}

	var stored string
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT graph_id FROM %s WHERE sid = %s AND ns = %s`, c.table, ph(1), ph(2)),
		sessionID, ns,
	).Scan(&stored); err != nil {
		return fmt.Errorf("sql cp SaveNS read graph_id: %w", err)
	}
	if stored != "" && cp.GraphID != "" && stored != cp.GraphID {
		return fmt.Errorf("%w: sid=%q ns=%q existing=%q new=%q",
			compose_v2.ErrGraphIDConflict, sessionID, ns, stored, cp.GraphID)
	}

	write := fmt.Sprintf(
		`UPDATE %s SET parent_ns = %s, status = %s, schema_ver = %s, graph_id = %s, created_at = %s, payload = %s
		 WHERE sid = %s AND ns = %s`,
		c.table, ph(1), ph(2), ph(3), ph(4), ph(5), ph(6), ph(7), ph(8),
	)
	if _, err := tx.ExecContext(ctx, write,
		cp.ParentNS, int(cp.Status), cp.SchemaVer, cp.GraphID, cp.CreatedAt, bindJSON(payload),
		sessionID, ns,
	); err != nil {
		return fmt.Errorf("sql cp SaveNS: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sql cp SaveNS commit: %w", err)
	}
	committed = true
	return nil
}

// LoadNS 实现 compose_v2.Checkpointer.LoadNS. 不存在返 (nil, nil).
func (c *Checkpointer) LoadNS(ctx context.Context, sessionID, ns string) (*compose_v2.Checkpoint, error) {
	ph := c.dialect.Placeholder
	q := fmt.Sprintf(
		`SELECT payload FROM %s WHERE sid = %s AND ns = %s`,
		c.table, ph(1), ph(2),
	)

	var payload []byte
	err := c.db.QueryRowContext(ctx, q, sessionID, ns).Scan(&payload)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sql cp LoadNS: %w", err)
	}

	cp, err := compose_v2.UnmarshalCheckpoint(payload)
	if err != nil {
		return nil, fmt.Errorf("sql cp LoadNS unmarshal: %w", err)
	}
	return cp, nil
}

// ExistsNS 实现 compose_v2.Checkpointer.ExistsNS.
func (c *Checkpointer) ExistsNS(ctx context.Context, sessionID, ns string) (bool, error) {
	ph := c.dialect.Placeholder
	q := fmt.Sprintf(
		`SELECT 1 FROM %s WHERE sid = %s AND ns = %s LIMIT 1`,
		c.table, ph(1), ph(2),
	)

	var one int
	err := c.db.QueryRowContext(ctx, q, sessionID, ns).Scan(&one)
	if errors.Is(err, dbsql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sql cp ExistsNS: %w", err)
	}
	return true, nil
}

// DeleteSubtree 实现 compose_v2.Checkpointer.DeleteSubtree.
//
// **协议** (跟 InMemoryCheckpointer.DeleteSubtree 一致):
//   - 删 sessionID 下 nsPrefix 这次执行的子树 (nsPrefix 自身 + 经 `/` 往下的后代)
//   - 范围由 [compose_v2.IsNSInSubtree] 定义 — 段级匹配, 不是裸字符前缀; 同一个节点的
//     `#`/`@` 兄弟执行不在内
//   - prefix="" 删整 session
//   - 返删除数量
//
// # 实现：枚举 + 直接调权威判据，**不再有 SQL 复刻**
//
// 先 `SELECT ns WHERE sid = ?`（走主键前缀索引、只回一列短串），在 Go 里用
// [compose_v2.IsNSInSubtree] 过滤，再按 ns 精确 `DELETE ... IN (...)`（分批，见
// [deleteSubtreeBatchSize]）。
//
// **为什么不再用 LIKE**：把「是不是子树」翻译成 LIKE 模式，等于让判据在一门**会解释
// 输入**的语言里再活一份。这个模块为此付过两次代价，两次都静默：
//   - 分隔符集漏了一种（`@` 重访实例段）⟹ 那种段的 cp 残留，挂起的 HITL 档下次同
//     sid 跑还会被 auto-load 捡起；
//   - 前缀里的 `_` / `%` 被当成通配符 ⟹ `LIKE 'round_1/%'` 连 `roundX1/child` 一起
//     删——而 `round_%d` 正是 LoopAgent 自己生成的 ns 段。
//
// 换成枚举之后，判据只剩权威函数那一份：将来加第四种段分隔符，**backend 一行都不用改**。
//
// # 代价：两个新的非原子语义（本方法自己的契约，到此为止）
//
//  1. **SELECT 与 DELETE 之间有窗口**：期间新插入的、本该属于这棵子树的 ns 会逃过删除。
//     旧的单条 LIKE 语句没有这个窗口。
//  2. **多批之间也不是原子的**：子树超过 [deleteSubtreeBatchSize] 时拆成多条 DELETE，
//     中途某批失败返回的是「**已删的条数** + 那个错」（实测：批 1 删 500 成功、批 2 失败
//     ⟹ 返 `(500, err)`），子树被删掉一半。
//
// **空前缀（删整 session）不受这两条影响**——它走上面那条单条 DELETE 快路径，不 SELECT
// 也不分批，天然原子。
//
// ⚠️ **本 godoc 到此为止，不再讲「谁调它、残留会不会被别人扫掉」。**
// 那段话在这里写过三版，每版都在修上一个错的同时造出新错（笼统说会 → 说反一档且漏列
// 一处 → 修了一张表漏了另一张、还留下一句失效的总结）。根因不是粗心：它的输入维——调用点
// 集合、每个调用点运行期的 ns、祖先 Run 的退出路径——**全在别的包**，而 `sql/` 是独立
// go module，import 不到它们，注释审计的符号对账也够不着。散文在这儿承载了本该由代码
// 承载的状态。
//
// 「残留会不会被扫掉」是**调用方那一侧的性质**，规则收敛在 [compose_v2.Checkpointer]
// 的 `DeleteSubtree` 接口 godoc 里（一条不枚举调用点的判据）。
//
// 剩下真正能撞第 1 条窗口的是「同一个 sid 上并发跑多个 Run」——那本身就是业务错，
// 框架另有 [compose_v2.WithStaleWriteGuard] 应对。
//
// PG / MySQL / SQLite 共用本段，方言只差 [Dialect.Placeholder] 渲染出的占位符。
func (c *Checkpointer) DeleteSubtree(ctx context.Context, sessionID, nsPrefix string) (int32, error) {
	ph := c.dialect.Placeholder

	// 空前缀 = 删整 session，不需要子树判据，一条语句搞定。
	if nsPrefix == "" {
		q := fmt.Sprintf(`DELETE FROM %s WHERE sid = %s`, c.table, ph(1))
		result, err := c.db.ExecContext(ctx, q, sessionID)
		if err != nil {
			return 0, fmt.Errorf("sql cp DeleteSubtree: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("sql cp DeleteSubtree RowsAffected: %w", err)
		}
		return int32(n), nil
	}

	// 1. 枚举本 session 的全部 ns。`sid` 是主键前缀（PRIMARY KEY (sid, ns)），
	//    这一步走索引、只回一列短字符串。
	listQ := fmt.Sprintf(`SELECT ns FROM %s WHERE sid = %s`, c.table, ph(1))
	rows, err := c.db.QueryContext(ctx, listQ, sessionID)
	if err != nil {
		return 0, fmt.Errorf("sql cp DeleteSubtree list: %w", err)
	}
	var victims []string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("sql cp DeleteSubtree scan: %w", err)
		}
		// 2. **判据直接调权威函数**，不再有 SQL 复刻。
		if compose_v2.IsNSInSubtree(nsPrefix, ns) {
			victims = append(victims, ns)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("sql cp DeleteSubtree rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("sql cp DeleteSubtree close: %w", err)
	}
	if len(victims) == 0 {
		return 0, nil
	}

	// 3. 按 ns 精确删。分批是必需的：PG 的参数上限是 65535，MySQL 受
	//    max_allowed_packet 约束——一个 session 的 ns 数虽小，但没有硬上界。
	var total int64
	for start := 0; start < len(victims); start += deleteSubtreeBatchSize {
		end := start + deleteSubtreeBatchSize
		if end > len(victims) {
			end = len(victims)
		}
		batch := victims[start:end]
		args := make([]any, 0, len(batch)+1)
		args = append(args, sessionID)
		marks := make([]string, len(batch))
		for i, ns := range batch {
			marks[i] = ph(i + 2) // ph(1) 是 sid
			args = append(args, ns)
		}
		q := fmt.Sprintf(`DELETE FROM %s WHERE sid = %s AND ns IN (%s)`,
			c.table, ph(1), strings.Join(marks, ", "))
		result, err := c.db.ExecContext(ctx, q, args...)
		if err != nil {
			return int32(total), fmt.Errorf("sql cp DeleteSubtree: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return int32(total), fmt.Errorf("sql cp DeleteSubtree RowsAffected: %w", err)
		}
		total += n
	}
	return int32(total), nil
}

// 编译期 interface 实现检查.
var _ compose_v2.Checkpointer = (*Checkpointer)(nil)

// queryNSCheckpoints 共享 helper — 给 ListPending / LoadAllInSession 用.
// 跑 SELECT, scan ns + payload, unmarshal cp.
func (c *Checkpointer) queryNSCheckpoints(ctx context.Context, q string, args ...any) ([]compose_v2.NSCheckpoint, error) {
	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sql cp query: %w", err)
	}
	defer rows.Close()

	result := []compose_v2.NSCheckpoint{}
	for rows.Next() {
		var (
			ns      string
			payload []byte
		)
		if err := rows.Scan(&ns, &payload); err != nil {
			return nil, fmt.Errorf("sql cp scan: %w", err)
		}
		cp, err := compose_v2.UnmarshalCheckpoint(payload)
		if err != nil {
			return nil, fmt.Errorf("sql cp unmarshal: %w", err)
		}
		result = append(result, compose_v2.NSCheckpoint{NS: ns, Checkpoint: cp})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql cp rows: %w", err)
	}
	return result, nil
}

// 辅助: 用于 SQL IN 子句拼装
func placeholders(d Dialect, start, n int) string {
	if n == 0 {
		return ""
	}
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = d.Placeholder(start + i)
	}
	return strings.Join(parts, ", ")
}

// deleteSubtreeBatchSize 是 [Checkpointer.DeleteSubtree] 每条 DELETE 的 ns 上限。
//
// PG 的绑定参数上限是 65535、MySQL 受 max_allowed_packet 约束。一个 session 的 ns
// 数量 = 编排嵌套深度 × 分支数，实际是几十到几百量级，通常一批就够；分批只是为了
// 让「没有硬上界」这件事不变成运行时报错。
const deleteSubtreeBatchSize = 500
