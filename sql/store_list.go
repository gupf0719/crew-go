package sql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"crew-go/store"
)

// ListNamespaces 实现 store.Store.ListNamespaces.
//
// 支持的选项:
//   - WithListPrefix: 段级前缀过滤, 在 SQL 里做 (见 nsExactOrDescendantSQL).
//   - WithListMaxDepth: 截断到前 N 段并去重, 分页取在截断之后的列表上.
//   - WithListLimit / WithListOffset: 分页, limit 缺省 100.
//   - WithListSuffix **不支持**, 传了返 ErrUnsupportedFilterOp——静默忽略的话, 业务在
//     InMemoryStore 上靠 Suffix 过滤跑通, 换到本后端拿到的却是全部 ns, 且不报错.
//
// 不带 maxDepth 时每行就是一个不同的完整 ns, 去重、排序、分页全部交给 SQL——次序跟着 ns
// 列的排序规则走, Setup 建的表三个方言都是字节序 (PG `COLLATE "C"`、MySQL VARBINARY、
// SQLite BINARY), 与 InMemoryStore 一致; 业务自管的表要照做 (见 Setup). 带 maxDepth 时走
// listTruncatedNamespaces, 理由与代价见它的注释.
func (s *Store) ListNamespaces(ctx context.Context, opts ...store.ListOption) ([]store.Namespace, error) {
	cfg := store.ParseListOptions(opts...)
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if err := store.ValidateOffset(cfg.Offset); err != nil {
		return nil, err
	}

	if len(cfg.Suffix) > 0 {
		return nil, fmt.Errorf("sql store ListNamespaces: WithListSuffix not supported in PoC: %w",
			store.ErrUnsupportedFilterOp)
	}

	var (
		where string
		args  []any
	)
	if len(cfg.Prefix) > 0 {
		if err := cfg.Prefix.Validate(); err != nil {
			return nil, err
		}
		nsCond, nsArgs := nsExactOrDescendantSQL(s.dialect, cfg.Prefix, 1)
		where = " WHERE " + nsCond
		args = nsArgs
	}

	if cfg.MaxDepth > 0 {
		return s.listTruncatedNamespaces(ctx, where, args, cfg)
	}

	q := fmt.Sprintf(
		`SELECT DISTINCT ns FROM %s%s ORDER BY ns LIMIT %d OFFSET %d`,
		s.table, where, cfg.Limit, cfg.Offset,
	)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sql store list_namespaces: %w", err)
	}
	defer rows.Close()

	out := []store.Namespace{}
	for rows.Next() {
		var nsJoined string
		if err := rows.Scan(&nsJoined); err != nil {
			return nil, fmt.Errorf("sql store list_namespaces: scan: %w", err)
		}
		// 读出端信任 DB: 非法 ns 在写入端就被 Validate 拒了, 这里不再逐段校验——
		// 读出时跳过行会让 LIMIT/OFFSET 翻页漏数据.
		out = append(out, splitNS(nsJoined))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql store list_namespaces: rows: %w", err)
	}
	return out, nil
}

// listTruncatedNamespaces 处理带 maxDepth 的 ListNamespaces: SQL 只做前缀过滤与 DISTINCT,
// 把前缀下所有不同的完整 ns 取回来, 截断、去重、排序、分页在 Go 里做, 与 InMemoryStore
// 的次序逐项一致.
//
// **分页必须在截断去重之后**. 对原始行先 LIMIT/OFFSET 再截断, 一页里被截成同一个前缀的
// 几行只剩一条 (少返), 下一页从原始行接着数, 截出来的又是上一页那个 (跨页重复)——框架按
// 写入者扇出取长期记忆时 (前缀 = 基准 ns、maxDepth = 深一层、limit = 写入者上限), 前缀
// 下的完整 ns 一多, 排在后面的写入者就静默漏列. 截断也不保序: 原始串 "m.w2-x" 排在
// "m.w2.a" 前面 ('-' 是 0x2D、'.' 是 0x2E), 截断之后 "m.w2" 却排在 "m.w2-x" 前面,
// 所以也不能按原始行的次序边读边截、凑够一页就停.
//
// **为什么不在 SQL 里截断**:
//   - 取前 N 段在三个方言里各有各的写法、没有共同的: MySQL 用 SUBSTRING_INDEX, PG 把
//     string_to_array 切片再拼回去, SQLite 没有按分隔符切的函数、要靠递归 CTE 逐段拆.
//     放进 SQL 就得给 Dialect 加一个方法, 业务自实现的方言也得各写一份、各自写对.
//   - 次序要和 InMemoryStore 一样按 Namespace.String() 的字节序, 而 SQL 的 ORDER BY 跟着
//     列的排序规则走: Setup 建的表是字节序, 业务自管的表不一定——PG 列不声明 `COLLATE "C"`
//     时用库的默认规则, ICU en-US 的库里 "m.a" 排在 "m.W" 前面, 字节序正相反. 在 Go 里
//     排与表怎么建无关.
//
// **代价**: 每次调用取回前缀下所有不同的完整 ns (前缀过滤与 DISTINCT 仍在 SQL 里做),
// 开销随它们的个数线性增长、与 limit 无关——和 redis 后端同一个量级 (它取的是全部 ns,
// 连前缀都不过滤). 框架自己的调用这个数是写入者个数级别; 一个前缀下有海量 ns 又要带
// maxDepth 翻页的业务, 开销会随 ns 个数一起涨.
func (s *Store) listTruncatedNamespaces(ctx context.Context, where string, args []any, cfg store.ListOptions) ([]store.Namespace, error) {
	q := fmt.Sprintf(`SELECT DISTINCT ns FROM %s%s`, s.table, where)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sql store list_namespaces: %w", err)
	}
	defer rows.Close()

	seen := map[string]struct{}{}
	keys := []string{}
	for rows.Next() {
		var nsJoined string
		if err := rows.Scan(&nsJoined); err != nil {
			return nil, fmt.Errorf("sql store list_namespaces: scan: %w", err)
		}
		k := truncateJoinedNS(nsJoined, cfg.MaxDepth)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql store list_namespaces: rows: %w", err)
	}

	// joinNS 与 Namespace.String() 用同一个分隔符, 拼接串的字节序就是 InMemoryStore 的次序.
	slices.Sort(keys)

	// 负 offset 契约没有定义, 各后端表现不一 (开放项见 doc/code/todo.md); 这里按 0 处理,
	// 不让切片越界 panic.
	offset := max(cfg.Offset, 0)
	if offset >= len(keys) {
		return []store.Namespace{}, nil
	}
	keys = keys[offset:]
	if len(keys) > cfg.Limit {
		keys = keys[:cfg.Limit]
	}
	out := make([]store.Namespace, 0, len(keys))
	for _, k := range keys {
		out = append(out, splitNS(k))
	}
	return out, nil
}

// truncateJoinedNS 返回拼接形态的 ns 的前 depth 段; 不足 depth 段时原样返回. depth 须大于 0.
func truncateJoinedNS(joined string, depth int) string {
	// SplitN 最多切成 depth+1 份, 多出来的第 depth+1 份是剩下的整段尾巴.
	if parts := strings.SplitN(joined, nsSeparator, depth+1); len(parts) > depth {
		return strings.Join(parts[:depth], nsSeparator)
	}
	return joined
}
