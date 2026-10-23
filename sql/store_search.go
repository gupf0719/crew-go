package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"crew-go/store"
)

// Search 实现 store.Store.Search.
//
// 支持 namespace prefix 检索 + filter (顶级 key 精确匹配 + 6 比较 op) + 分页.
// 不支持语义搜索 (WithQuery, 业务接 pgvector / Milvus 自扩), 不支持嵌套 filter
// (`{"a": {"b": ...}}`——in-memory 会按嵌套部分匹配, 本后端返 ErrUnsupportedFilterOp,
// 跨后端不一致, 业务别依赖).
//
// **filter 语义以 [store.MatchFilter] 为准**（type bracketing: 类型对不上的行不命中、
// 不报错; `ErrFilterTypeMismatch` 只在 filter 值自身非数值时返, 由 [store.ValidateFilter]
// 静态判定、不碰数据）, 三个 dialect 的 JSONField* 方法把它翻成 SQL, conformance 逐格钉.
//
// limit 默认 10 (跟 InMemory 一致).
func (s *Store) Search(ctx context.Context, namespacePrefix store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	if err := namespacePrefix.Validate(); err != nil {
		return nil, err
	}

	// 用 store.ParseSearchOptions 拿 SearchOptions (跨包 export helper)
	cfg := store.ParseSearchOptions(opts...)
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}

	if err := store.ValidateOffset(cfg.Offset); err != nil {
		return nil, err
	}
	// 静态校验 (op 名 + 范围 op 的 filter 值类型), 与客户端过滤后端同一份判据
	if err := store.ValidateFilter(cfg.Filter); err != nil {
		return nil, err
	}

	// 构造 WHERE 子句: ns 前缀匹配 + filter
	nsCond, args := nsExactOrDescendantSQL(s.dialect, namespacePrefix, 1)
	paramOffset := len(args) + 1

	filterCond, filterArgs, err := s.buildFilterSQL(cfg.Filter, paramOffset)
	if err != nil {
		return nil, err
	}
	args = append(args, filterArgs...)

	where := nsCond
	if filterCond != "" {
		where += " AND " + filterCond
	}

	// **时新优先**（updated_at 降序）——Search 的顺序契约，见 `store/conformance` 的
	// `Search_StableOrder_RecentFirst`。Search 恒带 limit，所以顺序**就是**「截断时留下
	// 谁」；按 key 排等于按首字母挑，跟这条数据对调用方有没有用零相关。
	//
	// ⚠️ 两点已知局限：
	//   - 同 `updated_at` 时回落 (ns, data_key)。业务若用秒级 `TIMESTAMP` 列（不调
	//     Setup 自建表时可能如此），一次突发写入会全落进同一秒 ⟹ 退化成字典序。
	//     框架自带 DDL 用 TIMESTAMPTZ / TIMESTAMP(6)，不受影响。
	//   - **`updated_at` 上没有索引**（Setup 只建 (ns)）。数据量大时这条 ORDER BY 会
	//     走排序而不是索引扫描。
	q := fmt.Sprintf(
		`SELECT ns, data_key, value, created_at, updated_at FROM %s WHERE %s ORDER BY updated_at DESC, ns, data_key LIMIT %d OFFSET %d`,
		s.table, where, cfg.Limit, cfg.Offset,
	)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sql store search: %w", err)
	}
	defer rows.Close()

	items := []*store.SearchItem{}
	for rows.Next() {
		var (
			ns        string
			key       string
			valueJSON []byte
			createdAt nullableTime
			updatedAt nullableTime
		)
		if err := rows.Scan(&ns, &key, &valueJSON, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("sql store search: scan: %w", err)
		}

		value, err := unmarshalValue(valueJSON)
		if err != nil {
			return nil, fmt.Errorf("sql store search: %w", err)
		}

		// iter-5-I Option A: 读出端信任 DB (写入 fail-fast 已防).
		// 删 5c35175 silent skip 避免 SQL LIMIT/OFFSET 翻页漏数据 + 删
		// iter-4 H4 splitNS 段验证空想威胁 (strings.Split 总返 len≥1 slice 不 panic).

		items = append(items, &store.SearchItem{
			Item: store.Item{
				Namespace: splitNS(ns),
				Key:       key,
				Value:     value,
				CreatedAt: createdAt.t.UTC(),
				UpdatedAt: updatedAt.t.UTC(),
			},
			Score: 0, // PoC 无语义搜索, score=0
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql store search: rows: %w", err)
	}

	return items, nil
}

// rangeOpSQL 是 4 个数值范围 op → SQL 比较符. `$eq` / `$ne` 不在这里——它们走
// JSONFieldEquals (原生 JSON 相等), 不走数值路径.
var rangeOpSQL = map[string]string{
	"$gt":  ">",
	"$gte": ">=",
	"$lt":  "<",
	"$lte": "<=",
}

// buildFilterSQL 把 filter map 翻译为 SQL WHERE 子句 + 参数.
//
// 键按字典序遍历——生成的 SQL 对同一个 filter 恒定 (prepared statement cache 友好,
// 测试可钉字符串). 每个条件是一段自包含的括号表达式, 用 AND 连接.
//
//   - `{"key": v}` / `{"key": {"$eq": v}}` → dialect.JSONFieldEquals, 参数 = v 的 JSON 文本
//   - `{"key": {"$ne": v}}` → (JSONFieldExists AND NOT (JSONFieldEquals)), 参数同上
//   - `{"key": {"$gt"|"$gte"|"$lt"|"$lte": n}}` → dialect.JSONFieldNumericCompare, 参数 = Go 数值
//     (json.Number 转 int64 / float64 再绑, driver 才认)
//   - key 必过 identifier 校验 (两条路径都拼进 SQL 字面量, 防注入)
//   - op map 里出现非 `$` 键 (嵌套 filter) → ErrUnsupportedFilterOp
//
// 调用前 store.ValidateFilter 已保证 op 名合法、范围 op 的值是数值; 这里对两者仍
// 有兜底 (返同款 sentinel), 但那是防御不是契约.
func (s *Store) buildFilterSQL(filter map[string]any, paramOffset int) (string, []any, error) {
	if len(filter) == 0 {
		return "", nil, nil
	}

	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var conds []string

	binder := &paramBinder{dialect: s.dialect, offset: paramOffset}

	equals := func(key string, want any) error {
		jsonText, err := json.Marshal(want)
		if err != nil {
			return fmt.Errorf("sql store filter key %q: marshal value: %w", key, err)
		}
		conds = append(conds, s.dialect.JSONFieldEquals("value", key,
			bindJSON(jsonText), jsonKindOf(jsonText), binder))
		return nil
	}

	for _, key := range keys {
		want := filter[key]

		opMap, isOpMap := want.(map[string]any)
		if !isOpMap {
			if err := equals(key, want); err != nil {
				return "", nil, err
			}
			continue
		}

		opNames := make([]string, 0, len(opMap))
		for op := range opMap {
			opNames = append(opNames, op)
		}
		sort.Strings(opNames)

		for _, opName := range opNames {
			opValue := opMap[opName]
			switch opName {
			case "$eq":
				if err := equals(key, opValue); err != nil {
					return "", nil, err
				}
			case "$ne":
				jsonText, err := json.Marshal(opValue)
				if err != nil {
					return "", nil, fmt.Errorf("sql store filter key %q: marshal value: %w", key, err)
				}
				// ⚠️ 顺序敏感：SQL 里 Exists 在前、Equals 在后，绑参必须同序
				// （`?` 是位置绑定）。别把这两行调过来。
				exists := s.dialect.JSONFieldExists("value", key, binder)
				eq := s.dialect.JSONFieldEquals("value", key,
					bindJSON(jsonText), jsonKindOf(jsonText), binder)
				conds = append(conds, fmt.Sprintf("(%s AND NOT (%s))", exists, eq))
			case "$gt", "$gte", "$lt", "$lte":
				num, ok := numericArg(opValue)
				if !ok {
					return "", nil, fmt.Errorf("%w: %s on field %q requires a numeric filter value, got %T",
						store.ErrFilterTypeMismatch, opName, key, opValue)
				}
				conds = append(conds, s.dialect.JSONFieldNumericCompare("value", key,
					rangeOpSQL[opName], num, binder))
			default:
				if !strings.HasPrefix(opName, "$") {
					return "", nil, fmt.Errorf("%w: nested filter {%q: {%q: ...}} is not supported by the sql backend (only top-level keys)",
						store.ErrUnsupportedFilterOp, key, opName)
				}
				return "", nil, fmt.Errorf("%w: %s", store.ErrUnsupportedFilterOp, opName)
			}
		}
	}

	return strings.Join(conds, " AND "), binder.args, nil
}
