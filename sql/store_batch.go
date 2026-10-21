package sql

import (
	"context"

	"crew-go/store"
)

// Batch 实现 store.Store.Batch.
//
// 顺序执行 ops, **per-Op 错误隔离** — 失败 Op 的 Result.Err 非 nil, 其他 Op 仍执行
// 并照常持久化. 不是 SQL TX 原子.
//
// ⚠️ **这不是「PoC 简化」, 是契约本身**. per-Op 隔离是 store.Store.Batch 的唯一合法失败
// 语义 (见其 godoc): 做成单事务原子写的后端跑 conformance 会当场红在
// `valid PutOp value should be persisted despite sibling failing` 上. 所以**别加**
// 「opt-in 事务模式」那类开关 — 两种语义都放行的代价是业务换后端时「兄弟 Op 保不保得住」
// 变成掷骰子, 而两边都 err == nil.
//
// ⚠️ **也别教业务"自己 BeginTx 包起来"** (这里曾这么写): NewStore 收的是 *sql.DB,
// 每个方法都从连接池取连接, 而 *sql.Tx 独占一条连接 — 外面开的事务罩不住 Store 的调用.
// 要事务只能绕开本 Store 直接用 database/sql.
func (s *Store) Batch(ctx context.Context, ops []store.Op) ([]store.Result, error) {
	results := make([]store.Result, len(ops))

	for i, op := range ops {
		switch o := op.(type) {
		case store.GetOp:
			item, err := s.Get(ctx, o.Namespace, o.Key)
			results[i] = store.Result{Item: item, Err: err}

		case store.PutOp:
			var putOpts []store.PutOption
			if o.TTL > 0 {
				putOpts = append(putOpts, store.WithTTL(o.TTL))
			}
			err := s.Put(ctx, o.Namespace, o.Key, o.Value, putOpts...)
			results[i] = store.Result{Err: err}

		case store.SearchOp:
			searchOpts := make([]store.SearchOption, 0, 4)
			if o.Filter != nil {
				searchOpts = append(searchOpts, store.WithFilter(o.Filter))
			}
			if o.Query != "" {
				searchOpts = append(searchOpts, store.WithQuery(o.Query))
			}
			if o.Limit > 0 {
				searchOpts = append(searchOpts, store.WithLimit(o.Limit))
			}
			if o.Offset > 0 {
				searchOpts = append(searchOpts, store.WithOffset(o.Offset))
			}
			items, err := s.Search(ctx, o.NamespacePrefix, searchOpts...)
			results[i] = store.Result{Items: items, Err: err}

		case store.ListNamespacesOp:
			listOpts := make([]store.ListOption, 0, 5)
			if len(o.Prefix) > 0 {
				listOpts = append(listOpts, store.WithListPrefix(o.Prefix))
			}
			if len(o.Suffix) > 0 {
				listOpts = append(listOpts, store.WithListSuffix(o.Suffix))
			}
			if o.MaxDepth > 0 {
				listOpts = append(listOpts, store.WithListMaxDepth(o.MaxDepth))
			}
			if o.Limit > 0 {
				listOpts = append(listOpts, store.WithListLimit(o.Limit))
			}
			if o.Offset > 0 {
				listOpts = append(listOpts, store.WithListOffset(o.Offset))
			}
			nss, err := s.ListNamespaces(ctx, listOpts...)
			results[i] = store.Result{Namespaces: nss, Err: err}
		}
	}

	return results, nil
}
