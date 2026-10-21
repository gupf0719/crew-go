package redis

import (
	"context"
	"fmt"

	"crew-go/store"
)

// Batch 实现 store.Store.Batch.
//
// **per-Op 错误隔离 best-effort**: 某 Op 失败 Result.Err 非 nil, 其他 Op 仍执行
// (跟 InMemoryStore.Batch 同款语义).
//
// **不用 pipeline**: 业务 Batch 各 Op 失败相互独立 — pipeline 整批失败语义
// 跟 best-effort 冲突.
//
// ⚠️ **别把它做成原子事务**(Lua / MULTI-EXEC): per-Op 错误隔离是 `store.Store.Batch`
// **唯一合法**的语义(2026-09-07 定), conformance 的
// `Batch_PerOp_Error_Isolated_BestEffort` 钉着——做成整批原子的后端跑它必红.
func (s *Store) Batch(ctx context.Context, ops []store.Op) ([]store.Result, error) {
	results := make([]store.Result, len(ops))
	for i, op := range ops {
		switch o := op.(type) {
		case store.GetOp:
			item, err := s.Get(ctx, o.Namespace, o.Key)
			results[i] = store.Result{Item: item, Err: err}
		case store.PutOp:
			var opts []store.PutOption
			if o.TTL != 0 {
				opts = append(opts, store.WithTTL(o.TTL))
			}
			err := s.Put(ctx, o.Namespace, o.Key, o.Value, opts...)
			results[i] = store.Result{Err: err}
		case store.SearchOp:
			var opts []store.SearchOption
			if o.Filter != nil {
				opts = append(opts, store.WithFilter(o.Filter))
			}
			if o.Query != "" {
				opts = append(opts, store.WithQuery(o.Query))
			}
			if o.Limit > 0 {
				opts = append(opts, store.WithLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, store.WithOffset(o.Offset))
			}
			items, err := s.Search(ctx, o.NamespacePrefix, opts...)
			results[i] = store.Result{Items: items, Err: err}
		case store.ListNamespacesOp:
			var opts []store.ListOption
			if len(o.Prefix) > 0 {
				opts = append(opts, store.WithListPrefix(o.Prefix))
			}
			if len(o.Suffix) > 0 {
				opts = append(opts, store.WithListSuffix(o.Suffix))
			}
			if o.MaxDepth > 0 {
				opts = append(opts, store.WithListMaxDepth(o.MaxDepth))
			}
			if o.Limit > 0 {
				opts = append(opts, store.WithListLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, store.WithListOffset(o.Offset))
			}
			nss, err := s.ListNamespaces(ctx, opts...)
			results[i] = store.Result{Namespaces: nss, Err: err}
		default:
			results[i] = store.Result{Err: fmt.Errorf("redis store batch: unknown Op type %T", op)}
		}
	}
	return results, nil
}
