package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"crew-go/store"
)

// Search 实现 store.Store.Search.
//
// **PoC 限制**: SCAN + 客户端 filter, 大集合 (>10万 key) 慢. 业务大规模建议接
// mem0 后端或自实现 RediSearch backend.
//
// **支持**:
//   - ns prefix 匹配 (自身 + 全后代, 跟 sql.nsExactOrDescendantSQL 同款)
//   - filter exact match (顶级 key) + 6 比较 op ($eq/$ne/$gt/$gte/$lt/$lte)——语义与
//     校验都直接用 store.ValidateFilter / store.MatchFilter (客户端过滤的唯一实现, 不另抄)
//   - offset/limit 分页
//   - 时新优先 (UpdatedAt 降序，同值回落 ns/key)
//
// **不支持**:
//   - 语义搜索 (WithQuery, PoC 不内置)
//   - 嵌套 filter (`{"nested": {"sub": ...}}` 业务嵌套不支持)
func (s *Store) Search(ctx context.Context, namespacePrefix store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	if len(namespacePrefix) == 0 {
		return nil, store.ErrEmptyNamespace
	}
	if err := namespacePrefix.Validate(); err != nil {
		return nil, err
	}

	cfg := store.ParseSearchOptions(opts...)
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}
	if err := store.ValidateOffset(cfg.Offset); err != nil {
		return nil, err
	}

	if err := store.ValidateFilter(cfg.Filter); err != nil {
		return nil, err
	}

	// SCAN 拿所有匹配 ns prefix (自身 + 后代) 的 item key
	prefixJoined := joinNS(namespacePrefix)
	selfPattern, descPattern := storeItemPatternForNS(s.prefix, prefixJoined)

	keys, err := s.scanAll(ctx, selfPattern)
	if err != nil {
		return nil, fmt.Errorf("redis store search: scan self: %w", err)
	}
	descKeys, err := s.scanAll(ctx, descPattern)
	if err != nil {
		return nil, fmt.Errorf("redis store search: scan descendant: %w", err)
	}
	keys = append(keys, descKeys...)

	if len(keys) == 0 {
		return []*store.SearchItem{}, nil
	}

	// MGET 批量拿 record (iter-5-K 修: 加 chunk 边界防大集合 server-side block)
	rawValues, err := s.mgetChunked(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("redis store search: mget: %w", err)
	}

	matched := make([]*store.SearchItem, 0, len(keys))
	for i, raw := range rawValues {
		if raw == nil {
			// key 在 SCAN 后被并发 delete — 跳过 (跟 InMemoryStore RLock 读语义对齐)
			continue
		}
		rawStr, ok := raw.(string)
		if !ok {
			continue
		}

		var rec storeRecord
		if err := json.Unmarshal([]byte(rawStr), &rec); err != nil {
			// 跨 backend backup 数据损坏 — 跳过 (损坏记录不致命, 容错跳过)
			continue
		}
		value, err := decodeValue(rec.Value)
		if err != nil {
			continue
		}

		// filter 过滤
		ok2, err := store.MatchFilter(value, cfg.Filter)
		if err != nil {
			return nil, err
		}
		if !ok2 {
			continue
		}

		// 反向解析 key 拿 (ns, dataKey)
		ns, dataKey, parseOK := parseStoreItemKey(s.prefix, keys[i])
		if !parseOK {
			continue
		}

		matched = append(matched, &store.SearchItem{
			Item: store.Item{
				Namespace: ns,
				Key:       dataKey,
				Value:     value,
				CreatedAt: rec.CreatedAt.UTC(),
				UpdatedAt: rec.UpdatedAt.UTC(),
			},
			Score: 0, // PoC 无语义搜索, score=0
		})
	}

	// **时新优先**（UpdatedAt 降序）——Search 的顺序契约，见 `store/conformance` 的
	// `Search_StableOrder_RecentFirst`。Search 恒带 limit，所以顺序**就是**「截断时留下
	// 谁」；按 key 排等于按首字母挑，跟这条数据对调用方有没有用零相关。
	//
	// ⚠️ 同时间戳时这里只能回落字典序——`InMemoryStore` 用进程内单调序号做第二键消除
	// 非确定性，那个手段跨进程后端用不了。Redis 后端的时间戳分辨率若不足以分开一次
	// 突发写入，同一批数据的截断结果会随写入时钟抖动（跟 InMemory 不同，这是本后端的
	// 已知局限）。
	sort.Slice(matched, func(i, j int) bool {
		ti, tj := matched[i].UpdatedAt, matched[j].UpdatedAt
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		if matched[i].Namespace.String() != matched[j].Namespace.String() {
			return matched[i].Namespace.String() < matched[j].Namespace.String()
		}
		return matched[i].Key < matched[j].Key
	})

	// offset/limit 应用
	if cfg.Offset >= len(matched) {
		return []*store.SearchItem{}, nil
	}
	matched = matched[cfg.Offset:]
	if len(matched) > cfg.Limit {
		matched = matched[:cfg.Limit]
	}
	return matched, nil
}
