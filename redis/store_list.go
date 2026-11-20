package redis

import (
	"context"
	"fmt"
	"sort"

	"crew-go/store"
)

// ListNamespaces 实现 store.Store.ListNamespaces.
//
// 按存活条目核对 namespace。已删除或到期的条目不占分页，空 namespace 在核对时原子回收。
//
// **支持**:
//   - prefix / suffix 段级过滤 (跟 InMemoryStore.matchesPrefix/Suffix 同款)
//   - maxDepth 截断 + dedup
//   - offset / limit 分页
//   - 稳定字典序
func (s *Store) ListNamespaces(ctx context.Context, opts ...store.ListOption) ([]store.Namespace, error) {
	cfg := store.ParseListOptions(opts...)
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if err := store.ValidateOffset(cfg.Offset); err != nil {
		return nil, err
	}

	// prefix / suffix 自身需合法 (非空时才校验)
	if len(cfg.Prefix) > 0 {
		if err := cfg.Prefix.Validate(); err != nil {
			return nil, err
		}
	}
	if len(cfg.Suffix) > 0 {
		if err := cfg.Suffix.Validate(); err != nil {
			return nil, err
		}
	}

	nsJoineds, err := s.namespaceNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis store list_namespaces: %w", err)
	}

	candidates := make([]store.Namespace, 0, len(nsJoineds))
	for _, nsJoined := range nsJoineds {
		ns := splitNS(nsJoined)
		if ns == nil {
			continue
		}
		if !matchesPrefix(ns, cfg.Prefix) {
			continue
		}
		if !matchesSuffix(ns, cfg.Suffix) {
			continue
		}
		candidates = append(candidates, ns)
	}

	all, err := s.liveNamespaces(ctx, candidates)
	if err != nil {
		return nil, fmt.Errorf("redis store list_namespaces liveness: %w", err)
	}

	if cfg.MaxDepth > 0 {
		for i, ns := range all {
			if len(ns) > cfg.MaxDepth {
				all[i] = ns[:cfg.MaxDepth:cfg.MaxDepth]
			}
		}
		all = dedupNamespaces(all)
	}

	// 稳定字典序
	sort.Slice(all, func(i, j int) bool {
		return all[i].String() < all[j].String()
	})

	// offset / limit
	if cfg.Offset >= len(all) {
		return []store.Namespace{}, nil
	}
	all = all[cfg.Offset:]
	if len(all) > cfg.Limit {
		all = all[:cfg.Limit]
	}
	return all, nil
}

// matchesPrefix 跟 InMemoryStore.matchesPrefix 同款 (段级前缀).
func matchesPrefix(ns, prefix store.Namespace) bool {
	if len(prefix) == 0 {
		return true
	}
	if len(prefix) > len(ns) {
		return false
	}
	for i, seg := range prefix {
		if ns[i] != seg {
			return false
		}
	}
	return true
}

// matchesSuffix 跟 InMemoryStore.matchesSuffix 同款 (段级后缀).
func matchesSuffix(ns, suffix store.Namespace) bool {
	if len(suffix) == 0 {
		return true
	}
	if len(suffix) > len(ns) {
		return false
	}
	offset := len(ns) - len(suffix)
	for i, seg := range suffix {
		if ns[offset+i] != seg {
			return false
		}
	}
	return true
}

// dedupNamespaces 截断后可能重复 (e.g., users.u1 + users.u2 maxDepth=1 都变 users).
func dedupNamespaces(nss []store.Namespace) []store.Namespace {
	seen := make(map[string]bool, len(nss))
	out := make([]store.Namespace, 0, len(nss))
	for _, ns := range nss {
		k := ns.String()
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, ns)
	}
	return out
}
