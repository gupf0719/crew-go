package conformance

import (
	"context"
	"errors"
	"testing"

	"crew-go/store"
)

// testBatch 验证 Batch 多 op 混合 + per-Op result.
func testBatch(t *testing.T, factory StoreFactory) {
	t.Run("Batch_Mixed_Ops", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("batch")

		// 预置 1 条
		_ = st.Put(ctx, ns, "existing", "old")

		results, err := st.Batch(ctx, []store.Op{
			store.PutOp{Namespace: ns, Key: "new1", Value: "n1"},
			store.PutOp{Namespace: ns, Key: "new2", Value: "n2"},
			store.GetOp{Namespace: ns, Key: "existing"},
			store.GetOp{Namespace: ns, Key: "missing"},
		})
		if err != nil {
			t.Fatalf("Batch: %v", err)
		}
		if len(results) != 4 {
			t.Fatalf("expected 4 results, got %d", len(results))
		}

		// 前 2 个 PutOp 成功 (Result.Err nil)
		for i, r := range results[:2] {
			if r.Err != nil {
				t.Errorf("PutOp[%d] should succeed, got err: %v", i, r.Err)
			}
		}

		// 第 3 个 GetOp(existing) 返 Item.Value="old"
		if results[2].Item == nil || results[2].Item.Value != "old" {
			t.Errorf("GetOp(existing) should return Item{Value:'old'}, got %+v", results[2].Item)
		}

		// 第 4 个 GetOp(missing) 返 Item=nil + Err=nil
		if results[3].Item != nil {
			t.Errorf("GetOp(missing) should return nil Item, got %+v", results[3].Item)
		}

		// 后置验证: 2 个新 Put 确实写入
		newItem, _ := st.Get(ctx, ns, "new1")
		if newItem == nil || newItem.Value != "n1" {
			t.Errorf("PutOp.new1 not persisted, got %+v", newItem)
		}
	})

	t.Run("Batch_Empty_Ops_Returns_Empty_Results", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		results, err := st.Batch(ctx, []store.Op{})
		if err != nil {
			t.Errorf("Batch with empty ops: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected empty results, got %d", len(results))
		}
	})

	t.Run("Batch_SearchOp_Returns_Items_Slice", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("search_in_batch")

		_ = st.Put(ctx, ns, "a", 1)
		_ = st.Put(ctx, ns, "b", 2)

		results, err := st.Batch(ctx, []store.Op{
			store.SearchOp{NamespacePrefix: ns},
		})
		if err != nil {
			t.Fatalf("Batch SearchOp: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Err != nil {
			t.Errorf("SearchOp err: %v", results[0].Err)
		}
		if len(results[0].Items) != 2 {
			t.Errorf("expected 2 items, got %d", len(results[0].Items))
		}
	})

	t.Run("Batch_PerOp_Error_Isolated_BestEffort", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("ok")

		// 1 个合法 + 1 个 invalid namespace
		results, _ := st.Batch(ctx, []store.Op{
			store.PutOp{Namespace: ns, Key: "k", Value: "v"},
			store.PutOp{Namespace: store.Namespace{"has.dot"}, Key: "k", Value: "v"},
		})
		// 第一个应成功, 第二个应失败 (per-Op err 隔离, 不整 batch fail)
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if results[0].Err != nil {
			t.Errorf("valid PutOp should succeed, got err: %v", results[0].Err)
		}
		if results[1].Err == nil {
			t.Error("invalid namespace PutOp should have Err non-nil")
		} else if !errors.Is(results[1].Err, store.ErrInvalidNamespace) &&
			!errors.Is(results[1].Err, store.ErrEmptyNamespace) {
			// 钉具体 sentinel — 业务 backend 用 generic err (e.g., "invalid arg")
			// 也能 PASS 是双层防护遮蔽 (audit 视角 2 H5)
			t.Errorf("invalid namespace PutOp should return ErrInvalidNamespace, got %v", results[1].Err)
		}
		// 后置验证: 第一个 Put 真持久化
		item, _ := st.Get(ctx, ns, "k")
		if item == nil {
			t.Error("valid PutOp value should be persisted despite sibling failing")
		}
	})
}
