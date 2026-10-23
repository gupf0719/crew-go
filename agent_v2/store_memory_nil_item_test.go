package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/store"
)

// 记忆后端的 Search 结果里夹着 nil 元素（自研后端的常见写法错误：预分配了 slice 又没填满）：框架跳过它，照常注入
// 其余记忆，不 panic。原来排序与按写入者扇出那几步直接解引用（it.Namespace、it.Value），preamble 整个崩掉。

type nilItemStore struct {
	store.Store
}

func (s nilItemStore) Search(ctx context.Context, prefix store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	got, err := s.Store.Search(ctx, prefix, opts...)
	if err != nil {
		return nil, err
	}
	return append([]*store.SearchItem{nil}, append(got, nil)...), nil
}

func TestStoreMemory_NilSearchItemsSkipped(t *testing.T) {
	for _, multiWriter := range []bool{false, true} {
		name := map[bool]string{false: "single_writer", true: "multi_writer"}[multiWriter]
		t.Run(name, func(t *testing.T) {
			base := store.NS("mem", "u1")
			inner := store.NewInMemoryStore()
			ctx := context.Background()
			if err := inner.Put(ctx, base, "k1", "用户喜欢绿茶"); err != nil {
				t.Fatal(err)
			}
			if multiWriter {
				// 两个写入者各一条，走按写入者扇出那条路
				if err := inner.Put(ctx, store.NS("mem", "u1", "w1"), "k2", "用户住在杭州 绿茶"); err != nil {
					t.Fatal(err)
				}
				if err := inner.Put(ctx, store.NS("mem", "u1", "w2"), "k3", "用户养猫 绿茶"); err != nil {
					t.Fatal(err)
				}
			}
			var seen []*schema.Message
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("mem_nil"),
				agent_v2.WithModel(newStubModelCapturing([]*schema.Message{schema.AssistantMessage("好的")}, &seen)),
				agent_v2.WithStore(nilItemStore{inner}),
				agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
				agent_v2.WithMaxRounds(2))
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("推荐一款绿茶")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			injected := false
			for _, m := range seen {
				if strings.Contains(m.Content, "用户喜欢绿茶") {
					injected = true
				}
			}
			if !injected {
				t.Errorf("夹着 nil 元素时其余记忆也该照常注入，模型看到的：%d 条消息", len(seen))
			}
		})
	}
}
