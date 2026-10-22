package history_conformance_test

// 内存参考实现跑一遍 conformance——既验证 InMemoryHistoryStore 本身合规，也保证这套
// 断言不是空转的（一套自己都跑不通、或者随便什么实现都能过的 conformance 毫无价值）。

import (
	"context"
	"encoding/json"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agent_v2/history_conformance"
	"crew-go/schema"
)

func TestInMemoryHistoryStore_Conformance(t *testing.T) {
	history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
		return agent_v2.NewInMemoryHistoryStore()
	})
}

// jsonRoundTripStore 把每条消息按 JSON 编码再解回来存——持久后端的常见形状。它也要过：conformance 对标注值的要求
// 是「按 JSON 类型读回」（数字读回成 float64、数组读回成 []any 都算），不是原样保住 Go 类型。
type jsonRoundTripStore struct {
	inner *agent_v2.InMemoryHistoryStore
}

func (s *jsonRoundTripStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	out := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			out = append(out, nil)
			continue
		}
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		var cp schema.Message
		if err := json.Unmarshal(b, &cp); err != nil {
			return err
		}
		out = append(out, &cp)
	}
	return s.inner.Append(ctx, key, out)
}

func (s *jsonRoundTripStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	return s.inner.Load(ctx, key, q)
}

func TestJSONRoundTripHistoryStore_Conformance(t *testing.T) {
	history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
		return &jsonRoundTripStore{inner: agent_v2.NewInMemoryHistoryStore()}
	})
}
