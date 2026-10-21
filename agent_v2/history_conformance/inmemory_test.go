package history_conformance_test

// 内存参考实现跑一遍 conformance——既验证 InMemoryHistoryStore 本身合规，也保证这套
// 断言不是空转的（一套自己都跑不通、或者随便什么实现都能过的 conformance 毫无价值）。

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/agent_v2/history_conformance"
)

func TestInMemoryHistoryStore_Conformance(t *testing.T) {
	history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
		return agent_v2.NewInMemoryHistoryStore()
	})
}
