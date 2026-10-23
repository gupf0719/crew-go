// Package history_conformance 提供 agent_v2.HistoryStore 的一致性测试套件——业务
// 实现自定义 backend（Redis / PG / MySQL 等）后，在自己的 _test.go 里 import 本包 +
// 调 RunHistoryStoreTests 验证语义合规。
//
// 用法：
//
//	package mybackend_test
//
//	import (
//	    "testing"
//	    "crew-go/agent_v2"
//	    "crew-go/agent_v2/history_conformance"
//	    "myorg/mybackend"
//	)
//
//	func TestMyRedisHistory_Conformance(t *testing.T) {
//	    history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
//	        hs := mybackend.NewRedisHistory("redis://localhost:6379")
//	        t.Cleanup(func() { _ = hs.Close() })
//	        return hs
//	    })
//	}
//
// **factory 模式**：每个子测试拿全新实例，子测试间状态隔离。业务 backend 自带 Close
// 时在 factory 内 t.Cleanup 注册。
//
// # 为什么这几条值得机械验一遍
//
// 历史层的失效方式全是**静默**的——写丢一条不报错、顺序乱了不报错、串了会话也不报
// 错。业务发现问题时看到的只是「模型好像忘了上一轮说过什么」，从那里回溯到「我的
// backend 没保住顺序」要绕很远。所以这套专挑那些**实现错了但不会 panic** 的点：
//
//   - **顺序** — 历史是时序回放，乱序即失真。按 map 存、按 hash 分片、并发写不加锁，
//     都可能悄悄打乱。
//   - **append-only** — 框架每次只交新产生的那几条，从不回头改写。实现方若做了去重、
//     覆盖或截断，「上次那轮到底说了什么」就不可信了。逐字重复的两条**必须都留着**
//     （模型真的可能连说两句一样的话）。
//   - **key 两个维度都要区分** — 只按 ConvID 分而忽略 Owner，是把别人的对话拉给
//     当前用户看；只按 Owner 分，是把所有会话搅成一锅。前者是数据泄露。
//   - **字段完整** — Message 上 ToolCalls / Annotations 这些不是装饰：工具调用链断
//     了模型会重复调用，Annotations 丢了归属就没了（框架正是靠它标「这条是谁说的」）。
//   - **空会话不是错误** — 「没聊过」和「出错了」必须能区分，否则第一轮对话就报错。
//   - **分页读** — 框架每次 Run 只读最近一段（`HistoryQuery` 的 Before + Limit）。Limit 取成最早
//     的 N 条、游标随追加漂移、More 报错，都不报错——只是模型看到的上文错了一截。
//   - **按条件读** — 框架的去重只靠 `HistoryQuery` 的 MessageID / Task / Agent 问后端，「只看自己」
//     的视图按归属读。条件没生效或按「或」处理，去重误判「记过」、该记的没记；More 把不满足条件的
//     更早记录也算进去，框架判后端违约、同一句话记两份。
//
// # 覆盖什么（19 条子测试）
//
// RoundTrip / AppendOnly / Order / KeyIsolation（含**分隔符歧义**：`{acme/eu, c-1}` 与
// `{acme, eu/c-1}` 拼串主键会撞）/ MessageFields（ToolCalls 全字段 + Annotations +
// **tool 消息正文** + **ReasoningContent**）/ EmptyConv（含带 Limit 读）/ EmptyBatch / LoadSnapshot /
// AppendNoMutation（不得原地改写入参）/ Concurrent / 分页五条：LoadLimitReturnsLatest /
// PagingCoversAllOnce / PagingStableUnderAppend（往前翻页时别人在追加）/ LimitBeyondTotal /
// BeforeOldestIsEmpty / 按条件读四条：FilterByMessageID（同一个 ID 的每一份）/ FilterConditionsAreANDed /
// FilterPagingCoversMatchesOnce（含「最早一条满足条件的之前还有不满足的」时 More 为 false、翻页中途追加）/
// FilterNoMatchIsEmpty。
//
// 其中两条最容易被自研 backend 漏掉：**tool 消息的正文**（配对信息全对但正文丢了，模型
// 下一轮不知道查到的是什么）和 **ReasoningContent**（thinking 类模型要求上一轮的 reasoning
// 原样回传，不传第 2 轮直接 400——而只映射 Role/Content/ToolCalls 是最自然的写法）。
//
// # 不覆盖什么（业务自负）
//
//   - **跨进程 wire format** — JSONB / bytes 编码由业务 backend 的 serializer 自测。
//     这里的参考实现是内存的，序列化环节根本不经过。
//   - **容量与截断** — 全套最大写入量是 30 条，一个上限 50/100 的后端照样全绿。
//   - **性能 / 吞吐 / 大 payload** — 业务自压测。
//   - **TTL / 归档 / 清理策略** — 接口层没有这些语义，业务自定。
package history_conformance

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// HistoryStoreFactory 每次调用返回一个全新的、空的 HistoryStore 实例。
type HistoryStoreFactory func() agent_v2.HistoryStore

// RunHistoryStoreTests 跑全部一致性子测试。
func RunHistoryStoreTests(t *testing.T, factory HistoryStoreFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("history_conformance: factory 不能为 nil")
	}

	t.Run("AppendLoadRoundTrip", func(t *testing.T) { testAppendLoadRoundTrip(t, factory) })
	t.Run("AppendOnly", func(t *testing.T) { testAppendOnly(t, factory) })
	t.Run("AppendNoMutation", func(t *testing.T) { testAppendNoMutation(t, factory) })
	t.Run("Order", func(t *testing.T) { testOrder(t, factory) })
	t.Run("KeyIsolation", func(t *testing.T) { testKeyIsolation(t, factory) })
	t.Run("MessageFields", func(t *testing.T) { testMessageFields(t, factory) })
	t.Run("EmptyConversation", func(t *testing.T) { testEmptyConversation(t, factory) })
	t.Run("EmptyAppend", func(t *testing.T) { testEmptyAppend(t, factory) })
	t.Run("LoadSnapshotIsolation", func(t *testing.T) { testLoadSnapshotIsolation(t, factory) })
	t.Run("LongKeys", func(t *testing.T) { testLongKeys(t, factory) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, factory) })
	t.Run("LoadLimitReturnsLatest", func(t *testing.T) { testLoadLimitReturnsLatest(t, factory) })
	t.Run("PagingCoversAllOnce", func(t *testing.T) { testPagingCoversAllOnce(t, factory) })
	t.Run("PagingStableUnderAppend", func(t *testing.T) { testPagingStableUnderAppend(t, factory) })
	t.Run("LimitBeyondTotal", func(t *testing.T) { testLimitBeyondTotal(t, factory) })
	t.Run("BeforeOldestIsEmpty", func(t *testing.T) { testBeforeOldestIsEmpty(t, factory) })
	t.Run("FilterByMessageID", func(t *testing.T) { testFilterByMessageID(t, factory) })
	t.Run("FilterConditionsAreANDed", func(t *testing.T) { testFilterConditionsAreANDed(t, factory) })
	t.Run("FilterPagingCoversMatchesOnce", func(t *testing.T) { testFilterPagingCoversMatchesOnce(t, factory) })
	t.Run("FilterNoMatchIsEmpty", func(t *testing.T) { testFilterNoMatchIsEmpty(t, factory) })
}

// mustLoad 读一次会话，出错即 fatal。
func mustLoad(t *testing.T, hs agent_v2.HistoryStore, key agent_v2.HistoryKey) []*schema.Message {
	t.Helper()
	got, err := agent_v2.LoadAllHistory(context.Background(), hs, key)
	if err != nil {
		t.Fatalf("Load(%s): %v", key, err)
	}
	return got
}

// mustAppend 写一批，出错即 fatal。
func mustAppend(t *testing.T, hs agent_v2.HistoryStore, key agent_v2.HistoryKey, msgs ...*schema.Message) {
	t.Helper()
	if err := hs.Append(context.Background(), key, msgs); err != nil {
		t.Fatalf("Append(%s, %d 条): %v", key, len(msgs), err)
	}
}
