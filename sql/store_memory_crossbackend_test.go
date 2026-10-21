package sql_test

import (
	"context"
	"testing"

	_ "modernc.org/sqlite"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	agentSQL "crew-go/sql"
	"crew-go/store"
)

// echoModel 回固定答复并记下每次收到的 messages。
type echoModel struct {
	reply    string
	captured [][]*schema.Message
}

func (m *echoModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	cp := make([]*schema.Message, len(in))
	copy(cp, in)
	m.captured = append(m.captured, cp)
	return schema.AssistantMessage(m.reply), nil
}
func (m *echoModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *echoModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestStoreMemory_InjectionIsIdenticalAcrossBackends 钉「长期记忆注入的内容，在
// in-memory 与真 SQLite 上逐字相同」。
//
// # 这条修的是什么
//
// 框架此前往 store 写整条 *schema.Message。in-memory 原样持有 Go 值、持久后端存 JSON
// 读回 map，于是读侧的 `it.Value.(*schema.Message)` 断言**一边成功一边失败**：
// in-memory 注入真 assistant 消息，SQLite / redis / PG / MySQL 注入的是**整条消息的
// JSON 文本**包成的 SystemMessage。两边都 err == nil。
//
// 改成存**答复文本**之后 string 过 JSON 往返还是 string，两类后端逐字相同。
//
// # 为什么这条必须跨真引擎测
//
// 分歧的成因是「后端有没有做 JSON 往返」，而 in-memory 与 sqlmock **都不做**——只在
// agent_v2 内部测永远看不见它。这里用真 SQLite 引擎做对照组。
func TestStoreMemory_InjectionIsIdenticalAcrossBackends(t *testing.T) {
	const seeded = "用户偏好深色主题"
	ns := store.NS("mem")

	run := func(t *testing.T, st store.Store) []*schema.Message {
		t.Helper()
		ctx := context.Background()
		// 先跑一轮让框架自己写下记忆（而不是手工 Put——手工塞什么都行，
		// 那样测的就不是框架的写入形态了）
		m1 := &echoModel{reply: seeded}
		ag1 := agent_v2.NewBasicAgent(
			agent_v2.WithName("bot"), agent_v2.WithModel(m1),
			agent_v2.WithStore(st), agent_v2.WithStoreMemory(ns, nil),
			agent_v2.WithMaxRounds(2),
		)
		if _, err := ag1.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我喜欢深色主题")); err != nil {
			t.Fatalf("run1: %v", err)
		}
		// 第二轮：记忆该被注入
		m2 := &echoModel{reply: "知道了"}
		ag2 := agent_v2.NewBasicAgent(
			agent_v2.WithName("bot"), agent_v2.WithModel(m2),
			agent_v2.WithStore(st), agent_v2.WithStoreMemory(ns, nil),
			agent_v2.WithMaxRounds(2),
		)
		if _, err := ag2.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我偏好什么主题")); err != nil {
			t.Fatalf("run2: %v", err)
		}
		if len(m2.captured) == 0 {
			t.Fatal("模型未被调用")
		}
		var injected []*schema.Message
		for _, msg := range m2.captured[0] {
			if v, ok := msg.Annotations[agent_v2.StoreInjectedAnnotation].(bool); ok && v {
				injected = append(injected, msg)
			}
		}
		return injected
	}

	memInj := run(t, store.NewInMemoryStore())

	db, err := agentSQL.OpenSQLiteForTest()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlSt := agentSQL.NewStore(db, agentSQL.SQLite{})
	if err := sqlSt.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	sqlInj := run(t, sqlSt)

	if len(memInj) == 0 || len(sqlInj) == 0 {
		t.Fatalf("两边都该注入到记忆：in-memory %d 条 / sqlite %d 条（0 条时下面的比对恒真）",
			len(memInj), len(sqlInj))
	}
	if len(memInj) != len(sqlInj) {
		t.Fatalf("注入条数不同：in-memory %d / sqlite %d", len(memInj), len(sqlInj))
	}
	for i := range memInj {
		if memInj[i].Content != sqlInj[i].Content {
			t.Errorf("第 %d 条注入内容不同（这正是被修的那个分歧）:\n  in-memory %q\n  sqlite    %q",
				i, memInj[i].Content, sqlInj[i].Content)
		}
		if memInj[i].Role != sqlInj[i].Role {
			t.Errorf("第 %d 条注入 role 不同: in-memory %s / sqlite %s", i, memInj[i].Role, sqlInj[i].Role)
		}
	}
	// 正向断言：注入的确实是那条记忆，不是空壳（否则「两边相同」在空内容上恒真）
	if memInj[0].Content != seeded {
		t.Errorf("注入内容应是写下的那条答复 %q，得到 %q", seeded, memInj[0].Content)
	}
}
