package agent_v2_test

// 会话历史的两条身份线：**agent 名**（谁说的，决定读回时哪些算自己的）和 **本轮 input
// 在不在 prior 里**（决定要不要再喂一遍）。
//
// 这两条都是 prior 换源之后才变成正确性判据的——换源前 agent 名只是日志标签、input
// 归谁写也无所谓。两条各自失效的症状都是**静默的**：不报错、不空指针，只是模型看到的
// 东西不对，然后给出一个看起来挺合理的答案。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// promptRecorder 记下每次调用时 LLM 实际收到的消息流。
//
// 这一族测试全部依赖它——断言「历史写进去了」不够，那只证明了写入侧；真正要钉的是
// **模型最终看到了什么**，中间任何一层过滤错了都会在这里现形。
type promptRecorder struct {
	mu   sync.Mutex
	seen [][]*schema.Message
	resp []*schema.Message
	idx  int
}

func (m *promptRecorder) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	cp := make([]*schema.Message, len(in))
	copy(cp, in)
	m.seen = append(m.seen, cp)
	i := m.idx
	m.idx++
	m.mu.Unlock()
	if i >= len(m.resp) {
		return schema.AssistantMessage("[stub 耗尽]"), nil
	}
	return m.resp[i], nil
}

func (m *promptRecorder) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *promptRecorder) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*promptRecorder)(nil)

// call 返回第 n 次调用时模型收到的内容（越界返 nil）。
func (m *promptRecorder) call(n int) []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n >= len(m.seen) {
		return nil
	}
	return m.seen[n]
}

func (m *promptRecorder) dump() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	for i, call := range m.seen {
		b.WriteString("  第 " + itoa(i+1) + " 次调用模型收到:\n")
		if len(call) == 0 {
			b.WriteString("    (空 —— 模型什么都没看到)\n")
		}
		for _, msg := range call {
			b.WriteString("    [" + string(msg.Role) + "] " + msg.Content + "\n")
		}
	}
	return b.String()
}

// containsMsg 报消息流里有没有哪条的内容含 want。
func containsMsg(msgs []*schema.Message, want string) bool {
	for _, m := range msgs {
		if m != nil && strings.Contains(m.Content, want) {
			return true
		}
	}
	return false
}

// TestHistory_RequiresExplicitName：开历史却不起名，构造期就得拦下来。
//
// 不起名时框架自动生成的名字每次构造都不一样。历史按归属 agent 名读回，名字一换就一条
// 都对不上——而后端确实读成功了（只是过滤后空了），于是**不会回落到 session events**，
// 模型只剩当前这一句话。开了历史反而比不开更失忆，且全程无报错。
//
// 命中的正是历史层的主场景：每连接构造一个 agent、进程重启换名——恰恰是「跨 Run 留住
// 对话」最需要生效的地方。
func TestHistory_RequiresExplicitName(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("开了 WithHistory 却没 WithName，构造期应当 panic —— " +
				"放过去的话，历史会写进去但永远读不回来，且不报任何错")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "WithName") {
			t.Fatalf("panic 信息没指出该配什么: %v", r)
		}
	}()
	agent_v2.NewBasicAgent(
		agent_v2.WithModel(&promptRecorder{}),
		agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-a"),
	)
}

// TestHistory_NewInstanceSeesPriorRuns：换一个 agent 实例，历史还认得。
//
// 这是历史层存在的理由本身——agui 每个连接、a2a 每个请求都在重新构造 agent，历史必须
// 跟着**会话**走而不是跟着实例走。
func TestHistory_NewInstanceSeesPriorRuns(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")

	newAgent := func(m model.ChatModel) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("assistant"),
			agent_v2.WithModel(m),
			agent_v2.WithHistory(hs, "tenant-a"),
		)
	}

	m1 := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("答复一")}}
	if _, err := newAgent(m1).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问题一")); err != nil {
		t.Fatalf("第一次: %v", err)
	}

	// 新连接 → 新实例 + 新 session，只有 ConvID 是同一个。
	m2 := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("答复二")}}
	if _, err := newAgent(m2).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问题二")); err != nil {
		t.Fatalf("第二次: %v", err)
	}

	got := m2.call(0)
	for _, want := range []string{"问题一", "答复一", "问题二"} {
		if !containsMsg(got, want) {
			t.Errorf("新实例没看到 %q —— 历史跟着实例走了，没跟着会话走\n%s", want, m2.dump())
		}
	}
}

// TestHistory_HandoffSharedView_SeesCurrentInput：转交视图下，模型仍要收到本轮问题。
//
// 转交的共享历史视图（Raw / Rewrite）故意不重复 append 本轮 input，因为 HandoffAgent
// 已经把它写进 session、prior 捞得到。但历史层的 prior **恒不含本轮 input**——写入点
// 在读之后，是顺序决定的。两边混起来的后果：
//
//	第 1 轮：模型收到空 prompt
//	第 2 轮：模型收到的是**上一轮**的问题
//
// 第 2 轮是更坏的那个：它对着旧问题给出一个看起来完全合理的答案，错位一轮且无任何报错。
func TestHistory_HandoffSharedView_SeesCurrentInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	m := &promptRecorder{resp: []*schema.Message{
		schema.AssistantMessage("答复一"),
		schema.AssistantMessage("答复二"),
	}}
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(m),
		agent_v2.WithHistory(hs, "tenant-a"),
	)
	// 默认视图就是 Rewrite（共享历史），正是出问题的那档。
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	for _, q := range []string{"问题一", "问题二"} {
		if _, err := ho.Run(ctx, sess, schema.UserMessage(q)); err != nil {
			t.Fatalf("Run(%s): %v", q, err)
		}
	}

	for i, want := range []string{"问题一", "问题二"} {
		got := m.call(i)
		if len(got) == 0 {
			t.Errorf("第 %d 轮模型收到空 prompt\n%s", i+1, m.dump())
			continue
		}
		if !containsMsg(got, want) {
			t.Errorf("第 %d 轮模型没收到本轮问题 %q —— 它会对着别的问题作答，且不报错\n%s",
				i+1, want, m.dump())
		}
	}

	// 第二轮还要看得见第一轮，否则就成了每轮从头开始。
	if second := m.call(1); !containsMsg(second, "问题一") || !containsMsg(second, "答复一") {
		t.Errorf("第 2 轮没看到第 1 轮的往返\n%s", m.dump())
	}
}

// TestHistory_HandoffSharedView_NoInputDup：转交视图下也不能把本轮问题喂两遍。
//
// 跟上一条是同一枚硬币的两面：判据从「什么视图」改成「prior 从哪来」之后，得确认没把
// 原来防的 input dup 放回来。
func TestHistory_HandoffSharedView_NoInputDup(t *testing.T) {
	m := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("答复")}}
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(m))
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front)

	// 不开历史 —— prior 走 session events，本轮 input 已在里面。
	if _, err := ho.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("唯一的问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var n int
	for _, msg := range m.call(0) {
		if strings.Contains(msg.Content, "唯一的问题") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("本轮问题在 prompt 里出现 %d 次, want 1\n%s", n, m.dump())
	}
}

// TestHistory_NestedSubAgent_ReadsSameStreamItWrites：嵌套时，子 agent 读的和写的必须
// 是同一条流。
//
// 一场会话只该有一条流——子 agent 的产出照样进父的 (Owner, ConvID)，靠归属标记区分是谁
// 说的。这是写侧有意的设计。但读侧若还认子自己的 `WithHistory` 配置，就会去一个**从来
// 没人往里写过**的 store 上读：读到空、还报「读成功」压掉 events 回落，子 agent 每轮都
// 是失忆状态。
func TestHistory_NestedSubAgent_ReadsSameStreamItWrites(t *testing.T) {
	parentHS := agent_v2.NewInMemoryHistoryStore()
	childHS := agent_v2.NewInMemoryHistoryStore() // 子自己配的——嵌套下不生效

	childM := &promptRecorder{resp: []*schema.Message{
		schema.AssistantMessage("子答复一"),
		schema.AssistantMessage("子答复二"),
	}}
	child := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"),
		agent_v2.WithModel(childM),
		agent_v2.WithHistory(childHS, "tenant-CHILD"),
	)
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(child, agent_v2.WithAgentToolDescription("问子 agent")))

	callChild := func(arg string) *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{
			Type:     "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: "tc1", Name: "child", Arguments: arg},
		}}}
	}
	parentM := &promptRecorder{resp: []*schema.Message{
		callChild(`{"input":"子问题一"}`), schema.AssistantMessage("父答复一"),
		callChild(`{"input":"子问题二"}`), schema.AssistantMessage("父答复二"),
	}}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentM),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(parentHS, "tenant-PARENT"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	for _, q := range []string{"父问题一", "父问题二"} {
		if _, err := parent.Run(ctx, sess, schema.UserMessage(q)); err != nil {
			t.Fatalf("Run(%s): %v", q, err)
		}
	}

	// 写：一条流，都在父的 key 下，各自标着归属。
	inParent, _ := parentHS.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-PARENT", ConvID: "conv-1"})
	var childMsgs int
	for _, m := range inParent {
		if owner, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string); owner == "child" {
			childMsgs++
		}
	}
	if childMsgs == 0 {
		t.Error("子 agent 的产出没进父的流——嵌套时该沿用父的写入器")
	}
	// 也要验上界：写入器若被重复挂载（父一个、子又建一个都指向父 store），同一句话会记
	// 两遍，只验 >0 看不出来。
	//
	// 这里父跑两轮、每轮各调一次子 agent，子每次记「一条 input + 一条产出」= 4 条。
	if childMsgs != 4 {
		t.Errorf("子 agent 名下 %d 条, want 4（父两轮 × 每轮「input + 产出」）——"+
			"多了疑似写入器被重复挂载，少了是漏记", childMsgs)
	}
	if inChild, _ := childHS.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-CHILD", ConvID: "conv-1"}); len(inChild) != 0 {
		t.Errorf("子自己的 store 里有 %d 条——嵌套时子的 WithHistory 不该生效", len(inChild))
	}

	// 读：子第二次被调用时，得看见自己第一次说的话。
	if !containsMsg(childM.call(1), "子答复一") {
		t.Errorf("子 agent 看不到自己上一轮说的话——读的 store 跟写的不是同一个，"+
			"而且读到空还会报「读成功」压掉 events 回落\n%s", childM.dump())
	}
}

// TestHistory_HandoffSharedView_InputRecordedOnce：转交多跳，用户那句话在历史里只留一份。
//
// HandoffAgent 的每跳 input 恒为**原始用户输入**（后接靠共享历史看前序处理）。每跳的
// preamble 都记一遍的话，历史里同一句用户的话就有 N 份——下轮 Rewrite/Raw 投影会把它们全
// 带给模型（user 消息在跨 agent 改写里原样透传），模型看见用户把同一句话说了 N 遍；业务
// 回放同样失真。
//
// 判据**不能照抄 events 侧**那行只在 SelfOnly 才写的 AddEvent：events 侧敢那么写是因为
// HandoffAgent 自己会把 input 写进 session 兜底，而历史层没有对应的写入者——只有
// BasicAgent 的 preamble 写。照抄的话共享视图下用户那句话干脆进不了历史。所以是「只有第
// 一跳记」，下面两条断言正好卡住这两头。
func TestHistory_HandoffSharedView_InputRecordedOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc_1", "billing")})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单已处理")})),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxRounds(5),
	)
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-handoff-once")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-handoff-once"})
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "USER_ASK_MARK") {
			n++
		}
	}
	switch {
	case n == 0:
		t.Error("用户那句话根本没进历史——共享视图下 HandoffAgent 不写历史层，" +
			"只有第一跳的 preamble 会写，把它也挡掉就永远没人写了")
	case n > 1:
		t.Errorf("用户那句话在历史里有 %d 份——每跳 input 恒为原始输入，每跳都记就是重复。"+
			"下轮 Rewrite/Raw 投影会把它们全带给模型，看起来像用户说了 %d 遍", n, n)
	}
	if !containsMsg(got, "账单已处理") {
		t.Error("接手方的产出没进历史")
	}
}

// TestHistory_NestedHandoff_InputRecordedOnce：嵌套 HandoffAgent 时原始 input 仍只一份。
//
// 「只记第一跳」若用 handoff 的 hops 判断，嵌套时会失效——hops 在每层各自从 0 起，外层
// 第一跳 + 内层第一跳各记一次，实测原始 input 进历史 2 份。改用整场对话级的 session 标记
// （跨嵌套共享同一个 parentSession）后不管嵌套多深都只认第一次。
func TestHistory_NestedHandoff_InputRecordedOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()

	deskA := agent_v2.NewBasicAgent(
		agent_v2.WithName("deskA"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t1", "deskB")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	deskB := agent_v2.NewBasicAgent(
		agent_v2.WithName("deskB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("B 处理完")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	inner := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("inner"), agent_v2.WithHandoffInitial("deskA"),
	).AddAgent("deskA", deskA).AddAgent("deskB", deskB)

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t0", "inner")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	outer := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("outer"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("inner", inner)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-nested")
	if _, err := outer.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-nested"})
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "USER_ASK_MARK") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("嵌套 handoff 后原始 input 出现 %d 次, want 1——判据用了 handoff 的 hops，"+
			"每层各自从 0 起就会各记一次", n)
	}
	if !containsMsg(got, "B 处理完") {
		t.Error("最内层的产出没进历史")
	}
}

// TestHistory_Handoff_XProcResume_NoDupInput：跨进程 Resume 后原始 input 不重记。
//
// 去重状态跟着 input 走（nonce 随 cp + 历史序列化），不放会随场景丢失的载体。这条钉的正是
// 上一版 session 标记方案漏掉的窄窗口：第二跳 preamble 内中断（HistoryCompactor 抛 Interrupt）
// + 跨进程 Resume（新 session、标记丢）——那时第二跳 preamble 重跑记 input，而第一跳已记过。
// session 标记跨进程丢 → 2 份；nonce 在 input 里跨进程恢复 + 扫 prior（历史 backend 共享）→
// 1 份。
func TestHistory_Handoff_XProcResume_NoDupInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t0", "billing")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	// billing 第二跳 preamble 的 HistoryCompactor 第一次抛 Interrupt，Resume 放行。
	compactor := func(ctx context.Context, msgs []*schema.Message, keep int) ([]*schema.Message, error) {
		if _, ok := compose_v2.GetResumeAnswerText(ctx, "compact_id"); ok {
			return msgs, nil
		}
		return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "compact_id", Question: "?"}}
	}
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单完成")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithHistoryCompactor(compactor), agent_v2.WithMaxRounds(5),
	)
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-nw")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-nw"}

	sess := agent_v2.NewRunSession()
	if _, err := ho.Run(ctx, sess, schema.UserMessage("USER_MARK")); err != nil {
		if _, isInt := compose_v2.IsInterrupt(err); !isInt {
			t.Fatalf("Run 应中断在 billing preamble, got %v", err)
		}
	}
	// 跨进程：丢弃原 session，新 session Resume（session KV 全丢）。
	newSess := &agent_v2.RunSession{RunID: sess.RunID}
	if _, err := ho.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"compact_id": schema.UserMessage("go")}),
		agent_v2.WithResumeSession(newSess),
	); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	got, _ := hs.Load(ctx, key)
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "USER_MARK") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("跨进程 Resume 后原始 input %d 份, want 1——第二跳 preamble 重跑时，去重状态"+
			"若放 session（内存）会随进程丢失导致重记；nonce 在 input 里跨进程恢复才对\n%s",
			n, dumpHist(got))
	}
}

// TestHistory_Handoff_RealRepeatNotDeduped：nonce 去重不能误杀真实重复。
//
// nonce 区分「同一条 input 透传多跳」（同 nonce，去重）和「不同轮真的说了同样的话」（不同
// nonce，都留）。两次 Run 说同样的话，是两条独立的原始输入，各带各的 nonce，历史里该有两份
// ——否则就退回 append-only 契约禁止的「按内容去重」。
func TestHistory_Handoff_RealRepeatNotDeduped(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	newHo := func() agent_v2.Agent {
		front := agent_v2.NewBasicAgent(
			agent_v2.WithName("front"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("好的")})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
		)
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front)
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rep")
	// 两次 Run，说一模一样的话——两条独立原始输入。
	for i := 0; i < 2; i++ {
		if _, err := newHo().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("继续")); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-rep"})
	var n int
	for _, m := range got {
		if m != nil && m.Role == schema.RoleUser && m.Content == "继续" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("两次真实重复的输入只留了 %d 份, want 2——nonce 去重误杀了真实重复，"+
			"退回了 append-only 契约禁止的按内容去重\n%s", n, dumpHist(got))
	}
}

// TestHistory_Handoff_RawView_InputRecordedOnce：Raw 视图下 nonce 去重同样成立。
//
// nonce 判据看 prior，而 prior 是投影后的：Rewrite 把别人的 user 消息原样透传、Raw 加
// from_agent 标注但也保留 Annotations（含 nonce）。两个视图 nonce 都可见，所以去重都对。
// 前面几条测的是默认 Rewrite，这条补 Raw。
func TestHistory_Handoff_RawView_InputRecordedOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t0", "billing")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("完成")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
	)
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistoryRaw),
	).AddAgent("front", front).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-raw")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("RAW_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-raw"})
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "RAW_MARK") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("Raw 视图下原始 input %d 份, want 1——Raw 投影没保住 nonce？\n%s", n, dumpHist(got))
	}
}

// loadFailHistoryStore：Append 正常存，Load 报错——触发 preamble 读失败回落 session events。
type loadFailHistoryStore struct {
	inner *agent_v2.InMemoryHistoryStore
}

func (s *loadFailHistoryStore) Append(ctx context.Context, k agent_v2.HistoryKey, m []*schema.Message) error {
	return s.inner.Append(ctx, k, m)
}
func (s *loadFailHistoryStore) Load(context.Context, agent_v2.HistoryKey) ([]*schema.Message, error) {
	return nil, errFakeLoad
}

var errFakeLoad = errors.New("模拟后端读失败")

// TestHistory_Handoff_LoadFailFallback_InputNotLost：读失败回落 events 时 input 不能丢。
//
// nonce 去重判据看 prior，而读失败时 prior 回落到 session events。events 里有 HandoffAgent
// 自己 AddEvent 的那份带 nonce input（HandoffAgent 不写 history store）——若 nonce 判据也认
// 它，每一跳都会误判「已记过」，front 和 billing 谁都不记，input 在降级路径**彻底丢失**
// （实测 0 份）。所以 nonce 去重只信 history（fromHistory），读失败回落时回退每跳记：降级
// 路径宁可重复也不能丢失（丢了模型永久看不到这场对话的开头，比冗余一份严重得多）。
func TestHistory_Handoff_LoadFailFallback_InputNotLost(t *testing.T) {
	backend := &loadFailHistoryStore{inner: agent_v2.NewInMemoryHistoryStore()}
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t0", "billing")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(backend, "t"), agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("完成")})),
		agent_v2.WithHistory(backend, "t"), agent_v2.WithMaxRounds(5),
	)
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-loadfail")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("MARK_LF")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 绕过报错的 Load，直接数实际 Append 进去的。
	got, _ := backend.inner.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-loadfail"})
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "MARK_LF") {
			n++
		}
	}
	if n < 1 {
		t.Errorf("读失败回落后原始 input %d 份——降级路径把 input 丢了，模型将永久看不到"+
			"这场对话的开头。nonce 去重必须只信 history（fromHistory），读失败时回退每跳记\n%s",
			n, dumpHist(got))
	}
}

// TestHistory_Handoff_SessionReuse_SecondRunRecorded：复用同一 session 多轮，第二轮 input
// 要记。
//
// 复用同一 session 对象跑多个 Run 是官方支持模式（doc/v2/handoff.md 多轮示例）。上一版
// session 标记方案把「记过没」放 session 对象上、跨 Run 存活 → 第二个 Run 的 input 被误判
// 「记过了」漏记（0 份）。nonce 跟 input 走，每个 Run 是新的原始 input、新 nonce，prior 里
// 第一轮的是旧 nonce 不匹配 → 第二轮照记。
func TestHistory_Handoff_SessionReuse_SecondRunRecorded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	newHo := func() agent_v2.Agent {
		front := agent_v2.NewBasicAgent(
			agent_v2.WithName("front"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("好")})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5),
		)
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front)
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reuse")
	sess := agent_v2.NewRunSession() // 同一个 session 对象，跑两轮
	if _, err := newHo().Run(ctx, sess, schema.UserMessage("第一轮问题")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := newHo().Run(ctx, sess, schema.UserMessage("第二轮问题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-reuse"})
	// 上下界都要验：只验"存在"的话，去重反向失效（第二轮记 3 份）照样绿——而本测试的
	// 主题恰恰就是去重。
	var n2 int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "第二轮问题") {
			n2++
		}
	}
	if n2 != 1 {
		t.Errorf("复用 session 的第二轮 input %d 份, want 1——0 份是去重状态跨 Run 存活导致漏记，"+
			"多份是每跳都记\n%s", n2, dumpHist(got))
	}
}

// TestHistory_Handoff_LossyCompactor_NoDupInput：去重判据必须用**未削减**的 prior。
//
// 去重问的是「这条 input 落进历史没有」——持久化状态；而第 0 层的 HistoryCompactor /
// MaxHistoryPairs 截断产出的是「这轮喂给模型多少」——有损投影。两者一旦混用，带 nonce
// 的那条 user 消息被削掉后去重就以为没记过，于是每跳再记一份；历史 append-only，重复
// 永久留存，下一轮模型看到用户把同一句话说了两遍。
//
// 本例用**有损** compactor（只留最后一条，正是它被文档定位的用途）把带 nonce 的 input
// 从 prior 里削掉，断言历史里原始 input 仍恰好 1 份。
//
// **sanity-revert**：把 buildInitialMessages 里 priorHasRootInputNonce 的入参从
// priorForDedup 改回 prior，本测试 FAIL 并报「2 份」——精确指向去重读了有损视图。
func TestHistory_Handoff_LossyCompactor_NoDupInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// 有损：只保留最后一条，前面的（含带 nonce 的原始 input）全丢。
	lossy := func(_ context.Context, msgs []*schema.Message, _ int) ([]*schema.Message, error) {
		if len(msgs) <= 1 {
			return msgs, nil
		}
		return msgs[len(msgs)-1:], nil
	}
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("t0", "billing")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "t"),
		agent_v2.WithHistoryCompactor(lossy), agent_v2.WithMaxRounds(5),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单完成")})),
		agent_v2.WithHistory(hs, "t"),
		agent_v2.WithHistoryCompactor(lossy), agent_v2.WithMaxRounds(5),
	)
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-lossy")
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-lossy"}
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("USER_MARK")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, key)
	var n int
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "USER_MARK") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("有损 compactor 下原始 input %d 份, want 1——去重判据读的应是截断/压缩"+
			"**之前**的 prior（持久化状态），不是喂模型用的有损投影\n%s", n, dumpHist(got))
	}
}
