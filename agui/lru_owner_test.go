package agui_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// TestLRU_EvictionIsPerOwner 钉住 LRU 淘汰**不跨租户**（积累型攻击者那一档）。
//
// sessions map 的**键**有 owner 维，但**容量**一开始没有——淘汰遍历整张表挑最旧的
// 空闲 entry，不看它属于谁。于是 B 只要不停开新会话，就能把 A 的挂起会话挤掉（连同
// dropCP 清掉 A 的 cp），A 续跑拿到 no_session。那是跨租户 DoS：不泄漏内容，但能让
// 别人的会话凭空消失。
//
// 「加了 owner 维的是键、没加的是配额」是同一个洞的两半。
//
// ⚠️ 这条只覆盖**积累型**攻击者（会话留在表里、份额涨上去）。跑完即走那一档另有一条
// TestLRU_HitAndRunCannotEvict——它靠的是完全不同的一条规矩（到达者不赢平局），这条
// 用例对它一点也不承重。
func TestLRU_EvictionIsPerOwner(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&askThenAnswer{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(cp), agui.WithMaxSessions(2))

	// A 挂起一个会话
	aIID := findInterruptID(parseSSE(postAs(h, "tenant-A",
		`{"threadId":"a-thread","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if aIID == "" {
		t.Fatal("前提不成立：A 期望 ask_user 中断")
	}
	t.Logf("A 挂起成功, interruptId=%s", aIID)

	// B 连开两个会话，把表撑过上限（maxSessions=2）
	for _, tid := range []string{"b-1", "b-2"} {
		if findInterruptID(parseSSE(postAs(h, "tenant-B",
			`{"threadId":"`+tid+`","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String())) == "" {
			t.Fatalf("前提不成立：B 的 %s 期望 ask_user 中断", tid)
		}
	}
	t.Log("B 开了 2 个会话（maxSessions=2）")

	// A 回来续自己的会话
	ev := parseSSE(postAs(h, "tenant-A",
		`{"threadId":"a-thread","resume":[{"interruptId":"`+aIID+`","status":"resolved","payload":"a"}]}`).Body.String())
	if code := findErrorCode(ev); code == "no_session" {
		t.Fatalf("跨租户 DoS：B 开会话把 A 的挂起会话挤掉了（A resume 返 no_session）；A 侧事件 %v", eventTypes(ev))
	}

	// **正向断言**：淘汰机制本身还得工作——只有否定断言的话，「淘汰整个失效」
	// （比如 maxSessions 被忽略）也能让上面那条绿。B 自己开到第 3 个时，挤掉的
	// 必须是 B 自己最旧的那个。
	bFirst := findInterruptID(parseSSE(postAs(h, "tenant-B",
		`{"threadId":"b-3","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if bFirst == "" {
		t.Fatal("前提不成立：B 的 b-3 期望 ask_user 中断")
	}
	bEv := parseSSE(postAs(h, "tenant-B",
		`{"threadId":"b-1","resume":[{"interruptId":"x","status":"resolved","payload":"a"}]}`).Body.String())
	if code := findErrorCode(bEv); code != "no_session" {
		t.Errorf("配额没生效：B 开到超上限后，它自己最旧的会话应被淘汰（错误码 %q）", code)
	}
}

// askOrAnswer 按 input 分叉：用户文本含 "ask" 就抛 ask_user 中断（会话挂起等人答），
// 否则一轮答完（会话跑完即走）。
//
// **必须由 model 读 input 决定**：AgentFactory 的签名里没有请求，而「跑完即走」是这批
// 用例里攻击者那一侧的**核心形状**——它的 entry 转眼被 removeEntry 掉，份额恒 1。
type askOrAnswer struct{}

func (m *askOrAnswer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			return schema.AssistantMessage("好的"), nil // 续跑那一轮：收尾
		}
	}
	last := ""
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			last = msgs[i].Content
			break
		}
	}
	if strings.Contains(last, "ask") {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc0", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"你的名字?"}`},
		}}), nil
	}
	return schema.AssistantMessage("直接答完"), nil
}

func (m *askOrAnswer) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *askOrAnswer) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// evictHandler 建一个「问或答由请求内容决定」的 handler（owner 从 ctx 取）。
func evictHandler(max int) http.Handler {
	return ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&askOrAnswer{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), agui.WithMaxSessions(max))
}

// suspend 开一个会话并让它停在 ask_user 中断上，返回 interruptId。
func suspend(t *testing.T, h http.Handler, owner, tid string) string {
	t.Helper()
	iid := findInterruptID(parseSSE(postAs(h, owner,
		`{"threadId":"`+tid+`","runId":"r","messages":[{"role":"user","content":"ask"}]}`).Body.String()))
	if iid == "" {
		t.Fatalf("前提不成立：%s/%s 该挂起在 ask_user 上", owner, tid)
	}
	return iid
}

// resumeCode 续跑一个挂起会话，返回错误码（"" = 没报错）。
func resumeCode(h http.Handler, owner, tid, iid string) string {
	return findErrorCode(parseSSE(postAs(h, owner,
		`{"threadId":"`+tid+`","resume":[{"interruptId":"`+iid+`","status":"resolved","payload":"a"}]}`).Body.String()))
}

// TestLRU_HitAndRunCannotEvict 钉住「**跑完即走**的到达者挤不掉在位者」。
//
// 「一个租户狂开只挤掉自己的」只对**积累型**攻击者成立：那种攻击者的会话留在表里，
// 份额涨上去，于是它自己成了最大那个。而**跑完即走**的请求 entry 转眼就被 removeEntry
// 掉，份额恒是 1、跟受害者持平——老代码在平局上回落全局最旧 ⟹ 淘汰全表最旧的那个
// （必是别人，因为刚插入的那条恒 busy、选不中）⟹ **新来的恒胜过在位的**。实测一个
// 立刻跑完的请求就能清掉 A 的挂起会话连同它的 cp，A 续跑拿到 no_session。
func TestLRU_HitAndRunCannotEvict(t *testing.T) {
	h := evictHandler(2)
	aIID := suspend(t, h, "tenant-A", "a-1")
	vIID := suspend(t, h, "tenant-V", "v-1") // 表满（maxSessions=2）

	// 攻击者：一轮答完，entry 随即释放，份额恒 1（跟 A、V 持平）。
	eEv := parseSSE(postAs(h, "tenant-E",
		`{"threadId":"e-1","runId":"r","messages":[{"role":"user","content":"go"}]}`).Body.String())
	if !contains(eventTypes(eEv), "RUN_FINISHED") {
		t.Fatalf("前提不成立：攻击者那一轮该跑完（否则测的是积累型攻击者）；事件 %v", eventTypes(eEv))
	}

	if code := resumeCode(h, "tenant-A", "a-1", aIID); code == "no_session" {
		t.Error("跨租户 DoS：一个跑完即走的请求把 A 的挂起会话挤掉了")
	}
	if code := resumeCode(h, "tenant-V", "v-1", vIID); code == "no_session" {
		t.Error("跨租户 DoS：一个跑完即走的请求把 V 的挂起会话挤掉了")
	}

	// **正向断言**：淘汰机制在同一张表上仍然工作——只有上面两条否定断言的话，
	// 「淘汰整个失效」（比如 maxSessions 被忽略）同样能让它们绿。
	//
	// 上面两次续跑成功后 A、V 的 entry 已释放，重新铺一遍：A 开两个，第三个进来时
	// A 的份额（含在途那条）严格最大 ⟹ 该淘汰 A 自己最旧的 a-2。
	h2 := evictHandler(2)
	a2IID := suspend(t, h2, "tenant-A", "a-2")
	suspend(t, h2, "tenant-V", "v-2")
	suspend(t, h2, "tenant-A", "a-3")
	if code := resumeCode(h2, "tenant-A", "a-2", a2IID); code != "no_session" {
		t.Errorf("配额没生效：A 开到超上限时该淘汰它自己最旧的 a-2（错误码 %q）", code)
	}
}

// TestLRU_OwnerWithNULIsItsOwnPartition 钉住「owner 含 NUL 时份额算在它自己头上」。
//
// 老代码用 `IndexByte(key, 0)` 从拼出来的键里**反推** owner——owner 自己含 NUL 时反推
// 出来的是它的第一段。于是 owner=`acme\x00evil` 的两个会话被算进 `acme` 的份额，
// **受害者 `acme` 成了「份额最大」那个**，挨淘汰的是它。
//
// 淘汰要的「这条 entry 属于谁」是插入那一刻手上就有的一等事实，不该去拆一个复合键反推。
// 现在 owner 是会话键的一个字段（sessionKey），没有可拆的东西了。
//
// 可达性：Go 的 net/http 拒绝 header 值里的 NUL，所以 header 型 resolver 到不了这儿；
// 但 owner 来自 JWT claim / DB 字段时到得了。
func TestLRU_OwnerWithNULIsItsOwnPartition(t *testing.T) {
	h := evictHandler(2)
	vIID := suspend(t, h, "acme", "v-1")
	eIID := suspend(t, h, "acme\x00evil", "e-1")
	suspend(t, h, "acme\x00evil", "e-2") // 第三个：攻击者份额 2 > 受害者 1

	if code := resumeCode(h, "acme", "v-1", vIID); code == "no_session" {
		t.Error("owner 含 NUL 的租户被算进了受害者那一份额，挨淘汰的成了受害者")
	}
	// **正向断言**：该出账的是攻击者自己最旧的那个（否则「谁也没淘汰」也能让上面那条绿）。
	if code := resumeCode(h, "acme\x00evil", "e-1", eIID); code != "no_session" {
		t.Errorf("份额最大的那个租户该淘汰自己最旧的会话（错误码 %q）", code)
	}
}

// TestOwner_NULCannotForgeSessionKey 钉住「客户端拼不出别人的会话键」。
//
// 这条比份额算错严重：owner 和 threadId 拼成一个字符串（`owner + NUL + threadId`）时，
// 只要有人的 owner 是别人 owner 的 NUL 前缀，**它就能用自己合法的身份 + 一个精心构造的
// threadId 拼出对方的键**——threadId 是 JSON 字段、完全由客户端控制。
//
//	受害者 owner=`acme\x00alice` threadId=`t`      → `acme\x00alice\x00t`
//	攻击者 owner=`acme`          threadId=`alice\x00t` → `acme\x00alice\x00t`  ← 同一个
//
// 于是攻击者续到了受害者挂起的会话：读到里面的提问、替对方回答。owner 形如
// `租户\x00用户` 时这就成立，而那正是 resolver 作者会写的形状（本仓 store 层的 bucket
// 键就是 NUL 拼的）。两维各占一个字段之后，「分隔符不出现在任一段里」这个前提不需要了。
func TestOwner_NULCannotForgeSessionKey(t *testing.T) {
	h := evictHandler(10)
	iid := suspend(t, h, "acme\x00alice", "t")

	// 攻击者用**自己合法的 owner** + 构造的 threadId 拼受害者的键。
	// `\u0000` 是 JSON 里 NUL 的转义写法——裸控制字符 JSON 不收，转义则照收不误。
	forged := findErrorCode(parseSSE(postAs(h, "acme",
		`{"threadId":"alice\u0000t","resume":[{"interruptId":"`+iid+`","status":"resolved","payload":"我是攻击者"}]}`).Body.String()))
	if forged != "no_session" {
		t.Errorf("拼串键被伪造：owner=acme 的请求续到了 owner=acme\\x00alice 的挂起会话（错误码 %q）", forged)
	}
	// **正向断言**：真正的主人还续得上（否则「两边都失败」也能让上面那条绿）。
	if code := resumeCode(h, "acme\x00alice", "t", iid); code != "" {
		t.Errorf("真正的主人续不上自己的会话了（错误码 %q）", code)
	}
}
