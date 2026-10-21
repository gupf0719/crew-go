package agui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// 「按 owner 分摊淘汰」买到了跨租户隔离，代价是**上限从硬变软**（WithMaxSessions 的
// godoc 写着这一条）。代价有多大不能靠算，这一组用真 HTTP 把三个量测出来钉住：
//
//   - 单租户部署会不会因此少几个可用会话？（不会——一个分区时它恒是最大那个，行为
//     跟全局 LRU 一模一样）
//   - 一个攻击者能把表撑多大？（**每个身份恰好多占一格**，跟它发多少请求无关——这是
//     软上限可接受的**全部**理由）
//   - 表到底会长到多大？（≈ 有挂起会话的租户数）
//
// 白盒是因为要数 h.sessions 的行数——「表有多大」正是这组要钉的量，从 HTTP 那侧看不见。

type capModel struct{}

func (m *capModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			return schema.AssistantMessage("ok"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc0", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"q?"}`},
	}}), nil
}

func (m *capModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *capModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type capOwnerKey struct{}

func capHandler(max int) *handler {
	return Handler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&capModel{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	},
		WithOwnerResolver(func(ctx context.Context) string {
			o, _ := ctx.Value(capOwnerKey{}).(string)
			return o
		}),
		WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		WithMaxSessions(max),
	).(*handler)
}

// openSuspended 让 owner 开一个会话并停在 ask_user 中断上。
func (h *handler) openSuspended(t *testing.T, owner, tid string) {
	t.Helper()
	body := `{"threadId":"` + tid + `","runId":"r","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), capOwnerKey{}, owner))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), "interrupt") {
		t.Fatalf("前提不成立：%s/%s 该挂起在 ask_user 上，实得 %s", owner, tid, rec.Body.String())
	}
}

func (h *handler) countByOwner() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := make(map[string]int, len(h.sessions))
	for k := range h.sessions {
		m[k.owner]++
	}
	return m
}

// TestCapacity_SingleTenantUnaffected：单租户部署（resolver 返固定值）**一个可用会话都
// 没少**——按 owner 分摊在这儿退化成全局 LRU。
//
// 这是「会不会为了隔离把大家的容量都砍了」那一问的答案。写成硬相等而不是 `<=`：`<=`
// 在「表被清空了」上同样成立，那正好是这条要防的退化。
func TestCapacity_SingleTenantUnaffected(t *testing.T) {
	const max, rounds = 5, 50
	h := capHandler(max)
	for i := 0; i < rounds; i++ {
		h.openSuspended(t, "solo", fmt.Sprintf("t%d", i))
	}
	if got := len(h.sessions); got != max {
		t.Errorf("单租户开 %d 个会话（上限 %d）后表里 %d 条，期望恰好 %d——"+
			"多了说明上限没生效，少了说明按 owner 分摊把单租户的容量砍了", rounds, max, got, max)
	}
}

// TestCapacity_OneIdentityBuysOneSlot：**一个身份恰好多占一格**，跟它发多少请求无关。
//
// 这条是「上限变软」可接受的全部理由：软上限的增量挂在**身份数**上，而身份来自已认证
// 凭据（多一格 = 多一个账号），不挂在请求数上（那才是攻击者能随手放大的量）。它同时
// 反过来钉住「不淘汰」那条兜底没被写成「谁也淘汰不了」——攻击者的 200 个请求里，
// 后 199 个都必须淘汰掉它自己前一个会话。
func TestCapacity_OneIdentityBuysOneSlot(t *testing.T) {
	const others, floods = 10, 200
	h := capHandler(2)
	for i := 0; i < others; i++ {
		h.openSuspended(t, fmt.Sprintf("owner%d", i), "t")
	}
	before := len(h.sessions)
	for i := 0; i < floods; i++ {
		h.openSuspended(t, "attacker", fmt.Sprintf("a%d", i))
	}

	if got := len(h.sessions); got != before+1 {
		t.Errorf("攻击者发了 %d 个请求，表从 %d 涨到 %d，期望只涨 1 格——"+
			"涨得更多说明一个身份能按请求数撑表", floods, before, got)
	}
	if got := h.countByOwner()["attacker"]; got != 1 {
		t.Errorf("攻击者占了 %d 条，期望 1 条", got)
	}
	// 正向：铺底那些租户一条没少（不是「表没涨是因为大家都被清了」）。
	for i := 0; i < others; i++ {
		owner := fmt.Sprintf("owner%d", i)
		if h.countByOwner()[owner] != 1 {
			t.Errorf("铺底租户 %s 的会话被攻击者挤掉了", owner)
		}
	}
}

// TestCapacity_SoftLimitGrowsWithTenants 把**代价本身**钉下来：表会长到「有挂起会话的
// 租户数」那么大，而不是停在 maxSessions。
//
// 这条不是在庆祝一个缺陷，是在防**两个方向**的静默变化：
//   - 有人把淘汰改回全局 LRU（表会停在 maxSessions）⟹ 跨租户 DoS 悄悄回来；
//   - 有人改成「满了就拒绝新请求」⟹ 这里会 fatal 在「该挂起却没挂起」上。
//
// 两种都是行为契约的改变，该走一次明确的决策，不该由一次重构顺手带过。
func TestCapacity_SoftLimitGrowsWithTenants(t *testing.T) {
	const tenants = 50
	h := capHandler(2)
	for i := 0; i < tenants; i++ {
		h.openSuspended(t, fmt.Sprintf("owner%d", i), "t")
	}
	if got := len(h.sessions); got != tenants {
		t.Errorf("%d 个租户各挂 1 个会话（上限 2）后表里 %d 条，期望 %d——"+
			"少了说明有人的会话被别人挤掉了（跨租户 DoS 回来了）", tenants, got, tenants)
	}
}
