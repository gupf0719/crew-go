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
	"net/http/httptest"
	"sync"
)

// ownedHandler 是测试用的 agui.Handler 包装——预置一个固定 owner resolver。
//
// 生产必配（见 [agui.WithOwnerResolver]），测试里 23 个调用点各写一遍闭包只是噪音。
// 想验证「不配就 panic」这条本身，看 TestHandler_OwnerResolverRequired。
func ownedHandler(build agui.AgentFactory, opts ...agui.Option) http.Handler {
	return agui.Handler(build, append([]agui.Option{
		agui.WithOwnerResolver(func(context.Context) string { return "test-owner" }),
	}, opts...)...)
}

// TestHandler_OwnerResolverRequired 钉住「不配 owner resolver 就不让跑」。
//
// AG-UI 的 threadId 由**客户端**生成、任何人都能递——服务端连「这个 id 是我发出去的」
// 这个前提都没有。不带 owner 维时谁猜中 threadId 谁就能续别人的挂起会话：读到里面的
// 提问、替对方回答、继承已转交的控制权。
//
// 选「装配期 panic」而不是「默认不分区」，是为了跟会话历史层保持一致——`history.go`
// 早一步立过同一条规矩（owner 必填、不给 panic）。同一个仓库里两种默认会让业务猜。
func TestHandler_OwnerResolverRequired(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("没配 WithOwnerResolver 应在构造期 panic")
		}
		msg, _ := r.(string)
		// 错误信息要给出**两条出路**（单租户怎么办 / 多租户怎么办），只说「必配」
		// 会让业务卡在这儿。
		for _, want := range []string{"WithOwnerResolver", "单租户", "多租户"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic 信息缺 %q，出路给不全：%s", want, msg)
			}
		}
	}()
	_ = agui.Handler(func(hook.Hooks) agent_v2.Agent { return nil })
}

// testOwnerKey 模拟认证中间件放进 request ctx 的主体。
type testOwnerKey struct{}

// ownerAwareHandler 的 resolver 从 ctx 取主体——**这就是生产该有的形状**：
// 身份来自认证中间件放进 ctx 的东西，不是客户端能填的字段。
func ownerAwareHandler(build agui.AgentFactory, opts ...agui.Option) http.Handler {
	return agui.Handler(build, append([]agui.Option{
		agui.WithOwnerResolver(func(ctx context.Context) string {
			o, _ := ctx.Value(testOwnerKey{}).(string)
			return o
		}),
	}, opts...)...)
}

func postAs(h http.Handler, owner, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), testOwnerKey{}, owner))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestHandler_OwnerIsolatesSuspendedSessions 钉住：一个租户**够不着**另一个租户的挂起会话，
// 即使它递上完全正确的 threadId。
//
// 这是 owner 维要挡的那件事的实质——不是「报错」而是「够不着」。AG-UI 的 threadId 由
// 客户端生成、会出现在前端代码里，猜中或抓包看到都不难；没有 owner 维时，拿着它发一个
// resume 就能读到别人挂起的提问、替对方回答。
//
// ⚠️ 断言落在**B 是否拿到了 A 的会话**，不落在「有没有报错」——B 读不到时框架会说
// `no_session`，那正是我们要的；但如果 owner 维失效，B 会**成功续跑 A 的会话**且没有
// 任何错误。所以还要正向断言 A 自己续得上（否则「两边都失败」也能让用例绿）。
func TestHandler_OwnerIsolatesSuspendedSessions(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"),
			agent_v2.WithModel(&askThenAnswer{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(cp))

	const tid = "thread-known-to-both"
	// 租户 A 跑一次 → ask_user 中断，挂起会话落在 A 的分区。
	iid := findInterruptID(parseSSE(postAs(h, "tenant-A",
		`{"threadId":"`+tid+`","runId":"r1","messages":[{"role":"user","content":"hi"}]}`).Body.String()))
	if iid == "" {
		t.Fatal("前提不成立：期望 ask_user 中断")
	}
	resumeBody := `{"threadId":"` + tid + `","resume":[{"interruptId":"` + iid + `","status":"resolved","payload":"answer"}]}`

	// 租户 B 递同一个 threadId + 同一个 interruptId → 应当**够不着**。
	bEvents := parseSSE(postAs(h, "tenant-B", resumeBody).Body.String())
	if code := findErrorCode(bEvents); code != "no_session" {
		t.Errorf("租户 B 用同一个 threadId 续到了租户 A 的挂起会话（错误码 %q，应为 no_session）——"+
			"会话键没有 owner 维", code)
	}

	// 正向：租户 A 自己续得上。**没有这一条，「两边都失败」也能让上面那条绿。**
	aEvents := parseSSE(postAs(h, "tenant-A", resumeBody).Body.String())
	if code := findErrorCode(aEvents); code != "" {
		t.Errorf("租户 A 续自己的会话失败了（错误码 %q）——owner 维把合法续接也挡了", code)
	}
}

// findErrorCode 取 RUN_ERROR 的 code；没有 RUN_ERROR 返空串。
func findErrorCode(events []map[string]any) string {
	for _, ev := range events {
		if ev["type"] != "RUN_ERROR" {
			continue
		}
		if c, ok := ev["code"].(string); ok {
			return c
		}
	}
	return ""
}

// askThenAnswer：第 1 次吐 ask_user 工具调用，之后直接答。
type askThenAnswer struct {
	mu sync.Mutex
	n  int
}

func (m *askThenAnswer) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if m.n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc0", Type: "function",
			Function: schema.FunctionCall{
				Name: agent_v2.AskUserToolName, Arguments: `{"question":"你的名字?"}`},
		}}), nil
	}
	return schema.AssistantMessage("好的"), nil
}

func (m *askThenAnswer) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *askThenAnswer) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
