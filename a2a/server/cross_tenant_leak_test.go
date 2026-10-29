package server_test

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"

	"crew-go/a2a/server"
	"crew-go/agent_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

type probeWhoKey struct{}

// TestCrossTenantLeak 全链路（真 HTTP + 真 SDK 客户端）钉住两条跨租户读路径都堵死。
//
// **路径 1 `message/send` 带别人的 taskId**：SDK 收到非空 taskId 就去 taskStore.Get
// 查、查到就把 StoredTask 塞进 ExecutorContext 交给我们。我们的挂起表按
// (owner, taskID) 分区，拒绝时只回一句与「task 不存在」一致的话。
//
// **路径 2 `tasks/get` 直接读**：这条**整条不经过 AgentExecutor**——handler 直接
// taskStore.Get 后返回完整 Task 含 History。当时 SDK 的 store 只在 Create 记 owner、
// List 时过滤，**Get / Update 不做归属检查**；业务换的后端仍可能不做，所以由 ownerScopedTaskStore 补。
//
// ⚠️ 修这条时踩到的时序坑：owner 一开始只在 Executor.Execute 里 push，而 task store 的
// Create **早于** Execute、tasks/get 更是完全不经过它 ⟹ 记的归属恒空、校验两边都空
// ⟹ 放行 ⟹ 泄漏原样发生。owner 必须在 CallInterceptor.Before push（唯一能覆盖整条
// 调用链 ctx 的位置）。
func TestCrossTenantLeak(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("asker"),
		agent_v2.WithModel(&askModel{questions: []string{"你的密码是什么"}}),
		agent_v2.WithAskUser())
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	base := "http://" + ln.Addr().String()
	// resolver 从 ctx 取——但 HTTP 侧 ctx 是 request ctx，我们用 header 模拟认证中间件
	h := server.Handler(ag, base, server.WithOwnerResolver(func(ctx context.Context) string {
		o, _ := ctx.Value(probeWhoKey{}).(string)
		return o
	}))
	// 中间件：把 X-Tenant 塞进 ctx（模拟认证中间件的产物）
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		who := r.Header.Get("X-Tenant")
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), probeWhoKey{}, who)))
	})
	go func() { _ = http.Serve(ln, wrapped) }()
	defer ln.Close()
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(base + "/.well-known/agent-card.json"); err == nil {
			resp.Body.Close()
			break
		}
	}

	mkClient := func(tenant string) *a2aclient.Client {
		c, err := a2aclient.NewFromCard(context.Background(), server.Card(ag, base),
			a2aclient.WithJSONRPCTransport(&http.Client{Transport: &tenantRT{tenant: tenant}}))
		if err != nil {
			t.Fatalf("client(%s): %v", tenant, err)
		}
		return c
	}

	// A 发起 → ask_user 中断，task 挂起（含 A 的问题内容）
	ctx := context.Background()
	ca := mkClient("tenant-A")
	var taskID a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("A send: %v", e)
		}
		if v, ok := ev.(*a2atypes.Task); ok {
			taskID = v.ID
		}
		if v, ok := ev.(*a2atypes.TaskStatusUpdateEvent); ok {
			taskID = v.TaskID
		}
	}
	t.Logf("A 的 taskID = %s", taskID)
	if taskID == "" {
		t.Fatal("没拿到 taskID")
	}

	// B 拿着 A 的 taskId 续发
	cb := mkClient("tenant-B")
	bmsg := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
		a2atypes.TaskInfo{TaskID: taskID}, a2atypes.NewTextPart("B 试图接手"))
	var bSaw []string
	bRejected := false
	for ev, e := range cb.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: bmsg}) {
		if e != nil {
			bRejected = true
			t.Logf("B send err（拒绝也算）: %v", e)
			break
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			for _, m := range v.History {
				txt, _ := partsText(m.Parts)
				bSaw = append(bSaw, txt)
			}
			bSaw = append(bSaw, "STATE:"+string(v.Status.State))
		case *a2atypes.TaskStatusUpdateEvent:
			if v.Status.Message != nil {
				txt, _ := partsText(v.Status.Message.Parts)
				bSaw = append(bSaw, txt)
			}
			bSaw = append(bSaw, "STATE:"+string(v.Status.State))
		}
	}
	all := strings.Join(bSaw, " | ")
	t.Logf("[路径1 message/send] B 看到的内容: %s", all)
	if strings.Contains(all, "A 的机密请求") || strings.Contains(all, "你的密码是什么") {
		t.Errorf("!!! 泄漏：B 经 message/send 看到了 A 的 task 内容")
	}
	// ⚠️ **上面那条否定断言在「B 什么也没收到」时恒真**——而 B 被 SDK 在进 Executor
	// 之前就拒掉时，收到的正是空。所以必须再钉一条：B 确实**走到了拒绝**，不是这条
	// 用例在对空串做 Contains。
	if !bRejected && all == "" {
		t.Error("B 既没被拒也没收到任何事件——这条用例在空转，测不出泄漏")
	}

	// 路径 2：B 直接调 tasks/get —— 这条**完全绕过我们的 Executor**，SDK handler
	// 直接查 taskStore（那个 Get 不做归属检查）后返回完整 task 含 History。
	// **先钉正向断言：A 自己读得到，且内容里确有那句机密。**
	//
	// 没有这一条的话，把 Get 改成「对所有人拒」（功能整个坏掉）也能让下面那条否定
	// 断言绿——实测过，那正是这条用例一开始的状态：三条断言全是否定式，在空转状态上
	// 同时为真。
	aGot, aErr := ca.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID})
	if aErr != nil {
		t.Fatalf("A 读自己的 task 失败了：%v——归属检查把合法方也挡了", aErr)
	}
	aHist := ""
	for _, m := range aGot.History {
		txt, _ := partsText(m.Parts)
		aHist += txt + " "
	}
	if !strings.Contains(aHist, "A 的机密请求") {
		t.Fatalf("前提不成立：A 自己读到的历史里没有那句机密（%q）——"+
			"下面那条「B 读不到」就测不出什么了", aHist)
	}

	got, err := cb.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID})
	if err != nil {
		t.Logf("[路径2 tasks/get] B 被拒: %v", err)
		return
	}
	var hist []string
	for _, m := range got.History {
		txt, _ := partsText(m.Parts)
		hist = append(hist, txt)
	}
	if got.Status.Message != nil {
		txt, _ := partsText(got.Status.Message.Parts)
		hist = append(hist, txt)
	}
	h2 := strings.Join(hist, " | ")
	t.Logf("[路径2 tasks/get] B 拿到 task state=%s history=%q", got.Status.State, h2)
	if strings.Contains(h2, "A 的机密请求") || strings.Contains(h2, "你的密码是什么") {
		t.Errorf("!!! 泄漏：B 经 tasks/get 读到了 A 的 task 历史")
	}
}

type tenantRT struct{ tenant string }

func (rt *tenantRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set("X-Tenant", rt.tenant)
	return http.DefaultTransport.RoundTrip(r)
}

func partsText(parts a2atypes.ContentParts) (string, bool) {
	var sb strings.Builder
	for _, p := range parts {
		if p != nil {
			sb.WriteString(p.Text())
		}
	}
	return sb.String(), sb.Len() > 0
}
