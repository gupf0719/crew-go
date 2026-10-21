package a2a_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// TestRemoteAgent_TaskIDIsTaskScoped 钉住「远端 taskID 的键带任务维」。
//
// # 这条守的伤害
//
// A2A 的 Task 是**一次任务**的句柄，而它被存在 `RunSession` 的 values 里——那是**对象级**
// 容器，按 `RunSession.RunID` 的契约会跨任务存活（`NextTask` 刻意保留 values / events）。
// 键不带任务维时：任务 A 的远端停在 input-required（taskID 落库）→ 用户放弃 A →
// 业务按契约 `NextTask()` 开任务 B → B 调同一个远程 agent → 读到 A 的 taskID → **B 的
// 无关内容被远端当成 A 那个提问的回答**，全程 err=nil。
//
// ⚠️ 这个坑**先于契约存在**（老写法复用 sess 一样踩），但契约把「复用 sess 对象」定为
// 推荐写法，于是它从「违约的人才踩」变成「照契约写也踩」——严重性变了，所以必须有哨兵。
//
// 断言选在 **wire 上的 taskID**，不是最终文本：远端此刻可以正常应答，最终文本「看着对」，
// 而错的是这条消息挂到了谁的任务上。
func TestRemoteAgent_TaskIDIsTaskScoped(t *testing.T) {
	var mu sync.Mutex
	var inbound []string // 每次请求 server 侧看到的入站 taskID

	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		mu.Lock()
		inbound = append(inbound, string(execCtx.TaskID))
		mu.Unlock()
		askExec(execCtx, yield) // 恒停在 input-required，制造「可续的远端任务」
	})
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, exec))
	if err != nil {
		t.Fatalf("new remote agent: %v", err)
	}

	sess := agent_v2.NewRunSession()
	ctx := context.Background()

	// 任务 A：远端进入 input-required ⟹ taskID 落进 session KV
	if _, err := ra.Run(ctx, sess, schema.UserMessage("确认转账 5000？")); err != nil {
		t.Fatalf("任务 A 该正常返回追问: %v", err)
	}
	mu.Lock()
	afterA := len(inbound)
	mu.Unlock()
	if afterA != 1 {
		t.Fatalf("fixture 没搭对：任务 A 该发出 1 次请求，实际 %d", afterA)
	}

	// 用户放弃 A，业务按契约开新任务
	sess.NextTask()

	// 任务 B：同一个远程 agent、完全无关的内容
	if _, err := ra.Run(ctx, sess, schema.UserMessage("查天气")); err != nil {
		t.Fatalf("任务 B 该正常返回追问: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(inbound) != 2 {
		t.Fatalf("fixture 没搭对：该有 2 次入站请求，实际 %d（%v）", len(inbound), inbound)
	}
	// **承重**：B 必须是一个全新的远端任务，不能续在 A 的 taskID 上。
	if inbound[1] != "" && inbound[1] == inbound[0] {
		t.Fatalf("任务 B 带着任务 A 的 taskID 上了 wire（%q）——"+
			"远端会把「查天气」当成「确认转账 5000？」的回答。\n"+
			"  taskID 是任务级句柄，键必须带任务维（sess.RunID）；"+
			"contextID 才是会话级、跨 NextTask 保留", inbound[1])
	}
}

// TestRemoteAgent_TaskIDSurvivesWithinSameTask 是上一条的**对偶**，防止「把键改窄」
// 修过头：同一个任务内的续跑必须仍读得到 taskID，否则 input-required 的多轮补充信息
// 每轮都开一个新远端任务，HITL 续跑静默失效。
//
// 形态取自 `example/a2a/hitl_smoke`：顶层同一个 sess **不换 RunID** 再 Run 一次补信息
// （`RemoteAgent.Resume` 不支持 standalone，同任务续跑就是这么写的）。
func TestRemoteAgent_TaskIDSurvivesWithinSameTask(t *testing.T) {
	var mu sync.Mutex
	var inbound []string

	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		mu.Lock()
		inbound = append(inbound, string(execCtx.TaskID))
		mu.Unlock()
		askExec(execCtx, yield)
	})
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, exec))
	if err != nil {
		t.Fatalf("new remote agent: %v", err)
	}

	sess := agent_v2.NewRunSession()
	ctx := context.Background()
	if _, err := ra.Run(ctx, sess, schema.UserMessage("订个会议室")); err != nil {
		t.Fatalf("第一轮: %v", err)
	}
	// **不调 NextTask**——这仍是同一个任务，只是在补远端要的信息
	if _, err := ra.Run(ctx, sess, schema.UserMessage("下午三点")); err != nil {
		t.Fatalf("同任务续跑: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(inbound) != 2 {
		t.Fatalf("fixture 没搭对：该有 2 次入站，实际 %d", len(inbound))
	}
	if inbound[0] == "" {
		t.Fatal("fixture 没搭对：第一轮该由 server 分配 taskID（入站为空是正常的，" +
			"但那样第二轮就无从对比）——检查 askExec 是否真的建了 task")
	}
	// **承重的正向断言**：第二轮必须带着第一轮那个 taskID。
	if inbound[1] != inbound[0] {
		t.Fatalf("同一个任务的第二轮没续上远端 task：第一轮 %q，第二轮 %q。\n"+
			"  键带任务维是对的，但任务维必须是 RunID（Run→Resume→同任务再 Run 都不变），"+
			"不能是每次调用都变的东西", inbound[0], inbound[1])
	}
}
