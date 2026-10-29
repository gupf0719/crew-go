package server_test

// 默认装配下匿名调用方（owner resolver 返空）与 owner 正好叫 "default" 的租户互不可见，连条数也不能漏。曾经内层后端按
// CallContext 的 User.Name 分区，拦截器给匿名调用方回落成 "default" ⟹ 两边撞进同一个分区；包装层信任这个后端按 owner
// 分区、原样透传它的 TotalSize ⟹ 匿名 ListTasks 拿到 tasks=0、total=3（审计实测，升级前就有）。

import (
	"context"
	"testing"

	"crew-go/agent_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

func TestCrossTenant_AnonymousAndDefaultOwnerAreSeparatePartitions(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("asker"),
		agent_v2.WithModel(&askModel{questions: []string{"q?"}}), agent_v2.WithAskUser())
	_, mk := probeServer(t, ag)
	ctx := context.Background()

	cd := mk("default")
	for i := 0; i < 3; i++ {
		msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("secret"))
		for _, e := range cd.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if e != nil {
				t.Fatalf("send: %v", e)
			}
		}
	}
	own, err := cd.ListTasks(ctx, &a2atypes.ListTasksRequest{})
	if err != nil || len(own.Tasks) != 3 {
		t.Fatalf("fixture 没搭对：owner=default 应看到自己的 3 个任务，实际 %v err=%v", own, err)
	}

	anon, err := mk("").ListTasks(ctx, &a2atypes.ListTasksRequest{})
	if err != nil {
		t.Fatalf("匿名 ListTasks：%v", err)
	}
	if len(anon.Tasks) != 0 || anon.TotalSize != 0 {
		t.Fatalf("匿名调用方不该看到 owner=default 的任务，连条数也不该：tasks=%d total=%d", len(anon.Tasks), anon.TotalSize)
	}
}
