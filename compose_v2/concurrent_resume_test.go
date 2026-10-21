//go:build concurrentprobe

// 探针：两个副本并发 Resume 同一 sessionID 会发生什么。
//
// # 为什么用 build tag 隔离
//
// 它**不是回归测试**——框架当前对这个场景没有任何防护，所以它只是把现状钉下来
// 让结论可复现，不是"跑绿就说明修好了"。默认 `go test ./...` 不编译它，避免被
// 当成"已覆盖"的信号。
//
//	go test -tags concurrentprobe ./compose_v2/ -run TestProbe_ConcurrentResume -v
//
// # 它证明什么
//
// cp 层无版本号 / 无锁 / 无 CAS（`Checkpointer` 接口签名即可看出），两个副本各自
// 从同一份 cp 载入、各自执行、各自写回：**有副作用的节点跑两次，且两边都返回
// err=nil**。`-race` 是干净的——这是逻辑双重执行，不是数据竞争，race detector
// 永远抓不到。
//
// 三方现状与方案取舍见 doc/description/three-way-comparison/checkpoint-storage.md
// 的「同一会话并发执行」一节。
package compose_v2_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestProbe_ConcurrentResume(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("probe")
		b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, ok := compose_v2.GetResumeAnswer(ctx, "__probe_q__"); !ok {
				return nil, &compose_v2.InterruptError{
					Reason: "probe",
					Query:  &compose_v2.AskQuery{InterruptID: "__probe_q__", Question: "?"},
				}
			}
			return nil, nil
		})
		// 有副作用的业务节点——现实里是发邮件 / 扣款 / 调外部 API
		b.AddNode("work", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ranWork.Add(1)
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", "work")
		b.AddEdge("work", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("probe_sid"))); !ok {
		t.Fatal("首跑应中断在 ask")
	}

	// 两个副本拿到同一份用户答案，各自 Resume——屏障逼真正并发
	ans := map[string]*schema.Message{"__probe_q__": schema.UserMessage("go")}
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("probe_sid"),
				compose_v2.WithResumeAnswers(ans))
		}(i)
	}
	close(start)
	wg.Wait()

	var okCount int
	for _, e := range errs {
		if e == nil {
			okCount++
		}
	}
	t.Logf("副本A err=%v", errs[0])
	t.Logf("副本B err=%v", errs[1])
	t.Logf("有副作用的 work 节点执行了 %d 次（单副本时应为 1）", ranWork.Load())
	t.Logf("报告成功的副本数 = %d", okCount)

	// 这里**断言现状**而不是断言"正确行为"——防护一旦落地，本探针会失败，
	// 那正是提醒回来更新它的信号。
	if ranWork.Load() != 2 || okCount != 2 {
		t.Errorf("现状已变（work=%d 次、%d 个副本报成功）——若这是因为加了并发防护，"+
			"请更新本探针；若不是，说明结论漂了，需要重新调查", ranWork.Load(), okCount)
	}
}
