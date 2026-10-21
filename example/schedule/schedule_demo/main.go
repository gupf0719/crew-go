// schedule_demo 演示 schedule 包的本机定时任务调度（与 agent 解耦的独立叶子包）——
// Every（固定间隔）/ Cron（crontab 表达式）/ Once（一次性）三种触发；handler 里干什么
// 随意（本包不 care），这里演示纯打印 + 跑一个 mock BasicAgent（「定时驱动 agent」）。
//
// 自包含，跑约 6 秒看几次触发后自动优雅停:
//
//	go run ./example/schedule/schedule_demo
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schedule"
	"crew-go/schema"
)

func main() {
	sched := schedule.NewLocal(
		schedule.WithErrorHandler(func(name string, err error) {
			log.Printf("[job error] %s: %v", name, err)
		}),
	)

	var beats atomic.Int64

	// ① Every：每 2 秒——纯 handler（拉指标 / 算数据 / 调接口都行）
	must(sched.Every("heartbeat", 2*time.Second, schedule.Simple(func() {
		fmt.Printf("  [%s] ① Every  heartbeat #%d\n", ts(), beats.Add(1))
	})))

	// ② Cron：crontab 表达式（每天 08:30 发日报）——本 demo 6 秒窗口内不触发，仅示范表达式合法
	must(sched.Cron("daily-report", "30 8 * * *", "Asia/Shanghai", schedule.Simple(func() {
		fmt.Printf("  [%s] ② Cron   生成日报\n", ts())
	})))

	// ③ Once：3 秒后一次——handler 里跑一个 mock BasicAgent（agent 与 schedule 解耦，随意组合）
	must(sched.Once("run-agent", time.Now().Add(3*time.Second), func(ctx context.Context) error {
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("scheduled_bot"),
			agent_v2.WithModel(&echoModel{}),
			agent_v2.WithSystemPrompt("你是定时任务里跑的助手。"),
		)
		events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("定时触发，报个到"))
		if err != nil {
			return err
		}
		if msg := agent_v2.FinalMessage(events); msg != nil {
			fmt.Printf("  [%s] ③ Once   scheduled agent> %s\n", ts(), msg.Content)
		}
		return nil
	}))

	fmt.Println("=== 调度器启动：Every 2s heartbeat + Cron 每天 08:30 日报 + Once 3s 跑 agent ===")
	// Start 非阻塞（每任务一 goroutine 循环触发），ctx 超时 ≈ 触发 Stop
	ctx, cancel := context.WithTimeout(context.Background(), 6500*time.Millisecond)
	defer cancel()
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}

	<-ctx.Done() // 等 ~6.5 秒
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	_ = sched.Stop(stopCtx) // 优雅停：等在跑的 handler 收尾
	fmt.Println("=== 调度器已优雅停止 ===")
}

func ts() string { return time.Now().Format("15:04:05") }

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// echoModel 是 mock LLM（不调真 key）。
type echoModel struct{}

func (echoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return echoModel{}, nil }

func (echoModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("[mock] 已报到 ✓"), nil
}

func (m echoModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, _ := m.Generate(ctx, msgs, opts...)
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}
