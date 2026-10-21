// fallback_smoke 演示 FallbackChatModel 故障切换的真 LLM 冒烟（3 场景）。
//
//	场景 1 同链接换模型：主端点故障（不可达）→ 切到 dashscope 同链接的**另一个**模型
//	场景 2 跨链接换 provider：主端点故障 → 切到 deepseek 自己的链接
//	场景 3 主候选健康：真 qwen 直接应答，**不触发切换**（验证 happy-path 透明、不误切）
//
// 主端点故障用不可达端点（http://127.0.0.1:1）模拟——网络错被包成
// model.APIError{StatusCode:0}，DefaultFailoverPolicy 判定该切。对 BasicAgent 完全透明
// （只是 WithModel 注入的 model 换成 fallback.New(...)）。
//
// 运行（需项目根 .env 的 QWEN_API_KEY；场景 2 另需 DEEPSEEK_API_KEY）：
//
//	go run ./example/fallback/fallback_smoke
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/model/openai"
	"crew-go/model/qwen"
	"crew-go/schema"
)

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）")
	}
	hasDeepSeek := os.Getenv("DEEPSEEK_API_KEY") != ""

	// 主候选：指向不可达地址 → 连接拒绝（网络错，StatusCode 0）→ 触发 failover。
	brokenPrimary := func() model.ChatModel {
		return openai.NewClient("dummy-key", "primary-model",
			openai.WithBaseURL("http://127.0.0.1:1"), openai.WithSystem("primary(down)"))
	}

	// 场景 1：主端点故障 → 同链接的另一个模型（期望切换）。
	//
	// **不写死模型名**（qwen.NewTestClient 的 godoc 明写这条）：写死的那个迟早会免费额度
	// 耗尽，届时这条场景以 403 失败，而排障的人看到的是「failover 发生了但候选也挂了」，
	// 得翻 llmtrace 才知道是模型名过期不是 fallback 坏了。这里取候选表里**第二个**——
	// `NewTestClient("")` 用的是第一个，取第二个才真的演示了「换模型」。
	backupName := qwen.TestModelCandidates[0]
	if len(qwen.TestModelCandidates) > 1 {
		backupName = qwen.TestModelCandidates[1]
	}
	ok1 := runScenario(
		fmt.Sprintf("场景 1 · 同链接换模型（主端点故障 → dashscope 内 %s）", backupName), true,
		[]model.ChatModel{brokenPrimary(), qwen.NewTestClient(backupName)},
	)

	// 场景 2：主端点故障 → deepseek 链接（期望切换）
	ok2 := true
	if hasDeepSeek {
		ok2 = runScenario(
			"场景 2 · 跨链接换 provider（主端点故障 → deepseek 链接）", true,
			[]model.ChatModel{brokenPrimary(), deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")},
		)
	} else {
		fmt.Println("\n[跳过] 场景 2 需 DEEPSEEK_API_KEY")
	}

	// 场景 3：主候选健康（真 qwen）→ 直接应答，**不切**（验证透明、不误切）。
	// 备用给另一个真模型，若误切也能看出来；断言 switched==0。
	backup3 := model.ChatModel(qwen.NewTestClient(""))
	if hasDeepSeek {
		backup3 = deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")
	}
	ok3 := runScenario(
		"场景 3 · 主候选健康（真 qwen）→ 直接应答不切", false,
		[]model.ChatModel{qwen.NewTestClient(""), backup3},
	)

	fmt.Println("\n───────────────────────────────────────")
	fmt.Printf("冒烟结果：场景1(切→%s)=%s  场景2(切→deepseek)=%s  场景3(健康不切)=%s\n",
		backupName, passLabel(ok1), passLabel(ok2), passLabel(ok3))
	if !ok1 || !ok3 {
		os.Exit(1)
	}
}

// runScenario 构造 fallback 链 + BasicAgent，跑一条 prompt，打印 failover 事件与应答，
// 并按 expectFailover 校验"是否发生切换"。返回是否符合预期且拿到非空应答。
func runScenario(title string, expectFailover bool, members []model.ChatModel) bool {
	fmt.Println("\n───────────────────────────────────────")
	fmt.Println(" " + title)
	fmt.Println("───────────────────────────────────────")

	switched := 0
	fb := fallback.New(members,
		fallback.WithFallbackHook(func(ctx context.Context, from, to int, err error) {
			switched++
			fmt.Printf("  [failover] 候选 %d → %d，原因: %v\n", from, to, err)
		}),
	)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("fallback_demo"),
		agent_v2.WithModel(fb), // 透明——和注入普通 model 无区别
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用一句话介绍你自己，并说出你是什么模型"))
	if err != nil {
		fmt.Printf("  [失败] Run 错误: %v\n", err)
		return false
	}
	msg := agent_v2.FinalMessage(events)
	if msg == nil || msg.Content == "" {
		fmt.Println("  [失败] 未返回内容")
		return false
	}

	// 校验切换预期
	if expectFailover && switched == 0 {
		fmt.Println("  [失败] 期望主候选故障切换，但未发生切换")
		return false
	}
	if !expectFailover && switched > 0 {
		fmt.Printf("  [失败] 主候选健康不应切换，却切了 %d 次（误切）\n", switched)
		return false
	}
	if !expectFailover {
		fmt.Println("  [✓] 主候选健康，未触发切换（透明直通）")
	}
	fmt.Printf("\n  应答> %s\n", msg.Content)
	return true
}

func passLabel(ok bool) string {
	if ok {
		return "✅PASS"
	}
	return "❌FAIL"
}
