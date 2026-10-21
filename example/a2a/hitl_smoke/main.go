// a2a_hitl_smoke 冒烟 RemoteAgent 的 Task 状态机 / input-required HITL / taskID 续跑 /
// 流式 / WithHumanGate 真人 HITL 闭环——helloworld 只盖 Message 路径，这些核心路径必须真跑
// （CLAUDE.md："build 通过≠能用"）。
//
// 自动拉起 example/a2a/rich_server 子进程，跑完 4 场景后 kill：
//
//	cd <crew-go 根目录>
//	go run ./example/a2a/hitl_smoke
//
// 4 场景：
//   - A：Task 状态机——单轮 submitted→completed（"task done: ..."）
//   - B：input-required 默认（返追问让 host 自动补）+ taskID 续跑（同 session 再发命中 server StoredTask）
//   - C：流式 artifact 聚合（"chunk-A chunk-B"）
//   - D：WithHumanGate 真人 HITL——嵌入 Sequential 骑其 cp，Run 抛 InterruptError → Resume 续跑
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

const base = "http://127.0.0.1:19091"

func main() {
	// 先 build server 二进制再直接 exec——绕开 `go run` 包装层（它把编译出的 server 放进
	// 新进程组，杀 go run 父进程留孤儿占端口）。直接 exec 时 srv.Process.Kill() 直击 server。
	serverBin := filepath.Join(os.TempDir(), "a2a_rich_server_smoke")
	build := exec.Command("go", "build", "-o", serverBin, "./example/a2a/rich_server")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		log.Fatalf("build rich server: %v", err)
	}
	defer func() { _ = os.Remove(serverBin) }()

	srv := exec.Command(serverBin, "-port", "19091")
	srv.Stdout = os.Stderr
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		log.Fatalf("启动 rich server: %v", err)
	}
	defer func() { _ = srv.Process.Kill() }()

	waitReady(base + "/.well-known/agent-card.json")
	ctx := context.Background()

	scenarioA(ctx)
	scenarioB(ctx)
	scenarioC(ctx)
	scenarioD(ctx)

	fmt.Println("\n=== A2A HITL 冒烟全 PASS（Task 状态机 / input-required 默认 / taskID 续跑 / 流式 / WithHumanGate HITL 闭环）===")
}

// waitReady 轮询 AgentCard 端点直到 server 就绪（go run 含编译延迟）。
func waitReady(url string) {
	for i := 0; i < 150; i++ {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Printf("✓ rich server 就绪 (%s)\n\n", url)
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("rich server 未就绪: %s", url)
}

// 场景 A：Task 状态机——单轮 submitted → completed。
func scenarioA(ctx context.Context) {
	ra := mustAgent(ctx, false)
	final := mustRun(ra, agent_v2.NewRunSession(), "complete now")
	expect("A/Task 完成", final, "task done: complete now")
}

// 场景 B：input-required 默认（返追问）+ taskID 续跑——同 session 再发，RemoteAgent
// loadIDs 拿上轮存的 taskID → server StoredTask 命中 → "got it: blue"。
func scenarioB(ctx context.Context) {
	ra := mustAgent(ctx, false) // 不开 humanGate
	sess := agent_v2.NewRunSession()

	first := mustRun(ra, sess, "ask me")
	if !strings.Contains(first.Content, "input_required") || !strings.Contains(first.Content, "Which color") {
		log.Fatalf("B/input-required 默认：期望追问，得到 %q", first.Content)
	}
	fmt.Printf("✓ B/input-required 默认返追问: %q\n", first.Content)

	second := mustRun(ra, sess, "blue") // 同 session → 续同一远程 task
	expect("B/taskID 续跑", second, "got it: blue")
}

// 场景 C：流式 artifact 聚合。
func scenarioC(ctx context.Context) {
	ra := mustAgent(ctx, false)
	final := mustRun(ra, agent_v2.NewRunSession(), "stream please")
	expect("C/流式聚合", final, "chunk-A chunk-B")
}

// 场景 D：WithHumanGate 真人 HITL——嵌入 Sequential 骑其 cp。Run 时远程 input-required →
// 抛 InterruptError → Sequential cp 存挂起态；Resume（WithResumeSession 让 taskID 经 session
// 存活 + WithResumeUserInputs 配对答复）重跑节点 → GetResumeAnswer 拿 "blue" + 续旧 task。
// "got it: blue" 同时验证：taskID 续跑 + stableID 跨 Run/Resume 稳定 + GetResumeAnswer 命中。
func scenarioD(ctx context.Context) {
	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHumanGate())
	if err != nil {
		log.Fatalf("D/NewRemoteAgent(humanGate): %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("hitl_seq")).AddStep("remote", ra)

	cp := compose_v2.NewInMemoryCheckpointer()
	hctx := compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()

	_, err = seq.Run(hctx, sess, schema.UserMessage("ask me"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		log.Fatalf("D/humanGate：期望 InterruptError，得到 err=%v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		log.Fatalf("D/humanGate：InterruptError 缺 Query/InterruptID: %+v", ie)
	}
	fmt.Printf("✓ D/WithHumanGate Run 抛中断: q=%q id=%q\n", ie.Query.Question, ie.Query.InterruptID)

	events, err := seq.Resume(hctx, sess.RunID,
		agent_v2.WithResumeSession(sess), // 同 session 让 taskID（session KV）跨 Resume 存活
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("blue"),
		}))
	if err != nil {
		log.Fatalf("D/Resume: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatal("D/Resume 无 final message")
	}
	expect("D/WithHumanGate HITL 续跑", final, "got it: blue")
}

// ---- helpers ----

func mustAgent(ctx context.Context, humanGate bool) *a2a.RemoteAgent {
	var opts []a2a.RemoteOption
	if humanGate {
		opts = append(opts, a2a.WithHumanGate())
	}
	ra, err := a2a.NewRemoteAgent(ctx, base, opts...)
	if err != nil {
		log.Fatalf("NewRemoteAgent: %v", err)
	}
	return ra
}

func mustRun(ra *a2a.RemoteAgent, sess *agent_v2.RunSession, text string) *schema.Message {
	events, err := ra.Run(context.Background(), sess, schema.UserMessage(text))
	if err != nil {
		log.Fatalf("Run(%q): %v", text, err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatalf("Run(%q): 无 final message", text)
	}
	return final
}

func expect(label string, msg *schema.Message, want string) {
	if msg.Content != want {
		log.Fatalf("%s：期望 %q，得到 %q", label, want, msg.Content)
	}
	fmt.Printf("✓ %s: %q\n", label, msg.Content)
}
