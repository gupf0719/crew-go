package agent_v2_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

type bgPanicStore struct{ store.Store }

func (bgPanicStore) Put(context.Context, store.Namespace, string, any, ...store.PutOption) error {
	panic("store backend panic")
}

// 后台任务结束时的渲染（转存走业务 store）在后台 goroutine 里、没有 recover——同步执行时同一个 panic 由
// runOne 兜住转成错误，后台执行时整个进程崩掉。子进程跑场景，父进程断言子进程正常退出。
func TestBackgroundTask_RenderPanicDoesNotCrashProcess(t *testing.T) {
	if os.Getenv("BG_RENDER_PANIC_CHILD") == "1" {
		mgr := agent_v2.NewBackgroundTaskManager(nil)
		release := make(chan struct{})
		big := strings.Repeat("x", 64*1024)
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("build", "慢工具", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				<-release
				return tool.Success(big), nil
			}))
		spill := agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1024,
			Store: bgPanicStore{store.NewInMemoryStore()}})
		m1 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("已开始")}}
		ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-crash")
		if _, err := bgAgent("coder", m1, tk, bgConfig(mgr), spill).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
			t.Fatal(err)
		}
		if lastTaskID(m1.input(1)) == "" {
			t.Fatal("fixture 没搭对：没转入后台")
		}
		close(release)
		time.Sleep(300 * time.Millisecond)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestBackgroundTask_RenderPanicDoesNotCrashProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), "BG_RENDER_PANIC_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		s := string(out)
		if i := strings.Index(s, "panic:"); i >= 0 {
			s = s[i:]
			if len(s) > 1200 {
				s = s[:1200]
			}
		}
		t.Fatalf("子进程（场景）异常退出：%v\n%s", err, s)
	}
}
