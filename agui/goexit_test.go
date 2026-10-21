package agui_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// TestServeRun_GoexitStillSettles 钉住：`ag.Run` 里发生 `runtime.Goexit` 时，前端仍拿到
// 终态事件、cp 不漏。
//
// Goexit 的来源很现实：业务 hook 里调了 testify 的 `require.*`、或 zap 的
// `WithFatalHook(zapcore.WriteThenGoexit)`。它**跳过所有后续普通语句**，而
// `ServeHTTP` 的兜底用 `recover()`——**Goexit 下 recover 返 nil**，那道兜底整个不触发。
//
// 修之前实测：事件序列停在 `RUN_STARTED`、客户端 `unexpected EOF`（违反 AG-UI 生命周期
// 契约），且每次漏一份**没有回收路径**的 cp——`storeInterrupt` 没跑 ⟹ 空壳 entry 的
// `sess == nil` ⟹ LRU 淘汰它时取不到 sid、连 cp 一起清不掉。
//
// a2a 那侧结构上免疫（收尾跑在另一条 goroutine），agui 是 inline 调用所以要显式补。
func TestServeRun_GoexitStillSettles(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"),
			agent_v2.WithModel(&goexitModel{}), agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(2))
	}, agui.WithCheckpointer(cp))

	// **必须跑在独立 goroutine 上**：`runtime.Goexit` 会终止当前 goroutine，在测试主
	// goroutine 上直接调 ServeHTTP 会让 test runner 崩（"test executed panic(nil) or
	// runtime.Goexit"）。真实部署里 handler 跑在 net/http 的连接 goroutine 上，用真
	// server 才是那个形状。
	srv := httptest.NewServer(h)
	defer srv.Close()
	post := func(body string) []map[string]any {
		resp, err := http.Post(srv.URL, "application/json", strings.NewReader(body))
		if err != nil {
			return nil // 连接被掐断（正是 Goexit 未收尾时的现象）
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return parseSSE(string(b))
	}
	ev := post(`{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"hi"}]}`)
	types := eventTypes(ev)

	// 1) 生命周期必须闭合——只有 RUN_STARTED 就是半截流。
	hasTerminal := false
	for _, ty := range types {
		if ty == "RUN_ERROR" || ty == "RUN_FINISHED" {
			hasTerminal = true
		}
	}
	if !hasTerminal {
		t.Errorf("Goexit 之后没有终态事件，前端收到半截流：%v", types)
	}

	// 2) cp 不能漏——那份档谁也够不着（Status=failed 列不出来、sess==nil 让 LRU 清不了）。
	//
	// sid 是服务端铸的，测试里拿不到；改从「后续同 threadId 的 run 能否正常跑」侧面验：
	// 空壳 entry 若没清掉，它占着那个会话键且 sess==nil。
	ev2 := post(`{"threadId":"t1","runId":"r2","messages":[{"role":"user","content":"再来"}]}`)
	if code := findErrorCode(ev2); code == "busy" {
		t.Error("Goexit 留下的空壳 entry 没清——同 threadId 的后续请求被判成 busy")
	}
}

// goexitModel：Generate 里直接 Goexit（模拟业务 hook 里的 fatal）。
type goexitModel struct{}

func (m *goexitModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	runtime.Goexit()
	return nil, nil
}

func (m *goexitModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	runtime.Goexit()
	return nil, nil
}

func (m *goexitModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
