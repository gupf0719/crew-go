package observability

// 每种退出形态映射到的 `gen_ai.status`，逐条钉死。
//
// # 为什么整表钉
//
// 这张表决定业务在监控平台上看到的**错误率**。多一种形态被归进 `error`，错误率就虚高
// 一截，而排查的人根本不会想到"这些 error 里混着用户点的停止按钮"。
//
// 实测抓到的那条：`ErrSoftCanceled` 归到了 `error`/`BusinessError`。它跟 ctx 取消是
// **同一件事**（业务主动喊停），只是优雅版——当前超步跑完再停 + 存档。区别在于它
// **不动 ctx**，而映射函数的取消分支判的是 `ctx.Err()`，所以永远看不到它。同样是停，
// 硬停记 `cancelled`、软停记 `error`，而"停止"恰恰是高频用户操作。

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
)

func TestMapErrToStatus_AllExitShapes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		cancelCtx bool // 是否真取消 ctx（ctx 取消那条路径判的是 ctx.Err() 不是 err）
		status    string
		errType   string
		why       string
	}{
		{
			name: "软取消", err: fmt.Errorf("stopped: %w", compose_v2.ErrSoftCanceled),
			status: "cancelled", errType: "SoftCanceled",
			why: "业务主动喊停（优雅版），跟 ctx 取消同一件事；归 error 会让停止按钮污染错误率",
		},
		{
			name: "ctx 硬取消", err: context.Canceled, cancelCtx: true,
			status: "cancelled", errType: "Canceled",
			why: "调用方要停",
		},
		{
			name: "interrupt", err: &compose_v2.InterruptError{
				Reason: "ask", Query: &compose_v2.AskQuery{InterruptID: "q", Question: "?"},
			},
			status: "blocked", errType: "",
			why: "在等人答，不是失败——blocked 分支**刻意不设 error.type**，" +
				"导出的是一条零错误信息的绿 span（这是 by design，见 OnAgentEnd 注释）",
		},
		{
			name: "普通业务错", err: errors.New("model api 503"),
			status: "error", errType: "BusinessError",
			why: "真失败，该进错误率",
		},
		{
			name: "终局失败", err: fmt.Errorf("cond: %w", compose_v2.ErrRunAborted),
			status: "error", errType: "BusinessError",
			why: "确实是失败（上游数据非法），跟软取消不同——它不是用户意图",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, addr, stop := startCaptureServer(t)
			defer stop()
			obs := newTestObs(t, addr)
			sid := "status_" + tc.name
			base, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()

			h := obs.AgentHooks()
			info := &hook.AgentInfo{Name: "a", Type: "base"}
			ctx, _ := h.OnAgentStart(compose_v2.WithSessionIDCtx(base, sid), info, &hook.AgentInput{})
			if tc.cancelCtx {
				cancelFn()
			}
			h.OnAgentEnd(agent_v2.WithEndError(ctx, tc.err), info, &hook.AgentOutput{})
			obs.CleanupSession(sid)
			obs.Close()
			time.Sleep(300 * time.Millisecond)

			sp := findSpanByOperation(srv.Spans(), "agent")
			if sp == nil {
				t.Fatal("应有 agent span")
			}
			if got := findAttrString(sp, "gen_ai.status"); got != tc.status {
				t.Errorf("gen_ai.status = %q, want %q\n  %s", got, tc.status, tc.why)
			}
			if got := findAttrString(sp, "error.type"); got != tc.errType {
				t.Errorf("error.type = %q, want %q", got, tc.errType)
			}
		})
	}
}
