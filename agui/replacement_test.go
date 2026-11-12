package agui_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/tool"
)

type replacementDeleteFailure struct {
	compose_v2.Checkpointer
	lastRoot string
	failTask string
	failures int
}

func (cp *replacementDeleteFailure) SaveNS(ctx context.Context, sid, ns string, value *compose_v2.Checkpoint) error {
	if err := cp.Checkpointer.SaveNS(ctx, sid, ns, value); err != nil {
		return err
	}
	if ns == "" {
		cp.lastRoot = sid
	}
	return nil
}

func (cp *replacementDeleteFailure) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if sid == cp.failTask {
		cp.failures++
		return 0, errors.New("checkpoint storage unavailable")
	}
	return cp.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

func TestHandler_ReplacementDeleteFailureKeepsPendingTask(t *testing.T) {
	cp := &replacementDeleteFailure{Checkpointer: compose_v2.NewInMemoryCheckpointer()}
	var executions atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("write_sample", "写入样本", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			executions.Add(1)
			return tool.Success("已写入"), nil
		}))
	previous := &askThenAnswerModel{}
	replacing := false
	handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		if replacing {
			return agent_v2.NewBasicAgent(
				agent_v2.WithName("replacement"),
				agent_v2.WithHooks(hooks),
				agent_v2.WithModel(&callToolsOnce{names: []string{"write_sample"}}),
				agent_v2.WithToolkit(kit),
			)
		}
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("previous"),
			agent_v2.WithHooks(hooks),
			agent_v2.WithModel(previous),
			agent_v2.WithAskUser(),
		)
	}, agui.WithCheckpointer(cp))

	first := postAGUI(handler, `{"threadId":"replace","runId":"first","messages":[{"role":"user","content":"帮我"}]}`)
	interruptID := findInterruptID(parseSSE(first.Body.String()))
	oldTask := cp.lastRoot
	if interruptID == "" || oldTask == "" {
		t.Fatalf("旧任务应挂起并落档，实际 %s", first.Body)
	}

	cp.failTask = oldTask
	replacing = true
	for i := 1; i <= 2; i++ {
		reply := postAGUI(handler, fmt.Sprintf(`{"threadId":"replace","runId":"replace-%d","messages":[{"role":"user","content":"写入样本"}]}`, i))
		events := parseSSE(reply.Body.String())
		if reply.Code != http.StatusOK || len(events) != 2 ||
			events[0]["type"] != "RUN_STARTED" || events[1]["type"] != "RUN_ERROR" || events[1]["code"] != "agent_error" {
			t.Errorf("第 %d 次替换应在开始后报告清档失败，实际 HTTP %d %s", i, reply.Code, reply.Body)
		}
	}
	if cp.failures != 2 || executions.Load() != 0 {
		t.Errorf("两次清档失败期间工具应执行零次，实际清档失败 %d 次、执行 %d 次", cp.failures, executions.Load())
	}
	if exists, err := cp.ExistsNS(t.Context(), oldTask, ""); err != nil || !exists {
		t.Fatalf("旧存档应保留，exists=%v err=%v", exists, err)
	}

	cp.failTask = ""
	replacing = false
	resumed := resumeEntries(t, handler, "replace", "resume", resolved(interruptID, "张三"))
	if outcomeType(resumed) != "success" || !strings.Contains(fmt.Sprint(resumed), "张三") {
		t.Fatalf("存储恢复后应能用原问题 ID 续跑，实际 %v", eventTypes(resumed))
	}
	if exists, err := cp.ExistsNS(t.Context(), oldTask, ""); err != nil || exists {
		t.Fatalf("旧任务完成后应清档，exists=%v err=%v", exists, err)
	}
}
