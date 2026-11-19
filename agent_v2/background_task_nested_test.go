package agent_v2_test

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

func TestBackgroundTask_NestedSiblingCannotReadOrCancel(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	job := newGatedTool("FIRST-SLOT-RESULT")
	tk := tool.NewToolkit()
	job.register(tk, "build")

	var id string
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`),
		func(in []*schema.Message) (*schema.Message, error) {
			id = lastTaskID(in)
			if id == "" {
				return nil, fmt.Errorf("第一个成员未收到后台句柄")
			}
			return schema.AssistantMessage("开始构建"), nil
		},
		func([]*schema.Message) (*schema.Message, error) {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "list", Type: "function", Function: schema.FunctionCall{
					Name: agent_v2.BackgroundTaskToolName, Arguments: `{"action":"list"}`}},
				{ID: "cancel", Type: "function", Function: schema.FunctionCall{
					Name: agent_v2.BackgroundTaskToolName, Arguments: fmt.Sprintf(`{"action":"cancel","id":%q}`, id)}},
			}), nil
		},
	}}
	child := bgAgent("worker", m, tk, bgConfig(mgr))
	parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("start", child).AddStep("other", child)
	ctx, cancel := context.WithTimeout(compose_v2.WithOwner(compose_v2.WithThreadIDCtx(t.Context(), "same-thread"), "alice"), 3*time.Second)
	defer cancel()
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("两个位置")); err != nil {
		t.Fatal(err)
	}
	list, rejected := "", ""
	for _, msg := range m.input(3) {
		switch schema.GetToolResultID(msg) {
		case "list":
			list = msg.Content
		case "cancel":
			rejected = msg.Content
		}
	}
	if list == "" || strings.Contains(list, id) || !strings.Contains(rejected, "[not_found]") {
		t.Fatalf("兄弟位置不能查询或取消首个任务，列表 %q，取消结果 %q", list, rejected)
	}
	if task, ok := mgr.Get(id); !ok || task.Status != agent_v2.BackgroundTaskRunning || job.cancelled.Load() {
		t.Fatalf("父任务退出后后台工具应继续，由宿主按句柄读回，现为 %q", task.Status)
	}
	job.open()
	got := waitTerminal(t, mgr, id)
	if got.Status != agent_v2.BackgroundTaskCompleted || got.Result.Output != "FIRST-SLOT-RESULT" || job.runs.Load() != 1 {
		t.Fatalf("宿主读取的结局不符，任务 %+v，工具运行 %d 次", got, job.runs.Load())
	}

	top := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep(agent_v2.BackgroundTaskToolName, fmt.Sprintf(`{"action":"status","id":%q}`, id)),
	}}
	if _, err := bgAgent("worker", top, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新任务")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputText(top.input(0)), "FIRST-SLOT-RESULT") || !strings.Contains(inputText(top.input(1)), "[not_found]") {
		t.Fatal("同名顶层 Agent 读取到了子调用的后台任务")
	}
}

func reopenBackgroundCheckpoints(t *testing.T, ctx context.Context, cp *compose_v2.InMemoryCheckpointer, runID string) *compose_v2.InMemoryCheckpointer {
	t.Helper()
	archives, err := cp.LoadAllInSession(ctx, runID)
	if err != nil || len(archives) != 2 {
		t.Fatalf("应有父、子两份存档，实际 %d 份，错误 %v", len(archives), err)
	}
	reopened := compose_v2.NewInMemoryCheckpointer()
	for _, archive := range archives {
		wire, err := compose_v2.MarshalCheckpoint(archive.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		saved, err := compose_v2.UnmarshalCheckpoint(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.SaveNS(ctx, runID, archive.NS, saved); err != nil {
			t.Fatal(err)
		}
	}
	return reopened
}

func TestBackgroundTask_NestedResumeRecoversAsyncResult(t *testing.T) {
	for _, cold := range []bool{false, true} {
		name := "保留原管理器"
		if cold {
			name = "从存档重建且管理器丢失"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(compose_v2.WithOwner(compose_v2.WithThreadIDCtx(t.Context(), "resume-thread"), "alice"), 4*time.Second)
			defer cancel()
			ctx = compose_v2.WithDurabilityCtx(ctx, compose_v2.DurabilitySync)
			finished := make(chan agent_v2.BackgroundTask, 1)
			mgr := agent_v2.NewBackgroundTaskManager(&agent_v2.BackgroundTaskManagerConfig{
				OnDone: func(task agent_v2.BackgroundTask) { finished <- task },
			})
			closeMgr(t, mgr)
			release := make(chan struct{})
			var runs atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("build", "生成构建截图", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				runs.Add(1)
				select {
				case <-release:
					return &tool.Result{Success: true, Output: "BUILD-SCREENSHOT", Parts: []schema.ToolResultPart{
						{Text: "BUILD-SCREENSHOT", MediaType: "image/png", Data: "QUJD"},
					}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}))
			parentWithChild := func(parentModel *bgScriptModel, child *agent_v2.BasicAgent) *agent_v2.BasicAgent {
				tools := tool.NewToolkit()
				tools.Register(agent_v2.AgentAsTool(child, agent_v2.WithAgentToolDescription("执行构建")))
				return agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(parentModel), agent_v2.WithToolkit(tools))
			}
			childModel := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
				func(in []*schema.Message) (*schema.Message, error) {
					if !strings.Contains(inputText(in), "构建") {
						return nil, fmt.Errorf("子模型未收到 AgentAsTool 的输入")
					}
					return schema.AssistantToolCallMessage([]schema.ToolCall{
						{ID: "b1", Type: "function", Function: schema.FunctionCall{Name: "build", Arguments: `{}`}},
						{ID: "a1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"继续构建？"}`}},
					}), nil
				},
			}}
			parent := parentWithChild(&bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("worker", `{"input":"构建"}`)}},
				bgAgent("worker", childModel, tk, bgConfig(mgr), agent_v2.WithAskUser()))
			cp := compose_v2.NewInMemoryCheckpointer()
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(compose_v2.WithCheckpointerCtx(ctx, cp), sess, schema.UserMessage("通过子 Agent 构建"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok || len(ie.AllQueries()) != 1 {
				t.Fatalf("子调用应在慢工具已交出句柄后等待答案，实际 %v", err)
			}
			close(release)
			var task agent_v2.BackgroundTask
			select {
			case task = <-finished:
			case <-ctx.Done():
				t.Fatal("没有进入进程内后台的工具结局")
			}
			if task.TaskID != sess.RunID || task.Status != agent_v2.BackgroundTaskCompleted {
				t.Fatalf("首次后台执行归属或结局错误 %+v", task)
			}

			reopened := reopenBackgroundCheckpoints(t, ctx, cp, sess.RunID)
			afterStatus := callStep(agent_v2.AskUserToolName, `{"question":"已经拿到截图，现在发布吗？"}`)
			if cold {
				mgr = agent_v2.NewBackgroundTaskManager(nil)
				closeMgr(t, mgr)
				afterStatus = sayStep("构建已处理")
			}

			resumedModel := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
				bgActionStep("status", ""),
				afterStatus,
			}}
			parent = parentWithChild(&bgScriptModel{}, bgAgent("worker", resumedModel, tk, bgConfig(mgr), agent_v2.WithAskUser()))
			_, err = parent.Resume(compose_v2.WithCheckpointerCtx(ctx, reopened), sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")}))
			if cold && err != nil {
				t.Fatal(err)
			}
			handles, notices := 0, 0
			for _, msg := range resumedModel.input(0) {
				if msg.Annotations[agent_v2.BackgroundTaskAnnotation] == task.ID {
					handles++
				}
				if msg.Annotations[agent_v2.BackgroundTaskNotificationAnnotation] == task.ID && strings.Contains(msg.Content, "BUILD-SCREENSHOT") {
					notices++
				}
			}
			if handles != 1 || runs.Load() != 1 {
				t.Fatalf("工具账应恢复原句柄且不重跑，观察到 %d 个句柄、%d 次运行", handles, runs.Load())
			}

			in := resumedModel.input(1)
			if len(in) == 0 {
				t.Fatal("续跑后的模型未收到查询结果")
			}
			last := in[len(in)-1]
			if cold {
				if notices != 0 || !strings.Contains(last.Content, "[not_found]") {
					t.Fatalf("管理器丢失时应保留句柄并报告找不到，通知 %d 条，查询 %q", notices, last.Content)
				}
				return
			}
			want := schema.ToolResultPart{Text: "BUILD-SCREENSHOT", MediaType: "image/png", Data: "QUJD"}
			if len(last.ContentBlocks) == 0 {
				t.Fatal("续跑查回的工具消息没有内容块")
			}
			result := last.ContentBlocks[0].Toolresult
			if notices != 1 || result == nil || len(result.Parts) != 2 || result.Parts[1] != want {
				t.Fatalf("同一子调用应收到完成通知且可取回附件，通知 %d 条，查询结果 %+v", notices, result)
			}
			nextPause, ok := compose_v2.IsInterrupt(err)
			if !ok || len(nextPause.AllQueries()) != 1 {
				t.Fatalf("查询结果提交后应再次挂起，实际 %v", err)
			}
			reopened = reopenBackgroundCheckpoints(t, ctx, reopened, sess.RunID)
			mgr = agent_v2.NewBackgroundTaskManager(nil)
			closeMgr(t, mgr)
			finalModel := &bgScriptModel{}
			parent = parentWithChild(&bgScriptModel{}, bgAgent("worker", finalModel, tk, bgConfig(mgr), agent_v2.WithAskUser()))
			if _, err := parent.Resume(compose_v2.WithCheckpointerCtx(ctx, reopened), sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{nextPause.AllQueries()[0].InterruptID: schema.UserMessage("发布")})); err != nil {
				t.Fatal(err)
			}
			restored := 0
			for _, msg := range finalModel.input(0) {
				for _, block := range msg.ContentBlocks {
					if block.Toolresult != nil && slices.Contains(block.Toolresult.Parts, want) {
						restored++
					}
				}
			}
			if restored != 1 || runs.Load() != 1 {
				t.Fatalf("新管理器应从存档恢复已提交的附件，实际 %d 个结果、%d 次执行", restored, runs.Load())
			}

			// 配一个更新的查询结果，让真实的嵌套终态落到 MicroCompact 的卸载候选中。
			st := store.NewInMemoryStore()
			microCtx := store.WithContext(compose_v2.WithSessionIDCtx(ctx, sess.RunID), st)
			micro := &agent_v2.MicroCompactConfig{
				Enabled: true, KeepRecent: 1, OffloadEnabled: true,
				CompactableTools: []string{agent_v2.BackgroundTaskToolName}, Placeholder: "已压缩",
			}
			newer := schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "newer", Type: "function", Function: schema.FunctionCall{
					Name: agent_v2.BackgroundTaskToolName, Arguments: `{"action":"list"}`}},
			})
			withNewer := append(append([]*schema.Message(nil), in...), newer, schema.ToolResultMessage("newer", "较新的结果"))
			shortened := agent_v2.MicroCompactRun(microCtx, withNewer, micro)
			items, err := st.Search(microCtx, micro.OffloadNamespace(microCtx))
			if err != nil || len(items) != 0 || shortened[len(in)-1].Content != "已压缩" {
				t.Fatalf("只应压缩嵌套终态，不应卸载到共用分区，entries=%d err=%v", len(items), err)
			}
		})
	}
}

type pauseAfterBackgroundWait struct {
	hook.NoOpHooks
	sawResult bool
	redact    bool
}

func (h *pauseAfterBackgroundWait) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out.Result == nil || !out.Result.Success || !strings.Contains(out.Result.Output, "WAIT-TO-COMMIT") {
		return ctx, nil
	}
	h.sawResult = true
	if h.redact {
		out.Result = &schema.ToolCallResult{ErrorCode: "redacted", Error: "工具结局已屏蔽"}
		return ctx, nil
	}
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "这批结果先暂停"})
	return ctx, err
}

func TestBackgroundTask_UncommittedWaitLeavesNotificationPending(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	job := newGatedTool("WAIT-TO-COMMIT")
	tk := tool.NewToolkit()
	job.register(tk, "build")
	h := &pauseAfterBackgroundWait{}
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`),
		func(in []*schema.Message) (*schema.Message, error) {
			job.open()
			return bgActionStep("wait", "")(in)
		},
	}}
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "wait-commit")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := bgAgent("worker", m, tk, bgConfig(mgr), agent_v2.WithHooks(h)).Run(ctx, sess, schema.UserMessage("先等结果"))
	if _, ok := compose_v2.IsInterrupt(err); !ok || !h.sawResult {
		t.Fatalf("应在读到终态之后、提交工具结果之前中断，实际 %v", err)
	}
	next := &bgScriptModel{}
	_, err = bgAgent("worker", next, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("下一轮"),
		agent_v2.WithAbandonedTasks(sess.RunID))
	if err != nil {
		t.Fatal(err)
	}
	notices := 0
	for _, msg := range next.input(0) {
		if agent_v2.IsBackgroundTaskNotification(msg) && strings.Contains(msg.Content, "WAIT-TO-COMMIT") {
			notices++
		}
	}
	if notices != 1 || job.runs.Load() != 1 {
		t.Fatalf("未提交的 wait 不应消耗通知，实际 %d 条通知、%d 次工具运行", notices, job.runs.Load())
	}
}

func TestBackgroundTask_WaitRewriteDoesNotRevealOriginal(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	job := newGatedTool("WAIT-TO-COMMIT")
	tk := tool.NewToolkit()
	job.register(tk, "build")
	h := &pauseAfterBackgroundWait{redact: true}
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`),
		func(in []*schema.Message) (*schema.Message, error) {
			job.open()
			return bgActionStep("wait", "")(in)
		},
	}}
	ctx := compose_v2.WithThreadIDCtx(t.Context(), "wait-redaction")
	if _, err := bgAgent("worker", m, tk, bgConfig(mgr), agent_v2.WithHooks(h)).
		Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("等构建")); err != nil {
		t.Fatal(err)
	}
	in := inputText(m.input(2))
	if !h.sawResult || !strings.Contains(in, "[redacted]") || strings.Contains(in, "WAIT-TO-COMMIT") || job.runs.Load() != 1 {
		t.Fatalf("提交的改写应挡住原始终态的后续自动通知，模型输入 %q", in)
	}
}

func TestBackgroundTask_NestedRetryCannotClaimHistoryHandle(t *testing.T) {
	done := make(chan agent_v2.BackgroundTask, 1)
	mgr := agent_v2.NewBackgroundTaskManager(&agent_v2.BackgroundTaskManagerConfig{OnDone: func(task agent_v2.BackgroundTask) {
		done <- task
	}})
	closeMgr(t, mgr)
	job := newGatedTool("REJECTED-ATTEMPT-RESULT")
	tk := tool.NewToolkit()
	job.register(tk, "build")
	ctx, cancel := context.WithTimeout(compose_v2.WithThreadIDCtx(t.Context(), "retry-thread"), 4*time.Second)
	defer cancel()

	var id string
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`),
		func(in []*schema.Message) (*schema.Message, error) {
			id = lastTaskID(in)
			if id == "" {
				return nil, fmt.Errorf("首次尝试未收到已归档的句柄")
			}
			job.open()
			select {
			case <-done:
				return nil, fmt.Errorf("模型暂时不可用")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		func(in []*schema.Message) (*schema.Message, error) {
			if !strings.Contains(inputText(in), id) {
				return nil, fmt.Errorf("重试未从会话历史拿到首个尝试的展示句柄")
			}
			return callStep(agent_v2.BackgroundTaskToolName, fmt.Sprintf(`{"action":"status","id":%q}`, id))(in)
		},
	}}
	child := bgAgent("worker", m, tk, bgConfig(mgr), agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "owner"))
	parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(1))).
		AddStep("retry", child)
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	if in := inputText(m.input(3)); !strings.Contains(in, "[not_found]") || strings.Contains(in, "REJECTED-ATTEMPT-RESULT") || job.runs.Load() != 1 {
		t.Fatalf("历史展示不得授权新尝试读取旧进程任务，模型输入 %q", in)
	}
}

func TestBackgroundTask_NestedToolExitReachesTerminal(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	tk := tool.NewToolkit()
	release := make(chan struct{})
	tk.Register(tool.NewFuncTool("build", "执行构建", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		select {
		case <-release:
			runtime.Goexit()
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	}))
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	child := bgAgent("worker", m, tk, bgConfig(mgr))
	parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("one", child)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m.input(1))
	if id == "" {
		t.Fatal("子调用没有交出后台句柄")
	}
	close(release)
	got := waitTerminal(t, mgr, id)
	if got.Status != agent_v2.BackgroundTaskFailed || got.Err == nil {
		t.Fatalf("工具协程提前退出后应记录失败终态，实际 %+v", got)
	}
}

type switchBackgroundOwner struct{ hook.NoOpHooks }

func (switchBackgroundOwner) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	return compose_v2.WithOwner(ctx, "bob"), nil
}

func TestBackgroundTask_NestedOwnerOverrideRunsSynchronously(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("build", "执行构建", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		if compose_v2.GetOwner(ctx) != "bob" || agent_v2.GetSession(ctx) == nil {
			return nil, fmt.Errorf("换主体的工具应在新主体的同步上下文执行")
		}
		return tool.Success("BOB-SYNC-RESULT"), nil
	}))
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	child := bgAgent("worker", m, tk, bgConfig(mgr), agent_v2.WithHooks(switchBackgroundOwner{}))
	parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("one", child)
	ctx := compose_v2.WithOwner(t.Context(), "alice")
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputText(m.input(1)), "BOB-SYNC-RESULT") {
		t.Fatal("模型未收到换主体后的同步结果")
	}
}

func TestBackgroundTask_NestedMiddlewareCannotChangeEligibleTool(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	tk := tool.NewToolkit()
	var calls atomic.Int32
	for _, name := range []string{"build", "sync_only"} {
		tk.Register(tool.NewFuncTool(name, "执行工具", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
			calls.Add(1)
			return tool.Success("实际执行"), nil
		}))
	}
	rewrite := agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, _ string, args map[string]any) (*tool.Result, error) {
			return next(ctx, "sync_only", args)
		}
	})
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	child := bgAgent("worker", m, tk, bgConfig(mgr), rewrite)
	parent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("one", child)
	if _, err := parent.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	if in := inputText(m.input(1)); calls.Load() != 0 || !strings.Contains(in, "[invalid_arguments]") {
		t.Fatalf("未重新准入的工具不应执行，调用 %d 次，模型输入 %q", calls.Load(), in)
	}
}
