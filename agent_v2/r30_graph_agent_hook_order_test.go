package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// onAgentHookOrderCounter — 测 OnAgentStart 报错时 OnAgentEnd 是否仍 fire 用.
// pre-R30-C-H1-fix GraphAgent 状态: defer 在 OnAgentStart 之前注册 → 报错时 defer 仍
// fire → OnAgentEnd 计数 1 (bug); post-fix: defer 在 OnAgentStart 成功后注册 → 报错
// 时 defer 未注册 → OnAgentEnd 计数 0 (correct).
type onAgentHookOrderCounter struct {
	hook.NoOpHooks
	startCount   atomic.Int32
	endCount     atomic.Int32
	failOnStart  bool
	errorCount   atomic.Int32
	startMessage string
}

func (h *onAgentHookOrderCounter) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.startCount.Add(1)
	if h.failOnStart {
		return ctx, errors.New(h.startMessage)
	}
	return ctx, nil
}

func (h *onAgentHookOrderCounter) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.endCount.Add(1)
	return ctx
}

func (h *onAgentHookOrderCounter) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	h.errorCount.Add(1)
	return ctx
}

// buildSimpleGraphAgent 构造一个最简 GraphAgent (1 node echo) 用于 hook 顺序测试.
func buildSimpleGraphAgent(t *testing.T, hooks hook.Hooks) *agent_v2.GraphAgent {
	t.Helper()
	inputCh := compose_v2.LastValue[*schema.Message]("__r30_test_input__")
	outputCh := compose_v2.LastValue[*schema.Message]("__r30_test_output__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)

	bld.AddNode("echo", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, schema.AssistantMessage(in.Content+" done"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "echo")
	bld.AddEdge("echo", compose_v2.EndNode)

	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("r30_test_graph"),
		agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)
}

// TestR30_C_H1_GraphAgent_Run_OnAgentStartErr_NoOnAgentEndFire — R30 audit C-H1 fix.
//
// **bug 历史**: GraphAgent.Run defer OnAgentEnd 注册在 OnAgentStart fire 之前 — 跟其他
// 8 编排 (BasicAgent / Sequential / Parallel / Loop / Routing / Reflect / Handoff / Plan)
// 顺序相反. OnAgentStart 报错时 defer 仍 fire → OTel span Start 未发 End 单飞 → 配对
// 断裂; 业务监控以为 agent 跑了实际没启动.
//
// **跟 fef2ca5 commit PlanAgent fix 同款 bug**: PlanAgent.Run 已修, GraphAgent 漏修
// (协议变更 "修一个漏一个" R32 复证模式).
//
// **fix**: 调换 defer 注册顺序 — 移到 OnAgentStart 成功**之后** (跟 plan_agent.go OnAgentStart
// fire/defer 注册 + 其他 8 编排 architecture 对齐, R30 84025d9 修后 9 编排全对).
//
// **sanity-revert**: 把 graph_agent.go Run 路径的 defer OnAgentEnd 块移回到 OnAgentStart
// 之前 → 本测试 fail "OnAgentEnd count = 1, want 0 (start failed → no End)".
func TestR30_C_H1_GraphAgent_Run_OnAgentStartErr_NoOnAgentEndFire(t *testing.T) {
	counter := &onAgentHookOrderCounter{failOnStart: true, startMessage: "biz reject"}
	ga := buildSimpleGraphAgent(t, counter)

	_, err := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("expected err from OnAgentStart, got nil")
	}
	if !strings.Contains(err.Error(), "OnAgentStart") || !strings.Contains(err.Error(), "biz reject") {
		t.Errorf("err 应含 'OnAgentStart' + 'biz reject', got %q", err)
	}

	startGot := counter.startCount.Load()
	endGot := counter.endCount.Load()

	if startGot != 1 {
		t.Errorf("OnAgentStart 应 fire 1 次, got %d", startGot)
	}
	if endGot != 0 {
		t.Errorf("R30 C-H1 fix: OnAgentStart 报错时 OnAgentEnd 应 fire 0 次 (defer 未注册), got %d\n"+
			"sanity-revert: 把 graph_agent.go Run defer OnAgentEnd 块移回 OnAgentStart 之前 → got = 1",
			endGot)
	}

	// OnAgentStart 报错时 fire OnError (非 Interrupt 路径)
	if counter.errorCount.Load() != 1 {
		t.Errorf("OnError 应 fire 1 次 (OnAgentStart err 非 InterruptError 路径), got %d",
			counter.errorCount.Load())
	}
}

// TestR30_C_H1_GraphAgent_Run_OnAgentStartOk_OnAgentEndFires — 正常路径 sanity check:
// OnAgentStart 成功时 OnAgentEnd 应 fire 1 次 (跟 ISSUE-82 单一所有权对称).
func TestR30_C_H1_GraphAgent_Run_OnAgentStartOk_OnAgentEndFires(t *testing.T) {
	counter := &onAgentHookOrderCounter{failOnStart: false}
	ga := buildSimpleGraphAgent(t, counter)

	_, err := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("expected success, got err: %v", err)
	}

	if counter.startCount.Load() != 1 {
		t.Errorf("OnAgentStart 应 fire 1 次, got %d", counter.startCount.Load())
	}
	if counter.endCount.Load() != 1 {
		t.Errorf("OnAgentStart 成功时 OnAgentEnd 应 fire 1 次, got %d", counter.endCount.Load())
	}
	if counter.errorCount.Load() != 0 {
		t.Errorf("正常路径 OnError 应 fire 0 次, got %d", counter.errorCount.Load())
	}
}

// buildInterruptingGraphAgent 构造抛 InterruptError 的 GraphAgent 用于 Resume 测试.
// 第一次 Run 抛 Interrupt 让 cp 保存, 之后 Resume 走 OnAgentStart 报错验 hook 顺序.
func buildInterruptingGraphAgent(t *testing.T, hooks hook.Hooks) *agent_v2.GraphAgent {
	t.Helper()
	inputCh := compose_v2.LastValue[*schema.Message]("__r30_int_input__")
	outputCh := compose_v2.LastValue[*schema.Message]("__r30_int_output__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)

	const askID = "r30_resume_int_id"
	bld.AddNode("interrupt_node", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 节点拿到 Resume answer 时正常完成; 否则抛 Interrupt.
		if msg, ok := compose_v2.GetResumeAnswer(ctx, askID); ok && msg != nil {
			return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, schema.AssistantMessage("resumed"))}, nil
		}
		return nil, &compose_v2.InterruptError{
			Reason: "r30 resume test interrupt",
			Query:  &compose_v2.AskQuery{InterruptID: askID, Question: "approve?"},
		}
	})
	bld.AddEdge(compose_v2.StartNode, "interrupt_node")
	bld.AddEdge("interrupt_node", compose_v2.EndNode)

	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("r30_resume_test_graph"),
		agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)
}

// TestR30_C_H1_GraphAgent_Resume_OnAgentStartErr_NoOnAgentEndFire — R30 audit C-H1
// Resume 路径同款 fix (84025d9 同时修 Run + Resume 两路径, 但原 regression test 只测 Run).
//
// **bug 历史**: GraphAgent.Resume defer OnAgentEnd 注册在 OnAgentStart fire 之前 — 跟
// Run 路径同款 + 跟其他 8 编排架构相反. OnAgentStart 报错时 defer 仍 fire → OTel span
// Start 未发 End 单飞 → 配对断裂.
//
// **R31 audit 视角 1 H-3 = 视角 3 MED-1 finding**: 原 regression test 漏 Resume 路径
// 覆盖 — sanity-revert Resume defer 位置时无测试 fail. 跟 "0aa15bf PlanWorkerFn 3
// worker 只改 1" 同款"测试只覆盖 N/M" 陷阱.
//
// **sanity-revert**: 把 graph_agent.go Resume 路径 defer OnAgentEnd 块 (line 304+) 移回
// OnAgentStart 之前 (pre-fix 位置) → 本测试 fail "Resume OnAgentEnd 累计应仍 1, got 2".
func TestR30_C_H1_GraphAgent_Resume_OnAgentStartErr_NoOnAgentEndFire(t *testing.T) {
	counter := &onAgentHookOrderCounter{failOnStart: false}
	ga := buildInterruptingGraphAgent(t, counter)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Phase 1: Run 抛 Interrupt → cp 写, defer fire OnAgentEnd (HITL Resume 闭环算 1)
	_, err := ga.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 应抛 InterruptError, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("应返 InterruptError, got %v", err)
	}

	runStart := counter.startCount.Load()
	runEnd := counter.endCount.Load()
	if runStart != 1 {
		t.Fatalf("Phase 1 Run OnAgentStart 应 fire 1 次, got %d", runStart)
	}
	if runEnd != 1 {
		t.Fatalf("Phase 1 Run OnAgentEnd 应 fire 1 次 (Interrupt 退出 defer 兜底), got %d", runEnd)
	}

	// Phase 2: 切 failOnStart=true 让 Resume 的 OnAgentStart 报错
	counter.failOnStart = true
	counter.startMessage = "resume biz reject"

	_, resumeErr := ga.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("yes"),
		}),
	)
	if resumeErr == nil {
		t.Fatalf("Resume 应抛 err from OnAgentStart, got nil")
	}
	if !strings.Contains(resumeErr.Error(), "OnAgentStart") || !strings.Contains(resumeErr.Error(), "resume biz reject") {
		t.Errorf("Resume err 应含 'OnAgentStart' + 'resume biz reject', got %q", resumeErr)
	}

	// Phase 2 关键断言:
	// - OnAgentStart 累计 2 (Phase 1 Run + Phase 2 Resume 各 1)
	// - OnAgentEnd 累计仍 1 (Phase 1 Run defer fire; Phase 2 Resume OnAgentStart 报错时
	//   defer 未注册 → 不 fire) — post-fix 行为
	// - pre-fix 行为: Phase 2 Resume defer 在 OnAgentStart 之前注册 → 报错时 defer 仍 fire
	//   → OnAgentEnd 累计 2 (Phase 1 + Phase 2 各 1)
	resumeStart := counter.startCount.Load()
	resumeEnd := counter.endCount.Load()
	if resumeStart != 2 {
		t.Errorf("Phase 2 Resume OnAgentStart 累计应 2 (Run 1 + Resume 1), got %d", resumeStart)
	}
	if resumeEnd != 1 {
		t.Errorf("R30 C-H1 Resume fix: OnAgentStart 报错时 OnAgentEnd 不 fire, 累计应仍 1 (仅 Phase 1 Run defer fire), got %d\n"+
			"sanity-revert: 把 graph_agent.go Resume 路径 defer OnAgentEnd 块 (line 304+) 移回 OnAgentStart 之前 → got = 2",
			resumeEnd)
	}

	// Resume OnAgentStart 报错时 fire OnError (非 Interrupt 路径)
	if counter.errorCount.Load() != 1 {
		t.Errorf("Resume OnAgentStart err 应 fire OnError 1 次, got %d", counter.errorCount.Load())
	}
}
