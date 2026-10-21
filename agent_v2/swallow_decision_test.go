package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
)

// TestSwallowDecision_NoDirectControlFlowSignal 机械对账：`agent_v2` 里**不许**直接调
// [compose_v2.IsControlFlowSignal] 来决定「这个错吞不吞」。
//
// ⚠️ 为什么要机械对账而不是靠注释：这条规矩**已经失效过一次**。那个谓词回答的是「这是不是
// 控制流信号」（retry 层 + fallback 层共用），而吞不吞是**另一个问题**——两者只在
// `ErrRunAborted` / `ErrCheckpointNotResumable` 上分叉，而恰恰是那两个绝不能吞。
// 实测代价：子 agent 返 `ErrRunAborted` 经 `AgentAsTool` 转成 `tool.Failure` + `err=nil`
// ⟹ 模型重试同一个工具 ⟹ 循环到 maxRounds ⟹ 业务拿到 `MaxIterationsError`。
//
// 本仓规约：「必须与 X 保持一致」是设计债的自白，注释拦不住——改代码的人看的往往是另一处。
//
// 真要新开一处直调（而不是用 [compose_v2.MustPropagate]），加进 allowlist 并写明
// 为什么它回答的是「是不是控制流信号」而不是「吞不吞」。
func TestSwallowDecision_NoDirectControlFlowSignal(t *testing.T) {
	// 用 git grep 只扫**已跟踪**文件：裸 grep 会把 `.claude/worktrees/` 下别的 checkout
	// 副本一起扫进来，纯噪声。
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("不在 git 仓库里：%v", err)
	}
	allowed := map[string]string{} // 目前一处都不该有
	out, _ := exec.Command("git", "-C", strings.TrimSpace(string(root)), "grep", "-n",
		"--full-name", "compose_v2\\.IsControlFlowSignal(", "--", "agent_v2/").Output()
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}
		file, body := parts[0], strings.TrimSpace(parts[2])
		// 注释里提这个名字是正常的（解释两者区别），只挡真调用。
		if strings.HasPrefix(body, "//") || strings.HasPrefix(body, "*") {
			continue
		}
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		if _, ok := allowed[file]; ok {
			continue
		}
		t.Errorf("%s 直接调了 IsControlFlowSignal 决定吞不吞：\n  %s\n"+
			"「是不是控制流信号」和「吞不吞」是两个问题，只在 ErrRunAborted / "+
			"ErrCheckpointNotResumable / ErrGraphIDConflict 上分叉，而那三个恰恰绝不能吞。用 compose_v2.MustPropagate。",
			file, body)
	}
}

type abortingSubAgent struct{ calls *int }

func (a *abortingSubAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*a.calls++
	// 真形态：业务的 cond 报错走 CondFailAbort、或 validateInterrupt 五条之一命中，
	// 框架 `%w` 上 ErrRunAborted 冒泡到这儿。
	return nil, fmt.Errorf("子 agent 违反协议: %w", compose_v2.ErrRunAborted)
}
func (a *abortingSubAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *abortingSubAgent) Name() string        { return "sub_worker" }
func (a *abortingSubAgent) Description() string { return "子任务" }

// TestAgentAsTool_TerminalFailurePropagates 钉住**终局失败不许被转成 tool.Failure**。
//
// ⚠️ 审计实测的伤害：转成 tool.Failure + `err=nil` 之后，模型看到一段错误文本、
// 按典型行为重试同一个工具 ⟹ 子 agent 再跑、再 abort ⟹ 循环到 maxRounds ⟹
// 业务最终拿到 `MaxIterationsError`，**跟真实原因毫无关系**，而副作用跑了 N 次。
//
// `ErrRunAborted` 的全部来源都是「业务违反框架协议」（cond 的 Abort 档 +
// `validateInterrupt` 五条），定义就是「重跑必然一样」——让模型绕过一个**编程错误**，
// 结果是业务拿到一个貌似合理的答案、永远不知道自己配错了。
func TestAgentAsTool_TerminalFailurePropagates(t *testing.T) {
	calls := 0
	tl := agent_v2.AgentAsTool(&abortingSubAgent{calls: &calls})
	res, err := tl.Call(context.Background(), map[string]any{"input": "go"})
	if err == nil {
		t.Fatalf("终局失败被吞成了 tool 结果（err=nil, result=%+v）——"+
			"模型会看到它、重试、循环到 maxRounds，业务拿到的错跟真实原因无关", res)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("传出来了但哨兵没保住：errors.Is(err, ErrRunAborted)=false, err=%v\n"+
			"下游（前端适配 / A2A server）按哨兵分流，丢了它那些专属分支就永不触发", err)
	}
	if calls != 1 {
		t.Errorf("子 agent 被调用 %d 次，期望 1——重跑必然一样的错不该被重试", calls)
	}
}

// innerTimeoutAgent 模拟「子 agent 内部用了自己的 ctx，超时后 wrap 上来」。
// 调用方的 ctx 全程健康。
type innerTimeoutAgent struct{ calls *int }

func (a *innerTimeoutAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*a.calls++
	inner, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()
	<-inner.Done()
	return nil, fmt.Errorf("子 agent 内部第 3 步超时: %w", inner.Err())
}
func (a *innerTimeoutAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *innerTimeoutAgent) Name() string        { return "sub" }
func (a *innerTimeoutAgent) Description() string { return "子任务" }

// TestAgentAsTool_InnerCtxTimeoutIsToolFailure 钉住**「谁的 ctx 死了」要问 ctx，不能查错误链**。
//
// 子 agent 内部用自己的派生 ctx 超时并 wrap 上来时，`errors.Is(err, context.DeadlineExceeded)`
// 是 true——但**调用方的 ctx 全程健康**。这是一次**任务失败**（该让模型换条路），
// 不是调用方喊停（该整轮停）。
//
// 本仓规约原话：`errors.Is(err, context.X)` 可以用来贴标签 / 报状态 / 打 span，
// **不能用来决定要不要再做一次**。
//
// sanity-revert 锚点：把 [compose_v2.MustPropagate] 的 ctx 档改回查错误链（例如让它去调
// `IsControlFlowSignal`）⟹ 这条红：整个 Run 被一个子 agent 的内部超时拖挂。
func TestAgentAsTool_InnerCtxTimeoutIsToolFailure(t *testing.T) {
	calls := 0
	tl := agent_v2.AgentAsTool(&innerTimeoutAgent{calls: &calls})
	ctx := context.Background()
	res, err := tl.Call(ctx, map[string]any{"input": "go"})
	if ctx.Err() != nil {
		t.Fatal("fixture 没搭对：调用方的 ctx 该是健康的")
	}
	if err != nil {
		t.Fatalf("子 agent 的**内部**超时被当成了调用方喊停、原样传了出去 ⟹ 整轮会挂。\n"+
			"正确行为是转成模型看得见的工具失败让它换条路。err=%v", err)
	}
	if res == nil || res.Error == nil {
		t.Errorf("该给模型一条失败的 tool_result，实际 res=%+v", res)
	}
	if calls != 1 {
		t.Errorf("子 agent 被调用 %d 次，期望 1", calls)
	}
}

// TestSwallowDecision_RoutingClassifierPropagatesMustPropagate：RoutingAgent 的分类器是一个吞点（分类没成
// 就降级到兜底分支）。MustPropagate 判为必传的错必须穿过它；普通业务错照常降级（对照，防空转）。
//
// 曾经这里手写判据、只认中断与 ctx 错：终局失败、软取消、陈旧写回、坏档都被降级成兜底，`err == nil`
// （审计实测四种全中）。
func TestSwallowDecision_RoutingClassifierPropagatesMustPropagate(t *testing.T) {
	for _, c := range []struct {
		name    string
		err     error
		mustErr error // nil = 应降级到兜底
	}{
		{"ErrRunAborted", fmt.Errorf("分类器违反协议: %w", compose_v2.ErrRunAborted), compose_v2.ErrRunAborted},
		{"ErrCheckpointStale", fmt.Errorf("存档已被推进: %w", compose_v2.ErrCheckpointStale), compose_v2.ErrCheckpointStale},
		{"ErrSoftCanceled", fmt.Errorf("分类器被软取消: %w", compose_v2.ErrSoftCanceled), compose_v2.ErrSoftCanceled},
		{"ErrCheckpointNotResumable", fmt.Errorf("坏存档: %w", compose_v2.ErrCheckpointNotResumable), compose_v2.ErrCheckpointNotResumable},
		{"普通业务错（对照）", errors.New("LLM API 503"), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			cls := &effFunc{name: "cls"}
			cls.fn = func(context.Context, agent_v2.Agent, *schema.Message, []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				return nil, c.err
			}
			rt, math, fb := effRouting(cls)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			_, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("1+1"))
			if c.mustErr == nil {
				if err != nil || fb.runs.Load() != 1 {
					t.Fatalf("普通业务错应降级到兜底：err=%v fallback=%d", err, fb.runs.Load())
				}
				return
			}
			if !errors.Is(err, c.mustErr) {
				t.Fatalf("分类器返回的必传错被吞了：err=%v（math %d，fallback %d）", err, math.runs.Load(), fb.runs.Load())
			}
			if n := fb.runs.Load(); n != 0 {
				t.Errorf("必传错不该降级到兜底分支，兜底却跑了 %d 次", n)
			}
		})
	}
}

// TestSwallowDecision_RoutingClassifierSoftCancelKeepsRoute：分类器是两步图，第一步里用户喊停，第二步
// 本来定 math。停下之后续跑应接着跑完分类器、走 math。
//
// 曾经软取消被降级成兜底、随 classify 节点提交进存档，续跑直接走兜底分支——分类器再也没机会定
// 路由（审计实测：不停走 math，停下后续跑走兜底）。
func TestSwallowDecision_RoutingClassifierSoftCancelKeepsRoute(t *testing.T) {
	sig := compose_v2.NewSoftCancelSignal()
	var triggered atomic.Bool
	cls := newTwoNodeAgent(t, "cls",
		func(ctx context.Context, _ string) (string, error) {
			if triggered.CompareAndSwap(false, true) {
				sig.Trigger("用户点了停止")
			}
			return "ok", nil
		},
		func(ctx context.Context, _, _ string) (string, error) { return "math", nil })
	rt, math, fb := effRouting(cls)
	base := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := rt.Run(compose_v2.WithSoftCancelCtx(base, sig), sess, schema.UserMessage("1+1"))
	if !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("应停在软取消上：%v", err)
	}
	events, err := rt.Resume(base, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑：%v", err)
	}
	final := ""
	if m := agent_v2.FinalMessage(events); m != nil {
		final = m.Content
	}
	if math.runs.Load() != 1 || fb.runs.Load() != 0 {
		t.Errorf("续跑应接着跑完分类器、走 math：最终 %q（math %d，fallback %d）", final, math.runs.Load(), fb.runs.Load())
	}
}

// erringSubAgent 每次 Run 都返回给定的错。
type erringSubAgent struct {
	err   error
	calls int
}

func (a *erringSubAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.calls++
	return nil, a.err
}
func (a *erringSubAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *erringSubAgent) Name() string        { return "sub" }
func (a *erringSubAgent) Description() string { return "子任务" }

// TestAgentAsTool_GraphIDConflictPropagates：子 agent 撞上身份冲突（同一个父 sid 上先后跑了拓扑
// 不同的两棵编排树）必须往上传——模型怎么换参数重试都撞同一个槽。曾经不在 MustPropagate 的集合里，
// 被转成 tool.Failure、err == nil。
//
// 对照：ErrNoPendingCheckpoint **不在**集合里——它说的是被续的那个任务已经结束 / 从没跑过，是另一个
// 任务的外部状态，照常转成模型看得见的工具失败（见 MustPropagate godoc「不在集合里的」）。两个方向
// 都钉住，免得有人顺手把它也加进去、或者把身份冲突又拿出来。
func TestAgentAsTool_GraphIDConflictPropagates(t *testing.T) {
	t.Run("ErrGraphIDConflict 往上传", func(t *testing.T) {
		sub := &erringSubAgent{err: fmt.Errorf("子图载入子档: %w", compose_v2.ErrGraphIDConflict)}
		res, err := agent_v2.AgentAsTool(sub).Call(context.Background(), map[string]any{"input": "go"})
		if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
			t.Fatalf("身份冲突被吞成了工具结果：err=%v result=%+v", err, res)
		}
		if sub.calls != 1 {
			t.Errorf("子 agent 被调用 %d 次，期望 1", sub.calls)
		}
	})
	t.Run("ErrNoPendingCheckpoint 照常转成工具失败", func(t *testing.T) {
		sub := &erringSubAgent{err: fmt.Errorf("续的那个任务已经结束: %w", compose_v2.ErrNoPendingCheckpoint)}
		res, err := agent_v2.AgentAsTool(sub).Call(context.Background(), map[string]any{"input": "go"})
		if err != nil {
			t.Fatalf("被续的任务已经结束是任务失败，该让模型看见，不该让整个 Run 失败：%v", err)
		}
		if res == nil || res.Error == nil {
			t.Errorf("该给模型一条失败的 tool_result，实际 res=%+v", res)
		}
	})
}

// erringActionSkillBox 建一个 calc skill，它的 action 「run」执行 fn。
func erringActionSkillBox(t *testing.T, fn func(ctx context.Context) error) skill.SkillBox {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "calc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"),
		[]byte("---\nname: calc\ndescription: 计算\n---\n计算助手\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	box, err := skill.NewDefaultSkillBox(root, skill.WithSkillActionFunc("calc",
		skill.SkillActionDescriptor{Name: "run", Description: "执行"},
		func(ctx context.Context, _ map[string]any) (*skill.SkillExecutionResult, error) {
			if err := fn(ctx); err != nil {
				return nil, err
			}
			return &skill.SkillExecutionResult{Success: true, Output: "done"}, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	return box
}

// TestSwallowDecision_SkillActionPropagatesMustPropagate：run_skill_action 是一个吞点（动作出错就转成
// 工具失败回灌模型）。MustPropagate 判为必传的错必须穿过它；普通业务错照常回灌（对照，防空转）。
//
// 曾经这里手写判据、只认中断与 ctx 错：动作返回 ErrRunAborted / ErrSoftCanceled 被转成一条工具结果，
// 模型接着答「算好了」，Run 照常成功（审计实测）。
func TestSwallowDecision_SkillActionPropagatesMustPropagate(t *testing.T) {
	for _, c := range []struct {
		name    string
		fn      func(ctx context.Context) error
		mustErr error // nil 且 wantInt=false = 普通业务错，应回灌模型
		wantInt bool
	}{
		{name: "中断", wantInt: true, fn: func(ctx context.Context) error {
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认执行？"})
			return err
		}},
		{name: "ErrRunAborted", mustErr: compose_v2.ErrRunAborted},
		{name: "ErrSoftCanceled", mustErr: compose_v2.ErrSoftCanceled},
		{name: "ErrCheckpointStale", mustErr: compose_v2.ErrCheckpointStale},
		{name: "ErrCheckpointNotResumable", mustErr: compose_v2.ErrCheckpointNotResumable},
		{name: "ErrGraphIDConflict", mustErr: compose_v2.ErrGraphIDConflict},
		{name: "普通业务错（对照）", fn: func(context.Context) error { return errors.New("下游 503") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			fn := c.fn
			if fn == nil {
				sentinel := c.mustErr
				fn = func(context.Context) error { return fmt.Errorf("动作失败: %w", sentinel) }
			}
			m := &captureToolsModel{responses: []*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1", Function: schema.FunctionCall{
					Name: agent_v2.RunSkillActionToolName, Arguments: `{"skill":"calc","action":"run"}`}}}),
				schema.AssistantMessage("算好了"),
			}}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("skill_swallow"), agent_v2.WithModel(m),
				agent_v2.WithSkills(skill.SkillSource{SkillBox: erringActionSkillBox(t, fn)}),
				agent_v2.WithMaxRounds(5))
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("算一下"))

			switch {
			case c.wantInt:
				if _, ok := compose_v2.IsInterrupt(err); !ok {
					t.Fatalf("动作里的提问被吞了：err=%v", err)
				}
			case c.mustErr != nil:
				if !errors.Is(err, c.mustErr) {
					t.Fatalf("动作返回的必传错被吞成了工具结果：err=%v（模型被调 %d 次）", err, len(m.inputs))
				}
			default:
				if err != nil {
					t.Fatalf("普通业务错应回灌模型、Run 照常走完：%v", err)
				}
				if len(m.inputs) != 2 || !toolMessageContains(m.inputs[1], "下游 503") {
					t.Errorf("模型第二次调用应看到那条失败的工具结果，实际调用 %d 次", len(m.inputs))
				}
				return
			}
			if n := len(m.inputs); n != 1 {
				t.Errorf("必传错应终止本轮，模型却被调了 %d 次", n)
			}
		})
	}
}

// toolMessageContains 报 msgs 里有没有一条 tool 消息含 s。
func toolMessageContains(msgs []*schema.Message, s string) bool {
	for _, msg := range msgs {
		if msg != nil && msg.Role == schema.RoleTool && strings.Contains(msg.Content, s) {
			return true
		}
	}
	return false
}
