package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 框架**不遍历编排树去找历史配置**——这条用例守的就是「别再把遍历写回来」。
//
// # 为什么这不是个能靠写好一点解决的问题
//
// 曾经写过一版递归遍历（编排向子、子再向子的子要配置），三种死法各自独立：
//
//   - **环**：编排可以互相引用，而环停在一条运行期永远走不到的分支上时静态遍历照样会
//     走进去 ⟹ **fatal stack overflow，进程直接死**。Go 的栈溢出不是 panic，各 Run 顶上
//     的 recover 兜不住。实测两个 Sequential 互相 AddStep 就够。
//   - **指数级**：共享同一个 sub-agent 实例的 DAG 上每加一层 ×2（实测 depth 18 从
//     0.59ms 涨到 143ms），而且**没开历史时最贵**——找不到配置才会把整棵树走完，而
//     历史是 opt-in。
//   - **去不了重**：业务用不可比较的类型实现 Agent（值接收者 + 含 slice/map 字段，把
//     框架编排内嵌进自己的 struct 做装饰就是这个形状）时拿不到身份，上面两条都收不了口。
//     深度封顶封的是路径**长度**，而成本是路径**条数**——实测值类型 wrapper 的共享 DAG
//     depth 30 已经 21.9 秒。
//
// 这些全是「去找」这个动作自造的问题。配置改成在**运行入口**声明（`WithHistoryCtx`）
// 之后一条都不存在。既有的「配在叶子上」写法仍然工作——编排只向**直接子**借，一层，
// 不递归。
func TestHistory_NoTreeTraversalForConfig(t *testing.T) {
	t.Run("环形编排不炸", func(t *testing.T) {
		hs := agent_v2.NewInMemoryHistoryStore()
		leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
		rt := agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("rt"),
			agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) {
				return "a", nil
			}))
		rt.AddBranch("a", leaf)
		rt.AddBranch("b", rt) // 结构成环，运行期永远走不到

		ctx := compose_v2.WithThreadIDCtx(context.Background(), "cyc")
		if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
			t.Fatalf("环形结构不该让 Run 失败：%v", err)
		}
		if msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "cyc"}); len(msgs) == 0 {
			t.Error("历史是空的——直接子里就有配置源，不该找不到")
		}
	})

	t.Run("共享子实例的深 DAG 不是指数级", func(t *testing.T) {
		hs := agent_v2.NewInMemoryHistoryStore()
		var cur agent_v2.Agent = agent_v2.NewBasicAgent(agent_v2.WithName("leaf"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
			agent_v2.WithMaxRounds(2))
		const depth = 24
		for i := 0; i < depth; i++ {
			rt := agent_v2.NewRoutingAgent(
				agent_v2.WithRoutingName("rt"),
				agent_v2.WithRouter(func(context.Context, *schema.Message) (compose_v2.NodeID, error) {
					return "a", nil
				}))
			rt.AddBranch("a", cur)
			rt.AddBranch("b", cur) // 同一个实例两条边——遍历不去重就是 2^depth
			cur = rt
		}
		// 配置走运行入口声明：编排里一个 BasicAgent 都不用配，这也正是遍历方案覆盖不了
		// 的那种形状。
		ctx := agent_v2.WithHistoryCtx(
			compose_v2.WithThreadIDCtx(context.Background(), "dag"), hs, "t")
		t0 := time.Now()
		if _, err := cur.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if el := time.Since(t0); el > 2*time.Second {
			t.Errorf("深度 %d 的共享 DAG 跑了 %v——有人把树遍历写回来了", depth, el)
		}
		if msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "dag"}); len(msgs) == 0 {
			t.Error("历史是空的——运行入口声明没生效")
		}
	})

	t.Run("不可比较的业务 Agent 不炸", func(t *testing.T) {
		hs := agent_v2.NewInMemoryHistoryStore()
		seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("s")).
			AddStep("v", uncomparableAgent{name: "va", junk: map[string]string{"k": "v"}}).
			AddStep("leaf", agent_v2.NewBasicAgent(agent_v2.WithName("leaf"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2)))
		ctx := compose_v2.WithThreadIDCtx(context.Background(), "uncmp")
		if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
			t.Fatalf("不可比较的 Agent 不该让 Run 失败（曾经拿它当 map key 直接 panic）：%v", err)
		}
	})
}

// uncomparableAgent 是值接收者 + 含 map 字段——不可比较，拿它当 map key 会 panic。
// 业务把框架编排内嵌进自己的 struct 做装饰就是这个形状。
type uncomparableAgent struct {
	name string
	junk map[string]string
}

func (a uncomparableAgent) Run(_ context.Context, s *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	out := schema.AssistantMessage("值类型产出")
	s.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}
func (a uncomparableAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a uncomparableAgent) Name() string        { return a.name }
func (a uncomparableAgent) Description() string { return "不可比较的值类型 agent" }

// 运行入口声明配置时，写入器唯一 ⟹ 共享视图下同一条输入恰好一份，且跨次确定。
//
// 对照组是「靠向子借配置」：编排里夹了一层 Sequential 时 Parallel 借不到，三个叶子各建
// 一个写入器，写侧那次认领只在单个写入器内有定论 ⟹ 份数随调度翻。实测 20 轮
// `map[1:11 2:9]`，运行入口声明后 `map[1:20]`。
//
// 这条同时是「别再把树遍历写回来」的正面理由：遍历方案想解的就是这个问题，而运行入口
// 声明不用遍历就解了。
func TestHistory_CtxDeclaredConfig_SingleWriter(t *testing.T) {
	probe := func(seq int, useCtx bool) int {
		hs := agent_v2.NewInMemoryHistoryStore()
		conv := fmt.Sprintf("ctxdecl-%v-%d", useCtx, seq)
		mkSeq := func(n int) agent_v2.Agent {
			leaf := agent_v2.NewBasicAgent(agent_v2.WithName(fmt.Sprintf("b%d", n)),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantMessage(fmt.Sprintf("产出%d", n))})),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
			// 夹一层 Sequential：Parallel 向直接子借配置借不到（子是 Sequential 不是
			// BasicAgent），这正是「靠借」失效的形状。
			return agent_v2.NewSequentialAgent(
				agent_v2.WithSequentialName(fmt.Sprintf("s%d", n))).AddStep("s1", leaf)
		}
		par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
			AddBranch("br1", mkSeq(1)).AddBranch("br2", mkSeq(2)).AddBranch("br3", mkSeq(3))
		ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("front"),
			agent_v2.WithHandoffInitial("par")).AddAgent("par", par)

		ctx := compose_v2.WithThreadIDCtx(context.Background(), conv)
		if useCtx {
			ctx = agent_v2.WithHistoryCtx(ctx, hs, "t")
		}
		if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		n := 0
		for _, m := range msgs {
			if m.Role == schema.RoleUser && strings.Contains(m.Content, "原话") {
				n++
			}
		}
		return n
	}

	const rounds = 20
	dist := map[int]int{}
	for i := 0; i < rounds; i++ {
		dist[probe(i, true)]++
	}
	if len(dist) != 1 || dist[1] != rounds {
		t.Errorf("运行入口声明配置时写入器唯一，共享视图下同一条输入该恰好 1 份且跨次确定。\n"+
			"实际份数分布 %v（跑了 %d 轮）", dist, rounds)
	}
}
