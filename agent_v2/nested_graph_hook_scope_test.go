package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// graphStartCaller 在图的 OnGraphStart 里发起一次嵌套调用。
type graphStartCaller struct {
	agent_v2.NoOpOrchestrationHooks
	sub   agent_v2.Agent
	input func() string
	mu    sync.Mutex
	outs  []string
}

func (h *graphStartCaller) OnGraphStart(ctx context.Context) (context.Context, error) {
	events, err := agent_v2.RunSubAgent(ctx, "gs", h.sub, schema.UserMessage(h.input()))
	if err != nil {
		return ctx, err
	}
	h.mu.Lock()
	h.outs = append(h.outs, nestedFinal(events))
	h.mu.Unlock()
	return ctx, nil
}

// TestNestedCall_FromNestedGraphStartHookIsNotTopLevel：嵌套图的 OnGraphStart 钩子里发起的
// 调用，仍在外层节点的执行范围里，不是顶层新任务。
//
// 曾经进图就清掉「在节点里」的章，于是这条路上的调用被当成顶层：
//   - 不续存档：挂起的提问在续跑里从头跑，第一个节点重跑；
//   - 整个任务的 approve_always 规则被清空（会话的 values 与父任务共享）。
//
// 两格：父节点重跑时给同一份输入 ⟹ 续上自己的存档、用上答案；换了输入 ⟹ 那是另一次调用，
// 重新问、用户对旧输入的批准不会被套到新输入上。两格里任务攒下的规则都得还在。
func TestNestedCall_FromNestedGraphStartHookIsNotTopLevel(t *testing.T) {
	for _, changing := range []bool{false, true} {
		name := "同一份输入：续上存档"
		if changing {
			name = "换了输入：另一次调用"
		}
		t.Run(name, func(t *testing.T) {
			firsts := &countLog{}
			asker := newTwoNodeAgent(t, "asker",
				func(_ context.Context, c string) (string, error) {
					firsts.inc(c)
					return c, nil
				},
				func(ctx context.Context, c, _ string) (string, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认处理「" + c + "」？"})
					if err != nil {
						return "", err
					}
					return "执行了「" + c + "」(" + ans.Content + ")", nil
				})

			// changing 时父节点每次执行给的输入都不一样（第 1 次、第 2 次……），模拟上游重跑后输入变了
			var hostRuns atomic.Int32
			var cur atomic.Value
			hooks := &graphStartCaller{sub: asker, input: func() string { return cur.Load().(string) }}
			mid := newSingleNodeAgent(t, "mid", "m", nil, func(_ context.Context, c string) (string, error) {
				return "mid 收到:" + c, nil
			})
			host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
				n := hostRuns.Add(1)
				if changing {
					cur.Store(fmt.Sprintf("第%d次", n))
				} else {
					cur.Store("固定的活")
				}
				// 任务进行中攒下的 approve_always 规则（approval.go 的 approvalRulesKey：前缀 + RunID）。在
				// 节点体里设：顶层 Run 开新任务时本来就会合法地清一次，设在 Run 之前测不到嵌套调用的影响
				s := agent_v2.GetSession(ctx)
				s.SetValue("crew.approved_tools\x00"+s.RunID, map[string]bool{"transfer\x00*": true})
				events, err := agent_v2.RunSubAgent(ctx, "mid", mid, schema.UserMessage("x"), agent_v2.WithOrchHooks(hooks))
				if err != nil {
					return "", err
				}
				return nestedFinal(events), nil
			})

			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			sess := agent_v2.NewRunSession()

			_, err := host.Run(ctx, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在嵌套图 OnGraphStart 里那次调用的提问，got %v", err)
			}
			answers := map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("批准")}
			_, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			if v, _ := sess.GetValue("crew.approved_tools\x00" + sess.RunID); v == nil {
				t.Errorf("任务里的 approve_always 规则被清空了——钩子里的调用被当成了顶层新任务")
			}

			hooks.mu.Lock()
			outs := append([]string(nil), hooks.outs...)
			hooks.mu.Unlock()
			if !changing {
				if err != nil {
					t.Fatalf("续跑失败：%v", err)
				}
				if len(outs) != 1 || outs[0] != "执行了「固定的活」(批准)" {
					t.Errorf("续跑应接着那次调用执行、用上批准；实际 %v", outs)
				}
				if n := firsts.get("固定的活"); n != 1 {
					t.Errorf("被调子 agent 的第一个节点应只跑 1 次（续上自己的存档），实际 %d 次——钩子里的调用被当成顶层新任务、从头跑了", n)
				}
				return
			}
			// 换了输入：另一次调用，重新问；用户给「第1次」的批准不能被套到「第2次」上
			ie2, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("换了输入应重新问，got err=%v outs=%v", err, outs)
			}
			if !strings.Contains(ie2.Query.Question, "「第2次」") || ie2.Query.InterruptID == ie.Query.InterruptID {
				t.Fatalf("应就「第2次」重新问、提问身份不同，got %q（id 相同=%v）", ie2.Query.Question,
					ie2.Query.InterruptID == ie.Query.InterruptID)
			}
			if len(outs) != 0 {
				t.Errorf("换了输入的那次调用还没被批准，不该执行；实际 %v", outs)
			}
		})
	}
}

// ruleKeeper 在顶层图的 OnGraphStart 里先设一条任务规则，再发起一次嵌套调用，记下调用之后
// 规则还在不在。
type ruleKeeper struct {
	agent_v2.NoOpOrchestrationHooks
	sub      agent_v2.Agent
	mu       sync.Mutex
	survived []bool
}

func (h *ruleKeeper) OnGraphStart(ctx context.Context) (context.Context, error) {
	sess := agent_v2.GetSession(ctx)
	if sess == nil {
		return ctx, fmt.Errorf("钩子里拿不到会话")
	}
	// 规则集在会话 KV 里按任务分键（前缀 + "\x00" + RunID，同 approval.go 的 approvalRulesKey）：新任务开始时清的是这个键。
	// 曾经写的是不带 RunID 的旧键，清除从来碰不到它，「规则还在」恒真（审计发现）
	rulesKey := "crew.approved_tools\x00" + sess.RunID
	sess.SetValue(rulesKey, map[string]bool{"owner\x00transfer\x00\x01*": true})
	if _, err := agent_v2.RunSubAgent(ctx, "guard", h.sub, schema.UserMessage("检查一下")); err != nil {
		return ctx, err
	}
	v, _ := sess.GetValue(rulesKey)
	h.mu.Lock()
	h.survived = append(h.survived, v != nil)
	h.mu.Unlock()
	return ctx, nil
}

// TestNestedCall_FromTopLevelGraphStartHookKeepsTaskState：顶层图的 OnGraphStart 钩子里发起的
// 嵌套调用不在任何节点执行里，但它显然不是一个新任务——不能清掉任务攒下的审批规则（会话的
// values 与被调方共享）。
//
// 曾经只按「在不在节点执行里」判：这条路上的调用被当成顶层新任务，一进被调方就把整个任务的
// approve_always 规则清空（审计实测）。
func TestNestedCall_FromTopLevelGraphStartHookKeepsTaskState(t *testing.T) {
	guard := newSingleNodeAgent(t, "guard", "g", nil, func(_ context.Context, c string) (string, error) {
		return "查过了:" + c, nil
	})
	hooks := &ruleKeeper{sub: guard}
	host := newSingleNodeAgent(t, "host", "host", nil, func(_ context.Context, c string) (string, error) {
		return "done:" + c, nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"), agent_v2.WithOrchHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.survived) != 1 || !hooks.survived[0] {
		t.Fatalf("钩子里的嵌套调用把任务的审批规则清掉了（被当成了顶层新任务）：%v", hooks.survived)
	}
}

// hookVoter 在顶层图 host 节点的 OnNodeStart 里用同一份输入调两次同一个子 agent，记下两次的产出。
// （顶层图的 OnGraphStart 不能中断——图还没开始跑、没有可落的存档；OnNodeStart 在节点的执行
// 范围里，接得住。）
type hookVoter struct {
	agent_v2.NoOpOrchestrationHooks
	sub  agent_v2.Agent
	mu   sync.Mutex
	outs []string
}

func (h *hookVoter) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if nodeID != "host" {
		return ctx, nil
	}
	outs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		events, err := agent_v2.RunSubAgent(ctx, "vote", h.sub, schema.UserMessage("同一个问题"))
		if err != nil {
			return ctx, err
		}
		outs = append(outs, nestedFinal(events))
	}
	h.mu.Lock()
	h.outs = outs
	h.mu.Unlock()
	return ctx, nil
}

// TestNestedCall_HookPhaseIdenticalCallsEachKeepTheirOwnSlot：钩子相位里用同一份输入调两次，
// 第二次停在提问上。续跑时两次各自接回自己的槽：第二次续上自己的存档、用上答案。
//
// 钩子相位不在任何节点执行里，没有节点的计数器可用；框架给每次钩子调用配一个自己的计数器。
// 没有它时两次调用算出同一个身份、落在同一个槽上：续跑时第一次调用先把第二次的挂起档捡走、
// 拿第二次的答案当自己的结果，第二次再从头跑。
//
// ⚠️ 钩子相位里完成的调用**不被记住**（记录挂在节点执行上，钩子相位没有），续跑时第一次调用
// 会重跑——这一条断言的是「各自的槽」，不是「不重跑」。
func TestNestedCall_HookPhaseIdenticalCallsEachKeepTheirOwnSlot(t *testing.T) {
	var seq atomic.Int32
	firstRuns := &countLog{}
	voter := newTwoNodeAgent(t, "voter",
		func(_ context.Context, _ string) (string, error) {
			n := seq.Add(1)
			firstRuns.inc(fmt.Sprint(n))
			return fmt.Sprint(n), nil
		},
		func(ctx context.Context, _, n string) (string, error) {
			if n != "2" {
				return "票" + n, nil
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第 2 票投什么？"})
			if err != nil {
				return "", err
			}
			return "票2(" + ans.Content + ")", nil
		})
	hooks := &hookVoter{sub: voter}
	host := newSingleNodeAgent(t, "host", "host", nil, func(_ context.Context, c string) (string, error) {
		return "done:" + c, nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"), agent_v2.WithOrchHooks(hooks))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在第二次调用的提问，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("赞成")}),
		agent_v2.WithResumeOrchHooks(hooks),
		agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	hooks.mu.Lock()
	outs := append([]string(nil), hooks.outs...)
	hooks.mu.Unlock()
	if len(outs) != 2 || outs[1] != "票2(赞成)" {
		t.Fatalf("第二次调用应续上自己的存档、用上答案，两次产出依次为 %v", outs)
	}
	if strings.Contains(outs[0], "赞成") {
		t.Fatalf("第一次调用拿到了第二次的答案：%v", outs)
	}
	if n := firstRuns.get("2"); n != 1 {
		t.Errorf("第二次调用的第一个节点应只跑 1 次（续上自己的存档），实际 %d 次", n)
	}
}
