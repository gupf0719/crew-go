package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func init() {
	compose_v2.RegisterValueType[string]("string")
}

// TestStepResult_RerunnableRedoneOnCondFailRerunFrom：条件出边按 CondFailRerunFrom 报错、要求节点
// 重新产出时，按 Rerunnable 记下的那一步（节点的产出）作废、重跑时重新做；普通记录（副作用）照样
// 拿回、不重放。
//
// 不作废的话，节点里记下的那份产出在重跑时原样拿回，cond 用同一份产出再判一次、再报错，续跑
// 多少次都一样——嵌套调用的完成记录就是这种记录（agent_v2.RunSubAgent 调的 planner）。
//
// 重做出来的那份记在新代际里：之后节点又中断（这里问人确认新计划）、再续跑时它照常拿回，不再做
// 第三遍。
func TestStepResult_RerunnableRedoneOnCondFailRerunFrom(t *testing.T) {
	var charges, llmCalls int
	b := compose_v2.NewBuilder().WithName("rerunnable")
	planCh := compose_v2.LastValue[string]("plan")
	compose_v2.WithChannel(b, planCh)
	b.AddNode("plan", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "charge"); !ok {
			charges++
			if err := compose_v2.SaveStepResult(ctx, "charge", "charged"); err != nil {
				return nil, err
			}
		}
		p, ok := compose_v2.GetStepResult[string](ctx, "llm")
		if !ok {
			llmCalls++
			p = fmt.Sprintf("plan-v%d", llmCalls)
			if err := compose_v2.SaveStepResult(ctx, "llm", p, compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		if p != "plan-v1" {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "按 " + p + " 执行？"}); err != nil {
				return nil, err
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planCh, p)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if p, _ := compose_v2.Get(chs, planCh); p == "plan-v1" {
			return "", errors.New("计划不合格")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s"),
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...); err == nil {
		t.Fatal("首跑的计划不合格，cond 应报错")
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("续跑应重新产出计划、停在确认上，got %v；llmCalls=%d", err, llmCalls)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}))...); err != nil {
		t.Fatalf("确认之后应收敛，got %v；llmCalls=%d", err, llmCalls)
	}
	if llmCalls != 2 {
		t.Errorf("可重做的那一步应在 RerunFrom 之后重新做一次、之后的续跑拿回，llmCalls=%d（want 2）", llmCalls)
	}
	if charges != 1 {
		t.Errorf("普通记录（副作用）应在重跑时拿回、不重放，charges=%d（want 1）", charges)
	}
	if p, _ := compose_v2.Get(cm.Channels(), planCh); p != "plan-v2" {
		t.Errorf("最终计划 = %q，want plan-v2", p)
	}
}

// TestStepResult_RerunnableKeptAcrossOtherReruns：可重做的记录只在「要求重新产出」时作废。中断
// 之后的续跑照常拿回——否则问过人之后重跑节点，调过的模型又调一遍、问过的子 agent 又问一遍。
func TestStepResult_RerunnableKeptAcrossOtherReruns(t *testing.T) {
	var llmCalls int
	b := compose_v2.NewBuilder().WithName("rerunnable_keep")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "llm"); !ok {
			llmCalls++
			if err := compose_v2.SaveStepResult(ctx, "llm", "out", compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s"),
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问上：%v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}))...); err != nil {
		t.Fatalf("续跑: %v", err)
	}
	if llmCalls != 1 {
		t.Errorf("中断之后的续跑应拿回可重做的记录，llmCalls=%d（want 1）", llmCalls)
	}
}

// hookPhaseProbe 在 OnNodeStart 里用那一格（SaveNodeState / GetNodeState）也按 key 记一步——都应记在
// 这个节点这次执行名下、钩子自己的相位里，碰不到节点体的。
type hookPhaseProbe struct {
	compose_v2.NoOpOrchestrationHooks
	mu           sync.Mutex
	hookExec     int
	hookScopes   []string
	hookSlotSeen []string
}

func (h *hookPhaseProbe) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hookScopes = append(h.hookScopes, compose_v2.SubCallScope(ctx))
	if v, ok := compose_v2.GetNodeState[string](ctx); ok {
		h.hookSlotSeen = append(h.hookSlotSeen, v)
	} else if err := compose_v2.SaveNodeState(ctx, "hook 写的"); err != nil {
		return ctx, err
	}
	if _, ok := compose_v2.GetStepResult[string](ctx, "x"); !ok {
		h.hookExec++
		if err := compose_v2.SaveStepResult(ctx, "x", "hook"); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// TestNodeState_HookPhaseHasItsOwnSlot：OnNodeStart 里
//
//   - SaveNodeState 记在钩子自己那一格——节点体那一格归节点体，钩子写进去会覆盖节点自己记的
//     进度（实测：tools 节点的工具记账被覆盖，已执行过的工具续跑时再执行一遍）；
//   - SaveStepResult 记在这个节点这次执行名下，节点中断之后重跑、钩子再触发时拿得回——钩子里
//     做成的事不重做（实测：钩子里经 RunSubAgent 问过人的把关，续跑时又问一遍、只答当前一问的
//     前端永远走不完）；
//   - 钩子里的 key 跟节点体里同一个 key 是两条（相位分域）；
//   - SubCallScope 在钩子里非空、在节点体里为空。
func TestNodeState_HookPhaseHasItsOwnSlot(t *testing.T) {
	hooks := &hookPhaseProbe{}
	var bodyExec int
	var bodyScopes, slotSeen []string
	b := compose_v2.NewBuilder().WithName("hook_phase")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		bodyScopes = append(bodyScopes, compose_v2.SubCallScope(ctx))
		if v, ok := compose_v2.GetNodeState[string](ctx); ok {
			slotSeen = append(slotSeen, v)
		} else if err := compose_v2.SaveNodeState(ctx, "节点体的进度"); err != nil {
			return nil, err
		}
		if _, ok := compose_v2.GetStepResult[string](ctx, "x"); !ok {
			bodyExec++
			if err := compose_v2.SaveStepResult(ctx, "x", "body"); err != nil {
				return nil, err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s"),
		compose_v2.WithHooks(hooks),
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问上：%v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}))...); err != nil {
		t.Fatalf("续跑: %v", err)
	}
	if len(hooks.hookSlotSeen) != 1 || hooks.hookSlotSeen[0] != "hook 写的" {
		t.Errorf("续跑时钩子应读回自己那一格记的东西，got %q", hooks.hookSlotSeen)
	}
	if len(slotSeen) != 1 || slotSeen[0] != "节点体的进度" {
		t.Errorf("续跑时节点体读回的应是自己记的进度，got %q（钩子写的若进了这一格就会覆盖它）", slotSeen)
	}
	if hooks.hookExec != 1 {
		t.Errorf("钩子里做成的那一步应在重跑时拿回，hookExec=%d（want 1）", hooks.hookExec)
	}
	if bodyExec != 1 {
		t.Errorf("节点体里同一个 key 应是另一条记录、各自拿回，bodyExec=%d（want 1）", bodyExec)
	}
	for _, s := range hooks.hookScopes {
		if s == "" {
			t.Errorf("钩子相位的 SubCallScope 不该为空：%q", hooks.hookScopes)
		}
	}
	for _, s := range bodyScopes {
		if s != "" {
			t.Errorf("节点体的 SubCallScope 应为空，got %q", bodyScopes)
		}
	}
}

// TestSubCallScope_DistinctPerNodeAndVisit：钩子相位的名字含所属节点与它的第几次执行——两个节点、
// 同一个节点循环的两次执行，在钩子里是不同的相位。否则钩子里经 RunSubAgent 用同一份输入发起的
// 调用在它们之间撞成同一次（实测终局 ErrRunAborted，报「答案已兑付」）。
func TestSubCallScope_DistinctPerNodeAndVisit(t *testing.T) {
	type rec struct{ node, scope string }
	var mu sync.Mutex
	var seen []rec
	hooks := &scopeRecorder{record: func(node compose_v2.NodeID, scope string) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, rec{string(node), scope})
	}}
	b := compose_v2.NewBuilder().WithName("scope_visit")
	cnt := compose_v2.Sum("cnt")
	compose_v2.WithChannel(b, cnt)
	noop := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(cnt, 1)}, nil
	}
	b.AddNode("A", noop)
	b.AddNode("B", noop)
	b.AddEdge(compose_v2.StartNode, "A")
	b.AddConditionalEdge("A", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if c, _ := compose_v2.Get(chs, cnt); c < 2 {
			return "A", nil
		}
		return "B", nil
	})
	b.AddEdge("B", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks)); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("应触发 3 次 OnNodeStart（A 两次、B 一次），got %v", seen)
	}
	distinct := map[string]bool{}
	for _, r := range seen {
		distinct[r.scope] = true
	}
	if len(distinct) != 3 {
		t.Errorf("A 的两次执行与 B 在钩子里应是三个不同的相位，got %d 个：%v", len(distinct), seen)
	}
}

type scopeRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	record func(compose_v2.NodeID, string)
}

func (h *scopeRecorder) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.record(id, compose_v2.SubCallScope(ctx))
	return ctx, nil
}

// TestWithNodeStateSealed：交出去的一次执行看不见调用方节点那一格——它在自己的分域里有自己的一格
// 和自己的步骤记录，不同分域互不可见、也碰不到调用方节点体自己的同名 key。
func TestWithNodeStateSealed(t *testing.T) {
	var got []string
	b := compose_v2.NewBuilder().WithName("sealed")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if err := compose_v2.SaveNodeState(ctx, "mine"); err != nil {
			return nil, err
		}
		a := compose_v2.WithNodeStateSealed(ctx, "call-a")
		if v, ok := compose_v2.GetNodeState[string](a); ok {
			got = append(got, "交出去的执行读到了调用方那一格："+v)
		}
		if err := compose_v2.SaveNodeState(a, "a 的"); err != nil {
			return nil, err
		}
		if v, _ := compose_v2.GetNodeState[string](a); v != "a 的" {
			got = append(got, "交出去的执行读不回自己那一格："+v)
		}
		if v, _ := compose_v2.GetNodeState[string](ctx); v != "mine" {
			got = append(got, "调用方那一格被改成了 "+v)
		}
		if err := compose_v2.SaveStepResult(a, "k", "from-a"); err != nil {
			return nil, err
		}
		if v, ok := compose_v2.GetStepResult[string](a, "k"); !ok || v != "from-a" {
			got = append(got, fmt.Sprintf("同一分域读不回自己记的步骤：%q %v", v, ok))
		}
		if _, ok := compose_v2.GetStepResult[string](ctx, "k"); ok {
			got = append(got, "调用方节点体读到了交出去那次执行记的步骤")
		}
		if _, ok := compose_v2.GetStepResult[string](compose_v2.WithNodeStateSealed(ctx, "call-b"), "k"); ok {
			got = append(got, "另一个分域读到了 call-a 记的步骤")
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatal(err)
	}
	for _, s := range got {
		t.Error(s)
	}
}
