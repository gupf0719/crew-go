package sql_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
)

func TestSQLiteGraphAgentResumesJoinAndDeferredSubgraph(t *testing.T) {
	cp, db := openSQLiteCheckpointer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	ctx = compose_v2.WithDurabilityCtx(ctx, compose_v2.DurabilitySync)
	session := agent_v2.NewRunSession()

	left := compose_v2.LastValue[string]("left")
	right := compose_v2.LastValue[string]("right")
	receipt := compose_v2.LastValue[string]("receipt")
	joined := compose_v2.LastValue[string]("joined")
	private := compose_v2.LastValue[string]("private")
	results := compose_v2.Append[string]("results")
	sub := compose_v2.NewBuilder().WithName("review-barriers").WithDeferredNodes("report", "deliver")
	for _, channel := range []*compose_v2.Channel[string]{left, right, receipt, joined, private} {
		compose_v2.WithChannel(sub, channel)
	}
	compose_v2.WithChannel(sub, results)
	var prepared, accepted, receipted, merged, reported, delivered atomic.Int32
	sub.AddNode("left", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		prepared.Add(1)
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "deliver", Input: []compose_v2.ChannelWrite{compose_v2.Write(private, "alpha")}},
			compose_v2.SendDirective{Target: "deliver", Input: []compose_v2.ChannelWrite{compose_v2.Write(private, "beta")}},
		)
		return []compose_v2.ChannelWrite{compose_v2.Write(left, "L")}, nil
	})
	sub.AddNode("relay", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	sub.AddNode("right", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "右侧确认？"})
		if err != nil {
			return nil, err
		}
		accepted.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(right, answer.Content)}, nil
	})
	sub.AddNode("receipt", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		receipted.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(receipt, "receipt")}, nil
	})
	sub.AddNode("merge", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		got := compose_v2.MustGet(ch, left) + "+" + compose_v2.MustGet(ch, right) + "+" + compose_v2.MustGet(ch, receipt)
		if got != "L+R+receipt" {
			return nil, fmt.Errorf("join saw incomplete results %q", got)
		}
		merged.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(joined, "L+R")}, nil
	})
	sub.AddNode("report", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if compose_v2.MustGet(ch, joined) != "L+R" {
			return nil, errors.New("report ran before the joined result was committed")
		}
		reported.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(results, []string{"L+R"})}, nil
	})
	sub.AddNode("deliver", func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		result, _ := compose_v2.Get(ch, joined)
		label, _ := compose_v2.Get(ch, private)
		if result != "L+R" || (label != "alpha" && label != "beta") {
			return nil, fmt.Errorf("delivery missing joined result or private input: %q, %q", result, label)
		}
		if label == "beta" {
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发送 beta？"})
			if err != nil {
				return nil, err
			}
			label += "/" + answer.Content
		}
		delivered.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(results, []string{label})}, nil
	})
	sub.AddEdge(compose_v2.StartNode, "left").AddEdge(compose_v2.StartNode, "relay").
		AddEdge(compose_v2.StartNode, "report").AddEdge("left", "report").
		AddEdge("relay", "right").AddEdge("relay", "receipt").
		AddJoinEdge([]compose_v2.NodeID{"right", "left"}, "merge").
		AddJoinEdge([]compose_v2.NodeID{"merge"}, "report")
	child, err := sub.Compile()
	if err != nil {
		t.Fatal(err)
	}
	b := compose_v2.NewBuilder().WithName("root-barriers")
	compose_v2.WithChannel(b, results)
	b.AddSubgraphNode("review", child, compose_v2.WithExport(results)).
		AddEdge(compose_v2.StartNode, "review").AddEdge("review", compose_v2.EndNode)
	root, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	newAgent := func() *agent_v2.GraphAgent {
		return agent_v2.NewGraphAgent(root, agent_v2.WithGraphAgentName("schedule"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
				items, _ := compose_v2.Get(ch, results)
				return schema.AssistantMessage(strings.Join(items, "|"))
			}))
	}

	_, err = newAgent().Run(compose_v2.WithCheckpointerCtx(ctx, cp), session, schema.UserMessage("执行"))
	rightID := requireScheduleQuestion(t, err, "右侧确认？")
	childNS := compose_v2.JoinVisitNS("", "review", 0)
	waiting, err := cp.LoadNS(ctx, session.RunID, childNS)
	if err != nil || waiting == nil ||
		!slices.Equal(waiting.JoinProgress[0], []compose_v2.NodeID{"left"}) ||
		!slices.Equal(waiting.CurrentFrontier, []compose_v2.FrontierMember{{NodeID: "right"}}) ||
		!slices.Equal(waiting.CompletedFrontier, []compose_v2.FrontierMember{{NodeID: "receipt"}}) ||
		len(waiting.DeferredFrontier) != 3 || len(waiting.FrontierInputs) != 2 ||
		prepared.Load() != 1 || merged.Load() != 0 || reported.Load() != 0 || delivered.Load() != 0 {
		t.Fatalf("普通工作挂起时应保留汇合和收尾，prepared=%d merged=%d reported=%d delivered=%d cp=%+v err=%v",
			prepared.Load(), merged.Load(), reported.Load(), delivered.Load(), waiting, err)
	}

	resume := func(id, answer string) ([]*agent_v2.AgentEvent, error) {
		reopened := crewsql.NewCheckpointer(db, crewsql.SQLite{})
		return newAgent().Resume(compose_v2.WithCheckpointerCtx(ctx, reopened), session.RunID,
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage(answer)}))
	}

	// 模拟后端保留了等待成员但损坏了它的输入绑定。父档仍需展示原来的问题。
	corrupt := *waiting
	corrupt.FrontierInputs = maps.Clone(waiting.FrontierInputs)
	for id, input := range corrupt.FrontierInputs {
		delete(corrupt.FrontierInputs, id)
		corrupt.FrontierInputs["iorphan"] = input
		break
	}
	if err := cp.SaveNS(ctx, session.RunID, childNS, &corrupt); err != nil {
		t.Fatal(err)
	}
	_, restoreErr := resume(rightID, "R")
	hostPending, err := cp.LoadNS(ctx, session.RunID, "")
	stillWaiting, childErr := cp.LoadNS(ctx, session.RunID, childNS)
	if !errors.Is(restoreErr, compose_v2.ErrCheckpointNotResumable) || err != nil || childErr != nil ||
		hostPending == nil || hostPending.Status != compose_v2.CPStatusPending || len(hostPending.PendingInterrupts) != 1 ||
		hostPending.PendingInterrupts[0].InterruptID != rightID ||
		stillWaiting == nil || stillWaiting.Epoch != waiting.Epoch || accepted.Load() != 0 {
		t.Fatalf("坏子档应保留，父档应保留原问题，err=%v parent=%+v child=%+v load=[%v %v]",
			restoreErr, hostPending, stillWaiting, err, childErr)
	}
	if err := cp.SaveNS(ctx, session.RunID, childNS, waiting); err != nil {
		t.Fatal(err)
	}

	_, err = resume(rightID, "R")
	deliveryID := requireScheduleQuestion(t, err, "发送 beta？")
	released, err := cp.LoadNS(ctx, session.RunID, childNS)
	if err != nil || released == nil || len(released.JoinProgress) != 0 || len(released.DeferredFrontier) != 0 ||
		len(released.CurrentFrontier) != 1 || released.CurrentFrontier[0].NodeID != "deliver" ||
		len(released.FrontierInputs) != 1 || merged.Load() != 1 || reported.Load() != 1 || delivered.Load() != 1 {
		t.Fatalf("汇合成功后应释放收尾并只保留挂起实例，merged=%d reported=%d delivered=%d cp=%+v err=%v",
			merged.Load(), reported.Load(), delivered.Load(), released, err)
	}

	events, err := resume(deliveryID, "sent")
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "L+R|alpha|beta/sent" {
		t.Fatalf("续跑应按原身份完成所有业务投递，final=%+v err=%v", final, err)
	}
	if prepared.Load() != 1 || accepted.Load() != 1 || receipted.Load() != 1 ||
		merged.Load() != 1 || reported.Load() != 1 || delivered.Load() != 2 {
		t.Fatalf("已完成的节点重跑，prepare=%d right=%d receipt=%d merge=%d report=%d deliver=%d",
			prepared.Load(), accepted.Load(), receipted.Load(), merged.Load(), reported.Load(), delivered.Load())
	}
	if slots, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(slots) != 0 {
		t.Fatalf("父子档均应清理：slots=%v err=%v", slots, err)
	}
}

func requireScheduleQuestion(t *testing.T, err error, question string) string {
	t.Helper()
	paused, ok := compose_v2.IsInterrupt(err)
	if !ok || len(paused.AllQueries()) != 1 {
		t.Fatalf("应挂起一次等待 %q，err=%v", question, err)
	}
	got := paused.AllQueries()[0]
	if got.Question != question || got.InterruptID == "" {
		t.Fatalf("挂起项缺少原问题或 ID：%+v", got)
	}
	return got.InterruptID
}
