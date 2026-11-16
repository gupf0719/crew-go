package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

type nestedSummaryViewKey struct{}

func TestAutoCompact_ChangedViewRestartsNestedApproval(t *testing.T) {
	const reviewInput = "Review the current working summary."
	var approved []string
	inputCh := compose_v2.LastValue[*schema.Message]("summary-review-input")
	outputCh := compose_v2.LastValue[*schema.Message]("summary-review-output")
	child := compose_v2.NewBuilder().WithName("summary-review-child")
	compose_v2.WithChannel(child, inputCh)
	compose_v2.WithChannel(child, outputCh)
	child.AddNode("approve", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		input, _ := compose_v2.Get(chs, inputCh)
		if input == nil || input.Content != reviewInput {
			return nil, errors.New("child did not receive its stable review input")
		}
		view, _ := ctx.Value(nestedSummaryViewKey{}).(string)
		if view == "" {
			return nil, errors.New("missing summary view in nested callback")
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve digest of " + view})
		if err != nil {
			return nil, err
		}
		if ans.Content != "approve "+view {
			return nil, fmt.Errorf("view %s received answer %q", view, ans.Content)
		}
		approved = append(approved, view)
		return nil, nil
	})
	child.AddNode("heading", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		view, _ := ctx.Value(nestedSummaryViewKey{}).(string)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "provide heading for " + view})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, schema.AssistantMessage(ans.Content))}, nil
	})
	child.AddEdge(compose_v2.StartNode, "approve").AddEdge("approve", "heading").AddEdge("heading", compose_v2.EndNode)
	childGraph, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	reviewer := agent_v2.NewGraphAgent(childGraph, agent_v2.WithGraphAgentName("reviewer"),
		agent_v2.WithGraphInputWriter(func(msg *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, msg)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			msg, _ := compose_v2.Get(chs, outputCh)
			return msg
		}),
	)
	summary := &fnModel{fn: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		view := "A"
		if countUserContent(in, "view B:") == 1 {
			view = "B"
		} else if countUserContent(in, "view A:") != 1 {
			return nil, errors.New("summary does not see the requested view")
		}
		// 审批对象由业务上下文交给子图，嵌套调用的普通消息身份保持稳定。
		ctx = context.WithValue(ctx, nestedSummaryViewKey{}, view)
		events, err := agent_v2.RunSubAgent(ctx, "review", reviewer, schema.UserMessage(reviewInput))
		if err != nil {
			return nil, err
		}
		result := agent_v2.FinalMessage(events)
		if result == nil {
			return nil, errors.New("review returned no heading")
		}
		return schema.AssistantMessage("<summary>" + result.Content + "</summary>"), nil
	}}
	var view string
	var gotSummary []*schema.Message
	builder := compose_v2.NewBuilder().WithName("summary-review")
	builder.AddNode("summary", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs := []*schema.Message{
			schema.UserMessage("view " + view + ": " + strings.Repeat("details ", 80)),
			schema.AssistantMessage("prior notes"), schema.UserMessage("latest query"),
		}
		var callErr error
		gotSummary, callErr = agent_v2.AutoCompactRun(ctx, compactionTestConfig(summary), msgs)
		return nil, callErr
	}).AddEdge(compose_v2.StartNode, "summary").AddEdge("summary", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	const sessionID = "summary-review-replay"
	run := func(next string, answers map[string]*schema.Message) error {
		view = next
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sessionID),
			compose_v2.WithResumeAnswers(answers))
	}
	idA := compactionQuestionID(t, run("A", nil), "approve digest of A")
	answers := map[string]*schema.Message{idA: schema.UserMessage("approve A")}
	_ = compactionQuestionID(t, run("A", answers), "provide heading for A")
	if len(approved) != 1 || approved[0] != "A" {
		t.Fatalf("first view was not approved: %v", approved)
	}
	// A 的第一个子图节点已经提交。B 要从自己的审批节点开始。
	idB := compactionQuestionID(t, run("B", answers), "approve digest of B")
	if idB == idA || len(approved) != 1 {
		t.Fatalf("视图更改后应单独审批，问题 %q/%q，已批准 %v", idA, idB, approved)
	}
	root, err := cp.LoadNS(ctx, sessionID, "")
	if err != nil || root == nil || len(root.PendingInterrupts) != 1 || root.PendingInterrupts[0].InterruptID != idB {
		t.Fatalf("root checkpoint should expose only the new approval: %+v, err=%v", root, err)
	}
	answers[idB] = schema.UserMessage("approve B")
	headingID := compactionQuestionID(t, run("B", answers), "provide heading for B")
	answers[headingID] = schema.UserMessage("approved B summary")
	if err := run("B", answers); err != nil || len(approved) != 2 || approved[1] != "B" ||
		countUserContent(gotSummary, "approved B summary") != 1 {
		t.Fatalf("approved replacement summary did not complete: approvals %v, summary %+v, err=%v", approved, gotSummary, err)
	}
	if snapshots, err := cp.LoadAllInSession(ctx, sessionID); err != nil || len(snapshots) != 0 {
		t.Fatalf("successful parent should clean its old and current descendants: %+v, err=%v", snapshots, err)
	}
}
