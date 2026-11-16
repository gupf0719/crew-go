package compose_v2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestResumeAnswerScopeSurvivesNestedGraphAndPhase(t *testing.T) {
	var accepted []string
	childBuilder := compose_v2.NewBuilder()
	childBuilder.AddNode("approve", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ctx = compose_v2.EnterPhase(ctx, "review")
		if _, ok := compose_v2.GetResumeContext(ctx); ok {
			return nil, errors.New("审批相位可直接读取恢复输入")
		}
		if _, approved := compose_v2.GetStepResult[bool](ctx, "approved"); !approved {
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认当前视图？"})
			if err != nil {
				return nil, err
			}
			accepted = append(accepted, answer.Content)
			if err := compose_v2.SaveStepResult(ctx, "approved", true); err != nil {
				return nil, err
			}
		}
		_, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "continue"), &compose_v2.AskQuery{Question: "继续？"})
		return nil, err
	})
	childBuilder.AddEdge(compose_v2.StartNode, "approve").AddEdge("approve", compose_v2.EndNode)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := compose_v2.NewBuilder()
	parentBuilder.AddSubgraphNode("summary", child)
	parentBuilder.AddEdge(compose_v2.StartNode, "summary").AddEdge("summary", compose_v2.EndNode)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	run := func(scope string, answers map[string]*schema.Message) error {
		return compose_v2.Run(compose_v2.WithResumeAnswerScope(ctx, scope), parent, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("summary-scope"),
			compose_v2.WithResumeAnswers(answers))
	}
	questionID := func(err error, question string) string {
		t.Helper()
		interrupted, ok := compose_v2.IsInterrupt(err)
		if !ok || len(interrupted.AllQueries()) != 1 {
			t.Fatalf("预期子图提出一条确认，实际 %v", err)
		}
		q := interrupted.AllQueries()[0]
		if q.Question != question {
			t.Fatalf("预期问题 %q，实际 %q", question, q.Question)
		}
		return q.InterruptID
	}
	firstID := questionID(run("view A", nil), "确认当前视图？")
	answers := map[string]*schema.Message{firstID: schema.UserMessage("approve A")}
	continueA := questionID(run("view A", answers), "继续？")
	answers[continueA] = schema.UserMessage("continue A")
	secondID := questionID(run("view B", answers), "确认当前视图？")
	if firstID == secondID || len(accepted) != 1 || accepted[0] != "approve A" {
		t.Fatalf("新视图兑付了旧答复，问题 %q/%q，已接受 %v", firstID, secondID, accepted)
	}
	answers[secondID] = schema.UserMessage("approve B")
	continueB := questionID(run("view B", answers), "继续？")
	if continueA == continueB {
		t.Fatalf("新视图复用了继续问题的 ID %q", continueB)
	}
	answers[continueB] = schema.UserMessage("continue B")
	if err := run("view B", answers); err != nil || len(accepted) != 2 || accepted[1] != "approve B" {
		t.Fatalf("同一视图未收到自己的答复，已接受 %v，错误 %v", accepted, err)
	}
}
