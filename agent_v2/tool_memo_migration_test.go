package agent_v2_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 将真实中断档的逐调用记录装成 v20 写出的批次格式，再跨 JSON 往返续跑。
func TestToolMemo_MigratesPendingV20Batch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	var paid, asks, heuristics atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(countingFuncTool("pay", &paid, false))
	tk.Register(tool.NewFuncTool("pause", "pause", map[string]any{"type": "object"},
		func(c context.Context, _ map[string]any) (*tool.Result, error) {
			asks.Add(1)
			answer, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "确认交付"})
			if err != nil {
				return nil, err
			}
			return tool.Success(answer.Content), nil
		}))
	original := toolCallsMessage("alias", "pause")
	calls := schema.GetToolCalls(original)
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(original),
		func(in []*schema.Message) (*schema.Message, error) {
			values := make(map[string]string)
			for _, msg := range in {
				if id := schema.GetToolResultID(msg); id != "" {
					values[id] = msg.Content
				}
			}
			if values[calls[0].ID] != "ran_pay" || values[calls[1].ID] != "yes" {
				t.Errorf("模型收到错误工具结果 %v", values)
			}
			return schema.AssistantMessage("done"), nil
		},
	}}
	base := []agent_v2.Option{agent_v2.WithName("legacy-ledger"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(&rewriteLedgerTool{to: "pay"})}
	a := agent_v2.NewBasicAgent(append(base, agent_v2.WithApprovalHeuristic(
		func(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
			heuristics.Add(1)
			if name == "pause" {
				return agent_v2.ApprovalDecisionAsk
			}
			return agent_v2.ApprovalDecisionAllow
		}))...)
	s := agent_v2.NewRunSession()
	_, err := a.Run(ctx, s, schema.UserMessage("start"))
	approval, ok := compose_v2.IsInterrupt(err)
	if !ok || len(approval.AllQueries()) != 1 || paid.Load() != 1 {
		t.Fatalf("expected completed pay and one approval, paid=%d err=%v", paid.Load(), err)
	}
	approvalID := approval.AllQueries()[0].InterruptID
	answers := map[string]*schema.Message{approvalID: schema.UserMessage("approve")}
	_, err = a.Resume(ctx, s.RunID, agent_v2.WithResumeSession(s), agent_v2.WithResumeUserInputs(answers))
	ask, ok := compose_v2.IsInterrupt(err)
	if !ok || len(ask.AllQueries()) != 1 || ask.AllQueries()[0].Question != "确认交付" || asks.Load() != 1 {
		t.Fatalf("expected approved tool's own question, calls=%d err=%v", asks.Load(), err)
	}

	saved, err := cp.LoadNS(ctx, s.RunID, "")
	if err != nil || saved == nil {
		t.Fatalf("expected pending checkpoint, got %v %v", saved, err)
	}
	legacy := oldToolMemoCheckpoint(t, saved, calls[0].ID+"#0#pay", calls[1].ID+"#1#pause")
	if err := cp.SaveNS(ctx, s.RunID, "", legacy); err != nil {
		t.Fatal(err)
	}

	// 丢掉批准答案，且换一条审批策略。旧档中的批准和决策仍适用于同一次调用。
	delete(answers, approvalID)
	answers[ask.AllQueries()[0].InterruptID] = schema.UserMessage("yes")
	resumed := agent_v2.NewBasicAgent(append(base, agent_v2.WithApprovalHeuristic(
		func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
			t.Error("approval should be replayed")
			return agent_v2.ApprovalDecisionDeny
		}))...)
	if _, err := resumed.Resume(ctx, s.RunID, agent_v2.WithResumeUserInputs(answers)); err != nil {
		t.Fatal(err)
	}
	if paid.Load() != 1 || asks.Load() != 2 || heuristics.Load() != 2 || m.count() != 2 {
		t.Fatalf("expected replay of pay and approval only, paid=%d pause=%d heuristic=%d model=%d",
			paid.Load(), asks.Load(), heuristics.Load(), m.count())
	}
}

type rewriteLedgerTool struct {
	hook.NoOpHooks
	to string
}

func (h *rewriteLedgerTool) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == "alias" {
		in.Name = h.to
	}
	return ctx, nil
}

func oldToolMemoCheckpoint(t *testing.T, saved *compose_v2.Checkpoint, payKey, pauseKey string) *compose_v2.Checkpoint {
	t.Helper()
	raw, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	var states map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wire["node_states"], &states); err != nil {
		t.Fatal(err)
	}
	legacy := struct {
		Done     map[string]json.RawMessage           `json:"done"`
		Approved map[string]bool                      `json:"approved"`
		Decided  map[string]agent_v2.ApprovalDecision `json:"decided"`
	}{make(map[string]json.RawMessage), make(map[string]bool), make(map[string]agent_v2.ApprovalDecision)}
	var base string
	for key, value := range states {
		var tagged struct {
			Type string
			Data struct {
				Result   json.RawMessage
				Approved bool
				Decision *agent_v2.ApprovalDecision
			}
		}
		if err := json.Unmarshal(value, &tagged); err != nil || tagged.Type != "*agent_v2.toolCallRecord" {
			continue
		}
		base, _, _ = strings.Cut(key, ":scope:")
		legacyKey := pauseKey
		if len(tagged.Data.Result) != 0 {
			legacyKey = payKey
			legacy.Done[legacyKey] = tagged.Data.Result
		}
		legacy.Approved[legacyKey] = tagged.Data.Approved
		if tagged.Data.Decision != nil {
			legacy.Decided[legacyKey] = *tagged.Data.Decision
		}
		delete(states, key)
	}
	if base == "" || legacy.Done[payKey] == nil || !legacy.Approved[pauseKey] ||
		legacy.Decided[pauseKey] != agent_v2.ApprovalDecisionAsk {
		t.Fatalf("fixture needs a completed result and approval record, got %+v", legacy)
	}
	tagged, err := json.Marshal(map[string]any{"type": "*agent_v2.toolCallMemo", "data": legacy})
	if err != nil {
		t.Fatal(err)
	}
	states[base] = tagged
	wire["node_states"], err = json.Marshal(states)
	if err != nil {
		t.Fatal(err)
	}
	wire["schema_ver"] = json.RawMessage("20")
	raw, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := compose_v2.UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
