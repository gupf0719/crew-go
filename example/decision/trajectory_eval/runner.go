package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type observations struct {
	Route        string
	Outcome      string
	Calls        []invocation
	Approvals    []string
	Final        string
	InputTokens  int
	OutputTokens int
}

type recorder struct {
	hook.NoOpHooks
	mu sync.Mutex
	o  observations
}

func (r *recorder) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	r.mu.Lock()
	r.o.Route = info.Name
	r.mu.Unlock()
	return ctx, nil
}

func (r *recorder) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out != nil && out.Usage != nil && !hook.IsModelResponseReused(out.Extra) {
		r.mu.Lock()
		r.o.InputTokens += int(out.Usage.InputTokens)
		r.o.OutputTokens += int(out.Usage.OutputTokens)
		r.mu.Unlock()
	}
	return ctx, nil
}

const lookupTool = "lookup_asset"

func buildWorkflow(llm model.ChatModel, rec *recorder) agent_v2.Agent {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool(lookupTool, "Look up the status of an asset by key", map[string]any{
		"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}},
		"required": []string{"key"},
	}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
		rec.mu.Lock()
		rec.o.Calls = append(rec.o.Calls, invocation{Name: lookupTool, Args: maps.Clone(args)})
		rec.mu.Unlock()
		key, _ := args["key"].(string)
		if key == "" {
			return nil, errors.New("asset key is required")
		}
		return tool.Success("asset " + key + "=ready"), nil
	}))
	lookup := agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithModel(llm),
		agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(rec),
		agent_v2.WithSystemPrompt("For input 'lookup <key>', call lookup_asset once with that key. If it succeeds, reply with the exact asset status from the tool. If denied, reply denied."),
		agent_v2.WithMaxRounds(3))
	help := agent_v2.NewBasicAgent(agent_v2.WithName("help"), agent_v2.WithModel(llm), agent_v2.WithHooks(rec),
		agent_v2.WithSystemPrompt("For a help request, reply exactly: use lookup <key>"))
	return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("trajectory"),
		agent_v2.WithRouter(func(_ context.Context, msg *schema.Message) (compose_v2.NodeID, error) {
			if strings.HasPrefix(msg.Content, "lookup ") {
				return "lookup", nil
			}
			return "help", nil
		})).AddBranch("lookup", lookup).AddBranch("help", help)
}

func runFixture(ctx context.Context, c fixture, llm model.ChatModel) (observations, error) {
	var rec recorder
	workflow := buildWorkflow(llm, &rec)
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	events, err := workflow.Run(ctx, sess, schema.UserMessage(c.Input))
	remaining := c.Answers
	for {
		if err == nil {
			rec.o.Outcome = "success"
			if final := agent_v2.FinalMessage(events); final != nil {
				rec.o.Final = final.Content
			}
			break
		}
		interrupt, ok := compose_v2.IsInterrupt(err)
		if !ok {
			return rec.o, fmt.Errorf("workflow execution: %w", err)
		}
		queries := interrupt.AllQueries()
		for _, query := range queries {
			rec.o.Approvals = append(rec.o.Approvals, query.ToolName)
		}
		if len(remaining) == 0 {
			rec.o.Outcome = "interrupt"
			break
		}
		if len(queries) == 0 || len(remaining) < len(queries) {
			return rec.o, errors.New("fixture answers do not cover the pending approval batch")
		}
		responses := make(map[string]*schema.Message, len(queries))
		for i, query := range queries {
			answer := remaining[i]
			if answer.Tool != query.ToolName {
				return rec.o, fmt.Errorf("approval %q does not match answer for %q", query.ToolName, answer.Tool)
			}
			responses[query.InterruptID] = schema.UserMessage(answer.Value)
		}
		remaining = remaining[len(queries):]
		events, err = workflow.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(responses))
	}
	if len(remaining) != 0 {
		return rec.o, fmt.Errorf("%d fixture answers were unused", len(remaining))
	}
	return rec.o, compare(c.Expect, rec.o)
}

func compare(want expectations, got observations) error {
	var errs []error
	if got.Route != want.Route || got.Outcome != want.Outcome {
		errs = append(errs, fmt.Errorf("route/outcome got %s/%s, want %s/%s", got.Route, got.Outcome, want.Route, want.Outcome))
	}
	if !slices.Equal(want.Approvals, got.Approvals) {
		errs = append(errs, fmt.Errorf("approvals got %v, want %v", got.Approvals, want.Approvals))
	}
	if !slices.EqualFunc(want.Calls, got.Calls, func(a, b invocation) bool {
		return a.Name == b.Name && reflect.DeepEqual(a.Args, b.Args)
	}) {
		errs = append(errs, fmt.Errorf("tool calls got %v, want %v", got.Calls, want.Calls))
	}
	if !strings.Contains(got.Final, want.FinalContains) {
		errs = append(errs, fmt.Errorf("final output %q does not contain %q", got.Final, want.FinalContains))
	}
	return errors.Join(errs...)
}

type mockModel struct{}

func (mockModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		switch msg.Role {
		case schema.RoleTool:
			if strings.Contains(msg.Content, "=ready") {
				return schema.AssistantMessage(msg.Content), nil
			}
			return schema.AssistantMessage("denied"), nil
		case schema.RoleUser:
			key, lookup := strings.CutPrefix(msg.Content, "lookup ")
			if !lookup {
				return schema.AssistantMessage("use lookup <key>"), nil
			}
			args, err := json.Marshal(map[string]string{"key": key})
			if err != nil {
				return nil, err
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "lookup-1", Type: "function", Function: schema.FunctionCall{Name: lookupTool, Arguments: string(args)},
			}}), nil
		}
	}
	return nil, errors.New("mock model did not receive a user message")
}

func (m mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m mockModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
