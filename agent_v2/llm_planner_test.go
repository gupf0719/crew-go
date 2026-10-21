package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// plannerStubModel 模拟 LLM 按 tool_call 返结构化输出——支持验证：
//   - first call 只挂 plan tool 时返 plan
//   - replan call (executed 非空) 时挂 plan+respond，按 stub.cycleResp 决定哪个
//   - lastMsgs 捕获 caller 传给 Generate 的 messages（验证 prompt 注入）
type plannerStubModel struct {
	calls     int
	lastTools []*schema.ToolInfo
	lastMsgs  []*schema.Message
	respFunc  func(call int, tools []*schema.ToolInfo) *schema.Message
}

func (m *plannerStubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	m.lastMsgs = msgs
	return m.respFunc(m.calls, m.lastTools), nil
}
func (m *plannerStubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *plannerStubModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.lastTools = tools
	return m, nil
}

var _ model.ChatModel = (*plannerStubModel)(nil)

// makeToolCallMsg 构造带 tool_call 的 assistant 消息——LLMPlanner 按 tool_call.Name
// 路由 plan/respond 解析。
func makeToolCallMsg(toolName, args string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_1", Function: schema.FunctionCall{Name: toolName, Arguments: args}},
	})
}

// TestLLMPlanner_FirstCallReturnsPlan 验证首次调用（executed nil）只挂 plan tool，
// LLM 返 plan tool_call → 解析为 PlanStep 列表。
func TestLLMPlanner_FirstCallReturnsPlan(t *testing.T) {
	stub := &plannerStubModel{
		respFunc: func(call int, tools []*schema.ToolInfo) *schema.Message {
			// 验证 first call 只挂 1 个 tool（plan）
			if len(tools) != 1 || tools[0].Name != "plan" {
				return makeToolCallMsg("plan", `{"steps":[{"description":"WRONG_should_be_plan_only","depends_on":[]}]}`)
			}
			return makeToolCallMsg("plan", `{"steps":[
				{"description":"step 1","depends_on":[]},
				{"description":"step 2","depends_on":[0]}
			]}`)
		},
	}
	planner := agent_v2.NewLLMPlanner(stub)
	steps, finalAns, err := planner(context.Background(), agent_v2.NewRunSession(), "do something", nil)
	if err != nil {
		t.Fatalf("planner err: %v", err)
	}
	if finalAns != "" {
		t.Fatalf("first call should not return finalAnswer, got %q", finalAns)
	}
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(steps))
	}
	if steps[0].Description != "step 1" || steps[1].Description != "step 2" {
		t.Fatalf("steps content wrong: %+v", steps)
	}
	// step 2 depends_on=[0] → IndexRef(0)
	if len(steps[1].InputRefs) != 1 || steps[1].InputRefs[0].Index == nil || *steps[1].InputRefs[0].Index != 0 {
		t.Fatalf("step 2 InputRefs wrong: %+v", steps[1].InputRefs)
	}
}

// TestLLMPlanner_ReplanWithRespond 验证 replan 调用（executed 非空）挂 plan+respond
// 两个 tool，LLM 选 respond → finalAnswer 非空 + steps 空 → 终止。
func TestLLMPlanner_ReplanWithRespond(t *testing.T) {
	stub := &plannerStubModel{
		respFunc: func(call int, tools []*schema.ToolInfo) *schema.Message {
			// 验证 replan 时挂 plan + respond
			toolNames := make([]string, len(tools))
			for i, tt := range tools {
				toolNames[i] = tt.Name
			}
			gotPlan, gotRespond := false, false
			for _, n := range toolNames {
				if n == "plan" {
					gotPlan = true
				}
				if n == "respond" {
					gotRespond = true
				}
			}
			if !gotPlan || !gotRespond {
				// 触发明显错误的输出让上层断言能 fail
				return makeToolCallMsg("plan", `{"steps":[{"description":"WRONG_replan_should_have_both_tools","depends_on":[]}]}`)
			}
			return makeToolCallMsg("respond", `{"answer":"all done"}`)
		},
	}
	planner := agent_v2.NewLLMPlanner(stub)
	executed := []agent_v2.ExecutedStep{{Step: "step1", Agent: "x", Result: "y"}}
	steps, finalAns, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", executed)
	if err != nil {
		t.Fatalf("planner err: %v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("respond call should return empty steps, got %d", len(steps))
	}
	if finalAns != "all done" {
		t.Fatalf("expected finalAnswer 'all done', got %q", finalAns)
	}
}

// TestLLMPlanner_SubAgentMetaInjection 验证 WithLLMPlannerSubAgents 把 sub-agent
// Description 注入 prompt + tool schema enum——加 sub-agent 不用改 PlannerPrompt。
func TestLLMPlanner_SubAgentMetaInjection(t *testing.T) {
	// 构造 stub sub-agents 实现 Description()
	makeStubAgent := func(name, desc string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithDescription(desc),
			agent_v2.WithModel(&plannerStubModel{respFunc: func(int, []*schema.ToolInfo) *schema.Message { return schema.AssistantMessage("") }}),
		)
	}
	subAgents := map[string]agent_v2.Agent{
		"researcher": makeStubAgent("researcher", "查资料、列事实"),
		"writer":     makeStubAgent("writer", "整理段落"),
	}

	var capturedTools []*schema.ToolInfo
	stub := &plannerStubModel{
		respFunc: func(call int, tools []*schema.ToolInfo) *schema.Message {
			capturedTools = tools
			return makeToolCallMsg("plan", `{"steps":[{"description":"x","sub_agent":"researcher","depends_on":[]}]}`)
		},
	}
	planner := agent_v2.NewLLMPlanner(stub, agent_v2.WithLLMPlannerSubAgents(subAgents))
	_, _, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", nil)
	if err != nil {
		t.Fatalf("planner err: %v", err)
	}

	// 验证 plan tool schema 含 sub_agent 字段 + enum 值
	if len(capturedTools) == 0 {
		t.Fatal("no tools captured")
	}
	planTool := capturedTools[0]
	props := planTool.Params["properties"].(map[string]any)
	steps := props["steps"].(map[string]any)
	items := steps["items"].(map[string]any)
	stepProps := items["properties"].(map[string]any)
	subAgentField, ok := stepProps["sub_agent"].(map[string]any)
	if !ok {
		t.Fatalf("plan tool schema missing sub_agent field: %+v", stepProps)
	}
	enum, ok := subAgentField["enum"].([]string)
	if !ok || len(enum) != 2 {
		t.Fatalf("sub_agent enum wrong: %+v", subAgentField["enum"])
	}
	// 验证字典序
	if enum[0] != "researcher" || enum[1] != "writer" {
		t.Fatalf("sub_agent enum order wrong (expect dict-sort): %+v", enum)
	}
	// 验证 description 含两个 agent 描述（interface contract pattern）
	desc, _ := subAgentField["description"].(string)
	if !strings.Contains(desc, "查资料") || !strings.Contains(desc, "整理段落") {
		t.Fatalf("sub_agent description should contain Description() text: %q", desc)
	}

	// **audit Bug #3 修复**：验证 user prompt 也注入了 sub-agent 列表（不仅 schema）
	// 之前测试只 capture tools，如 buildLLMPlannerUserInput 漏掉 sub-agent 列表
	// 测试无法 fail。现 capture lastMsgs 直接断言 prompt content。
	if len(stub.lastMsgs) < 2 {
		t.Fatalf("expected ≥2 msgs (system + user), got %d", len(stub.lastMsgs))
	}
	// 找 user msg
	var userPrompt string
	for _, m := range stub.lastMsgs {
		if m.Role == schema.RoleUser {
			userPrompt = m.Content
			break
		}
	}
	if !strings.Contains(userPrompt, "researcher") || !strings.Contains(userPrompt, "查资料") {
		t.Fatalf("user prompt should contain sub-agent listing 'researcher: 查资料', got:\n%s", userPrompt)
	}
	if !strings.Contains(userPrompt, "writer") || !strings.Contains(userPrompt, "整理段落") {
		t.Fatalf("user prompt should contain sub-agent listing 'writer: 整理段落', got:\n%s", userPrompt)
	}
}

// TestLLMPlanner_ErrorSentinels 验证哨兵错误分类——caller 用 errors.Is 分流。
func TestLLMPlanner_ErrorSentinels(t *testing.T) {
	t.Run("no_tool_call", func(t *testing.T) {
		stub := &plannerStubModel{
			respFunc: func(int, []*schema.ToolInfo) *schema.Message {
				return schema.AssistantMessage("free text not tool_call")
			},
		}
		planner := agent_v2.NewLLMPlanner(stub)
		_, _, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", nil)
		if !errors.Is(err, agent_v2.ErrPlannerNoToolCall) {
			t.Fatalf("expected ErrPlannerNoToolCall, got %v", err)
		}
	})

	t.Run("empty_steps", func(t *testing.T) {
		stub := &plannerStubModel{
			respFunc: func(int, []*schema.ToolInfo) *schema.Message {
				return makeToolCallMsg("plan", `{"steps":[]}`)
			},
		}
		planner := agent_v2.NewLLMPlanner(stub)
		_, _, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", nil)
		if !errors.Is(err, agent_v2.ErrPlannerEmptySteps) {
			t.Fatalf("expected ErrPlannerEmptySteps, got %v", err)
		}
	})

	t.Run("bad_tool_name", func(t *testing.T) {
		stub := &plannerStubModel{
			respFunc: func(int, []*schema.ToolInfo) *schema.Message {
				return makeToolCallMsg("hallucinated_tool", `{}`)
			},
		}
		planner := agent_v2.NewLLMPlanner(stub)
		// 用 executed 非空让 respond tool 也挂上——但 LLM 还是 hallucinate 别的
		executed := []agent_v2.ExecutedStep{{Step: "x", Agent: "y", Result: "z"}}
		_, _, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", executed)
		if !errors.Is(err, agent_v2.ErrPlannerBadToolCall) {
			t.Fatalf("expected ErrPlannerBadToolCall, got %v", err)
		}
	})

	t.Run("parse_args", func(t *testing.T) {
		stub := &plannerStubModel{
			respFunc: func(int, []*schema.ToolInfo) *schema.Message {
				return makeToolCallMsg("plan", `not valid json {`)
			},
		}
		planner := agent_v2.NewLLMPlanner(stub)
		_, _, err := planner(context.Background(), agent_v2.NewRunSession(), "goal", nil)
		if !errors.Is(err, agent_v2.ErrPlannerParseArgs) {
			t.Fatalf("expected ErrPlannerParseArgs, got %v", err)
		}
	})
}
