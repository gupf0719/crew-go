package observability_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"crew-go/agent_v2"
	"crew-go/decision"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

// inferenceModel 测试时保留实际绑定的工具，并把按次选项交给下游断言。
type inferenceModel struct {
	tools []*schema.ToolInfo
	reply func([]*schema.Message, []*schema.ToolInfo, *model.Options) (*schema.Message, error)
}

func (m inferenceModel) ModelName() string { return "shared-model" }

func (m inferenceModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.tools = tools
	return m, nil
}

func (m inferenceModel) Generate(_ context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	return m.reply(msgs, m.tools, model.Apply(opts...))
}

func (m inferenceModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	reply, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(reply), nil
}

func inferenceReply(text string) *schema.Message {
	reply := schema.AssistantMessage(text)
	reply.Metadata = &schema.MessageMetadata{Model: "shared-model", System: "mock", FinishReason: "stop",
		Usage: &schema.MessageUsage{InputTokens: 7, OutputTokens: 2}}
	return reply
}

// TestDecisionModelPurpose 让分类、评估、反馈和普通成员采用同名 agent 和模型，再按真实调用用途核对统计。
func TestDecisionModelPurpose(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("shared-agent"))
	makeAgent := func(reply func(string) string) *agent_v2.BasicAgent {
		m := inferenceModel{reply: func(msgs []*schema.Message, _ []*schema.ToolInfo, _ *model.Options) (*schema.Message, error) {
			return inferenceReply(reply(msgs[len(msgs)-1].Content)), nil
		}}
		return agent_v2.NewBasicAgent(append(opts, agent_v2.WithModel(m))...)
	}

	ordinary := makeAgent(func(input string) string {
		if strings.Contains(input, "补充金额") {
			return "金额 42"
		}
		return "已收到"
	})
	classifier := makeAgent(func(input string) string {
		if input != "查账单" {
			t.Errorf("分类器收到 %q", input)
		}
		return "billing"
	})
	router := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("route"), agent_v2.WithRoutingHooks(h.obs.AgentHooks()), agent_v2.WithRoutingClassifier(classifier)).
		AddBranch("billing", ordinary)
	events, err := router.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查账单"))
	if final := agent_v2.FinalMessage(events); err != nil || final.GetContent() != "已收到" {
		t.Fatalf("路由未执行命中的成员，消息 %v，错误 %v", final, err)
	}

	evaluator := makeAgent(func(input string) string {
		if !strings.Contains(input, "待评估方案") {
			t.Errorf("评估器收到 %q", input)
		}
		if strings.Contains(input, "金额 42") {
			return "yes"
		}
		return "缺少金额"
	})
	feedback := makeAgent(func(input string) string {
		if !strings.Contains(input, "缺少金额") {
			t.Errorf("反馈教练收到 %q", input)
		}
		return "补充金额"
	})
	reflector := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"), agent_v2.WithReflectHooks(h.obs.AgentHooks()), agent_v2.WithReflectMaxRounds(2),
		agent_v2.WithEvaluator(evaluator), agent_v2.WithReflectorAgent(feedback)).WithActor(ordinary)
	events, err = reflector.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("写账单"))
	if final := agent_v2.FinalMessage(events); err != nil || final.GetContent() != "金额 42" {
		t.Fatalf("反思未将反馈交回 actor，消息 %v，错误 %v", final, err)
	}

	want := map[string]int{"": 3, "routing": 1, "evaluation": 2, "reflection": 1}
	for _, name := range []string{"chat shared-model", "node model", "invoke_agent shared-agent"} {
		got := make(map[string]int)
		for _, span := range h.spansNamed(name) {
			role, ok := attrOf(span, observability.AttrModelPurpose)
			if ok && role.AsString() == "" {
				t.Errorf("%s 普通调用不应导出空用途属性", name)
			}
			got[role.AsString()]++
		}
		for role, n := range want {
			if got[role] != n {
				t.Errorf("%s 的用途 %q 数量 = %d，期望 %d（全部 %v）", name, role, got[role], n, got)
			}
		}
	}

	rm := h.collect(t)
	duration, ok := findMetric(rm, observability.MetricInferenceDuration)
	if !ok {
		t.Fatal("缺推理时长")
	}
	var ordinaryCount uint64
	for _, point := range duration.Data.(metricdata.Histogram[float64]).DataPoints {
		if _, found := point.Attributes.Value(observability.AttrModelPurpose); !found {
			ordinaryCount += point.Count
		}
	}
	if ordinaryCount != uint64(want[""]) {
		t.Errorf("原时间序列的普通调用数 = %d，期望 %d", ordinaryCount, want[""])
	}
	for _, role := range []string{"routing", "evaluation", "reflection"} {
		n := want[role]
		labels := map[string]string{"crew.model.purpose": role}
		for _, metric := range []string{observability.MetricInferenceDuration, observability.MetricInferenceTTFC, observability.MetricInvokeAgentInferences} {
			if got := histCount(t, rm, metric, labels); got != uint64(n) {
				t.Errorf("%s 的用途 %q 次数 = %d，期望 %d", metric, role, got, n)
			}
		}
		if got := sumInt(t, rm, observability.MetricUsageInputTokens, labels); got != int64(n*7) {
			t.Errorf("用途 %q 的输入 token = %d，期望 %d", role, got, n*7)
		}
		if got := sumInt(t, rm, observability.MetricNodeExecutions, map[string]string{"crew.model.purpose": role, "crew.node.id": "model"}); got != int64(n) {
			t.Errorf("用途 %q 的模型节点次数 = %d，期望 %d", role, got, n)
		}
	}
	h.assertAllEnded(t)
}

func TestEvaluatorFailureKeepsInferencePurpose(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background())
	actor := agent_v2.NewBasicAgent(append(opts, agent_v2.WithName("actor"), agent_v2.WithModel(&scriptModel{}))...)
	providerErr := errors.New("provider rejected evaluation")
	evaluator := agent_v2.NewBasicAgent(append(opts, agent_v2.WithName("evaluator"), agent_v2.WithModel(&scriptModel{fail: providerErr}))...)
	ag := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"), agent_v2.WithEvaluator(evaluator)).WithActor(actor)
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("evaluate")); !errors.Is(err, providerErr) {
		t.Fatalf("评估器应返回原始错误，得到 %v", err)
	}
	span := h.span(t, "chat")
	if got := strAttr(t, span, observability.AttrModelPurpose); got != "evaluation" {
		t.Errorf("评估失败的 chat span 用途 = %q", got)
	}
	rm := h.collect(t)
	if got := histCount(t, rm, observability.MetricInferenceDuration, map[string]string{"crew.model.purpose": "evaluation", "error.type": "model_call_error"}); got != 1 {
		t.Errorf("评估失败的推理时长样本 = %d，期望 1", got)
	}
	h.assertAllEnded(t)
}

func TestLLMRouteDeciderInferencePurpose(t *testing.T) {
	h := newHarness(t, observability.WithContentRecording(true))
	inner := newHarness(t)
	ctx, opts := h.obs.Install(context.Background())
	var calls int
	build := func(fail bool) agent_v2.RouteDecider {
		m := inferenceModel{reply: func(msgs []*schema.Message, tools []*schema.ToolInfo, request *model.Options) (*schema.Message, error) {
			calls++
			if msgs[len(msgs)-1].Content != "查账单" || len(tools) != 0 || request.ExtraBody["logprobs"] != true {
				t.Errorf("采样路由请求不符预期，消息 %v，工具 %v，配置 %v", msgs, tools, request)
			}
			if fail {
				return nil, errors.New("routing model unavailable")
			}
			reply := inferenceReply("A")
			logprob := -0.1
			reply.Metadata.Logprobs = []*schema.LogProbContent{{LogProb: schema.LogProb{TokenText: "A", Logprob: &logprob}}}
			return reply, nil
		}}
		return decision.NewLLMRouteDecider(m, func(sample decision.LLMRouteSample) (float64, bool) {
			return 0.9, sample.Target == "billing" && sample.Logprob == -0.1
		})
	}
	worker := agent_v2.NewBasicAgent(append(opts, agent_v2.WithName("worker"), agent_v2.WithModel(&scriptModel{}))...)
	router := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("route"),
		agent_v2.WithRouteDecider("first", build(true), 0.8),
		agent_v2.WithRouteDecider("second", build(false), 0)).AddBranch("billing", worker)
	chain := agent_v2.NewChainOrchHooks(h.obs.OrchHooks(inner.obs.OrchHooks()),
		agent_v2.NewChainOrchHooks(agent_v2.NoOpOrchestrationHooks{}, h.obs.OrchHooks()))
	events, err := router.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查账单"), agent_v2.WithOrchHooks(chain))
	if final := agent_v2.FinalMessage(events); err != nil || final.GetContent() != "最终答复：查账单" {
		t.Fatalf("采样路由未升级到第二档并执行成员，消息 %v，错误 %v", final, err)
	}
	classify := inner.span(t, "node classify")
	chats := h.spansNamed("chat shared-model")
	if calls != 2 || len(chats) != 2 {
		t.Fatalf("采样路由应有两次真实模型调用，执行 %d 次，找到 %d 个 chat span，全部 %v", calls, len(chats), names(h.spans()))
	}
	for _, span := range chats {
		if got := strAttr(t, span, observability.AttrModelPurpose); got != "routing" || span.Parent().SpanID() != classify.SpanContext().SpanID() {
			t.Errorf("采样路由 chat span 用途 = %q，父 span = %s", got, span.Parent().SpanID())
		}
		if got := strAttr(t, span, observability.AttrAgentName); got != "route" {
			t.Errorf("采样路由 chat span 的 agent 名 = %q，期望 route", got)
		}
		if _, ok := attrOf(span, observability.AttrInputMessages); ok {
			t.Error("采样路由的推理统计额外导出了模型输入")
		}
		if _, ok := attrOf(span, observability.AttrOutputMessages); ok {
			t.Error("采样路由的推理统计额外导出了模型输出")
		}
	}
	rm := h.collect(t)
	role := map[string]string{"crew.model.purpose": "routing"}
	if got := histCount(t, rm, observability.MetricInferenceDuration, role); got != 2 {
		t.Errorf("采样路由推理时长样本 = %d，期望 2", got)
	}
	if got := histCount(t, rm, observability.MetricInferenceDuration, map[string]string{"crew.model.purpose": "routing", "error.type": "model_call_error"}); got != 1 {
		t.Errorf("采样路由失败样本 = %d，期望 1", got)
	}
	if got := sumInt(t, rm, observability.MetricUsageInputTokens, role); got != 7 {
		t.Errorf("采样路由输入 token = %d，期望 7", got)
	}
	if got := histCount(t, inner.collect(t), observability.MetricInferenceDuration, role); got != 2 {
		t.Errorf("内层观测实例的采样路由推理时长样本 = %d，期望 2", got)
	}
	evs, err := router.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查账单"), agent_v2.WithOrchHooks(agent_v2.NoOpOrchestrationHooks{}))
	if final := agent_v2.FinalMessage(evs); err != nil || final.GetContent() != "最终答复：查账单" || calls != 4 {
		t.Fatalf("禁用采样路由观测后，业务仍应再调用两次，消息 %v，错误 %v，累计调用 %d", final, err, calls)
	}
	if got := len(h.spansNamed("chat shared-model")); got != 2 {
		t.Errorf("显式覆盖编排钩子后，采样路由 span 总数 = %d，期望 2", got)
	}
	h.assertAllEnded(t)
	inner.assertAllEnded(t)
}

func TestNestedLLMRouteDeciderOwnsItsInference(t *testing.T) {
	for _, tc := range []struct {
		name, router string
		detach       bool
	}{
		{name: "嵌套路由", router: "nested-route"},
		{name: "同名独立路由", router: "weather_agent", detach: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			modelStub := inferenceModel{reply: func(_ []*schema.Message, _ []*schema.ToolInfo, _ *model.Options) (*schema.Message, error) {
				reply := inferenceReply("A")
				logprob := -0.1
				reply.Metadata.Logprobs = []*schema.LogProbContent{{LogProb: schema.LogProb{TokenText: "A", Logprob: &logprob}}}
				return reply, nil
			}}
			router := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName(tc.router),
				agent_v2.WithRouteDecider("llm", decision.NewLLMRouteDecider(modelStub,
					func(sample decision.LLMRouteSample) (float64, bool) { return 1, sample.Target == "billing" }), 0)).
				AddBranch("billing", agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(&scriptModel{})))
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("route", "运行分类", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					input := schema.UserMessage("查账单")
					var events []*agent_v2.AgentEvent
					var err error
					if tc.detach {
						events, err = router.Run(agent_v2.DetachTask(ctx), agent_v2.NewRunSession(), input)
					} else {
						events, err = agent_v2.RunSubAgent(ctx, "nested-route", router, input)
					}
					if err != nil {
						return nil, err
					}
					return &tool.Result{Success: true, Output: agent_v2.FinalMessage(events).GetContent()}, nil
				}))
			events, err := runWeatherAgent(t, h, &scriptModel{toolName: "route", toolArgs: `{}`}, tk, "route this")
			if final := agent_v2.FinalMessage(events); err != nil || final.GetContent() != "最终答复：最终答复：查账单" {
				t.Fatalf("路由未返回成员输出，消息 %v，错误 %v", final, err)
			}
			if got := strAttr(t, h.span(t, "chat shared-model"), observability.AttrAgentName); got != tc.router {
				t.Errorf("路由 chat span 的 agent 名 = %q，期望 %q", got, tc.router)
			}
			if got := strAttr(t, h.span(t, "invoke_agent weather_agent"), "crew.agent.inference_calls"); got != "2" {
				t.Errorf("父 agent 的真实推理次数 = %s，期望 2", got)
			}
			h.assertAllEnded(t)
		})
	}
}

func TestInferenceOutputTypeFollowsNativeRequest(t *testing.T) {
	for _, tc := range []struct {
		name, wireFormat, outputType string
		opts                         []model.Option
	}{
		{name: "json_object", wireFormat: "json_object", outputType: "json", opts: []model.Option{model.WithJSONObject()}},
		{name: "json_schema", wireFormat: "json_schema", outputType: "json", opts: []model.Option{model.WithJSONSchema("answer", nil, false)}},
		{name: "text", wireFormat: "text", outputType: "text", opts: []model.Option{model.WithResponseFormat(&model.ResponseFormat{Type: "text"})}},
		{name: "清除格式", opts: []model.Option{model.WithJSONObject(), model.WithResponseFormat(nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("extract"), agent_v2.WithModel(inferenceModel{
				reply: func(_ []*schema.Message, _ []*schema.ToolInfo, request *model.Options) (*schema.Message, error) {
					var format string
					if request.ResponseFormat != nil {
						format = request.ResponseFormat.Type
					}
					if format != tc.wireFormat {
						t.Errorf("模型收到输出格式 %q，期望 %q", format, tc.wireFormat)
					}
					return inferenceReply(`{"value":"ready"}`), nil
				},
			}))
			ag := agent_v2.NewBasicAgent(opts...)
			evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("extract"), agent_v2.WithRunModelOptions(tc.opts...))
			if final := agent_v2.FinalMessage(evs); err != nil || final.GetContent() != `{"value":"ready"}` {
				t.Fatalf("最终答复 %v，错误 %v", final, err)
			}
			value, ok := attrOf(h.span(t, "chat shared-model"), observability.AttrOutputType)
			if ok != (tc.outputType != "") || value.AsString() != tc.outputType {
				t.Errorf("输出类型 = %q（存在 %t），期望 %q", value.AsString(), ok, tc.outputType)
			}
		})
	}
}

func TestInferenceOutputTypeFollowsFrameworkSchema(t *testing.T) {
	for _, tc := range []struct {
		name, args string
		valid      bool
	}{
		{name: "校验通过", args: `{"value":"ready"}`, valid: true},
		{name: "校验失败", args: `{"value":7}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var calls int
			m := inferenceModel{reply: func(_ []*schema.Message, tools []*schema.ToolInfo, request *model.Options) (*schema.Message, error) {
				calls++
				if len(tools) != 1 || tools[0].Name != agent_v2.FinalOutputToolName || request.ResponseFormat != nil ||
					request.ToolChoice == nil || *request.ToolChoice != schema.ToolChoiceRequired {
					t.Errorf("模型应收到框架 final_output 工具及选择约束，工具 %v，配置 %v", tools, request)
				}
				return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "out", Type: "function",
					Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: tc.args}}}), nil
			}}
			ctx, opts := h.obs.Install(context.Background(), agent_v2.WithName("extract"), agent_v2.WithModel(m),
				agent_v2.WithOutputSchema("answer", map[string]any{
					"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"},
				}, agent_v2.WithMaxValidationRetries(0)))
			ag := agent_v2.NewBasicAgent(opts...)
			evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("extract"))
			if tc.valid {
				if final := agent_v2.FinalMessage(evs); err != nil || final.GetContent() != tc.args {
					t.Fatalf("最终 JSON %v，错误 %v", final, err)
				}
			} else if _, ok := errors.AsType[*agent_v2.StructuredOutputError](err); !ok {
				t.Fatalf("应返回 schema 校验失败，得到 %v", err)
			}
			chats := h.spansNamed("chat shared-model")
			if calls != 1 || len(chats) != calls {
				t.Fatalf("框架结构化请求应实际调用一次，执行 %d 次，记录 %d 个 chat span", calls, len(chats))
			}
			for _, span := range chats {
				if got := strAttr(t, span, observability.AttrOutputType); got != "json" {
					t.Errorf("框架 schema 请求的输出类型 = %q，期望 json", got)
				}
			}
		})
	}
}

func TestBusinessFinalOutputToolHasNoRequestedOutputType(t *testing.T) {
	h := newHarness(t)
	m := &scriptModel{toolName: agent_v2.FinalOutputToolName, toolArgs: `{}`}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(agent_v2.FinalOutputToolName, "业务工具", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "业务结果"}, nil
		}))
	evs, err := runWeatherAgent(t, h, m, tk, "run tool")
	if final := agent_v2.FinalMessage(evs); err != nil || final.GetContent() != "最终答复：业务结果" {
		t.Fatalf("业务工具未执行，最终答复 %v，错误 %v", final, err)
	}
	for _, span := range h.spansNamed("chat mock-model") {
		if value, ok := attrOf(span, observability.AttrOutputType); ok {
			t.Errorf("业务同名工具不声明模型输出格式，得到 %v", value)
		}
	}
}
