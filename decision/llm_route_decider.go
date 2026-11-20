package decision

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"

	"crew-go/agent_v2"
	"crew-go/internal/inference"
	"crew-go/model"
	"crew-go/schema"
)

const routeLabels = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// LLMRouteSample 是交给业务校准函数的原始观测。Model 和 Provider 来自响应元数据，
// Provider 未配置时为空。Candidates 保留发送给模型的注册顺序，Logprob 是所选标签的对数概率。
type LLMRouteSample struct {
	Provider   string
	Model      string
	Target     string
	Candidates []agent_v2.RoutingChoice
	Logprob    float64
}

// NewLLMRouteDecider 用支持 logprobs 的非流式 Chat Completions 模型做一档文本路由。
// 使用未绑定工具的专用模型，业务采样和上线使用同一请求配置。
// 在 [agent_v2.WithRouteDecider] 中配置门槛。calibrate 必须按业务样本校准，返回 [0, 1] 的
// 置信度及是否适用。响应模型版本、候选或请求配置超出校准范围时返回 false。
// 同一决策方可被并发调用，calibrate 需要支持并发。
//
// 只采纳以 stop 结束、与唯一可见词元一致的已知标签。没有文本、候选数量超出 1–26、
// 拒答、缺少可用概率或没有适用校准时弃权。Scores 中所选分支保留 exp(Logprob) 供观测。
// 模型调用失败时返回原始错误，由 RoutingAgent 判定升级或结束。模型与校准函数均为必填。
func NewLLMRouteDecider(m model.ChatModel, calibrate func(LLMRouteSample) (float64, bool)) agent_v2.RouteDecider {
	if m == nil || calibrate == nil {
		panic("decision.NewLLMRouteDecider: model and calibrator are required")
	}
	switch v := reflect.ValueOf(m); v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		if v.IsNil() {
			panic("decision.NewLLMRouteDecider: nil model")
		}
	}
	return func(ctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		input := req.Input.GetContent()
		if strings.TrimSpace(input) == "" || len(req.Candidates) == 0 || len(req.Candidates) > len(routeLabels) {
			return agent_v2.RouteDecision{}, nil
		}
		reply, err := inference.Generate(ctx, m, []*schema.Message{
			schema.SystemMessage(llmRoutePrompt(req.Candidates)),
			schema.UserMessage(input),
		}, model.WithTemperature(0), model.WithMaxTokens(16), model.WithToolChoice(schema.ToolChoiceNone),
			model.WithExtraBody(map[string]any{"logprobs": true}))
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return agent_v2.RouteDecision{}, err
		}
		if reply == nil {
			return agent_v2.RouteDecision{}, errors.New("decision: routing model returned no message")
		}
		if reply.Metadata == nil {
			return agent_v2.RouteDecision{}, nil
		}
		dec := agent_v2.RouteDecision{Source: reply.Metadata.Model}
		target, logprob, ok := llmRouteEvidence(reply, req.Candidates)
		if !ok {
			return dec, nil
		}
		dec.Scores = map[string]float64{target: math.Exp(logprob)}
		if confidence, ok := calibrate(LLMRouteSample{
			Provider: reply.Metadata.System, Model: reply.Metadata.Model,
			Target: target, Candidates: slices.Clone(req.Candidates), Logprob: logprob,
		}); ok {
			dec.Target = target
			dec.Confidence = confidence
		}
		return dec, nil
	}
}

func llmRoutePrompt(choices []agent_v2.RoutingChoice) string {
	var prompt strings.Builder
	prompt.WriteString(DefaultRouteInstructions)
	prompt.WriteString("\nReturn exactly one label from the list. Return 0 if no choice applies.\n")
	for i, choice := range choices {
		prompt.WriteString(fmt.Sprintf("%c: %q %q\n", routeLabels[i], choice.Name, choice.Description))
	}
	return prompt.String()
}

func llmRouteEvidence(reply *schema.Message, choices []agent_v2.RoutingChoice) (string, float64, bool) {
	md := reply.Metadata
	if reply.Role != schema.RoleAssistant || reply.Refusal != "" || len(schema.GetToolCalls(reply)) > 0 ||
		strings.TrimSpace(md.Model) == "" || md.FinishReason != "stop" || len(md.Logprobs) != 1 {
		return "", 0, false
	}
	token := md.Logprobs[0]
	if token == nil || token.Logprob == nil || token.TokenText != reply.Content {
		return "", 0, false
	}
	if len(token.Bytes) != 0 && !slices.EqualFunc(token.Bytes, []byte(token.TokenText), func(wire int32, expected byte) bool {
		return wire == int32(expected)
	}) {
		return "", 0, false
	}
	label := strings.TrimSpace(token.TokenText)
	if len(label) != 1 {
		return "", 0, false
	}
	i := strings.IndexByte(routeLabels[:len(choices)], label[0])
	logprob := *token.Logprob
	// OpenAI 用 -9999 表示未提供准确值。校准前也拒绝非有限值和非法的正对数概率。
	if i < 0 || logprob == -9999 || logprob > 0 || math.IsNaN(logprob) || math.IsInf(logprob, 0) {
		return "", 0, false
	}
	return choices[i].Name, logprob, true
}
