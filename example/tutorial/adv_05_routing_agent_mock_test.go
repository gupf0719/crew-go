package tutorial_test

// adv_05 mock：RoutingAgent classifier + 4 路由（PostgreSQL → database 专家）
//
// 迁移到 agent_v2：
//   - constructor 改链式：NewRoutingAgent + AddBranch
//   - WithRoutingClassifier / WithRoutingFallback
//   - classifier 通过 fixedAgent 直接返路由名 message（绕开 BasicAgent ReAct loop）

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// classifierFixedAgent 测试用 stub——按 input 关键词返路由名 message
type classifierFixedAgent struct {
	name string
}

func (a *classifierFixedAgent) Run(_ context.Context, sess *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	target := "unknown"
	switch {
	case strings.Contains(input.Content, "PostgreSQL"), strings.Contains(input.Content, "SQL"), strings.Contains(input.Content, "数据库"):
		target = "database"
	case strings.Contains(input.Content, "DNS"), strings.Contains(input.Content, "网络"):
		target = "network"
	case strings.Contains(input.Content, "注入"), strings.Contains(input.Content, "安全"):
		target = "security"
	case strings.Contains(input.Content, "代码"), strings.Contains(input.Content, "Bug"):
		target = "code"
	}
	resp := schema.AssistantMessage(target)
	if sess != nil {
		sess.AddEvent(agent_v2.NewExitEvent(a.name, resp))
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, resp)}, nil
}
func (a *classifierFixedAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *classifierFixedAgent) Name() string        { return a.name }
func (a *classifierFixedAgent) Description() string { return "" }

func TestAdv_05_RoutingAgent_Mock(t *testing.T) {
	classifier := &classifierFixedAgent{name: "classifier"}

	makeExpert := func(name, answer string) agent_v2.Agent {
		stub := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
			return schema.AssistantMessage(answer)
		}}
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithSystemPrompt(name), agent_v2.WithModel(stub))
	}
	dbExpert := makeExpert("database", "建议加索引：CREATE INDEX idx ON table(col)")
	netExpert := makeExpert("network", "排查 DNS：dig + nslookup")
	secExpert := makeExpert("security", "用参数化查询防 SQL 注入")
	codeExpert := makeExpert("code", "Bug 修复：检查 nil 指针")
	general := makeExpert("general", "请提供更多信息")

	ra := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("tech_support_router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(general),
	).
		AddBranch("database", dbExpert).
		AddBranch("network", netExpert).
		AddBranch("security", secExpert).
		AddBranch("code", codeExpert)

	events, err := ra.Run(ctx60sMock(t), nil,
		schema.UserMessage("PostgreSQL 慢查询 100 万行 全表扫描"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("nil")
	}
	t.Logf("路由结果: %s", out.Content)
	if !strings.Contains(out.Content, "索引") {
		t.Errorf("期望路由到 database 专家（含'索引'），实际 %q", out.Content)
	}
}
