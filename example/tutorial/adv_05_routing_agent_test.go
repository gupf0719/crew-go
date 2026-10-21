//go:build integration

package tutorial_test

import (
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 5. RoutingAgent（进阶）— 四域技术支持路由（含 Fallback）
// ──────────────────────────────────────────────────────────────────

func TestAdv_05_RoutingAgent(t *testing.T) {
	provider := requireAPIKey(t)

	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithSystemPrompt(
			"你是技术支持工单分类器。根据用户描述，只输出下列类别之一（不加任何其他内容）：\n"+
				"database - 数据库问题\n"+
				"network  - 网络/连接问题\n"+
				"security - 安全/权限问题\n"+
				"code     - 代码/逻辑问题",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	dbExpert := newBasicAgentV2(provider, "database",
		"你是数据库专家（PostgreSQL/MySQL/Redis）。给出具体的查询优化、索引或连接池建议。")
	netExpert := newBasicAgentV2(provider, "network",
		"你是网络工程师。诊断 TCP/HTTP/DNS 问题，给出 Linux 命令级别的排查步骤。")
	secExpert := newBasicAgentV2(provider, "security",
		"你是安全工程师（OWASP Top10 专家）。分析漏洞并给出修复代码示例。")
	codeExpert := newBasicAgentV2(provider, "code",
		"你是高级 Go/Python 工程师。定位代码 Bug，给出带注释的修复版本。")
	generalSupport := newBasicAgentV2(provider, "general",
		"你是通用技术支持。对无法归类的问题给出初步排查建议，并告知用户可能需要升级工单。")

	ra := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("tech_support_router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(generalSupport),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"database": "SQL 慢查询、索引缺失、连接池耗尽、主从延迟等数据库问题",
			"network":  "API 超时、DNS 解析失败、TLS 握手错误、防火墙拦截等网络问题",
			"security": "SQL 注入、XSS、权限绕过、Token 泄露等安全漏洞",
			"code":     "空指针、内存泄漏、并发竞争、算法错误等代码逻辑问题",
		}),
	).
		AddBranch("database", dbExpert).
		AddBranch("network", netExpert).
		AddBranch("security", secExpert).
		AddBranch("code", codeExpert)

	events, err := ra.Run(ctx300s(t), nil,
		schema.UserMessage("我们的 PostgreSQL 查询在数据量超过 100 万行后变得非常慢，EXPLAIN 显示全表扫描，怀疑缺少索引"))
	if err != nil {
		t.Fatalf("RoutingAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)
	t.Logf("路由后专家回复: %s", out.Content)

	if out.Content == "" {
		t.Error("输出不应为空")
	}
}

// TestAdv_05_RoutingDescriptionsInjection 真实 LLM 端到端验证 WithRoutingDescriptions 注入生效。
//
// **与 TestAdv_05_RoutingAgent 的关键区别**：那个测试的 classifier system prompt 手抄了
// 分支清单（"database - 数据库问题\n..."），所以注入与否都能路由——**无法验证注入**。
// 本测试 classifier system prompt **故意不含任何分支名/清单**，只指示"从用户消息里给出的
// 清单选"——路由对完全依赖 WithRoutingDescriptions 把分支描述注入 classifier input。
// 若注入是 no-op（修复前的 dead-field bug），classifier 无从知道有哪些候选分支名，无法正确路由。
func TestAdv_05_RoutingDescriptionsInjection(t *testing.T) {
	provider := requireAPIKey(t)

	// classifier system prompt 不含任何分支名/清单——只能靠注入的清单路由
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithSystemPrompt(
			"你是路由分类器。用户消息里会给出一份『可路由的目标分支』清单（每行格式 branch_name: 描述）。"+
				"请严格从该清单里挑出最匹配用户问题的 branch_name，只输出这个 branch_name 本身，"+
				"不要输出任何其他文字，也不要编造清单里没有的名字。",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	billing := newBasicAgentV2(provider, "billing", "你处理账单/发票问题，简短回复。")
	shipping := newBasicAgentV2(provider, "shipping", "你处理物流/配送问题，简短回复。")
	refund := newBasicAgentV2(provider, "refund", "你处理退款/售后问题，简短回复。")

	ra := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("ecommerce_support"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"billing":  "账单、发票、扣费、付款方式相关问题",
			"shipping": "物流、配送、快递、签收、发货时效相关问题",
			"refund":   "退货、退款、售后、商品质量相关问题",
		}),
	).
		AddBranch("billing", billing).
		AddBranch("shipping", shipping).
		AddBranch("refund", refund)

	events, err := ra.Run(ctx300s(t), nil,
		schema.UserMessage("我下单的快递已经五天了还没送到，能帮我查下配送进度吗？"))
	if err != nil {
		t.Fatalf("RoutingAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)
	t.Logf("路由后回复: %s", out.Content)

	// 期望路由到 shipping（靠注入的 "shipping: 物流、配送..." 描述选中）——
	// classifier system prompt 无清单，路由正确完全证明 WithRoutingDescriptions 注入生效。
	if agent_v2.FinalMessageByAgent(events, "shipping") == nil {
		t.Errorf("WithRoutingDescriptions 注入未让 classifier 路由到 shipping 分支——"+
			"classifier system prompt 无候选清单，路由对全靠注入；若注入 no-op 则 classifier 不知分支名。final=%q", out.Content)
	}
}

// TestAdv_05_RoutingAgent_RouteTool 真 LLM × priority-1（tool_call 信号）：
// classifier 装备框架内置 route_to（WithRouteTool，return-direct）——四编排
// review RT-M3 后此路径首次有真 LLM 覆盖（TestAdv_05_RoutingAgent 走 priority-3
// 纯文本；单测 route_tool_test.go 走 stub model）。
func TestAdv_05_RoutingAgent_RouteTool(t *testing.T) {
	provider := requireAPIKey(t)

	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("tool_classifier"),
		agent_v2.WithSystemPrompt(
			"你是技术支持工单路由器。必须调用 route_to 工具选择目标分支（target 只能是 "+
				"database / code 之一），不要自己回答用户问题。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithRouteTool(),
	)

	dbExpert := newBasicAgentV2(provider, "database",
		"你是数据库专家。回答必须包含关键词「索引」。")
	codeExpert := newBasicAgentV2(provider, "code",
		"你是 Go 工程师。回答必须包含关键词「重构」。")

	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("tool_router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"database": "数据库问题（慢查询/索引/连接池）",
			"code":     "代码/逻辑问题",
		}),
	).
		AddBranch("database", dbExpert).
		AddBranch("code", codeExpert)

	events, err := r.Run(ctx300s(t), agent_v2.NewRunSession(),
		schema.UserMessage("我们的 PostgreSQL 订单表查询越来越慢，单表 5000 万行，怎么优化？"))
	if err != nil {
		t.Fatalf("RoutingAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)
	t.Logf("=== [RouteTool 路由终答（前 300 字符）] ===\n%.300s", out.Content)

	// 确定性锚：数据库问题应路由到 database 分支（其 prompt 强制含「索引」）
	if !strings.Contains(out.Content, "索引") {
		t.Errorf("应路由到 database 分支（回答含「索引」），got %.200s", out.Content)
	}
}
