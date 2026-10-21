//go:build integration

package tutorial_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// ──────────────────────────────────────────────────────────────────
// 10. Hooks — 生产级可观测性（ChainHooks 串联多个 Hook）
//
// 自定义 Hook 三步走：
//
//	第一步：定义结构体，嵌入 hook.NoOpHooks
//	        NoOpHooks 已将接口的 9 个方法全部实现为空操作，
//	        嵌入后只需覆盖关心的那几个，其余保持 NoOp 即可。
//
//	第二步：覆盖需要的回调方法
//	        框架提供的 9 个回调点：
//	          OnAgentStart      — Agent.Run 进入时，任何逻辑执行前
//	          OnAgentStart     — 首轮 LLM 请求前，可读取输入消息
//	          OnAgentEnd       — Agent.Run 返回前，可读取全部输出
//	          OnModelStart     — 每轮 LLM 请求发出前
//	          OnModelEnd       — 每轮 LLM 响应返回后，可读取 Token 用量
//	          OnModelEndStream — 流式响应时，可包装 StreamReader 做拦截
//	          OnToolStart      — 每次工具调用发起时
//	          OnToolEnd        — 每次工具调用完成后，可读取返回结果
//	          OnError          — 执行出错时（不阻止错误向上传播）
//
//	第三步：agent_v2.WithHooks(h) 注册给 Agent
//	        注册后框架全权负责在合适的时机触发，
//	        业务代码无需手动调用任何 Hook 方法。
//
// 多个 Hook 用 hook.NewChainHooks(...) 串联，框架按注册顺序依次触发。
// ──────────────────────────────────────────────────────────────────

// errorCaptureHook 捕获运行期错误，只需覆盖 OnError 一个方法。
// 其余 8 个方法通过嵌入的 hook.NoOpHooks 继承为空操作。
type errorCaptureHook struct {
	hook.NoOpHooks
	mu      sync.Mutex
	records []string
}

// OnError 由框架在 Agent 执行出错时自动触发。
func (h *errorCaptureHook) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	if err == nil {
		return ctx
	}
	h.mu.Lock()
	h.records = append(h.records, fmt.Sprintf("[%s] %s", info.Name, err.Message))
	h.mu.Unlock()
	return ctx
}

// lifecycleLogHook 记录 Agent 生命周期事件，覆盖 OnAgentStart 和 OnAgentEnd。
// 其余 7 个方法通过嵌入的 hook.NoOpHooks 继承为空操作。
type lifecycleLogHook struct {
	hook.NoOpHooks
	mu  sync.Mutex
	log []string
}

// OnAgentStart 由框架在 Agent.Run 进入时自动触发（任何 LLM 调用之前）。
func (h *lifecycleLogHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.mu.Lock()
	h.log = append(h.log, fmt.Sprintf("START agent=%s type=%s", info.Name, info.Type))
	h.mu.Unlock()
	return ctx, nil
}

// OnAgentEnd 由框架在 Agent.Run 返回前自动触发（所有 LLM 调用之后）。
func (h *lifecycleLogHook) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, output *hook.AgentOutput) context.Context {
	h.mu.Lock()
	h.log = append(h.log, fmt.Sprintf("END   agent=%s round=%d msgs=%d", info.Name, info.Round, len(output.Messages)))
	h.mu.Unlock()
	return ctx
}

func TestAdv_10_Hooks(t *testing.T) {
	// 本 Demo 完整演示"自定义 Hook"的三步用法：
	//   第一步：定义各 Hook 结构体（见本节开头的类型定义）
	//   第二步：ChainHooks 串联，框架按注册顺序依次触发每个 Hook
	//   第三步：WithHooks 注册给 Agent，此后框架全权负责调用，业务代码无感
	provider := requireAPIKey(t)

	// 网络诊断工具包
	pingTool := tool.NewFuncTool(
		"ping",
		"检测目标主机的网络连通性，返回延迟和丢包率",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"host": map[string]any{"type": "string", "description": "目标主机名或 IP"},
			},
			"required": []string{"host"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			host, _ := args["host"].(string)
			results := map[string]string{
				"8.8.8.8":     "延迟: 12ms，丢包率: 0%",
				"192.168.1.1": "延迟: 1ms，丢包率: 0%",
				"10.0.0.1":    "请求超时，丢包率: 100%",
			}
			if r, ok := results[host]; ok {
				return tool.Success(r), nil
			}
			return tool.Success(fmt.Sprintf("主机 %s：延迟 45ms，丢包率 0%%", host)), nil
		},
	)

	tracerouteTool := tool.NewFuncTool(
		"traceroute",
		"追踪数据包从本机到目标的路由路径",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"host": map[string]any{"type": "string", "description": "目标主机"},
			},
			"required": []string{"host"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			host, _ := args["host"].(string)
			return tool.Success(fmt.Sprintf(
				"路由追踪到 %s: 本机→网关(1ms)→ISP(5ms)→骨干网(20ms)→目标(45ms)", host,
			)), nil
		},
	)

	dnsLookupTool := tool.NewFuncTool(
		"dns_lookup",
		"查询域名的 DNS 解析结果",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"domain": map[string]any{"type": "string", "description": "要查询的域名"},
			},
			"required": []string{"domain"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			domain, _ := args["domain"].(string)
			return tool.Success(fmt.Sprintf("%s A记录: 93.184.216.34，TTL: 3600s", domain)), nil
		},
	)

	tk := tool.NewToolkit()
	tk.RegisterAll(pingTool, tracerouteTool, dnsLookupTool)

	// ── 第一步：实例化各 Hook（类型定义见本节开头）────────────────
	// 每个 Hook 只关注自己感兴趣的生命周期点，互不干扰：
	th := newTimingHook(t)         // 覆盖 OnModelStart/End、OnToolStart → 统计耗时和 Token
	errHook := &errorCaptureHook{} // 覆盖 OnError → 收集运行期错误
	lcHook := &lifecycleLogHook{}  // 覆盖 OnAgentStart/End → 记录生命周期事件

	// ── 第二步：ChainHooks 串联多个 Hook ─────────────────────────
	// 框架触发某个回调时，会按注册顺序依次调用每个 Hook 的对应方法：
	//   th.OnModelEnd → errHook.OnModelEnd(NoOp) → lcHook.OnModelEnd(NoOp)
	//   th.OnAgentStart(NoOp) → errHook.OnAgentStart(NoOp) → lcHook.OnAgentStart
	chainedHooks := hook.NewChainHooks(th, errHook, lcHook)

	// ── 第三步：WithHooks 注册给 Agent，框架从此接管触发 ─────────
	// 注册后无需在业务代码中手动调用任何 Hook 方法，
	// 框架在每个生命周期节点自动触发 chainedHooks 中的对应回调。
	netDiagAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("network_diagnostics"),
		agent_v2.WithSystemPrompt(
			"你是网络诊断专家。当用户报告网络问题时：\n"+
				"1. 用 ping 检测连通性\n"+
				"2. 若超时则用 traceroute 定位断点\n"+
				"3. 若是域名问题则用 dns_lookup 查询解析\n"+
				"综合工具结果给出专业诊断报告，包括问题根因和修复建议。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(chainedHooks), // 注册后框架全权负责调用，业务代码无需感知
		agent_v2.WithMaxRounds(6),
	)

	events, err := netDiagAgent.Run(ctx300s(t), nil,
		schema.UserMessage("我无法访问 example.com，ping 192.168.1.1 也超时，请帮我诊断"))
	if err != nil {
		t.Fatalf("网络诊断 Agent 失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)

	// 以下数据均由框架自动触发 Hook 回调填充，无需业务代码主动上报
	t.Logf("=== 可观测性统计（由 Hook 自动采集）===")
	t.Logf("模型调用次数: %d（每次 OnModelEnd 递增）", th.modelCalls)
	t.Logf("工具调用次数: %d（每次 OnToolStart 递增）", th.toolStarts)
	t.Logf("总输入 Token: %d，总输出 Token: %d（来自 OnModelEnd.output.Usage）",
		th.totalInputTok, th.totalOutputTok)
	lcHook.mu.Lock()
	t.Logf("生命周期日志（OnAgentStart/End 填充）: %v", lcHook.log)
	lcHook.mu.Unlock()
	errHook.mu.Lock()
	t.Logf("捕获错误（OnError 填充）: %v", errHook.records)
	errHook.mu.Unlock()
	t.Logf("诊断报告: %s", out.Content)

	if th.modelCalls == 0 {
		t.Error("期望至少一次模型调用")
	}
}
