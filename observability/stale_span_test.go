package observability

// `ErrCheckpointStale` 走到 agent span 上的处置。
//
// # 为什么单独测
//
// 这个信号跟其它错误不是一档：它意味着**同一会话正被两个副本处理**，副作用可能已经
// 重复发生了。业务在监控平台上看到 `gen_ai_stale_writes` 尖峰后的第一个动作一定是"点进去
// 看是哪个 agent、哪次会话"——所以 `metrics_mapping` 里必须有它的反查路径，否则那个
// 尖峰只是个数字。interrupt 当初加 mapping 就是这个理由，并发冲突只会更需要。
//
// 另一半是**脱敏**：守卫的错误文案里带 session id 和 epoch token，前者在 trace 里本就
// 以 `gen_ai.request.id` 存在（重复无益），后者对监控是纯噪声。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// staleModel 让 BasicAgent 的 model 节点吐出守卫会产出的错误形态，走完整的
// graph → agent wrapper → OnAgentEnd 链路（而不是直调 hook）。
type staleModel struct{}

func (staleModel) staleErr() error {
	return fmt.Errorf("node %q: %w", "model", &compose_v2.StaleWriteError{
		Op: "purge-success", SessionID: "sess-uuid-1234", NS: "sub#42",
		Baseline: "aabbccdd", Current: "eeff0011",
		Detail: "槽已被别人推进",
	})
}

func (m staleModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, m.staleErr()
}

func (m staleModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, m.staleErr()
}

func (m staleModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = staleModel{}

func TestStaleWrite_AgentSpan_MappingAndRedaction(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("stale_agent"),
		agent_v2.WithModel(staleModel{}),
		agent_v2.WithHooks(obs.AgentHooks()),
	)
	const sid = "stale_span_sid"
	ctx := compose_v2.WithSessionIDCtx(context.Background(), sid)
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	_, runErr := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	obs.CleanupSession(sid)

	if runErr == nil {
		t.Fatal("staleModel 恒报错，Run 不该成功")
	}
	time.Sleep(500 * time.Millisecond)

	spans := srv.Spans()
	agentSpan := findSpanByOperation(spans, "agent")
	if agentSpan == nil {
		t.Fatalf("应有 agent span, ops=%v", spanOps(spans))
	}

	if got := findAttrString(agentSpan, "gen_ai.status"); got != "error" {
		t.Errorf("gen_ai.status = %q, want error", got)
	}

	// 反查路径：监控前端按 metrics_mapping 从 metric 图点进 trace
	raw := findAttrString(agentSpan, "metrics_mapping")
	// wire 形态是 {"metricsMapping": {metricName: {label: value}}}
	var envelope struct {
		MetricsMapping map[string]map[string]string `json:"metricsMapping"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		t.Fatalf("metrics_mapping 不是合法 JSON (%v): %s", err, raw)
	}
	mapping := envelope.MetricsMapping
	labels, ok := mapping[MetricStaleWriteTotal]
	if !ok {
		t.Fatalf("metrics_mapping 缺 %s——业务在监控平台看到并发冲突尖峰后点不进 trace，\n"+
			"  只知道'有并发'却定位不到是哪个 agent / 哪次会话。\n  实际 mapping: %v",
			MetricStaleWriteTotal, mapping)
	}
	// label 必须与实际 fire 的那次**完全一致**（多一个少一个前端都反查落空）
	if len(labels) != 1 || labels[LabelStaleOp] != "purge-success" {
		t.Errorf("mapping label 与实际 fire 不一致: got %v, want {%s: purge-success}\n"+
			"  前端拿 mapping 里的 label 去 metric 侧过滤，对不上就查不到数据",
			labels, LabelStaleOp)
	}
	if len(mapping) > 5 {
		t.Errorf("metrics_mapping 条目 %d 超上限 5: %v", len(mapping), mapping)
	}

	// error.message 要**够排查**：内部上报面不做脱敏（跟 agui / a2a 对前端那两处
	// 相反）——撞的是哪个 ns、哪一代 epoch，正是查并发冲突时唯一有用的东西。
	errMsg := findAttrString(agentSpan, "error.message")
	for _, want := range []string{"sub#42", "purge-success"} {
		if !strings.Contains(errMsg, want) {
			t.Errorf("error.message 缺 %q，排查时定位不到冲突位置: %s", want, errMsg)
		}
	}
	if got := findAttrString(agentSpan, "error.type"); got != "checkpoint_stale" {
		t.Errorf("error.type = %q, want checkpoint_stale（业务靠它把并发冲突从其它错误里筛出来）", got)
	}
}

// TestStaleWrite_MessageShapeConsistent：两条产生 stale 的路径必须给出**同形状**的
// error.message。
//
// 走 model 节点时 OnError 抢先把完整错误写进 span，OnAgentEnd 里那段只是 fallback、
// 仅在编排层直接返 stale（根本没进 model）时才生效。两边给不同形状，业务按
// error.message 建告警规则就会漏掉一半。
func TestStaleWrite_MessageShapeConsistent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	staleErr := staleModel{}.staleErr()
	const sid = "stale_fallback_sid"
	ctx := compose_v2.WithSessionIDCtx(context.Background(), sid)
	h := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "fallback_agent", Type: "base"}
	ctx, _ = h.OnAgentStart(ctx, info, &hook.AgentInput{})
	// 不 fire OnError——模拟编排层直接返 stale，走 OnAgentEnd 的 fallback 分支。
	// retErr 不走参数，由 agent_v2 Run/Resume 在 defer 前 push 进 ctx。
	h.OnAgentEnd(agent_v2.WithEndError(ctx, staleErr), info, &hook.AgentOutput{})
	obs.CleanupSession(sid)
	time.Sleep(500 * time.Millisecond)

	agentSpan := findSpanByOperation(srv.Spans(), "agent")
	if agentSpan == nil {
		t.Fatal("应有 agent span")
	}
	errMsg := findAttrString(agentSpan, "error.message")
	for _, want := range []string{"sub#42", "purge-success"} {
		if !strings.Contains(errMsg, want) {
			t.Errorf("fallback 路径的 error.message 缺 %q——与 OnError 路径形状不一致，\n"+
				"  业务按 message 建的告警规则会漏掉这一半: %s", want, errMsg)
		}
	}
}
