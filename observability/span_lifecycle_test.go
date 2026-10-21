// span 生命周期正确性测试 — 验 tool 类 span 在 OnToolEnd 后被**正常 endSpan**
// (status=success), 而非被 OnGraphEnd sweep 兜底标 error.
//
// ────────────────────────────────────────────────────────────────────────
// **根因回归测试 (eaa3fdf HIGH-1 + 方案 C)**
//
// bug: OnToolStart 用 routeToolSpanKind 返回的 spanKind (skill/retrieval/embedding/
// guardrail/evaluator) 存 in-flight span; 但 OnToolEnd 硬编码 endSpan(ctx, "tool", ...)
// → registry key 含 kind 维度 → "tool" key 跟实际存的 "skill"/"retrieval" key 不匹配
// → LoadAndDelete miss → 该 span 不被正常 end → 残留 registry → OnGraphEnd sweep
// 兜底 emit 但标 status=error + error.type=InFlightSweep (假错).
//
// 后果: 业务监控 dashboard 看到所有 skill/retrieval/embedding/guardrail/evaluator
// span 全是假红假错 (实际成功). commit eaa3fdf 的 5 kind 区分 feature 实际失效.
//
// 方案 C 修复: registry key 对有 callID 的 tool 类 span 用 callID 唯一锚点不掺 kind/
// name (对齐 OTel span 句柄 / LangChain run_id 范式) → endSpan 不管传啥 kind 都用
// callID 配对成功 → span 正常 end status=success.
//
// **TDD 设计 (现状即 bug 态)**:
//   - RetrievalEndsWithSuccess / SkillEndsWithSuccess: 方案 C 前 FAIL (status=error
//     from sweep), 方案 C 后 PASS — 抓住 non-"tool" kind 命中的 bug.
//   - PlainToolEndsWithSuccess: 对照组, plain tool kind="tool" 跟硬编码 "tool" 恰好
//     匹配, 方案 C 前后都 PASS — 证明不是测试 harness 坏, 是 non-tool kind 才命中.
//
// ────────────────────────────────────────────────────────────────────────
package observability

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestSpanLifecycle_RetrievalEndsWithSuccess — retrieval span 应正常 end status=success.
func TestSpanLifecycle_RetrievalEndsWithSuccess(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "vector_search",
		CallId:        "call_lifecycle_r",
		ArgumentsJSON: `{"query":"x"}`,
		Extra:         map[string]any{"category": "retrieval"},
	})
	got := findSpanByOperation(spans, "retrieval")
	if got == nil {
		t.Fatalf("应有 retrieval span")
	}
	// 核心断言: status=success (正常 OnToolEnd end), 不是 sweep 兜底的 error.
	if st := findAttrString(got, "gen_ai.status"); st != "success" {
		t.Errorf("retrieval span 应 status=success (正常 OnToolEnd end), got %q — "+
			"bug: endSpan kind 不匹配 callID-key, span 被 OnGraphEnd sweep 兜底标 error", st)
	}
	// 不应带 sweep 兜底标记 (说明 OnToolEnd endSpan 正常配对了, 没漏给 sweep).
	if et := findAttrString(got, "error.type"); et == "InFlightSweep" {
		t.Errorf("retrieval span 不应被 sweep 兜底 (error.type=InFlightSweep) — " +
			"说明 OnToolEnd endSpan 没正常配对, kind-in-key 失配")
	}
}

// TestSpanLifecycle_SkillEndsWithSuccess — skill span 应正常 end status=success.
func TestSpanLifecycle_SkillEndsWithSuccess(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          agent_v2.ActivateSkillToolName,
		CallId:        "call_lifecycle_s",
		ArgumentsJSON: `{"skill":"code-review","query":"x"}`,
	})
	got := findSpanByOperation(spans, "skill")
	if got == nil {
		t.Fatalf("应有 skill span")
	}
	if st := findAttrString(got, "gen_ai.status"); st != "success" {
		t.Errorf("skill span 应 status=success, got %q — kind-in-key 失配 bug", st)
	}
	if et := findAttrString(got, "error.type"); et == "InFlightSweep" {
		t.Errorf("skill span 不应被 sweep 兜底 (error.type=InFlightSweep)")
	}
}

// TestSpanLifecycle_PlainToolEndsWithSuccess — 对照组: plain tool kind="tool" 跟硬编码
// "tool" 恰好匹配, 方案 C 前后都应 PASS. 证明测试 harness 本身正确, 是 non-tool kind
// 才命中 bug.
func TestSpanLifecycle_PlainToolEndsWithSuccess(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "calculator",
		CallId:        "call_lifecycle_t",
		ArgumentsJSON: `{"expr":"2+2"}`,
	})
	got := findSpanByOperation(spans, "tool")
	if got == nil {
		t.Fatalf("应有 tool span")
	}
	if st := findAttrString(got, "gen_ai.status"); st != "success" {
		t.Errorf("plain tool span 应 status=success, got %q", st)
	}
}

// TestSpanLifecycle_EmptyCallIDNotLeaked — 空 callID 的 tool 类 span 必须被兜底清理
// (不泄漏在 sess.spans), span 仍 emit 给 exporter.
//
// **方案 C 回归 (commit f823ff9)**: callID 为空时 registryKey 回退 spanKey → 用真实 kind
// 前缀存 (如 "retrieval|graphNS|name"); 但 OnGraphEnd/OnAgentEnd sweep 简化后只扫 "tool|"
// 前缀 → 漏掉非 "tool" kind 的空 callID span → 既不被 OnToolEnd 配对 (无 callID 锚点)
// 也不被 sweep 清 → 泄漏. 改动前 sweep 遍历 6 kind 含 "retrieval|" 能清.
//
// 根因修复: registryKey 对 tool 类 kind **始终**归一 "tool|" 前缀 (不依赖 callID 是否空),
// 让 sweep 单 "tool|" 前缀的假设成立. callID 非空时 "tool|graphNS|callID" 唯一; 空时退化
// "tool|graphNS|" (多个空 callID 冲突覆盖, 但至少被 sweep 清掉不泄漏).
//
// 空 callID 是协议违反 (生产 LLM tool_call.id 非空), 但 mock / 非标准 provider 可能触发 —
// 框架应兜底不泄漏. 此测验证: 空 callID retrieval span 仍被 emit (sweep 兜底).
func TestSpanLifecycle_EmptyCallIDNotLeaked(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "vector_search",
		CallId:        "", // 空 callID (非标准 provider / mock 场景)
		ArgumentsJSON: `{"query":"x"}`,
		Extra:         map[string]any{"category": "retrieval"},
	})
	// 空 callID 时 OnToolEnd 无唯一锚点无法正常配对, 但 OnGraphEnd sweep 必须兜底 emit
	// 该 span (不泄漏). 关键断言: span 被 emit (而非泄漏在 sess.spans 直到 CleanupSession).
	if findSpanByOperation(spans, "retrieval") == nil {
		t.Fatalf("空 callID retrieval span 应被 sweep 兜底 emit (不泄漏), got spans=%d — "+
			"方案 C 回归: sweep 只扫 \"tool|\" 前缀漏掉 \"retrieval|\" 前缀的空 callID span", len(spans))
	}
}
