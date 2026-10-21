package observability

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// TestObservability_RedactErrorMessage_OrchestrationPath 验证编排错误路径（OnNodeEnd 经
// mapErrToStatus）的 error.message 也过 RedactPatterns——B-HIGH-B1 只补了 OnError 通信路径、
// 漏 OnAgentEnd/OnGraphEnd/OnNodeEnd 三条编排路径 → 业务配了 RedactPatterns 仍让含 PII 的
// 编排错误明文上 trace。
//
// sanity：revert endSpan 的 o.redactString(errMsg)（改回裸 errMsg）后，error.message 含明文
// sk-secret123 → FAIL 精确指向 redact GAP。
func TestObservability_RedactErrorMessage_OrchestrationPath(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, err := New(Config{
		TraceEndpoint:        addr,
		Resource:             Resource{AppName: "t", EnvName: "t", IP: "1", SDKVersion: "1"},
		RedactPatterns:       []*regexp.Regexp{regexp.MustCompile(`sk-\w+`)},
		TraceExportBatchSize: 1, // 每 span 立即 flush 到 mock server（默认 5s tick 太慢）
		TraceQueueMaxSize:    100,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "redact_orch_sid")
	// OnNodeStart 建 node span；OnNodeEnd with error 含 PII（编排错误路径，非 OnError 通信错误）
	ctx2, _ := orch.OnNodeStart(ctx, "mynode")
	_ = orch.OnNodeEnd(ctx2, "mynode", errors.New("validation failed for api key sk-secret123 leaked"))
	obs.CleanupSession("redact_orch_sid")

	time.Sleep(300 * time.Millisecond)
	spans := srv.Spans()
	var errMsg string
	for _, sp := range spans {
		for _, a := range sp.GetAttributes() {
			if a.GetKey() == "error.message" {
				errMsg = a.GetValue().GetStringValue()
			}
		}
	}
	if errMsg == "" {
		t.Fatalf("应有 node span error.message, spans=%d", len(spans))
	}
	if strings.Contains(errMsg, "sk-secret123") {
		t.Fatalf("编排错误 error.message 应脱敏 PII, got %q（含 sk-secret123 = redact GAP 未修）", errMsg)
	}
	if !strings.Contains(errMsg, "[REDACTED]") {
		t.Fatalf("error.message 应含 [REDACTED], got %q", errMsg)
	}
}
