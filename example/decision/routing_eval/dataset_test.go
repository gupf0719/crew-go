package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const testRoutes = `[{"id":"business","title":"业务请求","routes":[
	{"name":"order","desc_zh":"查订单"},
	{"name":"refund","desc_zh":"查退款"}
]}]`

func TestExternalCasesUseDeclaredBranchesAndCache(t *testing.T) {
	dir := t.TempDir()
	scenarios := writeFixture(t, dir, "routes.json", testRoutes)
	cases := writeFixture(t, dir, "requests.jsonl",
		`{"id":"first","sc":"business","gold":"order","tags":["repeat"],"zh":"历史：已付款。最新：订单到了吗？"}`+"\n")
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body struct {
			State     string                     `json:"state"`
			Questions map[string]json.RawMessage `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		question := string(body.Questions["route"])
		if !strings.Contains(body.State, "已付款") || !strings.Contains(question, "查订单") || !strings.Contains(question, "查退款") {
			t.Errorf("external context or branches missing: %+v %s", body, question)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"model":"mock","answers":{"route":{"type":"choice","choice":"order","confidence":0.98,"probabilities":{"order":0.98,"refund":0.02}}},"usage":{"input_tokens":3,"output_tokens":1}}`) // 本地测试响应。
	}))
	defer srv.Close()
	t.Setenv("TYPESAFE_API_KEY", "fixture")
	opt := options{
		systems: []string{"jev"}, conds: []string{"zh", "zh-en", "en"}, jevBaseURL: srv.URL, jevModel: "mock",
		casePath: cases, scenarioPath: scenarios, concurrency: 1, outDir: filepath.Join(dir, "out"),
	}
	for range 2 {
		if err := run(opt); err != nil {
			t.Fatal(err)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("want one call across two runs and unavailable language conditions, got %d", got)
	}
	report, err := os.ReadFile(filepath.Join(opt.outDir, "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"外部业务集", "业务请求", "100.0%", "p50 / p90 / p95", "| repeat |"} {
		if !strings.Contains(string(report), want) {
			t.Errorf("report lacks %q", want)
		}
	}
	writeFixture(t, dir, "requests.jsonl",
		`{"id":"first","sc":"business","gold":"refund","zh":"历史：已付款。最新：订单到了吗？"}`+"\n")
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("editing gold should reuse the prediction, got %d requests", requests.Load())
	}
	report, err = os.ReadFile(filepath.Join(opt.outDir, "report.md"))
	if err != nil || !strings.Contains(string(report), "0.0%") {
		t.Fatalf("cached prediction was not evaluated with the new label: %v\n%s", err, report)
	}
}

func TestExternalCasesRejectDuplicateAndUnknownGold(t *testing.T) {
	dir := t.TempDir()
	scenarios := writeFixture(t, dir, "routes.json", testRoutes)
	good := `{"id":"first","sc":"business","gold":"order","zh":"查订单"}`
	for _, line := range []string{good, `{"id":"second","sc":"business","gold":"other","zh":"别的"}`} {
		path := writeFixture(t, dir, "cases.jsonl", good+"\n"+line+"\n")
		if _, err := loadCases(path, scenarios); err == nil || !strings.Contains(err.Error(), "line 2:") {
			t.Fatalf("invalid line should report its location, got %v", err)
		}
	}
}

func TestCascadeReportSeparatesAutoErrorsAndEscalation(t *testing.T) {
	no, yes := false, true
	cases := []evalCase{
		{ID: "a", Gold: "order"}, {ID: "b", Gold: "refund"}, {ID: "c", Gold: "order"},
	}
	result := runResult{System: "cascade:jev>qwen@0.85", Cond: "zh", Pass: 1, Records: map[string]record{
		"a": {Pred: "order", Escalated: &no, TotalMS: 10},
		"b": {Pred: "order", Escalated: &no, TotalMS: 20},
		"c": {Pred: "order", Escalated: &yes, TotalMS: 30},
	}}
	r := newReporter(cases, []runResult{result}, roles{})
	r.cascade()
	if got := r.b.String(); !strings.Contains(got, "| 3 | 66.7% | 33.3% | 50.0% |") {
		t.Fatalf("wrong auto and escalation denominators: %s", got)
	}
}

func writeFixture(t *testing.T, dir, name, data string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
