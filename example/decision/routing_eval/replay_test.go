package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayScansThresholdsWithoutCredentials(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	t.Setenv("QWEN_API_KEY", "")
	dir := t.TempDir()
	opt := options{
		systems: []string{"jev", "qwen"}, conds: []string{"zh"}, replay: true,
		thresholds: "0.9,0.8,0.9", jevModel: "fixture", jevBaseURL: "https://fixture.invalid", qwenModel: "fixture",
		casePath: writeFixture(t, dir, "requests.jsonl",
			`{"id":"a","sc":"business","gold":"order","zh":"已发货，查我的订单"}`+"\n"+
				`{"id":"b","sc":"business","gold":"refund","zh":"已申请退款，查进度"}`+"\n"+
				`{"id":"c","sc":"business","gold":"order","zh":"我想知道订单到哪了"}`+"\n"),
		scenarioPath: writeFixture(t, dir, "routes.json", testRoutes), outDir: dir,
	}
	cases, err := loadCases(opt.casePath, opt.scenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	cond, _ := conditionByName("zh") // 仓内固定的中文条件。
	jobs, _, err := planJobs(opt, []condition{cond})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("credentials should be optional for replay: %v %v", jobs, err)
	}
	high, mid := 0.95, 0.82
	for _, jb := range jobs {
		var entries map[string]record
		if isDecider(jb.sys) {
			entries = map[string]record{
				"a": {Pred: "order", Confidence: &high, TotalMS: 10},
				"b": {Pred: "order", Confidence: &mid, TotalMS: 20},
				"c": {Pred: noRoute, TotalMS: 10},
			}
		} else {
			entries = map[string]record{
				"a": {Pred: "order", TotalMS: 40},
				"b": {Pred: "refund", TotalMS: 40},
				"c": {Pred: "order", TotalMS: 80},
			}
		}
		for _, c := range cases {
			rec := entries[c.ID]
			rec.ID, rec.InputHash = c.ID, inputHash(jb.sys, c.scenario, c, jb.cond)
			entries[c.ID] = rec
		}
		if err := saveCache(cachePath(dir, jb), cases, entries); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(opt); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"离线重算", "模拟 P95",
		"| 0.8 | 66.7% | 33.3% | 50.0% | 66.7% | 83 |",
		"| 0.9 | 33.3% | 66.7% | 0.0% | 100.0% | 87 |",
		"| 只用 qwen | 0% | 100% | — | 100.0% | 76 |"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("report is missing %q:\n%s", want, raw)
		}
	}
	if strings.Count(string(raw), "| 0.9 |") != 1 {
		t.Fatal("duplicate threshold rendered")
	}
	opt.jevBaseURL = "https://changed.invalid"
	if err := run(opt); err == nil || !strings.Contains(err.Error(), "missing or stale cache for \"a\"") {
		t.Fatalf("changed endpoint must reject the old prediction without going online: %v", err)
	}
}

func TestScanThresholdsRejectsInvalid(t *testing.T) {
	for _, input := range []string{"NaN", "0.9,1.01", "-0.1", "0.5,"} {
		if _, err := scanThresholds(input); err == nil {
			t.Errorf("expected a validation error for %q", input)
		}
	}
}
