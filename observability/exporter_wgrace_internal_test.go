package observability

import (
	"sync"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TestExporter_WGRace_EnqueueClose 验证 wg 竞态修复——高频"单 enqueuer Enqueue + Close
// 紧邻"下不触发 `panic: sync: WaitGroup is reused before previous Wait has returned`。
//
// TraceExportBatchSize=1 让**每次** Enqueue 都走 batch-flush 的 wg.Add(1)，放大竞态窗口
// （单 enqueuer 隔离：洪泛多 enqueuer 反而因 in-flight export goroutine 让 counter 常驻 >1
// 掩盖窗口）。假 endpoint 走 grpc.NewClient lazy 连接不实连，export RPC 失败=drop 不 panic。
//
// sanity：revert wg 修复（Enqueue 的 closed 检查移回 mu 外、Close 改回 CompareAndSwap）后，
// -race 数千迭代内稳定 panic + DATA RACE（Enqueue 的 wg.Add vs Close 的 wg.Wait）。
func TestExporter_WGRace_EnqueueClose(t *testing.T) {
	newCfg := func() *Config {
		return &Config{
			TraceEndpoint:         "127.0.0.1:1", // 假地址：lazy 连接不实连
			TraceExportBatchSize:  1,             // 每 Enqueue 触发 wg.Add
			TraceQueueMaxSize:     100,
			TraceExportInterval:   time.Hour, // tick 不干扰
			MetricsExportInterval: time.Hour,
			TraceRPCTimeout:       time.Second,
			Resource:              Resource{AppName: "t", EnvName: "t", IP: "1", SDKVersion: "1"},
		}
	}
	// 每次新 exporter + 单 Enqueue goroutine + 主 goroutine 紧邻 Close。竞态窗口：Enqueue
	// 过 closed 检查后暂停 → Close close(done) 让 exportLoop done 路径抢 mu 退出 wg.Done
	// (counter→0) + wg.Wait 唤醒中 → Enqueue 恢复 wg.Add(0→1) 与 Wait 并发。窗口窄需大量
	// 迭代 + -race 放大检测（复审实测 ~2万迭代稳定崩溃）。
	for i := 0; i < 20000; i++ {
		e, err := newExporter(newCfg())
		if err != nil || e == nil {
			t.Fatalf("newExporter iter %d: %v", i, err)
		}
		// barrier 让 Enqueue 与 Close **真并发**启动（紧邻串行难命中窄窗口）。
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; e.Enqueue(&tracepb.Span{Name: "s"}) }()
		go func() { defer wg.Done(); <-start; _ = e.Close() }()
		close(start)
		wg.Wait()
	}
}
