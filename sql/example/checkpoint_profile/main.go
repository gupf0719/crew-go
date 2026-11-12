// checkpoint_profile 对照文件 SQLite 或 PostgreSQL 中的 BasicAgent 全量存档与大工具结果转存。
package main

import (
	"context"
	"crypto/rand"
	dbsql "database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	storage "crew-go/sql"
	"crew-go/tool"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

type samples struct {
	bytes, saveMS, suspendMS, resumeMS []float64
	snapshot                           *compose_v2.Checkpoint
}

type measuredCheckpointer struct {
	*storage.Checkpointer
	samples *samples
}

func (c measuredCheckpointer) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	start := time.Now()
	if err := c.Checkpointer.SaveNS(ctx, sid, ns, cp); err != nil {
		return err
	}
	c.samples.saveMS = append(c.samples.saveMS, elapsedMS(start))
	// 保留本次同步写入的快照，在完成耗时测量后计算字节数。
	c.samples.snapshot = cp
	return nil
}

type scriptedModel struct {
	rounds int
}

func (m scriptedModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	n := 0
	for _, msg := range messages {
		if schema.GetToolResultID(msg) == "confirm" {
			return schema.AssistantMessage("done"), nil
		}
		if strings.HasPrefix(schema.GetToolResultID(msg), "dump-") {
			n++
		}
	}
	if n >= m.rounds {
		return nil, fmt.Errorf("missing confirmation after %d tool rounds", n)
	}
	id := strconv.Itoa(n)
	calls := []schema.ToolCall{
		{ID: "dump-" + id, Type: "function", Function: schema.FunctionCall{Name: "dump", Arguments: `{"round":` + id + `}`}},
	}
	if n == m.rounds-1 {
		calls = append(calls, schema.ToolCall{ID: "confirm", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"确认继续？"}`}})
	}
	return schema.AssistantToolCallMessage(calls), nil
}

func (m scriptedModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m scriptedModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func runSample(ctx context.Context, cp measuredCheckpointer, st *storage.Store, payloads []string, spill bool) error {
	var calls atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("dump", "返回本轮构造的长结果", map[string]any{
		"type": "object", "properties": map[string]any{"round": map[string]any{"type": "integer"}},
	}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
		n, err := strconv.Atoi(fmt.Sprint(args["round"]))
		if err != nil || n < 0 || n >= len(payloads) {
			return nil, fmt.Errorf("invalid dump round %v", args["round"])
		}
		calls.Add(1)
		return &tool.Result{Success: true, Output: payloads[n]}, nil
	}))
	opts := []agent_v2.Option{
		agent_v2.WithName("checkpoint_profile"), agent_v2.WithModel(scriptedModel{len(payloads)}),
		agent_v2.WithToolkit(tk), agent_v2.WithAskUser(), agent_v2.WithMaxRounds(len(payloads) + 2),
		agent_v2.WithToolResultMaxBytes(len(payloads[0]) + 1),
	}
	if spill {
		opts = append(opts, agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Store: st}))
	}
	sess := agent_v2.NewRunSession()
	ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(ctx, sess.RunID), cp)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	start := time.Now()
	_, err := agent_v2.NewBasicAgent(opts...).Run(ctx, sess, schema.UserMessage("生成报表并确认"))
	cp.samples.suspendMS = append(cp.samples.suspendMS, elapsedMS(start))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		return fmt.Errorf("profile run did not stop at one question: %v", err)
	}
	start = time.Now()
	events, err := agent_v2.NewBasicAgent(opts...).Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			interrupt.AllQueries()[0].InterruptID: schema.UserMessage("继续"),
		}))
	cp.samples.resumeMS = append(cp.samples.resumeMS, elapsedMS(start))
	if err != nil {
		return err
	}
	if len(events) == 0 || int(calls.Load()) != len(payloads) {
		return fmt.Errorf("profile resume did not finish or re-executed tools: %d events, %d dumps", len(events), calls.Load())
	}
	if cp.samples.snapshot == nil {
		return fmt.Errorf("profile did not save a checkpoint")
	}
	data, err := compose_v2.MarshalCheckpoint(cp.samples.snapshot)
	if err != nil {
		return err
	}
	cp.samples.bytes = append(cp.samples.bytes, float64(len(data)))
	cp.samples.snapshot = nil
	return nil
}

func elapsedMS(start time.Time) float64 {
	return float64(time.Since(start)) / float64(time.Millisecond)
}

func p95(vals []float64) float64 {
	sort.Float64s(vals)
	return vals[int(math.Ceil(float64(len(vals))*0.95))-1]
}

func profile(ctx context.Context, db *dbsql.DB, dialect storage.Dialect, n, rounds, size int) error {
	prefix := "crew_profile_" + strings.ToLower(rand.Text())
	cpTable, storeTable := prefix+"_cp", prefix+"_store"
	cp := storage.NewCheckpointer(db, dialect, storage.WithCheckpointerTable(cpTable))
	st := storage.NewStore(db, dialect, storage.WithStoreTable(storeTable))
	defer func() {
		for _, table := range []string{storeTable, cpTable, cpTable + "_task_lease"} {
			if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
				log.Print(err)
			}
		}
	}()
	if err := cp.Setup(ctx); err != nil {
		return err
	}
	if err := st.Setup(ctx); err != nil {
		return err
	}
	payloads := make([]string, rounds)
	for i := range payloads {
		data := make([]byte, size)
		rand.Read(data)
		payloads[i] = base64.RawStdEncoding.EncodeToString(data)[:size]
	}

	fmt.Printf("samples=%d rounds=%d tool_bytes=%d\n", n, rounds, size)
	fmt.Println("mode\tcp_bytes_p95\tsave_p95_ms\tsuspend_p95_ms\tresume_p95_ms")
	for _, spill := range []bool{false, true} {
		// 预热一次，再计正式样本。
		if err := runSample(ctx, measuredCheckpointer{cp, &samples{}}, st, payloads, spill); err != nil {
			return err
		}
		m := &samples{}
		for range n {
			if err := runSample(ctx, measuredCheckpointer{cp, m}, st, payloads, spill); err != nil {
				return err
			}
		}
		if len(m.saveMS) != n {
			return fmt.Errorf("expected %d checkpoint saves, got %d", n, len(m.saveMS))
		}
		mode := "full"
		if spill {
			mode = "spill"
		}
		fmt.Printf("%s\t%.0f\t%.3f\t%.3f\t%.3f\n", mode, p95(m.bytes), p95(m.saveMS), p95(m.suspendMS), p95(m.resumeMS))
	}
	return nil
}

func main() {
	n := flag.Int("samples", 20, "每种模式的样本数")
	rounds := flag.Int("rounds", 8, "连续产生工具结果的轮数")
	size := flag.Int("tool-bytes", 65536, "每次工具输出的字节数")
	flag.Parse()
	if *n < 1 || *rounds < 1 || *size < 1 {
		log.Fatal("samples, rounds and tool-bytes must be positive")
	}
	slog.SetLogLoggerLevel(slog.LevelWarn)
	if err := runProfile(*n, *rounds, *size); err != nil {
		log.Fatal(err)
	}
}

func runProfile(n, rounds, size int) error {
	ctx := context.Background()
	driver, dsn := "pgx", os.Getenv("CREW_SQL_POSTGRES_DSN")
	var dialect storage.Dialect = storage.Postgres{}
	if dsn == "" {
		dir, err := os.MkdirTemp("", "crew-checkpoint-profile-")
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(dir) }() // 只清理本次创建的测量库。
		driver, dialect = "sqlite", storage.SQLite{}
		dsn = (&url.URL{Scheme: "file", Path: filepath.Join(dir, "profile.db"), RawQuery: "_pragma=busy_timeout(5000)&_txlock=immediate"}).String()
	}
	db, err := dbsql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }() // 测量结束后释放连接。
	db.SetMaxOpenConns(1)
	fmt.Printf("backend=%s\n", driver)
	return profile(ctx, db, dialect, n, rounds, size)
}
