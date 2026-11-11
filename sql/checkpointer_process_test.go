package sql_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/tool"
)

type sharedApprovalModel struct{}

func (sharedApprovalModel) Generate(_ context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	last := messages[len(messages)-1]
	if last.Role == schema.RoleUser && last.Content == "转账" {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "transfer", Type: "function",
			Function: schema.FunctionCall{Name: "transfer", Arguments: `{"amount":5000}`}}}), nil
	}
	return schema.AssistantMessage("好的"), nil
}

func (sharedApprovalModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil // 让 Agent 走 Generate。
}

func (m sharedApprovalModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func sharedApprovalAgent(history agent_v2.HistoryStore, executed func()) *agent_v2.BasicAgent {
	tools := tool.NewToolkit()
	tools.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			executed()
			return tool.Success("已转账"), nil
		}))
	return agent_v2.NewBasicAgent(agent_v2.WithName("shared-teller"), agent_v2.WithModel(sharedApprovalModel{}),
		agent_v2.WithToolkit(tools), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithHistory(history, "tenant"))
}

type pauseSharedResume struct {
	agent_v2.NoOpOrchestrationHooks
}

func (pauseSharedResume) OnNodeStart(ctx context.Context, node string) (context.Context, error) {
	if node != "tools" {
		return ctx, nil
	}
	if _, err := fmt.Fprintln(os.Stdout, "tools-ready"); err != nil {
		return ctx, err
	}
	var b [1]byte
	_, err := io.ReadFull(os.Stdin, b[:])
	return ctx, err
}

const (
	sharedCPDSNEnv   = "CREW_TEST_SHARED_CP_DSN"
	sharedResumeBusy = "resume-busy"
)

func TestSharedCheckpointResumeWorker(t *testing.T) {
	backend := sharedApprovalBackend{
		driver: os.Getenv("CREW_TEST_SHARED_CP_DRIVER"),
		dsn:    os.Getenv(sharedCPDSNEnv),
		prefix: os.Getenv("CREW_TEST_SHARED_CP_PREFIX"),
	}
	if backend.dsn == "" {
		t.Skip("由共享存档测试启动的子进程")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	_, cp, history := backend.open(t)
	ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(ctx, "shared-approval"), cp)
	agent := sharedApprovalAgent(history, func() {
		_, _ = fmt.Fprintln(os.Stdout, "transfer-executed") // 由父进程检查输出。
	})
	answers := map[string]*schema.Message{os.Getenv("CREW_TEST_SHARED_CP_QUESTION"): schema.UserMessage("approve")}
	opts := []agent_v2.ResumeOption{agent_v2.WithResumeUserInputs(answers)}
	paused := os.Getenv("CREW_TEST_SHARED_CP_MODE") == "pause"
	if paused {
		opts = append(opts, agent_v2.WithResumeOrchHooks(pauseSharedResume{}))
	}
	_, err := agent.Resume(ctx, os.Getenv("CREW_TEST_SHARED_CP_SID"), opts...)
	if paused {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if !errors.Is(err, compose_v2.ErrTaskBusy) {
		t.Fatalf("放弃期间续跑应返回 ErrTaskBusy，实际 %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, sharedResumeBusy); err != nil {
		t.Fatal(err)
	}
}

type sharedApprovalBackend struct {
	driver string
	dsn    string
	prefix string
}

func (b sharedApprovalBackend) open(t *testing.T) (*sql.DB, *crewsql.Checkpointer, *crewsql.HistoryStore) {
	t.Helper()
	var dialect crewsql.Dialect
	switch b.driver {
	case "sqlite":
		dialect = crewsql.SQLite{}
	case "pgx":
		dialect = crewsql.Postgres{}
	case "mysql":
		dialect = crewsql.MySQL{}
	default:
		t.Fatalf("不支持的存档测试驱动 %q", b.driver)
	}
	db, err := sql.Open(b.driver, b.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cpTable, historyTable := crewsql.DefaultCheckpointTable, crewsql.DefaultHistoryTable
	if b.prefix != "" {
		cpTable, historyTable = b.prefix+"_cp", b.prefix+"_history"
	}
	return db, crewsql.NewCheckpointer(db, dialect, crewsql.WithCheckpointerTable(cpTable)),
		crewsql.NewHistoryStore(db, dialect, crewsql.WithHistoryTable(historyTable))
}

type sharedApproval struct {
	ctx        context.Context
	backend    sharedApprovalBackend
	sid, askID string
	cp         *crewsql.Checkpointer
	history    *crewsql.HistoryStore
	key        agent_v2.HistoryKey
	agent      *agent_v2.BasicAgent
}

func newSQLiteSharedApproval(t *testing.T) sharedApproval {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shared.db")
	return newSharedApproval(t, sharedApprovalBackend{driver: "sqlite", dsn: "file:" + path + "?_pragma=busy_timeout(2000)"})
}

func newSharedApproval(t *testing.T, backend sharedApprovalBackend) sharedApproval {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "shared-approval"}
	ctx = compose_v2.WithThreadIDCtx(ctx, key.ConvID)
	db, cp, history := backend.open(t)
	if backend.prefix != "" {
		dropSQLTestTablesOnCleanup(t, db, backend.prefix+"_cp_task_lease", backend.prefix+"_cp", backend.prefix+"_history")
	}
	if err := cp.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := history.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	agent := sharedApprovalAgent(history, func() { t.Error("父进程的工具不应执行") })
	session := agent_v2.NewRunSession()
	_, err := agent.Run(ctx, session, schema.UserMessage("转账"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("应停在转账审批，实际 %v", err)
	}
	return sharedApproval{ctx: ctx, backend: backend, sid: session.RunID, askID: interrupt.AllQueries()[0].InterruptID,
		cp: cp, history: history, key: key, agent: agent}
}

func dropSQLTestTablesOnCleanup(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+strings.Join(tables, ", ")); err != nil {
			t.Errorf("删除测试表失败：%v", err)
		}
	})
}

func (f sharedApproval) worker(mode string) *exec.Cmd {
	cmd := exec.CommandContext(f.ctx, os.Args[0], "-test.run=^TestSharedCheckpointResumeWorker$")
	cmd.Env = append(os.Environ(), sharedCPDSNEnv+"="+f.backend.dsn, "CREW_TEST_SHARED_CP_DRIVER="+f.backend.driver,
		"CREW_TEST_SHARED_CP_PREFIX="+f.backend.prefix, "CREW_TEST_SHARED_CP_SID="+f.sid,
		"CREW_TEST_SHARED_CP_QUESTION="+f.askID, "CREW_TEST_SHARED_CP_MODE="+mode)
	return cmd
}

func (f sharedApproval) results(t *testing.T) []string {
	t.Helper()
	messages, err := agent_v2.LoadAllHistory(f.ctx, f.history, f.key)
	if err != nil {
		t.Fatal(err)
	}
	var results []string
	for _, m := range messages {
		if schema.GetToolResultID(m) == "transfer" && m.Annotations[agent_v2.HistoryTaskAnnotation] == f.sid {
			results = append(results, m.Content)
		}
	}
	return results
}

// TestSharedCheckpointer_AbandonDuringResume 用两个进程穿过真实 Agent 的读档、工具执行和历史收尾。
func TestSharedCheckpointer_AbandonDuringResume(t *testing.T) {
	runAbandonDuringResume(t, newSQLiteSharedApproval(t))
}

func runAbandonDuringResume(t *testing.T, f sharedApproval) {
	t.Helper()
	cmd := f.worker("pause")
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	reader := bufio.NewReader(out)
	if ready, err := reader.ReadString('\n'); err != nil || strings.TrimSpace(ready) != "tools-ready" {
		t.Fatalf("续跑进程未进入工具节点，实际 %q %v", ready, err)
	}
	_, abandonErr := f.agent.Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(f.sid))
	_, abortErr := f.agent.Resume(f.ctx, f.sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	if _, err := in.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	output, readErr := io.ReadAll(reader)
	if err := cmd.Wait(); err != nil || readErr != nil {
		t.Fatalf("续跑失败，stdout=%s，wait=%v，read=%v", output, err, readErr)
	}
	results := f.results(t)
	if !errors.Is(abandonErr, compose_v2.ErrTaskBusy) || !errors.Is(abortErr, compose_v2.ErrTaskBusy) ||
		strings.Count(string(output), "transfer-executed") != 1 || !reflect.DeepEqual(results, []string{"已转账"}) {
		t.Fatalf("续跑持有任务时应拒绝放弃且仅写真实结果，实际 abandon=%v abort=%v stdout=%s 结果=%q",
			abandonErr, abortErr, output, results)
	}
}

type pauseSharedDeletion struct {
	*crewsql.Checkpointer
	entered, release chan struct{}
}

func (p pauseSharedDeletion) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	close(p.entered)
	select {
	case <-p.release:
		return p.Checkpointer.DeleteSubtree(ctx, sid, ns)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestSharedCheckpointer_ResumeDuringAbandon(t *testing.T) {
	runResumeDuringAbandon(t, newSQLiteSharedApproval(t))
}

func runResumeDuringAbandon(t *testing.T, f sharedApproval) {
	t.Helper()
	paused := pauseSharedDeletion{Checkpointer: f.cp, entered: make(chan struct{}), release: make(chan struct{})}
	ctx := compose_v2.WithCheckpointerCtx(f.ctx, paused)
	done := make(chan struct{})
	var abortErr error
	go func() {
		defer close(done)
		_, abortErr = f.agent.Resume(ctx, f.sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	}()
	var unblocked bool
	defer func() {
		if !unblocked {
			close(paused.release)
		}
		<-done
	}()
	select {
	case <-paused.entered:
	case <-done:
		t.Fatalf("放弃未进入删档，实际 %v", abortErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	output, err := f.worker("busy").CombinedOutput()
	if err != nil || strings.Count(string(output), sharedResumeBusy+"\n") != 1 ||
		strings.Contains(string(output), "transfer-executed") {
		t.Fatalf("删档中续跑应被拒且工具不执行，实际 err=%v output=%s", err, output)
	}
	close(paused.release)
	unblocked = true
	<-done
	if abortErr != nil {
		t.Fatalf("放弃应正常完成，实际 %v", abortErr)
	}
	results := f.results(t)
	if len(results) != 1 || !strings.Contains(results[0], "已被放弃") {
		t.Fatalf("历史应只补一条放弃结果，实际 %q", results)
	}
	if _, err := f.agent.Resume(f.ctx, f.sid); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("放弃结束后应报告没有可续存档，实际 %v", err)
	}
}
