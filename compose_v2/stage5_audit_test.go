package compose_v2_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestC2_LoadAllInSession 验证 LoadAllInSession 返 sid 下所有 ns cp。
func TestC2_LoadAllInSession(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	for _, ns := range []string{"", "a", "a/b"} {
		_ = cp.SaveNS(ctx, "sid1", ns, &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending})
	}
	out, err := cp.LoadAllInSession(ctx, "sid1")
	if err != nil {
		t.Fatalf("LoadAllInSession: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("expect 3 cps, got %d", len(out))
	}
}

// TestC2_SaveBatch 验证一次写多 cp。
func TestC2_SaveBatch(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	batch := map[string]*compose_v2.Checkpoint{
		"":    {Status: compose_v2.CPStatusPending},
		"sub": {Status: compose_v2.CPStatusPending},
	}
	if err := cp.SaveBatch(ctx, "sid", batch); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}
	all, _ := cp.LoadAllInSession(ctx, "sid")
	if len(all) != 2 {
		t.Errorf("expect 2 cps after SaveBatch, got %d", len(all))
	}
}

// TestC2_Cleanup_KeepsLatestN 验证 Cleanup 按 sid 维度保最新 N 个。
func TestC2_Cleanup_KeepsLatestN(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	now := time.Now().UnixNano()
	for i, sid := range []string{"old", "mid", "new"} {
		_ = cp.SaveNS(ctx, sid, "", &compose_v2.Checkpoint{
			Status:    compose_v2.CPStatusPending,
			CreatedAt: now + int64(i),
		})
	}
	deleted, err := cp.Cleanup(ctx, 1)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expect 2 deleted (keep newest 1 of 3 sids), got %d", deleted)
	}
	// "new" sid 应仍存在
	if exists, _ := cp.ExistsNS(ctx, "new", ""); !exists {
		t.Error("'new' sid should be kept")
	}
	if exists, _ := cp.ExistsNS(ctx, "old", ""); exists {
		t.Error("'old' sid should be deleted")
	}
}

// TestC3_MaxIterationsError 验证 errors.As friendly。
func TestC3_MaxIterationsError(t *testing.T) {
	err := error(&compose_v2.MaxIterationsError{Iterations: 10})
	var typed *compose_v2.MaxIterationsError
	if !errors.As(err, &typed) {
		t.Fatal("errors.As failed")
	}
	if typed.Iterations != 10 {
		t.Errorf("Iterations=%d", typed.Iterations)
	}
	if !strings.Contains(err.Error(), "10") {
		t.Errorf("expect 10 in error: %v", err)
	}
}

// TestC3_MaxRoundsError 验证 MaxRoundsError 类型 OK。
func TestC3_MaxRoundsError(t *testing.T) {
	err := error(&compose_v2.MaxRoundsError{Rounds: 5})
	var typed *compose_v2.MaxRoundsError
	if !errors.As(err, &typed) {
		t.Fatal("errors.As failed")
	}
	if typed.Rounds != 5 {
		t.Errorf("Rounds=%d", typed.Rounds)
	}
}

// TestC3_ErrNoSubgraphCheckpoint 验证哨兵对 errors.Is 友好。
func TestC3_ErrNoSubgraphCheckpoint(t *testing.T) {
	wrapped := fmtErrf("wrapped: %w", compose_v2.ErrNoSubgraphCheckpoint)
	if !errors.Is(wrapped, compose_v2.ErrNoSubgraphCheckpoint) {
		t.Errorf("errors.Is failed: %v", wrapped)
	}
}

// fmtErrf 兼容 helper 避免引入 fmt 干扰断言。
func fmtErrf(format string, a ...any) error {
	return errFmt{format: format, args: a}
}

type errFmt struct {
	format string
	args   []any
}

func (e errFmt) Error() string {
	if len(e.args) > 0 {
		if err, ok := e.args[len(e.args)-1].(error); ok {
			return strings.Replace(e.format, "%w", err.Error(), 1)
		}
	}
	return e.format
}
func (e errFmt) Unwrap() error {
	for _, a := range e.args {
		if err, ok := a.(error); ok {
			return err
		}
	}
	return nil
}

// 注：原 TestC4_WithGraphID_RoundTrip 已删——WithGraphID/GetGraphID ctx pair 为废弃
// 残留（防串台实际走 Graph.ID() 直读 + cp.GraphID 字段，三方对照决策记录 checkpoint-storage.md
// 已确认此机制；LangGraph/eino 均无 graph 身份概念），ctx pair + 其 tautological 测试一并删除。

// TestC5_MarshalUnmarshalTagged_RoundTrip 验证导出 wrapper 正常工作。
func TestC5_MarshalUnmarshalTagged_RoundTrip(t *testing.T) {
	type myType struct {
		X int    `json:"x"`
		Y string `json:"y"`
	}
	compose_v2.RegisterValueType[*myType]("compose_v2_test.myType")
	v := &myType{X: 42, Y: "hello"}

	data, err := compose_v2.MarshalTagged(v)
	if err != nil {
		t.Fatalf("MarshalTagged: %v", err)
	}
	got, err := compose_v2.UnmarshalTagged(data)
	if err != nil {
		t.Fatalf("UnmarshalTagged: %v", err)
	}
	gotMy, ok := got.(*myType)
	if !ok {
		t.Fatalf("expect *myType, got %T", got)
	}
	if gotMy.X != 42 || gotMy.Y != "hello" {
		t.Errorf("roundtrip lost: %+v", gotMy)
	}
}

// TestC5_LookupKey_Type 验证 Lookup 函数 OK。
func TestC5_LookupKey_Type(t *testing.T) {
	compose_v2.RegisterValueType[int]("int")
	rt, ok := compose_v2.LookupType("int")
	if !ok {
		t.Fatal("LookupType missing")
	}
	if rt != reflect.TypeOf(0) {
		t.Errorf("LookupType: got %v", rt)
	}
	key, ok := compose_v2.LookupKey(reflect.TypeOf(0))
	if !ok || key != "int" {
		t.Errorf("LookupKey: ok=%v key=%q", ok, key)
	}
}

// TestC7_AdjustIndex_PullsBackToToolUse 验证切点会拉回到含对应 tool_use 的 assistant。
func TestC7_AdjustIndex_PullsBackToToolUse(t *testing.T) {
	asst := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "tc1",
		Type:     "function",
		Function: schema.FunctionCall{Name: "x", Arguments: "{}"},
	}})
	tres := schema.ToolResultMessage("tc1", "ok")
	final := schema.AssistantMessage("done")
	msgs := []*schema.Message{
		schema.UserMessage("q"),
		asst,
		tres,
		final,
	}
	// 想从 idx 2 切（tool_result），应被拉回 idx 1（assistant 含 tool_use）保配对
	got := compose_v2.AdjustIndexToPreserveAPIInvariants(msgs, 2)
	if got != 1 {
		t.Errorf("expect pulled back to 1, got %d", got)
	}
}

// TestC7_AdjustIndex_ZeroNoOp 验证 start <= 0 直接返 0。
func TestC7_AdjustIndex_ZeroNoOp(t *testing.T) {
	got := compose_v2.AdjustIndexToPreserveAPIInvariants(nil, -1)
	if got != 0 {
		t.Errorf("expect 0 for negative start, got %d", got)
	}
}

// TestC1_RecordingHooks_BasicCounts 验证 RecordingHooks 计数 OK。
func TestC1_RecordingHooks_BasicCounts(t *testing.T) {
	rec := &compose_v2.RecordingHooks{}
	bld := compose_v2.NewBuilder()
	bld.AddNode("n1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "n1")
	bld.AddEdge("n1", compose_v2.EndNode)
	g, _ := bld.Compile()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(rec)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	gs, ge := rec.SnapshotGraphLifecycle()
	if gs != 1 || ge != 1 {
		t.Errorf("expect 1 graphStart + 1 graphEnd, got %d/%d", gs, ge)
	}
	starts := rec.SnapshotNodeStarts()
	if len(starts) != 1 || starts[0] != "n1" {
		t.Errorf("expect [n1], got %v", starts)
	}
	ends := rec.SnapshotNodeEnds()
	if len(ends) != 1 || ends[0] != "n1" {
		t.Errorf("expect [n1] ends, got %v", ends)
	}
}

// TestC1_OrchHooksCtx_RoundTrip 验证 compose_v2 ctx-aware OrchHooks。
func TestC1_OrchHooksCtx_RoundTrip(t *testing.T) {
	rec := &compose_v2.RecordingHooks{}
	ctx := compose_v2.WithOrchHooks(context.Background(), rec)
	got := compose_v2.GetOrchHooks(ctx)
	if got != rec {
		t.Errorf("ctx round-trip failed")
	}
}

// TestC1_OrchHooksCtx_AutoFallback —— compose_v2.Run 入口未传 RunOption WithHooks
// 时，自动从 ctx 取 GetOrchHooks 注入（实现"嵌套 sub-agent 自动继承父 hooks"
// 在 compose_v2 引擎层真生效）。audit R4 H1 修复。
func TestC1_OrchHooksCtx_AutoFallback(t *testing.T) {
	ch := compose_v2.LastValue[string]("done")
	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, ch)
	g, err := bld.
		AddNode("n1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(ch, "ok")}, nil
		}).
		AddEdge(compose_v2.StartNode, "n1").
		AddEdge("n1", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	rec := &compose_v2.RecordingHooks{}
	ctx := compose_v2.WithOrchHooks(context.Background(), rec)

	cm := compose_v2.NewChannelManager()
	// **关键**：不传 WithHooks RunOption，期望 Run 入口自动从 ctx 取 hooks
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		t.Fatal(err)
	}

	starts := rec.SnapshotNodeStarts()
	if len(starts) != 1 || starts[0] != "n1" {
		t.Errorf("expect [n1] start (从 ctx 自动取 hooks 失败), got %v", starts)
	}
}

// TestC1_OrchHooksCtx_RunOptionPriority —— RunOption WithHooks 优先级高于 ctx OrchHooks。
func TestC1_OrchHooksCtx_RunOptionPriority(t *testing.T) {
	ch := compose_v2.LastValue[string]("done")
	bld := compose_v2.NewBuilder()
	bld = compose_v2.WithChannel(bld, ch)
	g, _ := bld.
		AddNode("n1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(ch, "ok")}, nil
		}).
		AddEdge(compose_v2.StartNode, "n1").
		AddEdge("n1", compose_v2.EndNode).
		Compile()

	ctxRec := &compose_v2.RecordingHooks{}
	optRec := &compose_v2.RecordingHooks{}
	ctx := compose_v2.WithOrchHooks(context.Background(), ctxRec)

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm, compose_v2.WithHooks(optRec)); err != nil {
		t.Fatal(err)
	}

	// optRec 应被调用（RunOption 优先），ctxRec 不被调用
	if len(optRec.SnapshotNodeStarts()) != 1 {
		t.Errorf("RunOption hooks 应优先；optRec starts=%d", len(optRec.SnapshotNodeStarts()))
	}
	if len(ctxRec.SnapshotNodeStarts()) != 0 {
		t.Errorf("ctx hooks 应被 RunOption 覆盖；ctxRec starts=%d", len(ctxRec.SnapshotNodeStarts()))
	}
}

// TestC3_MaxIterationsError_Generation —— 验证 graph_loop 超步耗尽真实抛出
// typed `*MaxIterationsError`（业务用 errors.As 解析具体值）。audit R4 H2 修复。
func TestC3_MaxIterationsError_Generation(t *testing.T) {
	ch := compose_v2.OnceFlag("never")
	bld := compose_v2.NewBuilder().WithMaxVisit(3)
	bld = compose_v2.WithChannel(bld, ch)
	g, err := bld.
		AddNode("loop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil // 不写终止信号——必死循环触顶 maxVisit
		}).
		AddEdge(compose_v2.StartNode, "loop").
		AddEdge("loop", "loop"). // 自环
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm)
	if runErr == nil {
		t.Fatal("期望超步耗尽返 error，实际 nil")
	}

	// 业务用 errors.As 解析具体 Iterations 值
	var mie *compose_v2.MaxIterationsError
	if !errors.As(runErr, &mie) {
		t.Fatalf("期望 *MaxIterationsError，实际 %T: %v", runErr, runErr)
	}
	if mie.Iterations < 3 {
		t.Errorf("Iterations=%d，期望 >= 3", mie.Iterations)
	}
}
