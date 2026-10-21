package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// captureEventsHook 记录 OnNodeEvent 触发的所有 events。
type captureEventsHook struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *captureEventsHook) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

// chunkStreamModel 流式产 N 个 chunk，每个 chunk 含 .Content。
type chunkStreamModel struct {
	contents []string
}

func (m *chunkStreamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(strings.Join(m.contents, "")), nil
}
func (m *chunkStreamModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](len(m.contents))
	for _, c := range m.contents {
		send(&schema.Message{Role: schema.RoleAssistant, Content: c})
	}
	schema.CloseSend(sr)
	return sr, nil
}
func (m *chunkStreamModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*chunkStreamModel)(nil)

// rewritingOnModelEndHook 在 OnModelEnd 改写 Message——业务用例如安全过滤、内容改写。
type rewritingOnModelEndHook struct {
	hook.NoOpHooks
	rewriteTo string
}

func (h *rewritingOnModelEndHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message = schema.AssistantMessage(h.rewriteTo)
	return ctx, nil
}

// TestRepro_R10_A1_OnModelEndOverridesChunksWithoutResetStream 验证 audit R10 A1：
// streaming 路径下 OnModelEnd hook 改 Message，前端按 chunks 拼出原内容，cp/channel
// 持久化改写后内容——前端视图与 cp 视图分裂。修复后框架 emit reset_stream 通知前端
// 清老 chunks buffer。
//
// **预期行为**：hook 改写后应 emit reset_stream event with reason=hook_overrode_message。
//
// **当前 bug**（修前）：hook 改写 silent 完成，前端无信号。
func TestRepro_R10_A1_OnModelEndOverridesChunksWithoutResetStream(t *testing.T) {
	stub := &chunkStreamModel{contents: []string{"hello ", "world"}}
	captureH := &captureEventsHook{}
	rewriteH := &rewritingOnModelEndHook{rewriteTo: "REWRITTEN BY HOOK"}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a1_agent"),
		agent_v2.WithModel(stub),
		agent_v2.WithHooks(rewriteH),
	)

	sess := &agent_v2.RunSession{RunID: "a1_sid"}
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("hi"),
		agent_v2.WithOrchHooks(captureH))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 chunks 记录了原内容（"hello " + "world"）
	captureH.mu.Lock()
	defer captureH.mu.Unlock()
	chunkContents := []string{}
	gotResetStream := false
	for _, ev := range captureH.events {
		if ev.Type == "chunk" {
			chunkContents = append(chunkContents, ev.Content)
		}
		if ev.Type == "reset_stream" {
			if reason, _ := ev.Metadata["reason"].(string); reason == "hook_overrode_message" {
				gotResetStream = true
			}
		}
	}
	combined := strings.Join(chunkContents, "")
	if combined != "hello world" {
		t.Fatalf("chunks 记录非原内容: got %q expected 'hello world'", combined)
	}
	if !gotResetStream {
		t.Fatalf("Bug A1 reproduced: OnModelEnd 改 Message 后框架未 emit reset_stream\n"+
			"前端按 chunks 拼出 'hello world' 但 cp 持久化 'REWRITTEN BY HOOK' 视图分裂\n"+
			"修复方向：modelNode 检测 hook 改写后 emit reset_stream(reason=hook_overrode_message)\n"+
			"实际 events: %v", eventTypes(captureH.events))
	}
}

// interruptingStreamModel 在 reader.Recv 中段返 *InterruptError——业务自定义
// model wrapper 在 mid-stream 检测到敏感内容/超 quota 主动中断 ask_user 的典型场景。
type interruptingStreamModel struct {
	preChunks []string
}

func (m *interruptingStreamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, fmt.Errorf("not impl")
}
func (m *interruptingStreamModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](len(m.preChunks) + 1)
	for _, c := range m.preChunks {
		send(&schema.Message{Role: schema.RoleAssistant, Content: c})
	}
	schema.SendError(sr, &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{Question: "continue?", InterruptID: "mid_stream_id"},
	})
	return sr, nil
}
func (m *interruptingStreamModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*interruptingStreamModel)(nil)

// TestRepro_R10_A4_InterruptMidStreamEmitsResetStream 验证 audit R10 A4：
// 业务 model wrapper 在 reader.Recv 中段抛 *InterruptError 时 chunks 已 emit，
// streamOrGenerate 应 emit reset_stream 通知前端清老 run_id buffer。
//
// **预期行为**：错误路径若 chunks 已 emit > 0 应附带 reset_stream event。
//
// **当前 bug**（修前）：错误路径直接 return err，前端 buffer 累积老 chunks。
func TestRepro_R10_A4_InterruptMidStreamEmitsResetStream(t *testing.T) {
	stub := &interruptingStreamModel{preChunks: []string{"partial-1 ", "partial-2 "}}
	captureH := &captureEventsHook{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a4_agent"),
		agent_v2.WithModel(stub),
	)

	sess := &agent_v2.RunSession{RunID: "a4_sid"}
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("hi"),
		agent_v2.WithOrchHooks(captureH))
	if err == nil {
		t.Fatal("expected error from mid-stream interrupt")
	}
	// 错误应是 InterruptError（unwrap 链穿透 fmt.Errorf wrap）
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		// 非 InterruptError 也应 emit reset_stream（任何 mid-stream 错误都应清前端）
		// 但本测试场景一定是 InterruptError
		t.Logf("warning: err 不是 InterruptError: %v (但 reset_stream 仍应 emit)", err)
	}

	captureH.mu.Lock()
	defer captureH.mu.Unlock()
	chunkCount := 0
	gotResetStream := false
	for _, ev := range captureH.events {
		if ev.Type == "chunk" {
			chunkCount++
		}
		if ev.Type == "reset_stream" {
			if reason, _ := ev.Metadata["reason"].(string); reason == "stream_recv_error" {
				gotResetStream = true
				// 验证 metadata 含 run_id + chunks_emitted
				if _, ok := ev.Metadata["run_id"].(string); !ok {
					t.Errorf("reset_stream 缺 run_id metadata")
				}
				if emitted, _ := ev.Metadata["chunks_emitted"].(int); emitted == 0 {
					t.Errorf("chunks_emitted 应 > 0，实际 %d", emitted)
				}
			}
		}
	}
	if chunkCount != 2 {
		t.Logf("warning: 期望 2 chunks emitted, got %d (chunks 顺序与 sr buffer 有关)", chunkCount)
	}
	if !gotResetStream {
		t.Fatalf("Bug A4 reproduced: mid-stream InterruptError 错误路径未 emit reset_stream\n"+
			"前端 buffer 含老 run_id 的 partial chunks，Resume 后产新 run_id chunks 累积重复\n"+
			"修复方向：streamOrGenerate 错误路径若 chunksEmitted > 0 emit reset_stream\n"+
			"实际 events: %v", eventTypes(captureH.events))
	}
}

func eventTypes(events []agent_v2.Event) []string {
	types := make([]string, len(events))
	for i, ev := range events {
		types[i] = ev.Type
	}
	return types
}

// 防 unused
var _ = errors.Is

// captureLogR10B 收集 slog records 让测试断言。
type captureLogR10B struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureLogR10B) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureLogR10B) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *captureLogR10B) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureLogR10B) WithGroup(_ string) slog.Handler      { return h }

// TestRepro_R10_B1_AutoLoadResumeUnmatchedWarning 验证 audit R10 Bug B1：
// auto-load Resume 路径同显式 WithResumeFrom 一样应做 unmatched ID 检查——
// R9 B4 fix 仅覆盖显式路径，auto-load 路径 silent 跳过。
func TestRepro_R10_B1_AutoLoadResumeUnmatchedWarning(t *testing.T) {
	captureH := &captureLogR10B{}
	origLogger := slog.Default()
	slog.SetDefault(slog.New(captureH))
	defer slog.SetDefault(origLogger)

	// Run 1：节点抛中断 + auto save cp
	throwFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "real_id", Question: "real?"},
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("ask", throwFn)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b1_sid"),
	)

	// Run 2：仅 WithCheckpointer + WithSessionID + WithResumeAnswers(typo_id)
	// 不传 WithResumeFrom——走 auto-load 路径
	cm2 := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b1_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"typo_id": schema.UserMessage("yes"), // 拼错 ID
		}),
	)

	// 验证：应有 unmatched warning（修复前 silent 跳过）
	captureH.mu.Lock()
	defer captureH.mu.Unlock()
	gotWarn := false
	for _, r := range captureH.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, "unmatched InterruptIDs") {
			gotWarn = true
			break
		}
	}
	if !gotWarn {
		t.Fatalf("Bug B1 reproduced: auto-load Resume 路径 unmatched ID 警告失效\n" +
			"R9 B4 fix 仅覆盖显式 WithResumeFrom，auto-load 路径漏检\n" +
			"修复方向：unmatched 检查在 auto-load 完成后也调一次（用 helper 共享）")
	}
}

// TestRepro_R10_B5_FreshRunAnswersWarn 验证 audit R10 Bug B5：
// caller 误用 WithResumeAnswers 但无 WithResumeFrom + 无 cp 来源（fresh Run 残留
// answers）时框架应 slog.Warn 提示。
func TestRepro_R10_B5_FreshRunAnswersWarn(t *testing.T) {
	captureH := &captureLogR10B{}
	origLogger := slog.Default()
	slog.SetDefault(slog.New(captureH))
	defer slog.SetDefault(origLogger)

	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}
	b := compose_v2.NewBuilder()
	b.AddNode("noop", noopFn)
	b.AddEdge(compose_v2.StartNode, "noop")
	b.AddEdge("noop", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	// caller 误用：仅 WithResumeAnswers 不带 cp 来源
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"x": schema.UserMessage("y"),
		}),
	)

	captureH.mu.Lock()
	defer captureH.mu.Unlock()
	gotWarn := false
	for _, r := range captureH.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, "WithResumeAnswers 无 cp 来源") {
			gotWarn = true
			break
		}
	}
	if !gotWarn {
		t.Fatalf("Bug B5 reproduced: caller 误用 WithResumeAnswers 不带 cp 来源时框架 silent 接受\n" +
			"修复方向：入口检查 cfg.resumeFrom + cfg.checkpointer/sessionID 都缺时 slog.Warn")
	}
}
