package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// spillRedactHook 是业务常见的「结果脱敏」钩子：只对 dump 这个工具的产出把秘密换掉（按 CallId 关联
// OnToolStart 里看到的工具名——OnToolEnd 的入参里没有工具名）。
type spillRedactHook struct {
	hook.NoOpHooks
	mu    sync.Mutex
	names map[string]string
}

func (h *spillRedactHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	h.mu.Lock()
	h.names[in.CallId] = in.Name
	h.mu.Unlock()
	return ctx, nil
}

func (h *spillRedactHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	h.mu.Lock()
	name := h.names[out.CallId]
	h.mu.Unlock()
	if name == "dump" && out.Result != nil {
		out.Result.Output = strings.ReplaceAll(out.Result.Output, "SECRET-TOKEN", "[REDACTED]")
	}
	return ctx, nil
}

func spillSecretOutput() string {
	out := bigOutput(3000)
	return strings.Replace(out, "第 01500 行：订单 A01500 已发货", "第 01500 行：订单 A01500 SECRET-TOKEN-9f8a", 1)
}

func spillSeenSecret(inputs [][]*schema.Message) bool {
	for _, in := range inputs {
		for _, msg := range in {
			if strings.Contains(msg.Content, "SECRET-TOKEN") {
				return true
			}
		}
	}
	return false
}

// 对照组：没开转存时，OnToolEnd 脱敏作用在全文上，模型任何一轮都看不到秘密。
func TestToolResultSpill_RedactHookNoSpillBaseline(t *testing.T) {
	m := &spillReaderModel{readArgs: `"pattern":"SECRET"`}
	h := &spillRedactHook{names: map[string]string{}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("spiller"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(dumpToolkit(spillSecretOutput(), false)), agent_v2.WithMaxRounds(6),
		agent_v2.WithStore(store.NewInMemoryStore()), agent_v2.WithHooks(h))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-redact-0")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	if spillSeenSecret(m.inputs) {
		t.Fatalf("没开转存时模型也看到了秘密——对照组 fixture 没搭对")
	}
}

// 开了转存：钩子只脱敏了预览，全文按钩子之前的原样进了 store；模型用 read_tool_output 读回秘密。
func TestToolResultSpill_RedactHookAppliesToRetrievableText(t *testing.T) {
	m := &spillReaderModel{readArgs: `"pattern":"SECRET"`}
	h := &spillRedactHook{names: map[string]string{}}
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(spillSecretOutput(), false), agent_v2.WithHooks(h))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-redact-1")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	if !spillIDRe.MatchString(m.toolResult(2)) {
		t.Fatalf("fixture 没搭对：没转存")
	}
	if spillSeenSecret(m.inputs) {
		t.Fatalf("OnToolEnd 脱敏被绕过：模型经 read_tool_output 读到了钩子本该改写掉的内容：\n%.600s", m.toolResult(3))
	}
}

// 失败结果不转存：开了转存、没配 ToolResultMaxBytes 时，tool.Failure 的大 message 原样整段进消息流。
func TestToolResultSpill_LargeFailureSpilled(t *testing.T) {
	big := bigOutput(3000) // ~9 万字节
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("build", "构建", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Failure("build_failed", big), nil
		}))
	m := &spillReaderModel{toolName: "build"}
	ag := spillAgent(m, store.NewInMemoryStore(), tk)
	if _, err := ag.Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-fail"), agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	seen := m.toolResult(2)
	if len(seen) > 5000 {
		t.Fatalf("失败结果 %d 字节整段进了消息流（转存阈值 1000）——失败路径不转存也不截断", len(seen))
	}
}

// 跨任务（同一个会话 ID、NextTask 换 RunID、开了会话历史）：下一个任务从历史里看到预览与 id，读得回。
type spillCrossTaskModel struct {
	mu     sync.Mutex
	calls  int
	inputs [][]*schema.Message
	last   string
}

func (m *spillCrossTaskModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	switch m.calls {
	case 1:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "dump", Arguments: `{}`}}}), nil
	case 2:
		return schema.AssistantMessage("任务一完成"), nil
	case 3: // 任务二：从历史里找 id
		id := ""
		for _, msg := range msgs {
			if mm := spillIDRe.FindStringSubmatch(msg.Content); mm != nil {
				id = mm[1]
			}
		}
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.ReadToolOutputToolName, Arguments: fmt.Sprintf(`{"id":%q,"offset":1500,"limit":1}`, id)}}}), nil
	default:
		m.last = msgs[len(msgs)-1].Content
		return schema.AssistantMessage("完成"), nil
	}
}
func (m *spillCrossTaskModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *spillCrossTaskModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestToolResultSpill_NextTaskReadsViaHistory(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	m := &spillCrossTaskModel{}
	ag := spillAgent(m, store.NewInMemoryStore(), dumpToolkit(bigOutput(3000), false), agent_v2.WithHistory(hs, "t"))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-xtask")
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("导出")); err != nil {
		t.Fatal(err)
	}
	sess.NextTask()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("看第 1500 行")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.last, "1500| 第 01500 行") {
		t.Fatalf("下一个任务读不回上一个任务转存的内容：\n%.400s", m.last)
	}
}
