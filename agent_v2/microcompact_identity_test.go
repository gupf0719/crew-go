package agent_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

type failFirstOffloadStore struct {
	store.Store
	attempts atomic.Int32
}

func (s *failFirstOffloadStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	if s.attempts.Add(1) == 1 {
		return errors.New("offload temporarily unavailable")
	}
	return s.Store.Put(ctx, ns, key, value, opts...)
}

type compactionCaptureModel struct {
	*stubModel
	inputs [][]*schema.Message
}

func (m *compactionCaptureModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.inputs = append(m.inputs, input)
	return m.stubModel.Generate(ctx, input, opts...)
}

func (m *compactionCaptureModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func TestMicroCompactOffload_ReusedIDAndFailedPut(t *testing.T) {
	const sharedID = "shared"
	const cleared = "[清理旧结果]"
	ctx := t.Context()
	ns := store.NS("offload-test")
	st := &failFirstOffloadStore{Store: store.NewInMemoryStore()}
	legacy := schema.ToolResultMessage(sharedID, "旧版引用的正文")
	if err := st.Store.Put(ctx, ns, sharedID, legacy); err != nil {
		t.Fatal(err)
	}

	read := func(path string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: sharedID, Function: schema.FunctionCall{
			Name: "Read", Arguments: `{"path":"` + path + `"}`,
		}}})
	}
	m := &compactionCaptureModel{stubModel: newStubModel([]*schema.Message{
		read("A"), read("B"), read("C"),
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: sharedID, Function: schema.FunctionCall{Name: "Inspect", Arguments: `{}`}}}),
		schema.AssistantMessage("完成"),
	})}
	tk := readToolkit(func(path string) string { return "正文 " + path })
	tk.Register(tool.NewFuncTool("Inspect", "检查", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success("保留检查结果"), nil
		}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("read_samples"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk), agent_v2.WithStore(st), agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{
			Enabled: true, KeepRecent: 1, OffloadEnabled: true, OffloadNS: ns, Placeholder: cleared,
		}))
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("读取样本"))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "完成" || len(m.inputs) != 5 {
		t.Fatalf("应运行完整的工具和模型链路，模型调用 %d 次，终答 %v，err=%v", len(m.inputs), final, err)
	}
	results := func(round int) []*schema.Message {
		var out []*schema.Message
		for _, message := range m.inputs[round-1] {
			if message.Role == schema.RoleTool {
				out = append(out, message)
				if schema.GetToolResultID(message) != sharedID {
					t.Errorf("第 %d 轮应保留模型协议 ID，实际 %+v", round, message)
				}
			}
		}
		return out
	}
	failed := results(3)
	if len(failed) != 2 || failed[0].Content != cleared || failed[0].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation] != nil || failed[1].Content != "正文 B" {
		t.Errorf("首次写失败时旧结果只能清理正文，最新 Read 应保留，实际 %+v", failed)
	}
	last := results(5)
	if len(last) != 4 {
		t.Fatalf("终轮应收到四个配对结果，实际 %d 条", len(last))
	}
	if last[2].Content != "正文 C" || last[3].Content != "保留检查结果" {
		t.Errorf("近期及白名单外结果应保留正文，实际 %q、%q", last[2].Content, last[3].Content)
	}
	keyA, _ := last[0].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string)
	keyB, _ := last[1].Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string)
	if keyA == "" || keyB == "" || keyA == keyB || keyA == sharedID || keyB == sharedID {
		t.Errorf("重试后两个旧结果应有独立的新引用，实际 %q、%q", keyA, keyB)
	}
	for _, ref := range []struct{ key, content string }{{keyA, "正文 A"}, {keyB, "正文 B"}, {sharedID, legacy.Content}} {
		item, err := st.Get(ctx, ns, ref.key)
		if err != nil || item == nil {
			t.Errorf("引用 %q 应可读，实际 item=%v err=%v", ref.key, item, err)
			continue
		}
		value, _ := item.Value.(map[string]any)
		if value["content"] != ref.content {
			t.Errorf("引用 %q 的正文为 %v，期望 %q", ref.key, value["content"], ref.content)
		}
	}
	if got := st.attempts.Load(); got != 3 {
		t.Errorf("应有一次失败和两次成功写入，实际尝试 %d 次", got)
	}
}
