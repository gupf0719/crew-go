package agent_v2

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

func spillFixture(t *testing.T, content string) (context.Context, *ToolResultSpillConfig, string) {
	t.Helper()
	cfg := &ToolResultSpillConfig{Threshold: 100, PreviewBytes: 50, Store: store.NewInMemoryStore()}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-u")
	kept, id, _ := spillToolResult(ctx, cfg, "dump", content)
	if id == "" || !strings.Contains(kept, id) {
		t.Fatalf("应转存成功并在说明里给出 id：%q", kept)
	}
	return ctx, cfg, id
}

// TestReadToolOutput_LongSingleLineFolded：单行的大 JSON 按 2000 字节折成多行编号，每一段都读得到——
// 只按换行切的话第 2000 字节之后永远读不到。
func TestReadToolOutput_LongSingleLineFolded(t *testing.T) {
	line := strings.Repeat("a", 4500) + "TAIL"
	ctx, cfg, id := spillFixture(t, line)
	r := readToolOutput(ctx, cfg, map[string]any{"id": id, "offset": float64(3), "limit": float64(1)})
	out, _ := r.Output.(string)
	if !r.Success || !strings.Contains(out, "3| ") || !strings.Contains(out, "TAIL") {
		t.Fatalf("单行 4504 字节应折成 3 行、第 3 行带 TAIL：%+v %q", r.Error, out)
	}
}

// TestReadToolOutput_ByteCap：一次读回的内容有上限（16000 字节）——取回工具自己不能变成新的大输出。
func TestReadToolOutput_ByteCap(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 3000; i++ {
		b.WriteString(strings.Repeat("x", 100))
		b.WriteByte('\n')
	}
	ctx, cfg, id := spillFixture(t, b.String())
	r := readToolOutput(ctx, cfg, map[string]any{"id": id, "limit": float64(2000)})
	out, _ := r.Output.(string)
	if len(out) > readToolOutputMaxBytes+500 {
		t.Fatalf("一次读回 %d 字节，超过上限 %d", len(out), readToolOutputMaxBytes)
	}
	if !strings.Contains(out, "接着读用 offset=") {
		t.Fatalf("读到上限时应告诉模型从哪接着读：%q", out[len(out)-200:])
	}
}

// TestReadToolOutput_OutOfRange：offset 超出范围报给模型，不当成空结果。
func TestReadToolOutput_OutOfRange(t *testing.T) {
	ctx, cfg, id := spillFixture(t, strings.Repeat("y\n", 200))
	r := readToolOutput(ctx, cfg, map[string]any{"id": id, "offset": float64(5000)})
	if r.Success || r.Error == nil || r.Error.Code != "out_of_range" {
		t.Fatalf("offset 越界应返回 out_of_range，got %+v", r)
	}
}

// TestMicroCompact_SpilledResultKeepsLocator：MicroCompact 清理一条产生时就转存过的结果时，占位符里保留
// 取回方式，也不把「转存说明」再卸载一遍（那样模型得先读回说明才拿得到 id）。
func TestMicroCompact_SpilledResultKeepsLocator(t *testing.T) {
	var msgs []*schema.Message
	for i := 0; i < 3; i++ {
		id := "c" + string(rune('0'+i))
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "Bash", Arguments: `{}`}}}))
		res := schema.ToolResultMessage(id, "【工具输出过大，已转存】……")
		res.Annotations = map[string]any{ToolResultSpillAnnotation: "sp_00" + id}
		msgs = append(msgs, res)
	}
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	out := MicroCompactRun(ctx, msgs, &MicroCompactConfig{Enabled: true, KeepRecent: 1, OffloadEnabled: true})
	cleared := out[1].Content
	if !strings.Contains(cleared, ReadToolOutputToolName) || !strings.Contains(cleared, "sp_00c0") {
		t.Fatalf("清理后的占位符应保留取回方式与 id：%q", cleared)
	}
	if items, _ := st.Search(ctx, store.NS("_micro_offload")); len(items) != 0 {
		t.Fatalf("转存说明又被卸载了一份（%d 条）", len(items))
	}
}
