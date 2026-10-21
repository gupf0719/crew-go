package agent_v2_test

// memo 与 4 层压缩的交互——**审计列为「未系统看的角度」，实测排除后留的哨兵**。
//
// # 曾经怀疑的失效（不成立）
//
// memo 存的是首跑产出的**原始** tool_result 对象；MicroCompact 会清白名单内旧
// tool_result 的内容。怀疑：Resume 时 memo 命中把未清版本写回 ⟹ 压缩效果被撤销、
// 上下文回弹。
//
// # 实测结论：不成立
//
// 本测试同时观测两侧，两个数都来自真跑：
//   - **喂给 LLM 的视图里压缩生效了**（`cleared=2 / full=1`，KeepRecent=1）；
//   - **cp 里一个占位符都没有、完整内容全在**。
//
// 所以 memo 和压缩作用在**两份不同的数据**上——memo 存的是 cp 那份（完整历史），
// 压缩只改喂给 LLM 的临时视图。两者不共享可变状态，memo 命中写回原始版本不构成回弹。
//
// # 挡住它的两层，都是承重的
//
//  1. **`MicroCompactRun` clone 不动原 message 指针**（microcompact.go 的 Step 3 +
//     `cloneToolResultWithPlaceholder`）⟹ 它产出的是新对象，channel 里那批没被碰。
//     **sanity-revert 实测承重**：把 clone 改成原地改，本测试立刻红（占位符进了 cp）。
//  2. **压缩结果是 modelFn 的局部变量**，而 `basicMessagesCh` 的三个 writer
//     （preamble 写初始 messages / model 写 LLM 输出 / tools 写 results）没有一个
//     写压缩视图 ⟹ 压缩根本没有回到 channel 的路径。
//
// 所以「压缩不进 cp」正是 memo 正确性的前提：一旦进了，memo 里存的原始版本在 Resume
// 命中时就会真的把压缩效果撤销掉。任何让压缩结果进入持久化状态的改动都会让本测试红。
//
// ⚠️ **写这条时踩过一次假绿，值得记**：第一版占位符用了 `<<<COMPACTED-BY-TEST>>>`，
// 而 Go 的 `json.Marshal` 默认把 `<` `>` `&` 转义成 `\u003c` 等 ⟹ 在 marshal 后的
// blob 里按原文 `Contains` 永远找不到 ⟹ sanity-revert 破了第 1 层保护测试仍绿。
// 我当时没去查为什么破不掉，反而编了个「多重保护」的解释写进注释——**证据与假设冲突
// 时该查证据，不是给假设找理由**。断言用的字面量必须确认它在被断言的那个表示形态里
// 长什么样。

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// compactWitnessModel 按序回放 replies，同时**记下每轮真正喂给 LLM 的 messages**。
// 没有它，本测试的「完整内容还在 cp 里」会变成恒真断言（压缩没触发时也成立）。
type compactWitnessModel struct {
	mu      sync.Mutex
	replies []*schema.Message
	idx     int
	seen    [][]*schema.Message // 每轮 LLM 视图快照
}

func (m *compactWitnessModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make([]*schema.Message, len(msgs))
	copy(snap, msgs)
	m.seen = append(m.seen, snap)
	if m.idx >= len(m.replies) {
		return schema.AssistantMessage("[exhausted]"), nil
	}
	r := m.replies[m.idx]
	m.idx++
	return r, nil
}

func (m *compactWitnessModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *compactWitnessModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// lastViewClearedCount 数最后一轮 LLM 视图里被清掉内容的 tool_result 条数。
func (m *compactWitnessModel) lastViewClearedCount(big string) (cleared, full int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.seen) == 0 {
		return 0, 0
	}
	for _, msg := range m.seen[len(m.seen)-1] {
		if msg == nil || msg.Role != schema.RoleTool {
			continue
		}
		if strings.Contains(msg.Content, big) {
			full++
		} else {
			cleared++
		}
	}
	return cleared, full
}

func TestToolMemo_CompactionNotWrittenBackToCheckpoint(t *testing.T) {
	const bigLen = 200
	big := strings.Repeat("X", bigLen)
	// 占位符由测试指定——不依赖默认常量的字面量（猜错了断言就恒不命中，是假绿的经典来源）。
	// ⚠️ **不能含 `<` `>` `&`**：Go 的 json.Marshal 默认把它们转义成 `\u003c` 等，
	// 在 marshal 后的 blob 里按原文 Contains 永远找不到——第一版用 `<<<...>>>` 正是
	// 这么假绿的（sanity-revert 破了压缩的 clone 保护，测试仍绿）。
	const placeholder = "COMPACTED_BY_TEST_MARKER"

	var reads atomic.Int32
	readTool := tool.NewFuncTool("read_file", "读文件", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			reads.Add(1)
			return tool.Success(big), nil
		})
	holdTool := tool.NewFuncTool("hold", "中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "cmp_q"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "cmp_q", Question: "?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(readTool)
	tk.Register(holdTool)

	witness := &compactWitnessModel{}
	rd := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: id, Function: schema.FunctionCall{Name: "read_file", Arguments: `{}`}}})
	}
	// 前 3 轮各调一次 read_file；KeepRecent=1 ⟹ 到第 4 轮时前几条**该被压缩掉**。
	// 第 4 轮：read_file + hold（中断）——让 memo 里存下 r4 的原始产出。
	witness.replies = []*schema.Message{
		rd("r1"), rd("r2"), rd("r3"),
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "r4", Function: schema.FunctionCall{Name: "read_file", Arguments: `{}`}},
			{ID: "h1", Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}},
		}),
		schema.AssistantMessage("done"),
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("cmpAg"),
		agent_v2.WithModel(witness),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{
			Enabled: true, KeepRecent: 1, CompactableTools: []string{"read_file"},
			Placeholder: placeholder,
		}),
		agent_v2.WithMaxRounds(8))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}
	if reads.Load() != 4 {
		t.Fatalf("前提不成立：首跑 read_file 应 4 次（够触发 KeepRecent=1 的清除），实际 %d", reads.Load())
	}

	// **前提：压缩确实生效了**。没有这条，下面「完整内容还在 cp 里」就是恒真断言
	// ——压缩压根没触发时它也成立（假绿）。
	cleared, fullInView := witness.lastViewClearedCount(big)
	if cleared == 0 {
		t.Fatalf("前提不成立：最后一轮 LLM 视图里没有任何被清的 tool_result"+
			"（cleared=%d full=%d）——MicroCompact 没触发，本测试测的是空气", cleared, fullInView)
	}
	t.Logf("压缩已生效：最后一轮 LLM 视图 cleared=%d / full=%d", cleared, fullInView)

	cp, _ := cper.LoadNS(ctx, sess.RunID, "")
	if cp == nil {
		t.Fatal("应有 cp")
	}
	blob, merr := compose_v2.MarshalCheckpoint(cp)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	s := string(blob)

	// 不变量 2：压缩没写回 cp——4 条 tool_result 的完整内容都还在
	if n := strings.Count(s, big); n < 4 {
		t.Errorf("cp 里完整内容只剩 %d 处（应 >=4）——压缩写回了 channel/cp", n)
	}
	// **主断言**：占位符不该出现在 cp 里——它只活在喂给 LLM 的临时视图。
	// （比数完整内容的份数更可靠：压缩产出的标志是占位符，而 tool_result 的内容
	// 在 Content 和 ContentBlocks 里各存一份，只污染其中一份时份数断言抓不到。）
	if strings.Contains(s, placeholder) {
		t.Errorf("cp 里出现了压缩占位符 %q——压缩结果进了持久化状态。\n"+
			"  一旦进了 cp，memo 里存的原始版本在 Resume 命中时会把压缩效果撤销掉：\n"+
			"  memo 和压缩就从「作用在两份不同数据上」变成「共享一份可变状态」", placeholder)
	}
	// 前提确认：memo 确实存了东西（否则本测试测的是空气）
	if len(cp.NodeStates) == 0 {
		t.Fatal("前提不成立：cp.NodeStates 为空，memo 没生效")
	}

	// 不变量 3：Resume 后 memo 命中，已成功的 read_file 不重跑
	before := reads.Load()
	if _, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("y")})); rerr != nil {
		t.Fatalf("Resume: %v", rerr)
	}
	if got := reads.Load() - before; got != 0 {
		t.Errorf("Resume 后 read_file 又跑了 %d 次，应 0 次（开着压缩时 memo 仍该命中）", got)
	}
}
