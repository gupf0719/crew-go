package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/store"
)

// audit⁵ HIGH-2 (MicroCompact offload to store) 守护测试.
//
// 学 eino reduction middleware truncate phase (defaultTruncHandler) — tool_result
// 超阈值时把原 content 写 Backend, messages 留 "[Output truncated to filepath]"
// + preview 占位符. LLM 看到 preview 知道有内容, 业务可通过 Backend.Read 拿全文.
//
// crew-go 实施: 复用 store.Store 作 Backend, OffloadEnabled opt-in, 默认 NS
// store.NS("_micro_offload"), key = tool_use_id, value = 原 *schema.Message.
//
// 测覆盖:
//  1. OffloadEnabled=true + store 注入 → store.Get(NS, callID) 拿到原 message
//  2. 占位符含 "[offloaded:<key>] preview: <head>" + 双 marker (Offloaded + OffloadKey)
//  3. OffloadEnabled=false (默认) → 行为同纯清占位符 (向后兼容)
//  4. OffloadEnabled=true 但 store 未注入 → 降级为纯占位符 + slog.Warn (不阻断)
//  5. store.Put 失败 → 降级为纯占位符 (业务后端 conn 错不阻断主链路)
//  6. 已 offloaded 的 message 不重写 (Resume 路径幂等性)
//
// sanity-revert: 删 microcompact.go HIGH-2 offload block → offloadStore 路径不
// 走 → cloneToolResultWithOffloadRef 不调 → store.Get 拿不到原 message → 此测 fail.

func TestAudit5_HIGH2_MicroCompact_OffloadToStore_StoresOriginalMessage(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)

	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
		// 用默认 OffloadNS (store.NS("_micro_offload"))
	}

	// 构 5 个 Read tool_result 触发清 3 个 (keep recent 2)
	msgs := []*schema.Message{}
	expectedOriginal := make(map[string]string) // callID → 原 content
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		content := fmt.Sprintf("LONG ORIGINAL CONTENT for call %d, this should be offloaded to store and retrievable", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"/x"}`}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", content))
		expectedOriginal[callID] = content
	}

	out := agent_v2.MicroCompactRun(ctx, msgs, cfg)

	// 早 3 个被 offload — 验 store.Get 拿到原 message
	offloadedCount := 0
	for i := 0; i < 3; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		item, err := st.Get(ctx, store.NS("_micro_offload"), callID)
		if err != nil {
			t.Fatalf("store.Get(%s) err: %v", callID, err)
		}
		if item == nil {
			t.Fatalf("audit⁵ HIGH-2 regression: callID=%s 应被 offload 到 store 但 store.Get 返 nil\n"+
				"sanity-revert: 删 microcompact.go offload block (offloadStore != nil 分支) → 此测 fail", callID)
		}
		storedMsg, ok := item.Value.(*schema.Message)
		if !ok || storedMsg == nil {
			t.Fatalf("store value should be *schema.Message, got %T", item.Value)
		}
		if storedMsg.Content != expectedOriginal[callID] {
			t.Errorf("offloaded content mismatch for %s: want %q got %q", callID, expectedOriginal[callID], storedMsg.Content)
		}
		offloadedCount++
	}
	if offloadedCount != 3 {
		t.Errorf("expected 3 offloaded messages, got %d", offloadedCount)
	}

	// 验 out 内对应 tool_result 已被替占位符 (含 "[offloaded:" 前缀 + key)
	offloadedInOut := 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedInOut++
			// 应有双 marker
			if m.Annotations == nil {
				t.Errorf("offloaded message missing Annotations")
				continue
			}
			if v, ok := m.Annotations[agent_v2.MicroCompactOffloadedAnnotation].(bool); !ok || !v {
				t.Errorf("missing MicroCompactOffloadedAnnotation marker")
			}
			if k, ok := m.Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string); !ok || k == "" {
				t.Errorf("missing MicroCompactOffloadKeyAnnotation key")
			}
			// preview 应是原 content 头部 200 字节 (这里原 content < 200 全保留)
			if !strings.Contains(m.Content, "LONG ORIGINAL") {
				t.Errorf("preview should contain original head, got %q", m.Content)
			}
		}
	}
	if offloadedInOut != 3 {
		t.Errorf("expected 3 messages with [offloaded: prefix in out, got %d", offloadedInOut)
	}
}

func TestAudit5_HIGH2_MicroCompact_OffloadDisabled_FallsBackToPlaceholder(t *testing.T) {
	// OffloadEnabled=false (默认) → 行为同现状: 替占位符, store 不被调
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: false,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", "content"))
	}
	out := agent_v2.MicroCompactRun(ctx, msgs, cfg)
	// 验 store 内 0 item (offload 未启用)
	items, _ := st.Search(ctx, store.NS("_micro_offload"))
	if len(items) != 0 {
		t.Errorf("OffloadEnabled=false 时不应 store.Put, got %d items in store", len(items))
	}
	// 验占位符是旧 cleared 不是 offloaded
	clearedCount, offloadedCount := 0, 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.Contains(m.Content, "cleared by microcompact") {
			clearedCount++
		}
		if strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedCount++
		}
	}
	if clearedCount != 3 {
		t.Errorf("应 3 个 cleared placeholder, got %d", clearedCount)
	}
	if offloadedCount != 0 {
		t.Errorf("OffloadEnabled=false 时不应出现 offloaded placeholder, got %d", offloadedCount)
	}
}

func TestAudit5_HIGH2_MicroCompact_OffloadNoStore_FallsBackToPlaceholder(t *testing.T) {
	// OffloadEnabled=true 但 ctx 内无 store → 降级为纯占位符 (slog.Warn 不阻断)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", "content"))
	}
	// 不注入 store
	out := agent_v2.MicroCompactRun(context.Background(), msgs, cfg)
	// 验占位符是旧 cleared (降级)
	clearedCount := 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.Contains(m.Content, "cleared by microcompact") {
			clearedCount++
		}
	}
	if clearedCount != 3 {
		t.Errorf("OffloadEnabled=true 但无 store 应降级纯占位符, got %d cleared (expected 3)", clearedCount)
	}
}

// failingStore 模拟 store.Put 失败 (业务后端 conn 错 / quota).
type failingStore struct {
	store.Store
}

func (f *failingStore) Put(_ context.Context, _ store.Namespace, _ string, _ any, _ ...store.PutOption) error {
	return errors.New("simulated backend conn error")
}

func TestAudit5_HIGH2_MicroCompact_OffloadStoreFails_FallsBackToPlaceholder(t *testing.T) {
	// store.Put 失败 → 降级为纯占位符 (不阻断主链路)
	st := &failingStore{Store: store.NewInMemoryStore()}
	ctx := store.WithContext(context.Background(), st)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", "content"))
	}
	out := agent_v2.MicroCompactRun(ctx, msgs, cfg)
	// 全部降级为纯 cleared
	clearedCount, offloadedCount := 0, 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.Contains(m.Content, "cleared by microcompact") {
			clearedCount++
		}
		if strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedCount++
		}
	}
	if clearedCount != 3 {
		t.Errorf("store.Put 失败应降级纯占位符, got %d cleared", clearedCount)
	}
	if offloadedCount != 0 {
		t.Errorf("store.Put 失败时不应有 offloaded placeholder, got %d", offloadedCount)
	}
}

// TestAudit5_HIGH2_AuditHIGH1_AutoCompactSkipsOffloadedInReinject 验
// audit HIGH-1 fix: AutoCompact.extractRecentFiles 不再把 offloaded 占位符当
// 真 Read 文件内容收进 reinject.
//
// sanity-revert: 删 autocompact.go:773 块内 MicroCompactOffloadedAnnotation 守
// + line 790 块 `strings.HasPrefix(content, "[offloaded:")` 兜底 → 此测 fail
// (reinjected 内含 "[offloaded:" 内容).
func TestAudit5_HIGH2_AuditHIGH1_AutoCompactSkipsOffloadedInReinject(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)

	// 构造一段: 5 个 Read tool_result, MicroCompact offload 早 3 个, 剩 2 个原文.
	// AutoCompact reinjectRecentFiles 倒扫——若 HIGH-1 漏 fix, offload 占位符也算
	// 真 Read 文件被 reinject 回 messages.
	microCfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	// content 必须够大触发 AutoCompact shouldTrigger (threshold=140) — 否则 AutoCompactRun
	// 直接 return messages 不跑 reinject. 每条加 padding 让 token 超阈值.
	bigPad := strings.Repeat("padding text to fill up token budget so AutoCompact really triggers ", 5)
	msgs := []*schema.Message{schema.SystemMessage("you are helper")}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		args := fmt.Sprintf(`{"path":"/file_%d.txt"}`, i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read", Arguments: args}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", fmt.Sprintf("ORIGINAL CONTENT for file_%d.txt %s", i, bigPad)))
	}
	// 加 user 后续轮触发 GroupMessagesByAPIRound 多组 (lastUserGroupIndex 要 >= 1)
	msgs = append(msgs, schema.UserMessage("now summarize please "+bigPad))
	msgs = append(msgs, schema.AssistantMessage("ok will do "+bigPad))
	// 跑 MicroCompact 把早 3 个 Read tool_result 替成 [offloaded:tc_N] 占位符
	out := agent_v2.MicroCompactRun(ctx, msgs, microCfg)

	// 验 out 内有 3 个 [offloaded:tc_x] 占位符
	offloadedSeen := 0
	for _, m := range out {
		if m != nil && strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedSeen++
		}
	}
	if offloadedSeen != 3 {
		t.Fatalf("setup expect 3 offloaded, got %d", offloadedSeen)
	}

	// 现在跑 AutoCompact reinjectRecentFiles (业务 reinject 默认 enabled 即 true)
	// 但 AutoCompactRun 整体会走 shouldTrigger 太麻烦, 直接 spot-check 反 extract
	// 子逻辑——这里走 trick: AutoCompact 内部不暴露 extractRecentFiles, 但可以
	// 用 AutoCompactRun 跑 full path 验最终 messages 不含 offloaded preview 被重注入.
	//
	// 用 stub LLM 让 AutoCompact 触发. 阈值小 + ReinjectRecentFiles=true.
	autoCfg := &agent_v2.AutoCompactConfig{
		Enabled:               true,
		Model:                 &alwaysSummaryModel{},
		ModelContextWindow:    200,
		SummaryMaxTokens:      10,
		SummaryBufferTokens:   50,
		ReinjectRecentFiles:   true,
		ReinjectMaxFiles:      10,
		ReinjectPerFileBudget: 1000,
		ReinjectTotalBudget:   10000,
	}
	// 注: 我们用 out 作输入 (含 [offloaded: 占位符) — 这就是 MicroCompact 之后
	// AutoCompact 在 modelFn 内串行 wrap 的真实路径.
	final, err := agent_v2.AutoCompactRun(ctx, autoCfg, out)
	if err != nil {
		t.Fatalf("AutoCompactRun err: %v", err)
	}
	// 验 reinjected 内不应含 [offloaded: 字面量 — fix 后 extractRecentFiles 跳 offloaded.
	// 找 reinject "<file path=" 标记 (autocompact buildReinjectionMessage 模板)
	for _, m := range final {
		if m == nil {
			continue
		}
		// reinjection message 是 user message with "# Recently Read Files" header
		if strings.Contains(m.Content, "# Recently Read Files") {
			if strings.Contains(m.Content, "[offloaded:") {
				t.Errorf("audit HIGH-1 regression: AutoCompact reinjectRecentFiles 不应把 offloaded 占位符当真文件内容回注入, got message:\n%s\n"+
					"sanity-revert: 删 autocompact.go:773 块内 MicroCompactOffloadedAnnotation 守 → 此测 fail", m.Content)
			}
		}
	}
}

// TestAudit5_HIGH2_AuditHIGH_C1_AutoCompactMarkerGuardIndependent 验
// audit HIGH-C1 (5 类炸弹 #4 双层防护遮蔽): autocompact.go::extractRecentFiles
// 有 marker guard (`MicroCompactOffloadedAnnotation`) + 字符串兜底 (`"[offloaded:"`
// 前缀) 双层. 之前 AuditHIGH1 测试 fixture content 含 "[offloaded:" 前缀 →
// 单独删 marker guard 测试仍 PASS (字符串兜底接管). 加 marker-only fixture
// (Annotations 含 marker 但 Content 不含前缀) 让 marker guard 跟字符串兜底真分叉.
//
// sanity-revert: 单独删 autocompact.go marker guard (保留字符串兜底) → 此测 fail
// (reinject 拿到 ORIG_MARKER_ONLY 内容当真 Read 文件).
func TestAudit5_HIGH2_AuditHIGH_C1_AutoCompactMarkerGuardIndependent(t *testing.T) {
	// 构造: marker-only message (Annotations 标 offloaded 但 Content 仍是原始
	// "ORIG_MARKER_ONLY content" — 不含 "[offloaded:" 前缀)
	bigPad := strings.Repeat("padding text to fill up token budget so AutoCompact really triggers ", 5)
	msgs := []*schema.Message{schema.SystemMessage("you are helper")}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		args := fmt.Sprintf(`{"path":"/file_%d.txt"}`, i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read", Arguments: args}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		toolMsg := schema.ToolResultMessage(callID, "Read", fmt.Sprintf("ORIG_MARKER_ONLY_%d %s", i, bigPad))
		if i < 3 {
			// 早 3 个加 marker 但 Content 仍原始 (没经过 cloneToolResultWithOffloadRef)
			if toolMsg.Annotations == nil {
				toolMsg.Annotations = map[string]any{}
			}
			toolMsg.Annotations[agent_v2.MicroCompactOffloadedAnnotation] = true
		}
		msgs = append(msgs, toolMsg)
	}
	msgs = append(msgs, schema.UserMessage("now summarize please "+bigPad))
	msgs = append(msgs, schema.AssistantMessage("ok will do "+bigPad))

	autoCfg := &agent_v2.AutoCompactConfig{
		Enabled:               true,
		Model:                 &alwaysSummaryModel{},
		ModelContextWindow:    200,
		SummaryMaxTokens:      10,
		SummaryBufferTokens:   50,
		ReinjectRecentFiles:   true,
		ReinjectMaxFiles:      10,
		ReinjectPerFileBudget: 1000,
		ReinjectTotalBudget:   10000,
	}
	final, err := agent_v2.AutoCompactRun(context.Background(), autoCfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun err: %v", err)
	}
	// 验 marker 范围 (tc_0/1/2 有 marker) 内的 file content 不进 reinject;
	// marker 范围外 (tc_3/4 无 marker) 仍可进 reinject (期望行为).
	for _, m := range final {
		if m == nil {
			continue
		}
		if strings.Contains(m.Content, "# Recently Read Files") {
			// tc_0/1/2 加了 marker — content 不应进 reinject
			for _, blocked := range []string{"ORIG_MARKER_ONLY_0", "ORIG_MARKER_ONLY_1", "ORIG_MARKER_ONLY_2"} {
				if strings.Contains(m.Content, blocked) {
					t.Errorf("audit HIGH-C1 regression (双层防护遮蔽): autocompact.go marker guard 独立工作失败 — marker-only message %q 仍被 reinject\n"+
						"sanity-revert: 单独删 autocompact.go::extractRecentFiles MicroCompactOffloadedAnnotation 守 → 此测 fail (字符串兜底接管不到 marker-only fixture, Content 仍是原始无 [offloaded: 前缀)\n"+
						"got message:\n%s", blocked, m.Content)
				}
			}
		}
	}
}

// TestAudit5_HIGH2_AuditMED1_OffloadPreviewFromContentBlocks 验
// audit MED-1 fix: m.Content="" + ContentBlocks 含 Toolresult.Content 时
// preview 不丢 (fallback 到 ContentBlocks 取头).
func TestAudit5_HIGH2_AuditMED1_OffloadPreviewFromContentBlocks(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	// 构 5 个 Read tool_result 但 m.Content="" 全靠 ContentBlocks (spec 模式)
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		// schema.ToolResultMessage 已经 set 了 Content + ContentBlocks 双写,
		// 但 spec 真实场景可能仅 ContentBlocks. 这里 mock 清掉 Content.
		toolMsg := schema.ToolResultMessage(callID, "Read", fmt.Sprintf("ORIG_CONTENT_FOR_BLOCKS_%d body", i))
		toolMsg.Content = "" // 模拟 ContentBlocks-only message
		msgs = append(msgs, toolMsg)
	}
	out := agent_v2.MicroCompactRun(ctx, msgs, cfg)
	// audit HIGH-C2 anchor: 验 offload path 真跑 (否则 fix revert 后所有 assertion
	// 套 if HasPrefix 内永不执行, 测试假绿)
	offloadedSeen := 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedSeen++
			// preview 应含 fallback 自 ContentBlocks 取的 head
			if !strings.Contains(m.Content, "ORIG_CONTENT_FOR_BLOCKS") {
				t.Errorf("audit MED-1 regression: ContentBlocks-only message offload preview 应 fallback 到 ContentBlocks 取 head, got %q\n"+
					"sanity-revert: 删 cloneToolResultWithOffloadRef previewSrc fallback 逻辑 → 此测 fail", m.Content)
			}
		}
	}
	if offloadedSeen == 0 {
		t.Fatal("audit HIGH-C2 anchor: offload path 未跑 — 测试 setup 错或 fix revert 后 assertion 永不执行假绿")
	}
}

// TestAudit5_HIGH2_AuditLOW3_OffloadPreviewUTF8Safe 验 UTF-8 safe boundary —
// 中文字符不会被切半截 (utf8.RuneStart 正确性).
func TestAudit5_HIGH2_AuditLOW3_OffloadPreviewUTF8Safe(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	// 构 5 个 Read tool_result, content 是中文 (UTF-8 3 byte/char) — preview 切 200 byte
	// 时要找 rune 边界保不切半. 中文字 "字" UTF-8 = 0xE5 0xAD 0x97 (3 byte).
	// 200 / 3 = 66.67, 取 198 byte = 66 字符. preview 应是 66 个中文字, 无乱码.
	msgs := []*schema.Message{}
	bigChinese := strings.Repeat("你好世界", 100) // 100 * 4 字 = 400 字 = 1200 byte
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", bigChinese))
	}
	out := agent_v2.MicroCompactRun(ctx, msgs, cfg)
	// audit HIGH-C2 anchor: offload path 必跑
	offloadedSeen := 0
	for _, m := range out {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if strings.HasPrefix(m.Content, "[offloaded:") {
			offloadedSeen++
			// 验 preview 部分 (去掉 "[offloaded:tc_x] preview: " 前缀) 是 valid UTF-8
			prefix := strings.Index(m.Content, "preview: ")
			if prefix < 0 {
				t.Fatal("preview prefix not found")
			}
			previewPart := m.Content[prefix+len("preview: "):]
			// valid UTF-8 — 用 std 验
			for i, r := range previewPart {
				if r == 0xFFFD { // U+FFFD = replacement char (无效 UTF-8 解码)
					t.Errorf("audit LOW-3 regression: UTF-8 boundary 错截 — preview 第 %d byte 是 invalid UTF-8 (rune=U+FFFD), preview=%q", i, previewPart)
					break
				}
			}
		}
	}
	if offloadedSeen == 0 {
		t.Fatal("audit HIGH-C2 anchor: offload path 未跑 — 测试 setup 错或 fix revert 后假绿")
	}
}

func TestAudit5_HIGH2_MicroCompact_OffloadIdempotent_OnResume(t *testing.T) {
	// 已 offloaded 的 message 重跑 MicroCompact 不重复 store.Put (Resume 幂等性).
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", fmt.Sprintf("orig content %d", i)))
	}
	// 第一次跑——offload 早 3 个
	out1 := agent_v2.MicroCompactRun(ctx, msgs, cfg)
	// audit HIGH-C2 anchor: 第一次跑必须真 offload, 否则后续幂等性验证假绿
	firstOffloadedCount := 0
	for _, m := range out1 {
		if m != nil && m.Annotations != nil {
			if v, ok := m.Annotations[agent_v2.MicroCompactOffloadedAnnotation].(bool); ok && v {
				firstOffloadedCount++
			}
		}
	}
	if firstOffloadedCount == 0 {
		t.Fatal("audit HIGH-C2 anchor: 第一次 MicroCompactRun 未真 offload — 测试 setup 错或 fix revert 后假绿")
	}
	// 用 out1 模拟 Resume 重跑 — 早 3 个 message 已带 MicroCompactOffloadedAnnotation marker
	out2 := agent_v2.MicroCompactRun(ctx, out1, cfg)

	// 第二次跑不应重写 — out1 跟 out2 内 offloaded message 应同指针 (Bug Y 防护语义同款)
	for i, m1 := range out1 {
		m2 := out2[i]
		if m1 == nil || m2 == nil {
			continue
		}
		if m1.Annotations != nil {
			if v, ok := m1.Annotations[agent_v2.MicroCompactOffloadedAnnotation].(bool); ok && v {
				if m1 != m2 {
					t.Errorf("audit⁵ HIGH-2 idempotency: offloaded message 第二次跑不应重 clone (Bug Y 同款防护), idx=%d", i)
				}
			}
		}
	}
}
