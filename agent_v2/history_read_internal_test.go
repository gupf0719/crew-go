package agent_v2

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// countingLoadStore 数 Load 读回了多少条，用来断言「只读了尾部」。
type countingLoadStore struct {
	*InMemoryHistoryStore
	loads atomic.Int64
	msgs  atomic.Int64
}

func (s *countingLoadStore) Load(ctx context.Context, key HistoryKey, q HistoryQuery) (HistoryPage, error) {
	p, err := s.InMemoryHistoryStore.Load(ctx, key, q)
	s.loads.Add(1)
	s.msgs.Add(int64(len(p.Messages)))
	return p, err
}

// synthHistory 造一场多 agent 交错的会话：每轮一条用户消息 + 若干个 agent 的工具往返（每次调用
// 1~maxCalls 个工具）+ 收尾答复，偶尔夹一次带 skill 正文标记的 activate_skill 往返。
func synthHistory(r *rand.Rand, turns int, slots []string, maxCalls int, task string) []*schema.Message {
	var out []*schema.Message
	seq := 0
	for t := 0; t < turns; t++ {
		owner := slots[r.Intn(len(slots))]
		out = append(out, stampHistoryAgent(schema.UserMessage(fmt.Sprintf("问题 %d", t)), owner, owner, task))
		rounds := r.Intn(3)
		for k := 0; k < rounds; k++ {
			n := 1 + r.Intn(maxCalls)
			var tcs []schema.ToolCall
			for i := 0; i < n; i++ {
				seq++
				tcs = append(tcs, schema.ToolCall{ID: fmt.Sprintf("c%d", seq), Type: "function",
					Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}})
			}
			out = append(out, stampHistoryAgent(schema.AssistantToolCallMessage(tcs), owner, owner, task))
			for _, tc := range tcs {
				res := schema.ToolResultMessage(tc.ID, "结果 "+tc.ID)
				if r.Intn(300) == 0 {
					res.Annotations = map[string]any{SkillBodyAnnotation: true}
				}
				out = append(out, stampHistoryAgent(res, owner, owner, task))
			}
		}
		out = append(out, stampHistoryAgent(schema.AssistantMessage(fmt.Sprintf("答复 %d", t)), owner, owner, task))
	}
	return out
}

// fullViewPrior 是只读尾部之前的做法：整场会话读出来、投影成视图、再做第 0 层截断。
func fullViewPrior(all []*schema.Message, owner string, mode HandoffHistoryMode, need int) []*schema.Message {
	var view []*schema.Message
	for _, m := range all {
		if v := projectHistoryForView(m, owner, mode); v != nil {
			view = append(view, v)
		}
	}
	return truncatePriorPreservingMarkers(view, need)
}

// sameMessageSeq 按模型看得到的内容比：角色、正文、tool_call 与 tool_result 的配对 ID。不比消息 ID——
// Rewrite 视图每次投影都现造一条转述消息、ID 每次不同。
func sameMessageSeq(a, b []*schema.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Content != b[i].Content || a[i].Role != b[i].Role ||
			schema.GetToolResultID(a[i]) != schema.GetToolResultID(b[i]) ||
			fmt.Sprint(schema.GetToolCalls(a[i])) != fmt.Sprint(schema.GetToolCalls(b[i])) {
			return false
		}
	}
	return true
}

// TestHistoryPrior_TailReadMatchesFullRead：只读尾部得到的 prior 跟「整场读出来再截」**逐条相同**。
//
// 只读尾部省的是读 I/O，不能改模型看到的东西。最容易错的是切点：第 0 层截断会把切点往前拉到声明
// tool_call 的那条 assistant，停读早了，被截在窗口外的就是那条 assistant——在窗口上截的结果里留下
// 孤儿 tool_result、或少了一组调用。随机造几百场会话（多个 agent 交错、每次 1~12 个并发调用、三种
// 视图、几种保留条数）逐场比对。
//
// 只比「窗口内没有更早 skill 正文」的会话：比窗口更早激活的 skill 正文不再进 prior 是有意的语义变化
// （见 historyPrior godoc），由 TestHistoryPrior_OldSkillBodyOutsideWindowNotLoaded 单独钉。
func TestHistoryPrior_TailReadMatchesFullRead(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	slots := []string{"a", "b", "c"}
	modes := []HandoffHistoryMode{HandoffHistorySelfOnly, HandoffHistoryRewrite, HandoffHistoryRaw}
	cases, compared := 0, 0
	for iter := 0; iter < 300; iter++ {
		turns := 5 + r.Intn(120)
		all := synthHistory(r, turns, slots, 12, "task-x")
		hs := NewInMemoryHistoryStore()
		key := HistoryKey{Owner: "t", ConvID: fmt.Sprintf("c%d", iter)}
		if err := hs.Append(context.Background(), key, all); err != nil {
			t.Fatal(err)
		}
		hc := &historyConfig{store: hs, owner: "t"}
		ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
		for _, mode := range modes {
			for _, need := range []int{2, 10, 40} {
				owner := slots[r.Intn(len(slots))]
				cases++
				got, ok := historyPrior(ctx, hc, owner, mode, need)
				if !ok {
					t.Fatalf("iter %d: 读历史失败", iter)
				}
				want := fullViewPrior(all, owner, mode, need)
				// 窗口外有更早的 skill 正文时两边按设计不同，跳过
				if hasOlderSkillBody(all, owner, mode, got) {
					continue
				}
				compared++
				if gotCut := truncatePriorPreservingMarkers(got, need); !sameMessageSeq(gotCut, want) {
					t.Fatalf("iter %d mode %v need %d owner %s：只读尾部的 prior 跟整场读出来再截不同\n got(%d)=%s\nwant(%d)=%s",
						iter, mode, need, owner, len(gotCut), ids(gotCut), len(want), ids(want))
				}
			}
		}
	}
	t.Logf("比对了 %d / %d 个用例", compared, cases)
	if compared < cases/2 {
		t.Fatalf("fixture 没搭对：%d 个用例里只比了 %d 个（被「窗口外有更早 skill 正文」跳过太多）", cases, compared)
	}
}

// hasOlderSkillBody 报：视图里有没有比读回来的那一段更早的 skill 正文。
func hasOlderSkillBody(all []*schema.Message, owner string, mode HandoffHistoryMode, window []*schema.Message) bool {
	if len(window) == 0 {
		return false
	}
	firstID := window[0].ID
	for _, m := range all {
		v := projectHistoryForView(m, owner, mode)
		if v == nil {
			continue
		}
		if v.ID == firstID {
			return false
		}
		if hasSkillMarker(v) {
			return true
		}
	}
	return false
}

func ids(ms []*schema.Message) string {
	s := ""
	for _, m := range ms {
		c := m.Content
		if len(c) > 8 {
			c = c[:8]
		}
		s += fmt.Sprintf("[%s %s]", m.Role, c)
	}
	return s
}

// TestHistoryPrior_ReadsBoundedTail：会话再长，一次只读到够用为止。
//
// 断言选在「读回了多少条」上——这个优化的全部意义就是它不随会话长度涨。2000 轮的会话、保留 20 对，
// 读回的条数要在百条量级；改回读全量时它是 8000 条。
func TestHistoryPrior_ReadsBoundedTail(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	all := synthHistory(r, 2000, []string{"a"}, 3, "task-x")
	st := &countingLoadStore{InMemoryHistoryStore: NewInMemoryHistoryStore()}
	key := HistoryKey{Owner: "t", ConvID: "long"}
	if err := st.Append(context.Background(), key, all); err != nil {
		t.Fatal(err)
	}
	hc := &historyConfig{store: st, owner: "t"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
	got, ok := historyPrior(ctx, hc, "a", HandoffHistorySelfOnly, 40)
	if !ok || len(got) < 40 {
		t.Fatalf("prior 读回 %d 条 ok=%v，want >= 40", len(got), ok)
	}
	if n := st.msgs.Load(); n > 400 {
		t.Fatalf("会话 %d 条、保留 40 条，却读回了 %d 条——没有只读尾部", len(all), n)
	}
	// 对照：不限条数时读到会话开头
	st.msgs.Store(0)
	if _, ok := historyPrior(ctx, hc, "a", HandoffHistorySelfOnly, 0); !ok {
		t.Fatal("读历史失败")
	}
	if n := st.msgs.Load(); n != int64(len(all)) {
		t.Fatalf("need=0 时应读到会话开头：读回 %d 条，会话 %d 条", n, len(all))
	}
}

// TestHistoryPrior_OldSkillBodyOutsideWindowNotLoaded：比读回的那一段更早激活的 skill 正文不再进 prior。
//
// 这是有意的语义变化（见 historyPrior godoc）：原先第 0 层截断从整场会话里保下所有 skill 正文，随激活
// 次数线性增长、没有上限；现在只保读到的这一段里的。钉住它，免得后来的人以为是 bug 改回去读全量。
func TestHistoryPrior_OldSkillBodyOutsideWindowNotLoaded(t *testing.T) {
	var all []*schema.Message
	mk := func(m *schema.Message) { all = append(all, stampHistoryAgent(m, "a", "a", "task-x")) }
	mk(schema.UserMessage("激活 skill"))
	mk(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "sk", Type: "function",
		Function: schema.FunctionCall{Name: activateSkillToolName, Arguments: `{}`}}}))
	body := schema.ToolResultMessage("sk", "skill 正文")
	body.Annotations = map[string]any{SkillBodyAnnotation: true}
	mk(body)
	for i := 0; i < 500; i++ {
		mk(schema.UserMessage(fmt.Sprintf("q%d", i)))
		mk(schema.AssistantMessage(fmt.Sprintf("a%d", i)))
	}
	hs := NewInMemoryHistoryStore()
	key := HistoryKey{Owner: "t", ConvID: "skill"}
	if err := hs.Append(context.Background(), key, all); err != nil {
		t.Fatal(err)
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
	got, _ := historyPrior(ctx, &historyConfig{store: hs, owner: "t"}, "a", HandoffHistorySelfOnly, 40)
	for _, m := range got {
		if hasSkillMarker(m) {
			t.Fatalf("1000 条之前激活的 skill 正文被读回来了——只读尾部失效（读回 %d 条）", len(got))
		}
	}
	if full := fullViewPrior(all, "a", HandoffHistorySelfOnly, 40); !hasSkillMarker(full[1]) {
		t.Fatalf("对照组没搭对：整场读出来再截时应保下那条 skill 正文")
	}
}

// TestFindHistory_ReadsOnlyMatches：「这个任务里记过 X 没有」按消息 ID 与任务章直接问后端，读回的只有命中的
// 记录——前面 5000 多条是别的任务写的，读的量跟它们无关。
//
// 断言三件事：本任务里的找得到、读回 1 条；别的任务写的同一个 ID 在 own 下找不到（去重作用域是当前任务）；
// own=false（跨归属）不按任务限、找得到。
func TestFindHistory_ReadsOnlyMatches(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	old := synthHistory(r, 1200, []string{"a"}, 2, "task-old")
	mine := []*schema.Message{
		stampHistoryAgent(schema.UserMessage("新问题"), "a", "a", "task-new"),
		stampHistoryAgent(schema.AssistantMessage("想一想"), "a", "a", "task-new"),
		stampHistoryAgent(schema.AssistantMessage("答复"), "a", "a", "task-new"),
	}
	st := &countingLoadStore{InMemoryHistoryStore: NewInMemoryHistoryStore()}
	key := HistoryKey{Owner: "t", ConvID: "scan"}
	_ = st.Append(context.Background(), key, old)
	_ = st.Append(context.Background(), key, mine)
	hc := &historyConfig{store: st, owner: "t"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
	ctx = compose_v2.WithSessionIDCtx(ctx, "task-new")

	hit, ok := findHistory(ctx, hc, HistoryQuery{MessageID: mine[0].ID}, true, anyHistoryRecord)
	if !ok || !hit {
		t.Fatalf("本任务记下的那条没找到：hit=%v ok=%v", hit, ok)
	}
	if n := st.msgs.Load(); n != 1 {
		t.Fatalf("会话 %d 条、命中 1 条，读回却是 %d 条——没按条件查", len(old)+3, n)
	}
	if hit, _ = findHistory(ctx, hc, HistoryQuery{MessageID: old[0].ID}, true, anyHistoryRecord); hit {
		t.Fatal("找到了别的任务的记录——own 的作用域该是当前任务")
	}
	if hit, _ = findHistory(ctx, hc, HistoryQuery{MessageID: old[0].ID}, false, anyHistoryRecord); !hit {
		t.Fatal("跨归属的查找不该按任务限")
	}
}

// TestFindHistory_InterleavedTasksStillFound：任务挂起期间同一会话里别的任务写了很多条，续跑时照样找得到本任务
// 更早的记录，读回的只有它自己——不靠「连续多少条别人的记录就停」或起点章去猜任务从哪开始。
func TestFindHistory_InterleavedTasksStillFound(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	before := synthHistory(r, 300, []string{"a"}, 2, "task-old")
	first := stampHistoryAgent(schema.UserMessage("挂起之前的原话"), "a", "a", "task-x")
	between := synthHistory(r, 400, []string{"chat"}, 3, "task-other") // 远超一页
	st := &countingLoadStore{InMemoryHistoryStore: NewInMemoryHistoryStore()}
	key := HistoryKey{Owner: "t", ConvID: "interleave"}
	_ = st.Append(context.Background(), key, before)
	_ = st.Append(context.Background(), key, []*schema.Message{first})
	_ = st.Append(context.Background(), key, between)
	hc := &historyConfig{store: st, owner: "t"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
	ctx = compose_v2.WithSessionIDCtx(ctx, "task-x")

	hit, ok := findHistory(ctx, hc, HistoryQuery{MessageID: first.ID}, true, anyHistoryRecord)
	if !ok || !hit {
		t.Fatalf("中间夹了 %d 条别的任务的记录，续跑时就找不到本任务挂起前的那条了", len(between))
	}
	if n := st.msgs.Load(); n != 1 {
		t.Fatalf("读回 %d 条，want 1（只有命中的那条）", n)
	}
}

// TestFindHistory_SameIDInOtherTaskIgnored：同一个消息 ID 在别的任务里也记过（业务复用同一个消息对象），own 下
// 只认本任务写的那份——别的任务的那份不算「这个任务里记过」。
func TestFindHistory_SameIDInOtherTaskIgnored(t *testing.T) {
	shared := schema.UserMessage("复用的固定指令")
	hs := NewInMemoryHistoryStore()
	key := HistoryKey{Owner: "t", ConvID: "reuse"}
	_ = hs.Append(context.Background(), key, []*schema.Message{
		stampHistoryAgent(shared, "a", "a", "task-1"),
		stampHistoryAgent(schema.AssistantMessage("答复 1"), "a", "a", "task-1"),
	})
	hc := &historyConfig{store: hs, owner: "t"}
	ctx := compose_v2.WithSessionIDCtx(compose_v2.WithThreadIDCtx(context.Background(), key.ConvID), "task-2")
	if hit, _ := findHistory(ctx, hc, HistoryQuery{MessageID: shared.ID}, true, anyHistoryRecord); hit {
		t.Fatal("任务 2 还没记过这条，却按任务 1 的那份判成记过了")
	}
	_ = hs.Append(context.Background(), key, []*schema.Message{stampHistoryAgent(shared, "a", "a", "task-2")})
	if hit, _ := findHistory(ctx, hc, HistoryQuery{MessageID: shared.ID}, true, anyHistoryRecord); !hit {
		t.Fatal("任务 2 记下之后应找得到")
	}
}

// TestHistoryPrior_SelfOnlyReadsOnlyOwnRecords：「只看自己」的视图按归属过滤，读回的只有自己的记录——自己发言
// 少的 agent 进一个长会话时，不用把别人的 5000 多条一页页翻过去才凑不够条数地停下。结果与整场读出来再截相同。
func TestHistoryPrior_SelfOnlyReadsOnlyOwnRecords(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	others := synthHistory(r, 1200, []string{"b"}, 2, "task-b")
	own := []*schema.Message{
		stampHistoryAgent(schema.UserMessage("a 的问题"), "a", "a", "task-a"),
		stampHistoryAgent(schema.AssistantMessage("a 的答复"), "a", "a", "task-a"),
	}
	all := append(append([]*schema.Message{}, own...), others...)
	st := &countingLoadStore{InMemoryHistoryStore: NewInMemoryHistoryStore()}
	key := HistoryKey{Owner: "t", ConvID: "selfonly"}
	_ = st.Append(context.Background(), key, all)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), key.ConvID)
	got, ok := historyPrior(ctx, &historyConfig{store: st, owner: "t"}, "a", HandoffHistorySelfOnly, 40)
	if !ok {
		t.Fatal("读失败")
	}
	if want := fullViewPrior(all, "a", HandoffHistorySelfOnly, 40); !sameMessageSeq(got, want) {
		t.Fatalf("按归属过滤读出来的 prior 与整场读出来再截不同：got %d 条 want %d 条", len(got), len(want))
	}
	if n := st.msgs.Load(); n != int64(len(own)) {
		t.Fatalf("会话 %d 条、自己的 %d 条，读回 %d 条——没按归属过滤", len(all), len(own), n)
	}
}

// TestInMemoryHistoryStore_InvalidCursor：游标不是这个后端铸的时报错，不当成「从头读」。
func TestInMemoryHistoryStore_InvalidCursor(t *testing.T) {
	hs := NewInMemoryHistoryStore()
	key := HistoryKey{Owner: "t", ConvID: "c"}
	_ = hs.Append(context.Background(), key, []*schema.Message{schema.UserMessage("x")})
	if _, err := hs.Load(context.Background(), key, HistoryQuery{Before: "not-a-cursor"}); err == nil {
		t.Fatal("非法游标应报错")
	}
}

// stuckStore 报「前面还有」却给不出能往前走的游标。
type stuckStore struct{ InMemoryHistoryStore }

func (s *stuckStore) Load(_ context.Context, _ HistoryKey, q HistoryQuery) (HistoryPage, error) {
	return HistoryPage{Messages: []*schema.Message{schema.UserMessage("x")}, Start: "5", More: true}, nil
}

// TestHistoryScan_StuckCursorIsAnError：后端违约（游标原地不动）时报错，不死循环。
func TestHistoryScan_StuckCursorIsAnError(t *testing.T) {
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "c")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok := historyPrior(ctx, &historyConfig{store: &stuckStore{}, owner: "t"}, "a", HandoffHistoryRaw, 0); ok {
			t.Error("后端游标原地不动时应报读失败（回落 events），而不是当成读成功")
		}
	}()
	<-done
}

// unfilteredStore 照旧接口实现 Load：过滤条件一概忽略，只认 Before / Limit。
type unfilteredStore struct{ *InMemoryHistoryStore }

func (s *unfilteredStore) Load(ctx context.Context, key HistoryKey, q HistoryQuery) (HistoryPage, error) {
	return s.InMemoryHistoryStore.Load(ctx, key, HistoryQuery{Before: q.Before, Limit: q.Limit})
}

// sameReplyModel 每次都答同一句。
type sameReplyModel struct{}

func (sameReplyModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("答复"), nil
}

func (sameReplyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (sameReplyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return sameReplyModel{}, nil
}

// TestHistory_BackendIgnoringFilters_StillRecordsEachInputOnce：后端没实现过滤条件时（照旧接口写的），框架逐条核对
// 返回的记录——结果照旧正确，只是慢，并告警一次。不核对的话「这个任务里记过这条输入没有」把返回的任何一条记录
// 都当命中：从第二个任务起用户输入一条都不记。用转交编排：共享视图下记原始输入要过这道去重（只看自己的视图
// 不过）。
func TestHistory_BackendIgnoringFilters_StillRecordsEachInputOnce(t *testing.T) {
	unfilteredBackendWarned.Store(false)
	var sb strings.Builder
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer func() { // 换回 slog 的默认 logger 不会把 log 包的输出改回去，要一起还原
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	}()

	hs := &unfilteredStore{InMemoryHistoryStore: NewInMemoryHistoryStore()}
	front := NewBasicAgent(WithName("front"), WithModel(sameReplyModel{}), WithHistory(hs, "t"), WithMaxRounds(2))
	ag := NewHandoffAgent(WithHandoffName("ho"), WithHandoffInitial("front")).AddAgent("front", front)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-unfiltered")
	for _, q := range []string{"问题一", "问题二", "问题三"} {
		if _, err := ag.Run(ctx, NewRunSession(), schema.UserMessage(q)); err != nil {
			t.Fatalf("Run(%s): %v", q, err)
		}
	}
	all, err := LoadAllHistory(ctx, hs.InMemoryHistoryStore, HistoryKey{Owner: "t", ConvID: "conv-unfiltered"})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"问题一", "问题二", "问题三"} {
		n := 0
		for _, m := range all {
			if m.Role == schema.RoleUser && m.Content == q {
				n++
			}
		}
		if n != 1 {
			t.Errorf("用户输入 %q 在历史里 %d 份, want 1——后端没按条件过滤时框架把别的记录当成了命中", q, n)
		}
	}
	if n := strings.Count(sb.String(), "没按 HistoryQuery 的过滤条件"); n != 1 {
		t.Errorf("后端没按条件过滤时应告警恰好 1 次，实际 %d 次：%s", n, sb.String())
	}
}
