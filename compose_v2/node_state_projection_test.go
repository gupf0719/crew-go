package compose_v2

// `Snapshot` 的 memo 投影导出（只带「从这份 cp 恢复时真读得到」的键）的哨兵。
//
// 投影漏了东西的失败形态是**静默多做一遍副作用**——不是报错、不是状态不等。所以主哨兵
// 是**差分对拍**：投影开 / 关跑同一个 HITL 场景，断言**副作用序列逐条相同**。
//
// ⚠️ fixture 必须有「同超步内一个成功一个中断」。单节点单实例时投影前后导出的集合恰好
// 相同 ⟹ 哨兵假绿（CLAUDE.md「早退兜底盖过 fix 路径」那类炸弹的变体）。

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"crew-go/schema"
)

type projSideEffects struct {
	mu  sync.Mutex
	log []string
}

func (s *projSideEffects) add(what string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, what)
}

func (s *projSideEffects) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.log))
	copy(out, s.log)
	return out
}

// buildProjGraph 造「同超步两个成员：一个做完副作用后成功、一个做完副作用后中断」。
//
// 两个成员都在做副作用**之前**查 memo、**之后**写 memo——这正是 SaveNodeState 规约
// 要求的顺序，也让「memo 丢了」直接表现为副作用重放。
func buildProjGraph(t *testing.T, se *projSideEffects) *Graph {
	t.Helper()
	ch := LastValue[string]("proj_out")
	b := NewBuilder().WithName("proj_graph")
	b = WithChannel(b, ch)

	mk := func(name string, interrupts bool) NodeFunc {
		return func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
			st := &nsProbeState{}
			if prev, ok := GetNodeState[*nsProbeState](ctx); ok && prev != nil {
				st = prev
			}
			if len(st.Done) == 0 {
				se.add(name + ":effect") // ← 真副作用，只该发生一次
				st.Done = append(st.Done, "effect")
				if err := SaveNodeState(ctx, st); err != nil {
					return nil, err
				}
			}
			if interrupts {
				if _, ok := GetResumeAnswer(ctx, "proj_q"); !ok {
					return nil, &InterruptError{
						Query: &AskQuery{InterruptID: "proj_q", Question: "?"},
					}
				}
			}
			return []ChannelWrite{Write(ch, name+":done")}, nil
		}
	}
	b.AddNode("okNode", mk("ok", false))
	b.AddNode("hangNode", mk("hang", true))
	b.AddEdge(StartNode, "okNode")
	b.AddEdge(StartNode, "hangNode")
	b.AddEdge("okNode", EndNode)
	b.AddEdge("hangNode", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return g
}

// runProjScenario 跑完整 Run→中断→Resume→完成，返回副作用序列。
func runProjScenario(t *testing.T, sid string) ([]string, *Checkpoint) {
	t.Helper()
	se := &projSideEffects{}
	g := buildProjGraph(t, se)
	cper := NewInMemoryCheckpointer()
	ctx := context.Background()

	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID(sid))
	if _, ok := IsInterrupt(err); !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}
	cp, _ := cper.LoadNS(ctx, sid, "")
	if cp == nil {
		t.Fatal("应有 cp")
	}

	// 走框架的 Resume 路径（裸 cm + WithResumeFrom）——手动 Restore 到裸 cm 上
	// 会因 channel 未注册而失败，且那不是生产路径
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID(sid),
		WithResumeFrom(cp), WithResumeAnswers(map[string]*schema.Message{"proj_q": schema.UserMessage("y")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	return se.snapshot(), cp
}

// TestNodeStateProjection_SideEffectsIdenticalWithAndWithout 主哨兵：差分对拍。
//
// 投影生效时跑一遍、把投影关掉再跑一遍，副作用序列必须逐条相同。sanity-revert 时
// （把投影改回导出整张表）本测试仍绿——那是**预期**：整张表是投影的超集，不会漏。
// 真正会让它红的是**投影漏了该带的键**（那时 hang 节点 Resume 后重做一遍 effect）。
func TestNodeStateProjection_SideEffectsIdenticalWithAndWithout(t *testing.T) {
	got, cp := runProjScenario(t, "proj_sid_1")

	// 每个副作用恰好一次——ok 节点成功后不该在 Resume 时重做，hang 节点重跑时读回自己的 memo
	counts := map[string]int{}
	for _, e := range got {
		counts[e]++
	}
	for _, want := range []string{"ok:effect", "hang:effect"} {
		if counts[want] != 1 {
			t.Errorf("副作用 %q 发生 %d 次，应恰 1 次（全序列 %v）\n"+
				"  2 次 = 投影漏了该带的键 ⟹ 该节点 Resume 时读不到自己的 memo ⟹ 重做副作用。\n"+
				"  这正是投影唯一的危险失效形态，且它不报错、状态也相等", want, counts[want], got)
		}
	}
	if len(cp.NodeStates) == 0 {
		t.Error("中断那份 cp 应带 memo（hang 节点的），投影把它也丢了")
	}
}

// TestNodeStateProjection_OnlyFrontierKeysExported 投影确实在筛：导出的键 ⊆ frontier。
//
// 这条是「投影真的生效了」的正面前提——没有它，上面那条在「投影根本没跑」时也绿。
func TestNodeStateProjection_OnlyFrontierKeysExported(t *testing.T) {
	_, cp := runProjScenario(t, "proj_sid_2")

	want := map[string]bool{}
	for _, m := range cp.CurrentFrontier {
		want[nodeStateKey(m.NodeID, m.InstanceID, cp.NodeVisits[m.NodeID])] = true
	}
	for k := range cp.NodeStates {
		if !want[k] {
			t.Errorf("cp 里有一个不属于 frontier 的 memo 键 %q——投影没生效或谓词跟读谓词不一致\n"+
				"  frontier=%v visits=%v", k, cp.CurrentFrontier, cp.NodeVisits)
		}
	}
	// 成功那个节点的 memo **不该**在 cp 里（它已 visit++，键不可达）
	okKey := nodeStateKey("okNode", "", cp.NodeVisits["okNode"]-1)
	if _, bad := cp.NodeStates[okKey]; bad {
		t.Errorf("成功节点的旧 memo 仍被导出（key=%q）——投影没起到削减作用", okKey)
	}
	t.Logf("cp.NodeStates=%d 条, frontier=%v, visits=%v", len(cp.NodeStates), cp.CurrentFrontier, cp.NodeVisits)
}

// TestNodeStateProjection_MidRunCheckpointsAreEmpty 归零验证。
//
// 每超步 durability cp 写在 updateFrontier(nextFrontier) 之后，nextFrontier 里的节点
// 这一超步还没执行、上一超步执行过的已 visit++ ⟹ 交集为空 ⟹ 投影后中途 cp 的
// NodeStates 恒为空。而 O(n²) 的字节正是这些中途 cp 贡献的——这条把「投影不是削小、
// 是归零」这个推断转成实测。
func TestNodeStateProjection_MidRunCheckpointsAreEmpty(t *testing.T) {
	se := &projSideEffects{}
	var mu sync.Mutex
	var midSizes []int

	ch := LastValue[int]("proj_n")
	b := NewBuilder().WithName("proj_chain")
	b = WithChannel(b, ch)
	for i := 0; i < 4; i++ {
		name := NodeID(fmt.Sprintf("n%d", i))
		b.AddNode(name, func(ctx context.Context, chs Channels) ([]ChannelWrite, error) {
			se.add(string(name))
			if err := SaveNodeState(ctx, &nsProbeState{Done: []string{string(name)}}); err != nil {
				return nil, err
			}
			v, _ := Get(chs, ch)
			return []ChannelWrite{Write(ch, v+1)}, nil
		})
	}
	b.AddEdge(StartNode, "n0")
	b.AddEdge("n0", "n1")
	b.AddEdge("n1", "n2")
	b.AddEdge("n2", "n3")
	b.AddEdge("n3", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := &sizeRecordingCper{
		Checkpointer: NewInMemoryCheckpointer(),
		onSave: func(cp *Checkpoint) {
			mu.Lock()
			midSizes = append(midSizes, len(cp.NodeStates))
			mu.Unlock()
		},
	}
	if err := Run(context.Background(), g, NewChannelManager(),
		WithCheckpointer(cper), WithSessionID("proj_chain_sid"),
		WithDurability(DurabilitySync)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(midSizes) == 0 {
		t.Fatal("前提不成立：sync durability 下应有中途 cp 写入")
	}
	for i, n := range midSizes {
		if n != 0 {
			t.Errorf("第 %d 份 cp 的 NodeStates 有 %d 条，应为 0——"+
				"中途 cp 的 frontier 与「有活 memo 的节点集」本该交集为空。\n"+
				"  全序列 %v", i, n, midSizes)
		}
	}
	t.Logf("中途 cp NodeStates 条数序列 = %v（全 0 = 投影把 O(n²) 归零）", midSizes)
}

// sizeRecordingCper 在每次 SaveNS 时回调，用于观测中途 cp。
type sizeRecordingCper struct {
	Checkpointer
	onSave func(*Checkpoint)
}

func (c *sizeRecordingCper) SaveNS(ctx context.Context, sid, ns string, cp *Checkpoint) error {
	if c.onSave != nil && cp != nil {
		c.onSave(cp)
	}
	return c.Checkpointer.SaveNS(ctx, sid, ns, cp)
}
