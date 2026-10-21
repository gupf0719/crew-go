package compose_v2

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestStepStateKey_PrintableForAnyKey：步骤记录的派生键是存档里 NodeStates 的 map 键，要经得起
// 每个后端的 JSON 往返——PostgreSQL 的 JSONB 拒收含 NUL 的字符串（整份存档写不进去，实测），
// 非法 UTF-8 往返后会被替换成 U+FFFD、续跑时读不回。不管业务给的 key 是什么，派生键都只含可
// 打印 ASCII、以节点执行键打头，且不同的 key 派生出不同的键。
//
// 可重做记录的键、带分域（tool_call / 钩子相位）的键同理，且存档投影按第一个冒号切回的归属
// 必须恒是节点执行键。
func TestStepStateKey_PrintableForAnyKey(t *testing.T) {
	const nodeKey = "0123456789abcdef"
	scoped := (&nodeStateAccessor{key: nodeKey, scope: "tc\x00call_0\x000"}).base()
	seen := map[string]string{}
	for _, k := range []string{"", "a", "a\x00b", "\xff\xfe", "中文", "a:step:b", nodeKey} {
		for _, d := range []string{
			stepStateKey(nodeKey, k), redoStepStateKey(nodeKey, 3, k),
			stepStateKey(scoped, k), redoStepStateKey(scoped, 0, k),
		} {
			for _, r := range d {
				if r < 0x20 || r > 0x7e {
					t.Fatalf("key %q 派生出的键 %q 含不可打印字符 %U", k, d, r)
				}
			}
			if nodeStateOwner(d) != nodeKey {
				t.Fatalf("key %q 派生出的键 %q 切不回节点执行键（切出 %q）", k, d, nodeStateOwner(d))
			}
			if prev, dup := seen[d]; dup {
				t.Fatalf("key %q 与 %q 派生出同一个键 %q", prev, k, d)
			}
			seen[d] = k
		}
	}
	if !strings.HasPrefix(stepStateKey(nodeKey, "a"), nodeKey+stepKeySep) {
		t.Fatalf("普通步骤键应形如 <节点执行键>%s<哈希>", stepKeySep)
	}
	if g, ok := redoGenOf(redoStepStateKey(scoped, 7, "a")); !ok || g != 7 {
		t.Fatalf("可重做记录的键应解析得出代际 7，got %d %v", g, ok)
	}
	if _, ok := redoGenOf(stepStateKey(scoped, "a:redo9:x")); ok {
		t.Fatal("普通步骤键不该被当成可重做记录（业务 key 先哈希，不会漏进 :redo 段）")
	}
}

// TestStepResult_StaleRedoRecordsNotExported：CondFailRerunFrom 推进代际之后，旧代际的可重做记录
// 从这份存档恢复时读不到，投影就不该导出它（同「投影谓词 = 读谓词」）——否则每报一次 RerunFrom，
// 存档里就多挂一整份作废的产出（嵌套调用的完成记录是整组事件）。代际本身与普通记录照常导出。
func TestStepResult_StaleRedoRecordsNotExported(t *testing.T) {
	RegisterValueType[string]("string")
	b := NewBuilder().WithName("stale_redo")
	b.AddNode("plan", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		if err := SaveStepResult(ctx, "charge", "done"); err != nil {
			return nil, err
		}
		return nil, SaveStepResult(ctx, "llm", "plan-v1", Rerunnable())
	})
	b.AddEdge(StartNode, "plan")
	b.AddConditionalEdge("plan", func(context.Context, Channels) (NodeID, error) {
		return "", errors.New("计划不合格")
	}, WithCondFailPolicy(CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := NewInMemoryCheckpointer()
	if err := Run(context.Background(), g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("s")); err == nil {
		t.Fatal("cond 应报错")
	}
	cp, err := cper.LoadNS(context.Background(), "s", "")
	if err != nil || cp == nil {
		t.Fatalf("应留下存档：cp=%v err=%v", cp, err)
	}
	var plain, gen int
	for k := range cp.NodeStates {
		switch {
		case strings.HasSuffix(k, rerunGenSuffix):
			gen++
		case strings.Contains(k, stepKeySep):
			plain++
		}
		if g, ok := redoGenOf(k); ok {
			t.Errorf("存档里导出了代际 %d 的可重做记录 %q——它已被 RerunFrom 作废、续跑读不到", g, k)
		}
	}
	if plain != 1 || gen != 1 {
		t.Errorf("普通记录与代际应各导出 1 条，got plain=%d gen=%d（全部键：%v）", plain, gen, keysOf(cp.NodeStates))
	}
}

func keysOf(m map[string]taggedValue) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
