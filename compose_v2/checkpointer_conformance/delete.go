package checkpointer_conformance

import (
	"context"
	"sort"
	"testing"

	"crew-go/compose_v2"
)

// testDelete 验证 DeleteSubtree 删 ns 自身与经 `/` 往下的后代（同一节点的 `#`/`@` 兄弟执行
// 不删，判据 compose_v2.IsNSInSubtree）+ 返删除数量。
func testDelete(t *testing.T, factory CheckpointerFactory) {
	t.Run("DeleteSubtree_Exact_NS_Returns_1", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "ns_a", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_b", newCheckpoint("g", compose_v2.CPStatusUnset))

		n, err := cp.DeleteSubtree(ctx, "sid", "ns_a")
		if err != nil {
			t.Fatalf("DeleteSubtree: %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1 cp deleted, got %d", n)
		}

		// ns_a 没了, ns_b 还在
		exists, _ := cp.ExistsNS(ctx, "sid", "ns_a")
		if exists {
			t.Error("ns_a should be deleted")
		}
		exists, _ = cp.ExistsNS(ctx, "sid", "ns_b")
		if !exists {
			t.Error("ns_b should be preserved")
		}
	})

	t.Run("DeleteSubtree_Prefix_Deletes_All_Descendants", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid", "parent", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "parent/child1", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "parent/child2/deep", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "unrelated", newCheckpoint("g", compose_v2.CPStatusUnset))

		n, _ := cp.DeleteSubtree(ctx, "sid", "parent")
		// 应删 3 个 (parent + parent/child1 + parent/child2/deep)
		if n != 3 {
			t.Errorf("expected 3 cp deleted under parent prefix, got %d", n)
		}

		// unrelated 仍在
		exists, _ := cp.ExistsNS(ctx, "sid", "unrelated")
		if !exists {
			t.Error("unrelated cp should be preserved")
		}
	})

	t.Run("DeleteSubtree_Empty_Prefix_Deletes_Whole_Session", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		_ = cp.SaveNS(ctx, "sid_to_clear", "ns_a", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid_to_clear", "ns_b", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "other_sid", "ns_a", newCheckpoint("g", compose_v2.CPStatusUnset))

		n, _ := cp.DeleteSubtree(ctx, "sid_to_clear", "")
		if n != 2 {
			t.Errorf("expected 2 cp deleted in session, got %d", n)
		}

		// other_sid 仍在
		exists, _ := cp.ExistsNS(ctx, "other_sid", "ns_a")
		if !exists {
			t.Error("other_sid cp should be preserved")
		}
	})

	t.Run("DeleteSubtree_NonExistent_Returns_0", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		n, err := cp.DeleteSubtree(ctx, "no_such_sid", "no_such_ns")
		if err != nil {
			t.Errorf("DeleteSubtree non-existent should not error, got: %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 deleted for non-existent, got %d", n)
		}
	})

	// audit 视角 1 H1: crew-go IsNSInSubtree 用 `root+/` 后缀分隔, 不裸 prefix.
	// 业务 PG/MySQL backend 若用裸前缀 `WHERE ns LIKE 'ns_a%'`
	// 会 false-positive 误删 `ns_ab` / `ns_a1`. conformance 加 sibling prefix 测.
	t.Run("DeleteSubtree_Sibling_Prefix_Not_Affected", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// "ns_a" 跟 "ns_ab" / "ns_a1" / "ns_abc" 是 sibling (共字符前缀但非 NS 祖先)
		// 真后代是 "ns_a/child" (用 / 分隔)
		//
		// ⚠️ sibling 必须**至少有一个比 prefix 长 2 个字符以上**的 (这里是
		// "ns_abc"): backend 常见写法是先按长度早退 (`if #ns > #prefix + 1`)
		// 再检分隔符, 只放长度恰好 +1 的 sibling 会被那道早退**免费**挡下 ——
		// 分隔符检查有没有在干活测不出来 (redis backend 实测: 把段级检查整个
		// 拆掉退化成裸前缀匹配, 只有 +1 sibling 的旧 fixture 照样全绿)。
		_ = cp.SaveNS(ctx, "sid", "ns_a", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_a/child", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_ab", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_a1", newCheckpoint("g", compose_v2.CPStatusUnset))
		_ = cp.SaveNS(ctx, "sid", "ns_abc", newCheckpoint("g", compose_v2.CPStatusUnset))

		// DeleteSubtree("ns_a") 应删 ns_a 自身 + ns_a/child (真后代 = 2 个), 不删
		// ns_ab / ns_a1 (sibling 共字符前缀不算后代)
		n, err := cp.DeleteSubtree(ctx, "sid", "ns_a")
		if err != nil {
			t.Fatalf("DeleteSubtree ns_a: %v", err)
		}
		if n != 2 {
			t.Errorf("expected 2 cps deleted (ns_a + ns_a/child), got %d", n)
		}

		// ns_ab / ns_a1 / ns_abc 必须仍在 — 这是业务 PG `LIKE 'ns_a%'` 经典坑
		for _, sib := range []string{"ns_ab", "ns_a1", "ns_abc"} {
			if ex, _ := cp.ExistsNS(ctx, "sid", sib); !ex {
				t.Errorf("%s should be preserved (sibling, not descendant)", sib)
			}
		}
		// ns_a + ns_a/child 应已删
		if ex, _ := cp.ExistsNS(ctx, "sid", "ns_a"); ex {
			t.Error("ns_a should be deleted")
		}
		if ex, _ := cp.ExistsNS(ctx, "sid", "ns_a/child"); ex {
			t.Error("ns_a/child should be deleted")
		}
	})

	// **一次执行只清自己的子树**：前缀自身 + 经 `/` 往下的后代（判据 compose_v2.IsNSInSubtree）。
	// 同一个节点的 `#`（fan-out 实例）与 `@`（重访实例）段是**兄弟执行**，各自清自己的，
	// 不在这里删。
	//
	// 连兄弟一起删不报错，只是兄弟还挂着的存档没了：同一次节点执行里对同一个名字调两次，
	// 第一次成功清理时把第二次的挂起档删了，第二次续跑时只能从头来——副作用重放，模型每次
	// 换 tool_call ID 时永不收敛（实测）。子图节点同一超步既有静态成员又有 SendFrom 实例时，
	// 静态成员成功把实例的挂起档删了，同一个形状。
	t.Run("DeleteSubtree_Sibling_Executions_Survive", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 该删的：前缀自身 + 经 `/` 往下的后代。
		//
		// "sub/" 是非法形态（尾部分隔符，框架不生成），但 SaveNS 不校验 ns，业务手写就存得进去。
		// 它是**唯一**比前缀恰好长 1 个字符的后代——写成 `#ns > #prefix + 1` 的长度早退会漏删
		// 它（redis Lua 曾经就是这么写的，实测只有它跟其他后端答得不一样）。判据同样以
		// compose_v2.IsNSInSubtree 为准：它认这一条。
		descendants := []string{"sub", "sub/", "sub/inner", "sub/inner/deep"}
		// 必须活下来的：
		//   sub@1 / sub@1/inner / sub@11 / sub@111  重访实例及其子树（兄弟执行）
		//   sub#0                                    fan-out 实例（兄弟执行）
		//   subx / subxy                             只是字符前缀相同——裸前缀匹配会误删
		//
		// ⚠️ 字符前缀那组必须**至少有一个比 prefix 长 2 个字符以上**的（这里是 "subxy"）：
		// backend 常见写法是先按长度早退（`if #ns > #prefix + 1`）再检分隔符，只放长度恰好
		// +1 的会被那道早退**免费**挡下——分隔符检查有没有在干活根本测不出来。
		survivors := []string{"sub@1", "sub@1/inner", "sub@11", "sub@111", "sub#0", "subx", "subxy"}
		// 全标 Pending——误删的正是「兄弟还在等答案的挂起档」
		for _, ns := range append(append([]string{}, descendants...), survivors...) {
			if err := cp.SaveNS(ctx, "sid", ns, newCheckpoint("g", compose_v2.CPStatusPending)); err != nil {
				t.Fatalf("SaveNS(%q): %v", ns, err)
			}
		}

		// 正向锚点：删之前这些挂起档确实都在。少了这一条，下面所有「活下来了」的断言在
		// 「backend 根本没存进去」的空数据上一样会红，但「删掉了」的断言会恒真。
		before, err := cp.ListPending(ctx, "sid")
		if err != nil {
			t.Fatalf("ListPending before delete: %v", err)
		}
		if want := len(descendants) + len(survivors); len(before) != want {
			t.Fatalf("fixture 没搭起来: 删之前应有 %d 份挂起档, 实得 %d", want, len(before))
		}

		n, err := cp.DeleteSubtree(ctx, "sid", "sub")
		if err != nil {
			t.Fatalf("DeleteSubtree(sid, \"sub\"): %v", err)
		}

		for _, ns := range descendants {
			if ex, _ := cp.ExistsNS(ctx, "sid", ns); ex {
				t.Errorf("ns %q 在 \"sub\" 的子树里却没被删", ns)
			}
		}
		for _, ns := range survivors {
			if ex, _ := cp.ExistsNS(ctx, "sid", ns); !ex {
				t.Errorf("ns %q 被误删——它不在 \"sub\" 这次执行的子树里（`#`/`@` 是同一个节点的"+
					"兄弟执行、字符前缀相同也不算）；判据以 compose_v2.IsNSInSubtree 为准", ns)
			}
		}
		if n != int32(len(descendants)) {
			t.Errorf("DeleteSubtree(sid, \"sub\") 返 %d, 期望 %d（%v）", n, len(descendants), descendants)
		}

		// 危害面直断：兄弟执行的挂起档必须还在，它们续跑时要捡回来
		after, err := cp.ListPending(ctx, "sid")
		if err != nil {
			t.Fatalf("ListPending after delete: %v", err)
		}
		left := make([]string, 0, len(after))
		for _, p := range after {
			left = append(left, p.NS)
		}
		sort.Strings(left)
		want := append([]string{}, survivors...)
		sort.Strings(want)
		if len(left) != len(want) {
			t.Fatalf("删完应剩 %v 这几份挂起档, 实剩 %v", want, left)
		}
		for i := range want {
			if left[i] != want[i] {
				t.Fatalf("删完应剩 %v 这几份挂起档, 实剩 %v", want, left)
			}
		}
	})

	// 前缀本身带 `@` 段时同样要段级匹配,不能退化成裸字符前缀: "sub@11"（visit=11）/
	// "sub@111"（visit=111）在字符上都以 "sub@1" 打头,但它们**不在** "sub@1" 这次执行的
	// 子树里 —— "sub@1" 之后是 "1" 不是层级分隔符 `/`。backend 若用裸字符前缀判子树
	// （如 LIKE 'sub@1%'，而不是 'sub@1' + '/' + '%'）, 清 visit=1 的子树会顺手把
	// visit=11 / visit=111 的档一起删掉。
	//
	// ⚠️ 两个 sibling 缺一不可: "sub@11" 只比 prefix 长 1 个字符, backend 那道
	// 「长度 +1 就早退」的兜底会**免费**替它挡下来; 真正验分隔符检查在干活的是
	// 长 2 个字符的 "sub@111"（同上一条用例的 ⚠️）。
	t.Run("DeleteSubtree_Visit_Sibling_Prefix_Not_Affected", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		descendants := []string{"sub@1", "sub@1/inner"}
		survivors := []string{"sub", "sub@11", "sub@111", "sub#0"}
		for _, ns := range append(append([]string{}, descendants...), survivors...) {
			_ = cp.SaveNS(ctx, "sid", ns, newCheckpoint("g", compose_v2.CPStatusUnset))
		}

		n, err := cp.DeleteSubtree(ctx, "sid", "sub@1")
		if err != nil {
			t.Fatalf("DeleteSubtree(sid, \"sub@1\"): %v", err)
		}

		for _, ns := range descendants {
			if ex, _ := cp.ExistsNS(ctx, "sid", ns); ex {
				t.Errorf("ns %q 是 \"sub@1\" 的后代却没被删", ns)
			}
		}
		for _, ns := range survivors {
			if ex, _ := cp.ExistsNS(ctx, "sid", ns); !ex {
				t.Errorf("ns %q 被误删——它不是 \"sub@1\" 的后代;"+
					"`@` 段也要段级匹配,不能用裸前缀 LIKE 'sub@1%%'", ns)
			}
		}
		if n != int32(len(descendants)) {
			t.Errorf("DeleteSubtree(sid, \"sub@1\") 返 %d, 期望 %d（%v）",
				n, len(descendants), descendants)
		}
	})

	// **ns 段里的模式元字符不得获得通配能力**（与上一条是**两个不同的缺陷**）。
	//
	// 上一条（sibling `subx`）靠「模式里带上段分隔符」修；这一条靠**转义**修。
	// `ValidateNodeIDForNS` 只拒 `/ # @`——`_` 与 `%` 都是合法的 ns 段字符，而且
	// **框架自己就在生成带 `_` 的段**：LoopAgent 的 `round_<N>`、PlanAgent 的
	// `<name>_step_<N>` / `tmp_step_<N>`、HandoffAgent 的 `agent_<x>_hop<N>`。
	//
	// SQL backend 把段拼进 `LIKE` 时，`_` 是**单字符通配符**：`LIKE 'round_1/%'`
	// 会连 `roundX1/child` 一起删掉，且 `err == nil`、返回计数还大于 0 看着成功。
	// 删掉的可能是别的分支**等答案的挂起档**。
	//
	// 正解是转义 + 显式 ESCAPE 子句，且**只转义 LIKE 那一臂**——等值臂不解释元字符，
	// 转了反而让 `round_1` 匹配不上自己。
	t.Run("DeleteSubtree_Metachars_In_NS_Are_Literal", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()

		// 该被 DeleteSubtree("round_1") 删的
		descendants := []string{"round_1", "round_1/inner"}
		// 必须活下来的——前三个只在 `_` 那一位上不同，正是通配符会吃掉的差异；round_1@2 是
		// 同一个节点的兄弟执行（见上面 DeleteSubtree_Sibling_Executions_Survive）
		siblings := []string{"roundX1", "roundX1/inner", "round-1", "round_1@2"}

		for _, ns := range append(append([]string{}, descendants...), siblings...) {
			if err := cp.SaveNS(ctx, "sid_mc", ns, newCheckpoint("g", compose_v2.CPStatusPending)); err != nil {
				t.Fatalf("SaveNS(%q): %v", ns, err)
			}
		}

		if _, err := cp.DeleteSubtree(ctx, "sid_mc", "round_1"); err != nil {
			t.Fatalf("DeleteSubtree: %v", err)
		}

		for _, ns := range descendants {
			ok, err := cp.ExistsNS(ctx, "sid_mc", ns)
			if err != nil {
				t.Fatalf("ExistsNS(%q): %v", ns, err)
			}
			if ok {
				t.Errorf("ns %q 是 round_1 的后代却没被删——子树匹配漏了", ns)
			}
		}
		for _, ns := range siblings {
			ok, err := cp.ExistsNS(ctx, "sid_mc", ns)
			if err != nil {
				t.Fatalf("ExistsNS(%q): %v", ns, err)
			}
			if !ok {
				t.Errorf("ns %q 不是 round_1 的后代却被删了——`_` 当成通配符吃掉了那一位，"+
					"删掉的可能是别的分支等答案的挂起档", ns)
			}
		}
	})

}
