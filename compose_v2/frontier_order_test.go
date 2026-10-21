package compose_v2

import "testing"

// TestAssembleFrontier_PreservesDeclarationOrder 钉住**出边的声明序不被 map 扔掉**。
//
// 出边本来就是有序的（`Graph.adj` 是 `[]edge`，按 `AddEdge` 声明序 append），
// `evalOutEdges` 也按那个序产 targets。**只有这里**灌进 map 再 range 把它丢了。
//
// ⚠️ 承重的理由不是「让它稳定」——稳定 ≠ 正确，报第一个和报第 K 个都是任选。承重的是
// **顺序信息本来就有**：N 个分支同时失败时返给调用方的那一个 error 按 frontier 序取，
// 而 map 遍历序让「报哪个」跨次漂。实测那个分布是 **87/13** 不是 50/50——
// **5 次的 CI 大概率一次都撞不上，40 次撞 4~5 次，线上按流量撞**。「测得过、线上炸、
// 复现不了」比五五开难查得多。
func TestAssembleFrontier_PreservesDeclarationOrder(t *testing.T) {
	targets := []NodeID{"z_last", "a_first", "m_mid"}
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		got, isEnd := assembleFrontier(targets)
		if isEnd {
			t.Fatal("fixture 没搭对：不该判 isEnd")
		}
		if len(got) != 3 {
			t.Fatalf("该有 3 个成员，实际 %d", len(got))
		}
		seen[string(got[0].NodeID)]++
	}
	if len(seen) != 1 {
		t.Errorf("200 次出现 %d 种首成员 %v——出边的声明序被 map 扔掉了", len(seen), seen)
	}
	if n := seen["z_last"]; n != 200 {
		t.Errorf("首成员该恒是声明序第一个 z_last，实际分布 %v", seen)
	}

	// 去重仍要生效，且**保留首次出现的位置**
	dup := []NodeID{"b", "a", "b", "c", "a"}
	got, _ := assembleFrontier(dup)
	if len(got) != 3 {
		t.Fatalf("去重后该剩 3 个，实际 %d", len(got))
	}
	for i, want := range []NodeID{"b", "a", "c"} {
		if got[i].NodeID != want {
			t.Errorf("去重该保留首次出现序，位置 %d 期望 %q 实际 %q", i, want, got[i].NodeID)
		}
	}
}
