package compose_v2

// 分隔符表的机械对账：`nsSeparators` 是唯一定义，每个消费者都必须从它派生。
//
// 钉的形态：这份数据此前散在 6 个函数里各枚举一遍，加第四种分隔符时没有任何东西
// 提醒还有五处要改。实测过——只改其中几处，全套测试（含三个 backend 的 conformance）
// **全绿**，而 ParentNS 已经开始把每一份存档的父链写错，且 err == nil。
//
// 本文件走白盒（`nsSeparators` 未导出），按规约用 _internal_test.go 后缀。

import (
	"errors"
	"testing"
)

// TestNSSeparators_AllConsumersDerive：对表里每一个分隔符，逐个消费者断言它认得。
// 加第四种分隔符时**不用改这条测试**——它自己会覆盖到。
func TestNSSeparators_AllConsumersDerive(t *testing.T) {
	if len(nsSeparators) == 0 {
		t.Fatal("nsSeparators 空——所有 ns 语义都会退化")
	}
	for _, sep := range nsSeparators {
		t.Run("sep="+sep.ch, func(t *testing.T) {
			child := "a" + sep.ch + "1"

			// ① IsNSInSubtree：层级分隔符往下的算 a 这次执行的子树；实例分隔符那种是 a 的
			// 兄弟执行，不算——算进来的话 a 成功清理时会连带删掉兄弟还挂着的存档
			if got, want := IsNSInSubtree("a", child), !sep.isInstance(); got != want {
				t.Errorf("IsNSInSubtree(%q, %q) = %v, want %v", "a", child, got, want)
			}
			// 且 sibling 不能被误认（段级匹配，不是裸前缀）
			if IsNSInSubtree("a", "ab"+sep.ch+"1") {
				t.Errorf("IsNSInSubtree 把 sibling 当成了子树成员（裸前缀匹配）")
			}

			// ② ValidateNodeIDForNS：含这个分隔符的 nodeID 必须被拒
			if err := ValidateNodeIDForNS("node" + sep.ch + "x"); !errors.Is(err, ErrInvalidNodeIDForNS) {
				t.Errorf("ValidateNodeIDForNS 放行了含 %q 的 nodeID —— 它会与派生 ns 撞槽, got %v", sep.ch, err)
			}

			// ③ ParentNS：两档的父都是 "a"，但**理由不同**——实例分隔符是「剥掉实例
			// 后缀、父就是节点本身」（`a#1` → `a`），层级分隔符是「剥掉末段」
			// （`a/1` → `a`）。两条路径在 ParentNS 里是不同分支，都得覆盖到。
			gotParent := ParentNS(child)
			wantParent := "a"
			if gotParent != wantParent {
				t.Errorf("ParentNS(%q) = %q, want %q —— cp.ParentNS 自动填充会写错父链", child, gotParent, wantParent)
			}

			// ④ SplitNS/parseSegment：实例分隔符必须解出 NodeID 与 kind
			segs, err := SplitNS(child)
			if err != nil {
				t.Fatalf("SplitNS(%q): %v", child, err)
			}
			if sep.isInstance() {
				if len(segs) != 1 || segs[0].NodeID != "a" || segs[0].Kind != sep.kind || segs[0].Index != 1 {
					t.Errorf("SplitNS(%q) = %+v —— 实例段没解对（NodeID 该是 %q、Kind 该是 %q）",
						child, segs, "a", sep.kind)
				}
			}
		})
	}
}

// TestNSSubtreeSeparator_IsTheTablesHierarchicalOne：给 redis Lua 用的那个导出值必须就是
// 表里唯一的层级分隔符——Lua 拿它复刻 IsNSInSubtree，两边不同源就会删错范围。
func TestNSSubtreeSeparator_IsTheTablesHierarchicalOne(t *testing.T) {
	var hier []string
	for _, sep := range nsSeparators {
		if !sep.isInstance() {
			hier = append(hier, sep.ch)
		}
	}
	if len(hier) != 1 || hier[0] != NSSubtreeSeparator() {
		t.Fatalf("表里的层级分隔符 %v，NSSubtreeSeparator() = %q——两者必须是同一个", hier, NSSubtreeSeparator())
	}
	if !IsNSInSubtree("a", "a"+NSSubtreeSeparator()+"b") {
		t.Errorf("IsNSInSubtree 不认 NSSubtreeSeparator() 划出的子树")
	}
}

// TestNSSeparatorsTable_WellFormed 校验分隔符表本身填得对——三种违规各自的后果都是
// 静默的，所以必须机械挡：
//
//   - **非单字节**：Go 侧 `strings.ContainsAny` / `strings.LastIndexAny` 把串当**字符集合**（实测
//     `strings.ContainsAny("a:b", "::") = true`），redis Lua 的 `string.sub(ns,n+1,n+1)` 只取
//     一个字符——多字节分隔符两边一起静默退化。
//   - **重复**：后一条永远走不到。
//   - **实例分隔符缺 kind**：`NSSegment.Kind` 变空串，下游按 kind 分派的逻辑全落空。
//
// ⚠️ 这里**不用 init panic**：表是包级字面量、只有改代码才会违规，而 init panic 会让
// 任何 import 本包的程序启动即崩（实测 15 个包连坐），生产进程白担这个风险。
func TestNSSeparatorsTable_WellFormed(t *testing.T) {
	seen := make(map[string]bool, len(nsSeparators))
	seenKind := make(map[string]string, len(nsSeparators))
	for _, sep := range nsSeparators {
		if len(sep.ch) != 1 {
			t.Errorf("分隔符 %q 不是单字节——Go 的 ContainsAny/LastIndexAny 与 redis Lua 的 "+
				"string.sub 都按单字符处理，多字节会静默退化", sep.ch)
		} else if c := sep.ch[0]; c < 0x21 || c > 0x7e {
			// 表 godoc 写着「ASCII」，而 len==1 只管字节数——"\xff" / "\n" 都能穿过去
			// （独立审计实测）。ns 会进日志、错误正文、cp 的存储键，非可打印字符在那些
			// 地方是排查灾难。
			t.Errorf("分隔符 %q 不是可打印 ASCII（0x21-0x7e）——ns 会进日志/错误正文/存储键", sep.ch)
		}
		if seen[sep.ch] {
			t.Errorf("分隔符 %q 重复——后一条永远走不到", sep.ch)
		}
		seen[sep.ch] = true
		// ⚠️ 这里曾有两条断言守「instance 字段与 kind 非空一致」。那两个字段已合并
		// （kind == "" 即层级分隔符），断言随之变成恒假的同义反复——不变量从「测出来」
		// 变成「表示不出来」，这比多留两条断言强。
		if sep.isInstance() {
			if prev, dup := seenKind[sep.kind]; dup {
				t.Errorf("分隔符 %q 与 %q 共用 kind %q——NSSegment.Kind 分不出它们，"+
					"下游按 kind 分派会把两种段当成一种", sep.ch, prev, sep.kind)
			}
			seenKind[sep.kind] = sep.ch
		}
	}
}
