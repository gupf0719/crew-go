package agent_v2

import (
	"math"
	"math/big"
	"runtime"
	"strings"
	"testing"
	"time"

	"crew-go/schema"
)

type niBase struct {
	Tags []string
	Meta map[string]any
}

type niTicket struct {
	niBase
	ID string
}

// TestCloneCalleeInput_EmbeddedUnexportedStruct_ExportedFieldsCopied：值结构体嵌入了一个未导出的结构体，它提升
// 上来的导出字段（encoding/json 照样编码、摘要也计入）要跟着深拷——被调方钩子原地改拷贝里的它们，不能
// 写回调用方。
//
// 曾经只递归导出字段，嵌入字段按名字算未导出、被跳过：钩子一改就写回调用方、续跑换身份重问，扇出时
// 还跟兄弟调用的摘要读撞成数据竞争（均实测）。
func TestCloneCalleeInput_EmbeddedUnexportedStruct_ExportedFieldsCopied(t *testing.T) {
	orig := schema.UserMessage("x")
	orig.Annotations = map[string]any{"t": niTicket{niBase: niBase{Tags: []string{"13800001111"}, Meta: map[string]any{"k": "v"}}, ID: "T-1"}}
	before := inputDigest(orig)
	c := cloneCalleeInput(orig).Annotations["t"].(niTicket)
	c.Tags[0] = "***"
	c.Meta["k"] = "***"
	got := orig.Annotations["t"].(niTicket)
	if got.Tags[0] != "13800001111" || got.Meta["k"] != "v" {
		t.Errorf("改拷贝写回了调用方：%+v", got)
	}
	if inputDigest(orig) != before {
		t.Error("调用方的输入摘要变了")
	}
}

type niScore struct {
	ID    string
	Score float64
}

type niMoney struct{ cents int64 }

// MarshalJSON 是指针接收者：encoding/json 只在值可寻址时（切片元素、结构体字段）调它。
func (m *niMoney) MarshalJSON() ([]byte, error) {
	return []byte(`"` + big.NewInt(m.cents).String() + `"`), nil
}

// niDigestWithNaN 给 v 配一个 NaN 兄弟（整体往返不回来 ⟹ 走逐叶子），返回摘要。
func niDigestWithNaN(v any) string {
	m := schema.UserMessage("x")
	m.Annotations = map[string]any{"bad": math.NaN(), "v": v}
	return inputDigest(m)
}

// TestInputDigest_PerLeaf_DistinguishesInsideStructsMapsAndMarshalers：整体往返不回来的输入走逐叶子。只在
// 别的叶子上不同的两份输入必须算出不同的摘要——哪怕不同的地方在结构体字段里、键不是字符串的 map 里、
// 复数上、指针接收者编码的切片元素里。
//
// 曾经这几种都整体算一个叶子：一个字段坏了（结构体里的分数是 NaN）其余字段（业务 ID）的区别整体丢掉，
// 用户对「处理 T-1 吗」的批准被直接兑付给 T-2（实测，err=nil）。
func TestInputDigest_PerLeaf_DistinguishesInsideStructsMapsAndMarshalers(t *testing.T) {
	cases := []struct {
		name string
		a, b any
	}{
		{"结构体字段（同一个结构体里另有 NaN）", niScore{ID: "T-1", Score: math.NaN()}, niScore{ID: "T-2", Score: math.NaN()}},
		{"指针指向的结构体", &niScore{ID: "T-1", Score: math.NaN()}, &niScore{ID: "T-2", Score: math.NaN()}},
		{"键不是字符串的 map", map[any]any{"a": 1.0, 2: "x"}, map[any]any{"a": 1.0, 2: "y"}},
		{"复数", complex(1, 2), complex(1, 3)},
		{"指针接收者编码的切片元素", []niMoney{{cents: 100}}, []niMoney{{cents: 200}}},
		{"big.Int 切片", []big.Int{*big.NewInt(100)}, []big.Int{*big.NewInt(200)}},
		{"嵌入的未导出结构体里的字段", niTicket{niBase: niBase{Tags: []string{"a"}}, ID: "T"}, niTicket{niBase: niBase{Tags: []string{"b"}}, ID: "T"}},
	}
	for _, c := range cases {
		if niDigestWithNaN(c.a) == niDigestWithNaN(c.b) {
			t.Errorf("[%s] 两份只差一个好叶子的输入算出了同一个摘要", c.name)
		}
		if niDigestWithNaN(c.a) != niDigestWithNaN(c.a) {
			t.Errorf("[%s] 同一份输入两次算出的摘要不同", c.name)
		}
	}
}

// TestInputDigest_PerLeaf_SharedContainersLinear：逐叶子路径里共享的子容器只走一遍。深 40 层、每层两个键
// 指向下一层同一个 map 的结构（总路径数 2^40），摘要要在很短的时间里算完，而且结果确定。
//
// 曾经共享的子容器每条路径都重走一遍：深 14 层就 20 秒以上没算完（实测），而这个计算每次 RunSubAgent 都
// 同步做、不看 ctx。
func TestInputDigest_PerLeaf_SharedContainersLinear(t *testing.T) {
	dag := func(depth int, leaf map[string]any) map[string]any {
		cur := leaf
		for i := 0; i < depth; i++ {
			cur = map[string]any{"a": cur, "b": cur}
		}
		return cur
	}
	// 先在浅的上面数：每个容器只走一遍时，NaN 那个坏叶子只出现一次，另有每层一条「引用」；重复走的话
	// NaN 出现 2^12 次。排在计时检查之前——重复走时深 40 那一步会一直算下去。
	_, bad := digestValue(map[string]any{"dag": dag(12, map[string]any{"x": math.NaN()})})
	if len(bad) != 13 {
		t.Fatalf("深 12 层的共享结构应记 1 个 NaN + 12 条引用，实际 %d 条坏叶子——共享的子容器被重复走了", len(bad))
	}
	leaf := map[string]any{"x": math.NaN(), "id": "leaf"}
	m := schema.UserMessage("x")
	m.Annotations = map[string]any{"dag": dag(40, leaf)}
	start := time.Now()
	d1 := inputDigest(m)
	d2 := inputDigest(m)
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("摘要用了 %v——共享的子容器被重复走了", el)
	}
	if d1 != d2 {
		t.Error("同一份输入两次算出的摘要不同")
	}
	leaf["id"] = "leaf-2"
	if inputDigest(m) == d1 {
		t.Error("共享子容器里的内容变了，摘要却没变——引用不能吞掉第一次出现处的内容")
	}
}

// TestInputDigest_PerLeaf_ManyBadLeavesLinear：一张每行都有一个缺失值（NaN）的大表走逐叶子。坏叶子很多时
// 摘要仍是线性的。
//
// 曾经每个键都全表扫一遍坏叶子清单（为了清掉撞键被覆盖的那个键留下的坏叶子）：4000 行 2 秒、行数翻倍
// 时间翻四倍，4 万行 15 秒没算完（实测）。
func TestInputDigest_PerLeaf_ManyBadLeavesLinear(t *testing.T) {
	rows := make([]any, 20000)
	for i := range rows {
		rows[i] = map[string]any{"id": float64(i), "v": math.NaN(), "c": "x"}
	}
	m := schema.UserMessage("x")
	m.Annotations = map[string]any{"rows": rows}
	start := time.Now()
	_ = inputDigest(m)
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("2 万个坏叶子的摘要用了 %v——不是线性的", el)
	}
}

// TestInputDigest_LargeText_NotCopied：合法 UTF-8 的大段文本直接按字节视图写进哈希，不先复制一份。
func TestInputDigest_LargeText_NotCopied(t *testing.T) {
	m := schema.UserMessage(strings.Repeat("多模态的大块内容", 1<<18)) // 约 6 MiB
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_ = inputDigest(m)
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Errorf("摘要分配了 %d 字节——大段文本被复制了一份", grew)
	}
}
