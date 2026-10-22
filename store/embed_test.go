package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/store"
)

// fakeEmbedder 按关键词造向量——不是真 embedding，但足以验证「排序按相似度而不是
// 按字典序」这件事：三个正交维度，命中哪个词就在哪一维上给分。
func fakeEmbedder(calls *int) store.Embedder {
	return store.EmbedderFunc(func(_ context.Context, texts []string) ([][]float32, error) {
		if calls != nil {
			*calls++
		}
		out := make([][]float32, len(texts))
		for i, t := range texts {
			v := make([]float32, 3)
			if strings.Contains(t, "猫") {
				v[0] = 1
			}
			if strings.Contains(t, "车") {
				v[1] = 1
			}
			if strings.Contains(t, "菜") {
				v[2] = 1
			}
			out[i] = v
		}
		return out, nil
	})
}

func TestIndex_SemanticOrderBeatsLexical(t *testing.T) {
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil)}))
	ctx := context.Background()
	ns := store.NS("mem")
	// **两个回落序都故意跟语义相关性相反**，zzz 是最该被召回的那条：
	//   - key 字典序：zzz 排最后
	//   - 时新序：zzz 写最早
	// 缺任何一个这条都会在某种时机下空转——原来 zzz 是**最后**写的，于是时间戳不撞桶
	// 时（回落时新序）关掉语义检索它照样第一。实测把索引摘掉：撞桶时返 aaa（红）、
	// 不撞时返 zzz（绿）——一条会看机器脸色的断言。
	_ = st.Put(ctx, ns, "zzz", "我家的猫很胖")
	time.Sleep(2 * time.Millisecond)
	_ = st.Put(ctx, ns, "aaa", "今天买了车")
	time.Sleep(2 * time.Millisecond)
	_ = st.Put(ctx, ns, "bbb", "晚饭做了菜")

	got, err := st.Search(ctx, ns, store.WithQuery("猫"), store.WithLimit(1))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1, got %d", len(got))
	}
	if got[0].Key != "zzz" {
		t.Errorf("语义检索没生效：query=猫 却返回 %q（时新序第一条是 bbb、字典序第一条是 aaa）", got[0].Key)
	}
	if got[0].Score <= 0 {
		t.Errorf("Score 没回填：%v", got[0].Score)
	}
}

func TestIndex_NotConfigured_QueryIgnored(t *testing.T) {
	st := store.NewInMemoryStore() // 不配索引
	ctx := context.Background()
	ns := store.NS("mem")
	// 写入顺序 aaa → zzz。**query 命中的是 zzz**（"猫"），而没配索引时 query 该被忽略
	// ⟹ 返回的必须是**时新序**的第一条（zzz，最后写的），不是语义最相关的那条。
	//
	// ⚠️ fixture 让「语义相关」和「时新序第一」**指向同一条**是不行的——那样分不出
	// 「query 被忽略了」还是「query 生效了」。所以这里靠**第三条**来区分：命中 query 的
	// 是 zzz、时新序第一的是 mmm，返回 mmm 才说明 query 真被忽略。
	//
	// ⚠️ 必须 sleep 拉开时间戳：连写会撞进同一个 µs 桶，那时回落 (ns,key) ⟹ 首条是
	// aaa 而不是 mmm——**而桶内顺序本来就不在契约里**（见 Search 的 godoc /
	// TestSearch_SameDataQueriedRepeatedlyIsStable）。不 sleep 的话这条测的是一个没被
	// 承诺的性质，实测 20 遍红一半。
	_ = st.Put(ctx, ns, "aaa", "今天买了车")
	time.Sleep(2 * time.Millisecond)
	_ = st.Put(ctx, ns, "zzz", "我家的猫很胖")
	time.Sleep(2 * time.Millisecond)
	_ = st.Put(ctx, ns, "mmm", "晚饭做了菜")

	got, _ := st.Search(ctx, ns, store.WithQuery("猫"), store.WithLimit(1))
	if len(got) != 1 || got[0].Key != "mmm" {
		t.Errorf("没配索引时 query 应被忽略、退回时新序（最后写的 mmm）；返回 zzz 说明 query 生效了、"+
			"返回 aaa 说明还在按字典序排。实际 %v", got)
	}
}

// TestIndex_UnembeddableItemsSurvive 钉住：没能 embed 的记录**不会凭空消失**。
//
// 业务 seed 的数据、embed 失败的、value 形态不被抽取器认识的，都在这一档。丢弃它们
// 会让「加了语义检索」变成「一部分数据看不见了」。
func TestIndex_UnembeddableItemsSurvive(t *testing.T) {
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil)}))
	ctx := context.Background()
	ns := store.NS("mem")
	_ = st.Put(ctx, ns, "k1", "我家的猫很胖")
	_ = st.Put(ctx, ns, "k2", 42) // int：默认抽取器不认识 ⟹ 无向量

	got, err := st.Search(ctx, ns, store.WithQuery("猫"))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("抽不出文本的记录不该消失，want 2 got %d", len(got))
	}
	if got[0].Key != "k1" {
		t.Errorf("打了分的应排在前面，实际首条 %q", got[0].Key)
	}
	if got[1].Score != 0 {
		t.Errorf("没向量的记录 Score 应为 0，实际 %v", got[1].Score)
	}
	// 同上：打了分的那条必须真有分，否则「k1 在前」只是字典序碰巧。
	if got[0].Score <= 0 {
		t.Errorf("打了分的记录 Score=%v——打分没在工作", got[0].Score)
	}
}

// TestIndex_MaxPoolingNotAverage 钉住多段文本取**最高**分而非平均。
//
// 平均会让「一段命中、其余无关」的记录被稀释掉——而那正是该被召回的形态。
func TestIndex_MaxPoolingNotAverage(t *testing.T) {
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil)}))
	ctx := context.Background()
	ns := store.NS("mem")
	// 三段文本，只有一段跟"猫"有关。取平均的话分会被另两段拉到 1/3。
	_ = st.Put(ctx, ns, "multi", map[string]any{
		"a": "我家的猫很胖", "b": "今天买了车", "c": "晚饭做了菜",
	})
	// 单段完全命中，但 key 字典序在后。
	_ = st.Put(ctx, ns, "zzz_single", "猫")

	got, err := st.Search(ctx, ns, store.WithQuery("猫"))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 got %d", len(got))
	}
	// max pooling 下两条都是满分 1.0，同分则回落字典序 ⟹ multi 在前。
	// 若实现成平均，multi 只有 1/3 分 ⟹ zzz_single 会排到前面。
	if got[0].Key != "multi" {
		t.Errorf("多段文本应取最高分（max pooling），实际排序 %q,%q（分 %.3f,%.3f）",
			got[0].Key, got[1].Key, got[0].Score, got[1].Score)
	}
	// ⚠️ **上面那条区分不了「max pooling 对」和「打分整个没跑」**——两条记录的期望
	// 名次恰好也是字典序名次（实测：把 cosine 改成恒返 0，用例照样绿）。再钉一条
	// 「分真的算出来了」。
	if got[0].Score <= 0 {
		t.Errorf("首条 Score=%v——打分没在工作，上面那条名次断言是靠字典序碰巧对的", got[0].Score)
	}
}

// TestIndex_EmbedBatchedOnPut 钉住一条记录的多段文本**一次 Embed 调用**发出去。
//
// embedding 服务按批计费和限流，逐条调既慢又贵。
func TestIndex_EmbedBatchedOnPut(t *testing.T) {
	calls := 0
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(&calls)}))
	ctx := context.Background()
	_ = st.Put(ctx, store.NS("mem"), "k", map[string]any{"a": "猫", "b": "车", "c": "菜"})
	if calls != 1 {
		t.Errorf("3 段文本应合并成 1 次 Embed 调用，实际 %d 次", calls)
	}
}

// TestIndex_GetContentValueIsEmbeddable 钉住一条**容易静默失效**的连接：实现了
// `GetContent() string` 的 value，默认抽取器必须认得出来。
//
// 背景：`store` 是叶子包，不能 import 上层的消息类型，所以默认抽取器按
// `interface{ GetContent() string }` 断言。那个断言跟任何具体类型之间**没有编译期
// 约束**——那个方法哪天没了，语义检索对这类 value 就完全不生效，而且**不报错**：
// 抽不出文本 ⟹ 无向量 ⟹ 永远排在打了分的后面。
//
// ⚠️ **这条服务的是业务自己塞进来的消息对象，不是框架写的记忆**（原名
// `...FrameworkWrittenValue...`，2026-09-07 写入侧改存答复文本后那个名字就不实了）。
// 框架今天写的是 `string`，走的是另一条分支，由下面的
// TestIndex_StringValueIsEmbeddable 钉。
func TestIndex_GetContentValueIsEmbeddable(t *testing.T) {
	var probe any = frameworkLikeValue{content: "内容"}
	if _, ok := probe.(interface{ GetContent() string }); !ok {
		t.Fatal("测试自身的 fixture 坏了")
	}
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil)}))
	ctx := context.Background()
	ns := store.NS("mem")
	_ = st.Put(ctx, ns, "aaa", frameworkLikeValue{content: "今天买了车"})
	_ = st.Put(ctx, ns, "zzz", frameworkLikeValue{content: "我家的猫很胖"})

	got, err := st.Search(ctx, ns, store.WithQuery("猫"), store.WithLimit(1))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 1 || got[0].Key != "zzz" {
		t.Fatalf("实现了 GetContent 的 value 应参与语义检索，实际 %v", got)
	}
	if got[0].Score <= 0 {
		t.Errorf("Score 应非零，实际 %v——value 没被 embed", got[0].Score)
	}
}

type frameworkLikeValue struct{ content string }

func (v frameworkLikeValue) GetContent() string { return v.content }

// TestSearch_SameDataQueriedRepeatedlyIsStable 钉住时新序**在业务实际的用法上**是确定的：
// 「写一次、查多次」——同一份已写入的数据，查多少次都是同一个顺序。
//
// ⚠️ 这条承重在**回落键**上：`time.Now()` 本机分辨率约 1µs，一次 Run 里连写多条记忆是
// 常态（实测 8 条连续 Put 只落进 2~3 个时间戳桶）。而 `matched` 是遍历 map 攒出来的、
// 每次顺序都不同，`sort.Slice` 又不稳定——**只按 UpdatedAt 比、不回落 (ns,key) 的话，
// 同一份数据每次查的结果都不一样**。撤掉回落键这条就红。
//
// 与此同时，本测试**故意不断言**桶内顺序等于写入顺序——那个更强的性质要求一个跨得过
// 序列化边界的写入票据（进程内单调序号跨不过，redis/sql 后端做不到 ⟹ 同一个契约在三个
// 后端上语义不同）。业务场景是「写一次、查多次」，桶内那 1µs 的先后没有语义；真需要严格
// 写入序的，自己在 value 里带序号。契约的措辞见 store.Store.Search 的 godoc。
//
// 顺带：仓里有同款先例——`Cleanup(keepLastN)` 曾因时间戳恒等回落 sid 字典序，
// 实测「保留的是最先创建的那批，GC 方向整个反过来」（见 checkpoint-storage.md）。
func TestSearch_SameDataQueriedRepeatedlyIsStable(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	ns := store.NS("burst")
	// 不加任何 sleep——这正是要测的场景：一次 Run 里连写多条
	for i := 0; i < 8; i++ {
		if err := st.Put(ctx, ns, fmt.Sprintf("k%d", i), i); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// 先量一下碰撞到底有多严重，让下一个人看到证据而不是读我的散文
	all, err := st.Search(ctx, ns)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	buckets := map[int64]int{}
	for _, it := range all {
		buckets[it.UpdatedAt.UnixNano()]++
	}
	t.Logf("8 条连续 Put 落进 %d 个不同时间戳（桶内先后无语义）", len(buckets))

	fingerprints := map[string]int{}
	for round := 0; round < 30; round++ {
		got, err := st.Search(ctx, ns, store.WithLimit(5))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		keys := make([]string, 0, len(got))
		for _, it := range got {
			keys = append(keys, it.Key)
		}
		fingerprints[strings.Join(keys, ",")]++
	}
	if len(fingerprints) != 1 {
		t.Errorf("同一份数据查 30 次出现 %d 种 top5 %v——"+
			"时间戳碰撞时没有回落到一个确定的键", len(fingerprints), fingerprints)
	}
}

// TestIndex_StringValueIsEmbeddable 钉住**框架今天真正写进去的那种 value**（裸 string）
// 参与语义检索。
//
// 2026-09-07 起 agent_v2 的长期记忆写的是答复文本（`string`），不再是整条消息——
// 换类型这件事**不会有任何编译期或运行期信号**，`Put` 照常 err == nil，只是抽不出
// 文本时向量没了、命中排到后面去。上面那条 `GetContent` 哨兵在类型切换后照样绿
// （它钉的是另一条分支），所以这条必须独立存在。
func TestIndex_StringValueIsEmbeddable(t *testing.T) {
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil)}))
	ctx := context.Background()
	ns := store.NS("mem")
	_ = st.Put(ctx, ns, "aaa", "今天买了车")
	_ = st.Put(ctx, ns, "zzz", "我家的猫很胖")

	got, err := st.Search(ctx, ns, store.WithQuery("猫"), store.WithLimit(1))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 1 || got[0].Key != "zzz" {
		t.Fatalf("裸 string 应参与语义检索，实际 %v——框架自己的记忆正是这个形态", got)
	}
	if got[0].Score <= 0 {
		t.Errorf("Score 应非零，实际 %v——value 没被 embed", got[0].Score)
	}
}

// 配了 Fields：Put 进来的是 struct 或 `map[string]string` 也按字段取文本——它们读回是 `map[string]any`，跟 filter
// 按字段命中是同一个形状。曾经默认抽取器只认原值本身是 `map[string]any` 的，这两种一律不 embed、score 恒为 0。
// tag 字段故意放了另一个关键词：Fields 没生效、取了全部字段的话第二条也会被 query 命中。
func TestIndex_FieldsApplyToJSONObjectShape(t *testing.T) {
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: fakeEmbedder(nil), Fields: []string{"text"}}))
	ctx := context.Background()
	ns := store.NS("mem")
	type note struct {
		Text string `json:"text"`
		Tag  string `json:"tag"`
	}
	if err := st.Put(ctx, ns, "ss", map[string]string{"text": "我家的猫很胖", "tag": "菜"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, ns, "st", note{Text: "晚饭做了菜", Tag: "猫"}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ q, want string }{{"猫", "ss"}, {"菜", "st"}} {
		got, err := st.Search(ctx, ns, store.WithQuery(c.q), store.WithLimit(2))
		if err != nil {
			t.Fatalf("Search %s: %v", c.q, err)
		}
		if len(got) != 2 || got[0].Key != c.want || got[0].Score <= 0 || got[1].Score > 0 {
			t.Errorf("query %s：want %s 第一且只有它得分，got %v", c.q, c.want, got)
		}
	}
}
