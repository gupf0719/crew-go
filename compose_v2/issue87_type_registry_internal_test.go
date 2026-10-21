package compose_v2

import (
	"reflect"
	"sync"
	"testing"
)

// TestISSUE87_MED2_RestoreSkipsUntypedNil 验 MED-2 fix — Restore 遇 untyped nil
// (cp wire `<nil>` tag) 时跳过写 cm.values 让 channel zero value 兜底,
// **不**写 cm.values[name] = untyped nil 后让 Get cast 失败 panic。
//
// **sanity-revert**: 把 `if v == nil { continue }` 改回 `if v != nil { 比对 }`
// (即原代码逻辑) → cm.values[name] = nil → Get 后 cast 失败 panic。
func TestISSUE87_MED2_RestoreSkipsUntypedNil(t *testing.T) {
	type myType struct{ N int }
	RegisterValueType[*myType]("issue87.myType")
	ch := LastValue[*myType]("issue87_ch")

	cm := NewChannelManager()
	cm.register(ch.asAny())

	// 构造 cp 含 untyped-nil tag
	cp := &Checkpoint{
		Channels: map[string]taggedValue{
			"issue87_ch": {Type: "<nil>", Data: nil},
		},
	}

	if err := cm.Restore(cp); err != nil {
		t.Fatalf("ISSUE-87 MED-2 fix: Restore 遇 untyped nil 应跳过不报错, got %v", err)
	}

	// Get 应返 channel zero value (nil pointer for *myType)
	v, ok := Get(cm.Channels(), ch)
	if v != nil {
		t.Errorf("Get 应返 zero value (nil), got %v", v)
	}
	if ok {
		t.Errorf("Get ok 应是 false (无值写入), got true")
	}
}

// TestISSUE87_MED1_RegisterValueTypeRaceFree 验 MED-1 fix — RegisterValueType
// 用 LoadOrStore 原子操作避免 TOCTOU race。
//
// 100 goroutine 并发注册同 type 同 name 应幂等不 panic, canonical name 确定。
//
// **audit follow-up 注意**: 同 type 同 name 即使 mu.Lock 被 revert, sync.Map
// 单 op 原子性也能让 canonical name 稳定 — 此测试**不能区分 fix 在 vs revert**。
// 真正测 mutex 跨两 sync.Map 原子性的测试看下面 TestISSUE87_AuditFollowup_HIGH1_*。
func TestISSUE87_MED1_RegisterValueTypeRaceFree(t *testing.T) {
	type myRaceType struct{ V int }
	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("ISSUE-87 MED-1 fix: 并发同 type 同 name 注册不应 panic, got %v", r)
				}
			}()
			RegisterValueType[*myRaceType]("issue87.race")
		}()
	}
	wg.Wait()

	// canonical name 应稳定
	key, ok := LookupKey(reflect.TypeOf(&myRaceType{}))
	if !ok {
		t.Fatalf("LookupKey 应找到 issue87.race")
	}
	if key != "issue87.race" {
		t.Errorf("canonical name 应是 issue87.race, got %q", key)
	}
}

// TestISSUE87_AuditFollowup_HIGH1_CrossMapMutexNoDeadlock 验 typeRegistryMu 跨两
// sync.Map (typeRegistry + typeNameByType) 串行化写**不死锁不 panic + 视图一致**。
//
// **诚实警示 (audit follow-up² HIGH-D1)**: 实测 sanity-revert 删 mu.Lock/Unlock
// 后本测试**仍 PASS** (20/20 -race) — sync.Map 单 op atomic 让 canonical name
// 稳定, mutex 真效果是"逻辑层 ordering" (first-write 严格 first-enter-wins),
// 单元测试无法可靠区分 fix 在 vs revert。**本测试不构成 mutex fix 的 sanity-revert
// 守护**, 只验"持锁不死锁 + canonical name 视图一致"基础不变量。mutex 真验证依赖
// stress test 多 instance / 业务长跑场景 ad-hoc 观察。
//
// 之前 commit 49d3065 误以为本测试拆了 ISSUE-87 假绿 — audit² 视角 D 实测揭露
// 仍是假绿。诚实文档化 > 假装能验。
func TestISSUE87_AuditFollowup_HIGH1_CrossMapMutexNoDeadlock(t *testing.T) {
	type myCrossMapType struct{ V int }
	const N = 200
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	wg.Add(N)

	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("audit follow-up MED-1 fix: 跨两 map 并发注册不应 panic, got %v", r)
				}
			}()
			startBarrier.Wait() // thunder-herd 屏障 — 强制真并发不被 Go scheduler 串行化
			// 同 type 不同 name (一半 nameA, 一半 nameB)
			if i%2 == 0 {
				RegisterValueType[*myCrossMapType]("issue87.crossmap.A")
			} else {
				RegisterValueType[*myCrossMapType]("issue87.crossmap.B")
			}
		}()
	}
	startBarrier.Done() // 释放屏障 — 200 goroutine 同时冲
	wg.Wait()

	// canonical name 应稳定 (取决于哪个 goroutine 先入临界段, A 或 B 都可) — 但**所有 N 次
	// LookupKey 必须返同一值**, 不能交错为 "时而 A 时而 B" (跨两 map race window 症状).
	key, ok := LookupKey(reflect.TypeOf(&myCrossMapType{}))
	if !ok {
		t.Fatalf("LookupKey 应找到 issue87.crossmap.*")
	}
	if key != "issue87.crossmap.A" && key != "issue87.crossmap.B" {
		t.Errorf("canonical name 应是 A 或 B, got %q", key)
	}

	// 100 次连续 lookup 必须返同一 canonical name (无视图分歧)
	for j := 0; j < 100; j++ {
		k, _ := LookupKey(reflect.TypeOf(&myCrossMapType{}))
		if k != key {
			t.Errorf("audit follow-up MED-1 fix: 跨两 map race window 让 canonical name 视图分歧, lookup#%d got %q, expected %q (first lookup)", j, k, key)
		}
	}
}

// TestISSUE87_LOW2_TypeRepInterfaceT 验 LOW-2 fix — typeRep 用
// reflect.TypeOf((*T)(nil)).Elem() 拿 T 自身, 不依赖零值——interface T 时
// 零值是 nil interface, reflect.TypeOf(nil_interface) == nil 让 typesEqual
// 比对 nil==nil 全过, 异 interface T 同名注册全过 → 后续 Get cast 错。
func TestISSUE87_LOW2_TypeRepInterfaceT(t *testing.T) {
	// interface T (Doer) 零值是 nil interface
	type Doer interface{ Do() }
	ch := LastValue[Doer]("issue87_iface_ch")
	h := ch.asAny()

	got := h.typeRep()
	if got == nil {
		// **ISSUE-87 audit LOW-1 fix**: t.Fatalf 早退避免下面 got.Kind() SIGSEGV panic
		t.Fatalf("ISSUE-87 LOW-2 fix: typeRep 不应为 nil (interface T 也应有 type), got nil\n" +
			"sanity-revert: typeRep 改回 reflect.TypeOf(h.ch.z) → interface 零值 nil → typeRep 返 nil")
	}
	expectedKind := reflect.Interface
	if got.Kind() != expectedKind {
		t.Errorf("typeRep().Kind() 应是 Interface, got %v", got.Kind())
	}
}
