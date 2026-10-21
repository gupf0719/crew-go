package checkpointer_conformance_test

// conformance 套自身的**元测试**：证明那些 round-trip 断言真的能抓住"业务 backend
// 漏序列化字段"。
//
// # 为什么需要这一层
//
// `InMemoryCheckpointer` 存的是裸 `*Checkpoint` 指针、Load 原样返回同一个指针——
// `loaded == original`，所以拿它跑 conformance 时**每一条 round-trip 断言都恒真**。
// 断言写得再全，用 InMemory 自测也证明不了它们有效。真正会掉字段的是跨进程 backend
// （PG JSONB / Redis bytes / 其它语言 runtime）。
//
// 下面用一个强制过 JSON 的装饰器再跑一遍同一套 conformance，让 fixture 覆盖到的
// 字段真的走一趟序列化。
//
// # 它抓什么、不抓什么（别把这层当成 wire 的全部防线）
//
// 装饰器的 marshal 和 unmarshal **走同一个 struct**，进出对称——所以它只抓「字段
// 整个被丢掉」（`json:"-"`、未导出、不可序列化类型），**抓不到键名改动**：把
// `json:"owner"` 随手改成别的拼写（甚至整个删掉 tag，Go 会按字段名匹配）它照样
// 绿，而业务 PG `->>'owner'` / 其它语言 runtime 当场就读不到。实测验过。
//
// 键名那层由 `compose_v2` 里的 `TestISSUE96_InstanceIDWireName` /
// `TestSendOwner_OwnerWireName` 盯着——它们解码进**独立 struct**、按字面键名断言。
// 同一个改键名实验：装饰器 PASS，那批 FAIL。
//
// 两者互补：**装饰器管广度**（全字段自动覆盖、加字段免手写 pin、防丢字段回归），
// **`*_WireName` 管深度**（键名契约，跨进程/跨语言消费者真正依赖的那层，也是
// issue-95#1 的 Owner 历史上真踩过的坑）。给新字段补了装饰器覆盖**不等于**
// wire 收口了，键名 pin 该补还得补。
//
// 另注意装饰器是 **fixture-bound 不是 field-bound**：只有 fixture 里设了值、
// 且有断言读的字段才受保护——加新字段时必须同步扩 fixture，否则这里一声不吭。

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/compose_v2/checkpointer_conformance"
)

// jsonWireCheckpointer 把每个 cp 在存入前 marshal、取出后 unmarshal——模拟真实的
// 跨进程 backend，让"字段有没有上 wire"变成可观测的。
//
// **凡是返回 cp 的方法都过 round-trip**：Save/Load 两侧之外，`ListPending` /
// `LoadAllInSession` 也过——它们返回的 cp 同样该是"反序列化出来的新对象"，跟真实
// backend 一致（不这么做，返回的就是存储里那个指针，又退回指针直通）。
// 只有 `ExistsNS` / `DeleteSubtree` / `Cleanup` 直接委托，它们返回 bool / 计数，
// 不含 cp 内容。
type jsonWireCheckpointer struct {
	inner compose_v2.Checkpointer
}

// roundTrip 走框架自己的 Marshal/Unmarshal——业务 backend 若复用这对函数就自动安全，
// 自己写序列化的则由 conformance 的各条断言把关。
func roundTrip(cp *compose_v2.Checkpoint) (*compose_v2.Checkpoint, error) {
	if cp == nil {
		return nil, nil
	}
	data, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	return compose_v2.UnmarshalCheckpoint(data)
}

func (w *jsonWireCheckpointer) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	out, err := roundTrip(cp)
	if err != nil {
		return err
	}
	return w.inner.SaveNS(ctx, sid, ns, out)
}

func (w *jsonWireCheckpointer) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := w.inner.LoadNS(ctx, sid, ns)
	if err != nil {
		return nil, err
	}
	return roundTrip(cp)
}

func (w *jsonWireCheckpointer) SaveBatch(ctx context.Context, sid string, cps map[string]*compose_v2.Checkpoint) error {
	out := make(map[string]*compose_v2.Checkpoint, len(cps))
	for ns, cp := range cps {
		rt, err := roundTrip(cp)
		if err != nil {
			return err
		}
		out[ns] = rt
	}
	return w.inner.SaveBatch(ctx, sid, out)
}

func (w *jsonWireCheckpointer) LoadAllInSession(ctx context.Context, sid string) ([]compose_v2.NSCheckpoint, error) {
	return w.roundTripList(w.inner.LoadAllInSession(ctx, sid))
}

func (w *jsonWireCheckpointer) ListPending(ctx context.Context, sid string) ([]compose_v2.NSCheckpoint, error) {
	return w.roundTripList(w.inner.ListPending(ctx, sid))
}

// roundTripList 把一批 NSCheckpoint 逐个过 wire。
func (w *jsonWireCheckpointer) roundTripList(in []compose_v2.NSCheckpoint, err error) ([]compose_v2.NSCheckpoint, error) {
	if err != nil {
		return nil, err
	}
	out := make([]compose_v2.NSCheckpoint, 0, len(in))
	for _, nc := range in {
		rt, rerr := roundTrip(nc.Checkpoint)
		if rerr != nil {
			return nil, rerr
		}
		out = append(out, compose_v2.NSCheckpoint{NS: nc.NS, Checkpoint: rt})
	}
	return out, nil
}

func (w *jsonWireCheckpointer) ExistsNS(ctx context.Context, sid, ns string) (bool, error) {
	return w.inner.ExistsNS(ctx, sid, ns)
}

func (w *jsonWireCheckpointer) DeleteSubtree(ctx context.Context, sid, nsPrefix string) (int32, error) {
	return w.inner.DeleteSubtree(ctx, sid, nsPrefix)
}

func (w *jsonWireCheckpointer) Cleanup(ctx context.Context, keepLastN int32) (int32, error) {
	return w.inner.Cleanup(ctx, keepLastN)
}

// TestJSONWireCheckpointer_Conformance 跑全套 conformance，但每个 cp 都真的过一遍
// JSON——**fixture 覆盖到的**字段若整个掉出 wire，会在这里现形（键名改动抓不到，
// 见文件头）。
//
// 反过来说：新增字段而不扩 fixture，本测试给不出任何信号（实测加一个带 `json:"-"`
// 的字段并让 Snapshot 真的填它，两条测试照常绿）。加字段时同步扩
// `SaveNS_Preserves_All_Plain_Fields` 的 fixture 是这层防线的前提。
//
// 与 `TestInMemoryCheckpointer_Conformance` 是互补关系：那条验语义（存进去取出来
// 一致），这条验 wire（字段扛得住序列化）。
//
// sanity 验过：给 `FrontierMember.InstanceID` 换成 `json:"-"` → 本测试 FAIL
// 并精确指向丢掉的实例身份，InMemory 那条照常绿。
//
// （诚实标注：该字段恰好也被 `TestISSUE96_InstanceIDWireName` 盯着，同一改动那边
// 也会 FAIL——所以这次 sanity 证明的是本测试有效，没有证明它相对既有键名 pin 的
// 增量。真正的增量在**没有**手写 pin 的那些字段上。
//
// 另一次 sanity 划清了两者的边界：把 `node_id` 键名改成 `nodeID`，**只有**手写那条
// FAIL，本测试和 `sql/payload_roundtrip` 都照常绿——marshal / unmarshal 走同一个
// Go 结构体，键名一起错就自洽。**键名的稳定性只能靠手写 pin 守**，round-trip 类
// 测试对它结构性失明。）
func TestJSONWireCheckpointer_Conformance(t *testing.T) {
	checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
		return &jsonWireCheckpointer{inner: compose_v2.NewInMemoryCheckpointer()}
	})
}
