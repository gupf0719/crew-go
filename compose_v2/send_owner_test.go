package compose_v2_test

// SendDirective owner 的 wire 协议。
//
// # issue-95#1（SendFrom 双发）现在是结构性不可达，不再靠 owner 兜
//
// 那个 bug 的形态是：部分失败超步的成功成员 A 把 SendFrom 存进 cp.DeferredSends 等
// Resume 重放（PA-H1），而 A 随后又被拉回来重跑、再产一份同样的 send ⟹ Target 被调度
// 两次。当时的解法是靠 `owner` 认出「哪条旧 deferred 是 A 的」并精确清掉。
//
// 出边计算归成员之后，**「已成功成员被拉回重跑」这个状态不可达**——一个成员算成功的
// 条件已经包含「它的出边算得出来」，算不出来的根本进不了 completed。那套 purge 机件
// （`mergeCompletedSiblings`）连同它的前提一并删除，哨兵改由
// `TestMultiInstance_CondFailedInstanceOnlyRerun` 的「Z 恰跑 3 次 / W 恰跑 2 次」承担。
//
// `owner` / `owner_instance_id` 字段**保留**：它们仍是「这条 deferred send 是谁发的」
// 的归属信息，跨进程排查时要用，且已进业务 backend 的 wire 契约。本文件只钉键名。

import (
	"encoding/json"
	"testing"

	"crew-go/compose_v2"
)

// TestSendOwner_OwnerWireName 钉住 owner 在 JSON 里的**键名**。
//
// **为什么钉键名而非 round-trip**：round-trip 是恒真的——去掉 `json:"owner"` tag
// 后 Go 仍按字段名大小写不敏感匹配，照样进出。真正会坏的是**跨进程/跨语言消费者**：
// 业务 PG backend 用 `->>'owner'` 取、其它语言 runtime 按 wire 名解——键名一改，
// 它们就静默读到空归属（排查一条 deferred send 是谁发的时候没得看）。
func TestSendOwner_OwnerWireName(t *testing.T) {
	data, err := compose_v2.MarshalCheckpoint(&compose_v2.Checkpoint{
		ID: "sid", GraphID: "g",
		DeferredSends: []compose_v2.SerializedSend{{Target: "z_node", Owner: "a_node"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw struct {
		DeferredSends []map[string]json.RawMessage `json:"deferred_sends"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if len(raw.DeferredSends) != 1 {
		t.Fatalf("deferred_sends: expected 1, got %d", len(raw.DeferredSends))
	}
	got, ok := raw.DeferredSends[0]["owner"]
	if !ok {
		t.Fatalf("cp wire 缺 deferred_sends[].owner 键（实际键: %v）\n"+
			"  跨进程消费者（业务 PG `->>'owner'` / 其它语言 runtime）按此名解——改名即静默读不到 → Target 双发",
			keysOf(raw.DeferredSends[0]))
	}
	if string(got) != `"a_node"` {
		t.Errorf("deferred_sends[].owner 值错: got %s, want \"a_node\"", got)
	}
	// omitempty：owner 空时不占 wire（旧 cp 也是这形态）
	data2, err := compose_v2.MarshalCheckpoint(&compose_v2.Checkpoint{
		ID: "sid", GraphID: "g",
		DeferredSends: []compose_v2.SerializedSend{{Target: "z_node"}},
	})
	if err != nil {
		t.Fatalf("marshal empty owner: %v", err)
	}
	var raw2 struct {
		DeferredSends []map[string]json.RawMessage `json:"deferred_sends"`
	}
	if err := json.Unmarshal(data2, &raw2); err != nil {
		t.Fatalf("unmarshal raw2: %v", err)
	}
	if _, present := raw2.DeferredSends[0]["owner"]; present {
		t.Errorf("owner 空时应 omitempty 不占 wire, 实际键: %v", keysOf(raw2.DeferredSends[0]))
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

var errCondTransient = &transientErr{}

type transientErr struct{}

func (*transientErr) Error() string { return "transient upstream data error" }
