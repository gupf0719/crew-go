package agent_v2

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"weak"

	"crew-go/compose_v2"
	"crew-go/store"
)

var handoffActiveCommitKind = compose_v2.NewTaskCommitSignalKind("agent_v2.handoff.active", commitHandoffActive)

// 默认 store 的写入可跨同一进程内的 Resume。只保留弱引用，实例释放后回收登记。
var handoffActiveStores sync.Map

type handoffActiveWrite struct {
	Namespace   store.Namespace `json:"namespace"`
	Target      string          `json:"target"`
	Owner       string          `json:"owner,omitempty"`
	MemoryStore string          `json:"memory_store,omitempty"`
}

func (h *HandoffAgent) initActiveStore() {
	h.defaultStore = store.NewInMemoryStore()
	h.activeStoreID = rand.Text()
	handoffActiveStores.Store(h.activeStoreID, weak.Make(h.defaultStore))
	runtime.AddCleanup(h.defaultStore, func(id string) { handoffActiveStores.Delete(id) }, h.activeStoreID)
}

func (h *HandoffAgent) stageActive(ctx context.Context, session *RunSession, target string) {
	if fromTopLevelHook, _ := ctx.Value(topLevelAgentStartKey{}).(bool); fromTopLevelHook {
		// 入口钩子里的图没有属于外层任务的节点提交点。
		return
	}
	thread := handoffThreadID(ctx, session)
	if thread == "" {
		return
	}
	base := compose_v2.TaskCommitContext(ctx)
	owner := compose_v2.GetOwner(base)
	write := handoffActiveWrite{
		Namespace: handoffActiveNS(h.name, owner, thread),
		Target:    target,
		Owner:     owner,
	}
	if store.FromContext(base) == nil {
		h.storeOnce.Do(h.initActiveStore)
		write.MemoryStore = h.activeStoreID
	}
	payload, _ := json.Marshal(write)       // 写入描述只包含字符串及其切片。
	key, _ := json.Marshal(write.Namespace) // namespace 是字符串切片。
	compose_v2.EmitSignal(ctx, handoffActiveCommitKind, compose_v2.Signal{
		Dest: identityString(write.MemoryStore, string(key)), Value: string(payload), Origin: compose_v2.GetGraphNS(ctx),
	})
}

func commitHandoffActive(ctx context.Context, signal compose_v2.Signal) error {
	var write handoffActiveWrite
	if err := json.Unmarshal([]byte(signal.Value), &write); err != nil {
		return fmt.Errorf("decode handoff active update: %w", err)
	}
	if write.Owner != compose_v2.GetOwner(ctx) {
		return fmt.Errorf("handoff active update owner differs from the task owner")
	}
	st := store.FromContext(ctx)
	if write.MemoryStore != "" {
		v, _ := handoffActiveStores.Load(write.MemoryStore)
		ref, _ := v.(weak.Pointer[store.InMemoryStore])
		memory := ref.Value()
		if memory == nil {
			return nil // 默认 store 随原实例释放。
		}
		st = memory
	}
	if st == nil {
		return fmt.Errorf("handoff active update requires the task store")
	}
	return st.Put(ctx, write.Namespace, handoffActiveKey, write.Target)
}
