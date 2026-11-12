package agent_v2

import (
	"crew-go/internal/streamdelta"
	"crew-go/schema"
)

type streamedToolCalls struct {
	blocks       []*schema.ContentBlock
	byIndex      map[int]int
	indexedSlots map[int]bool
	byID         map[string][]int
}

func (a *streamedToolCalls) add(chunk *schema.Message) {
	indices, _ := chunk.Annotations[streamdelta.ToolCallIndices].(map[int]int)
	used := make(map[int]bool, len(chunk.ContentBlocks))
	for _, index := range indices {
		if slot, ok := a.byIndex[index]; ok {
			used[slot] = true
		}
	}
	for i, block := range chunk.ContentBlocks {
		if block == nil || block.Toolcall == nil {
			continue
		}
		delta := block.Toolcall
		index, indexed := indices[i]
		var slot int
		var found bool
		if indexed {
			slot, found = a.byIndex[index]
			if !found && delta.Id != "" {
				slot, found = a.unindexedSlot(delta.Id, used)
			}
		} else if delta.Id != "" {
			// 无索引的片段按 ID 和本帧出现次序配对。
			for _, candidate := range a.byID[delta.Id] {
				if !used[candidate] {
					slot, found = candidate, true
					break
				}
			}
		}
		if !found {
			slot = len(a.blocks)
			cloned := *block
			cloned.Toolcall = &schema.ToolcallContentBlock{Type: delta.Type}
			a.blocks = append(a.blocks, &cloned)
		}
		used[slot] = true
		if indexed {
			if a.byIndex == nil {
				a.byIndex = make(map[int]int)
				a.indexedSlots = make(map[int]bool)
			}
			a.byIndex[index] = slot
			a.indexedSlots[slot] = true
		}
		call := a.blocks[slot].Toolcall
		if call.Id == "" && delta.Id != "" {
			call.Id = delta.Id
			a.rememberID(delta.Id, slot)
		}
		if delta.Name != "" {
			call.Name = delta.Name
		}
		call.Arguments += delta.Arguments
	}
}

// unindexedSlot 匹配稍后才补索引的片段，候选唯一时才沿用原调用。
func (a *streamedToolCalls) unindexedSlot(id string, used map[int]bool) (int, bool) {
	slot := -1
	for _, candidate := range a.byID[id] {
		if a.indexedSlots[candidate] || used[candidate] {
			continue
		}
		if slot >= 0 {
			return 0, false
		}
		slot = candidate
	}
	return slot, slot >= 0
}

func (a *streamedToolCalls) rememberID(id string, slot int) {
	if id == "" {
		return
	}
	if a.byID == nil {
		a.byID = make(map[string][]int)
	}
	a.byID[id] = append(a.byID[id], slot)
}
