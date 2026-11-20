package compose_v2

import (
	"fmt"
	"slices"
	"strconv"
)

type joinEdge struct {
	from []NodeID
	to   NodeID
}

// AddJoinEdge 在所有指定前驱各成功提交一次后调度 to。到达可以跨越多个超步，
// 同一轮中同名前驱多次提交只记一次。触发后重新收集本组到达记录。
// from 至少包含一个已注册节点，to 可以是已注册节点或 EndNode。
// 普通静态边、条件边和 SendFrom 仍独立触发各自的目标。
func (b *GraphBuilder) AddJoinEdge(from []NodeID, to NodeID) *GraphBuilder {
	sources := slices.Clone(from)
	slices.Sort(sources)
	b.joinEdges = append(b.joinEdges, joinEdge{from: sources, to: to})
	return b
}

// WithDeferredNodes 让已触发的节点等本图的普通工作排空后一起执行。
// 静态边、条件边、AND-join 和 SendFrom 都可以触发收尾。普通工作挂起或
// 失败时，收尾等待队列和实例输入随存档恢复。收尾节点可以通过出边继续调度工作。
func (b *GraphBuilder) WithDeferredNodes(nodes ...NodeID) *GraphBuilder {
	if len(nodes) > 0 && b.deferredNodes == nil {
		b.deferredNodes = make(map[NodeID]struct{}, len(nodes))
	}
	for _, id := range nodes {
		b.deferredNodes[id] = struct{}{}
	}
	return b
}

func (b *GraphBuilder) validateSchedule() error {
	for _, edge := range b.joinEdges {
		if len(edge.from) == 0 {
			return fmt.Errorf("join to %q requires predecessors", edge.to)
		}
		if _, ok := b.nodes[edge.to]; !ok && edge.to != EndNode {
			return fmt.Errorf("join to %q: target node not found", edge.to)
		}
		for i, id := range edge.from {
			if _, ok := b.nodes[id]; !ok {
				return fmt.Errorf("join to %q: predecessor %q not found", edge.to, id)
			}
			if i > 0 && edge.from[i-1] == id {
				return fmt.Errorf("join to %q: duplicate predecessor %q", edge.to, id)
			}
		}
	}
	for id := range b.deferredNodes {
		if _, ok := b.nodes[id]; !ok {
			return fmt.Errorf("deferred node %q is not registered", id)
		}
	}
	return nil
}

// planJoins 只在整个超步可以推进时计算下一份到达账。部分提交的成功兄弟
// 已记录在 CompletedFrontier，等该超步完成时随本次成功成员一起登记。
func (g *Graph) planJoins(cm *ChannelManager, succeeded, completed []FrontierMember) ([]NodeID, map[int][]NodeID) {
	if len(g.joinEdges) == 0 {
		return nil, nil
	}
	arrived := make(map[NodeID]bool, len(succeeded)+len(completed))
	for _, members := range [][]FrontierMember{succeeded, completed} {
		for _, member := range members {
			arrived[member.NodeID] = true
		}
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var targets []NodeID
	var waiting map[int][]NodeID
	for i, edge := range g.joinEdges {
		var seen []NodeID
		for _, id := range edge.from {
			_, already := slices.BinarySearch(cm.joinProgress[i], id)
			if arrived[id] || already {
				seen = append(seen, id)
			}
		}
		if len(seen) == len(edge.from) {
			targets = append(targets, edge.to)
		} else if len(seen) > 0 {
			if waiting == nil {
				waiting = make(map[int][]NodeID)
			}
			waiting[i] = seen
		}
	}
	return targets, waiting
}

type stepSchedule struct {
	frontier     []FrontierMember
	deferred     []FrontierMember
	joinProgress map[int][]NodeID
	inputs       map[string][]ChannelWrite
	sendWrites   []ChannelWrite
}

func (next *stepSchedule) setInput(id string, writes []ChannelWrite) {
	if next.inputs == nil {
		next.inputs = make(map[string][]ChannelWrite)
	}
	next.inputs[id] = writes
}

// planDeferred 从下一帧移出已触发的收尾节点。静态重复触发合并为一次，
// SendFrom 的每个实例保留自己的身份和输入。普通工作排空时整体释放队列。
func (g *Graph) planDeferred(cm *ChannelManager, next *stepSchedule) error {
	if len(g.deferredNodes) == 0 {
		return nil
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	next.deferred = slices.Clone(cm.deferredFrontier)
	waiting := make(map[FrontierMember]bool, len(next.deferred))
	for _, member := range next.deferred {
		waiting[member] = true
		if input, ok := cm.frontierInputs[member.InstanceID]; ok {
			next.setInput(member.InstanceID, input)
		}
	}
	active := next.frontier[:0]
	for _, member := range next.frontier {
		if _, deferred := g.deferredNodes[member.NodeID]; !deferred {
			active = append(active, member)
		} else if !waiting[member] {
			next.deferred = append(next.deferred, member)
			waiting[member] = true
		}
	}
	next.frontier = active
	// 这个队列会在同一个超步执行。等待期间就检查其宽度，避免跨超步积攒无界队列。
	if width := len(next.deferred); width > g.maxWidth {
		return fmt.Errorf("deferred frontier width %d exceeds max %d: %w: %w",
			width, g.maxWidth, &MaxWidthError{Width: width}, ErrRunAborted)
	}
	if len(active) == 0 {
		next.frontier, next.deferred = next.deferred, nil
	}
	return nil
}

func (g *Graph) validateCheckpointSchedule(cp *Checkpoint) error {
	if len(cp.CurrentFrontier) == 0 {
		if len(cp.JoinProgress) > 0 || len(cp.DeferredFrontier) > 0 {
			return fmt.Errorf("%w: scheduling progress without an active frontier", ErrCheckpointNotResumable)
		}
		return nil
	}
	for i, seen := range cp.JoinProgress {
		if i < 0 || i >= len(g.joinEdges) {
			return fmt.Errorf("%w: join %d is not defined", ErrCheckpointNotResumable, i)
		}
		sources := g.joinEdges[i].from
		if len(seen) == 0 || len(seen) >= len(sources) {
			return fmt.Errorf("%w: join %d is not partially satisfied", ErrCheckpointNotResumable, i)
		}
		for j, id := range seen {
			if _, ok := slices.BinarySearch(sources, id); !ok || j > 0 && seen[j-1] >= id {
				return fmt.Errorf("%w: join %d has invalid predecessor %q", ErrCheckpointNotResumable, i, id)
			}
		}
	}
	if len(g.deferredNodes) == 0 && len(cp.DeferredFrontier) == 0 {
		return nil
	}
	_, activeDeferred := g.deferredNodes[cp.CurrentFrontier[0].NodeID]
	if activeDeferred && len(cp.DeferredFrontier) > 0 {
		return fmt.Errorf("%w: deferred nodes are active while other deferred nodes wait", ErrCheckpointNotResumable)
	}
	active := [][]FrontierMember{cp.CurrentFrontier, cp.CompletedFrontier}
	instances := make(map[uint64]bool)
	for _, frame := range active {
		for _, member := range frame {
			if _, deferred := g.deferredNodes[member.NodeID]; deferred != activeDeferred {
				return fmt.Errorf("%w: node %q mixes ordinary and deferred work in one superstep",
					ErrCheckpointNotResumable, member.NodeID)
			}
			if !activeDeferred && len(cp.DeferredFrontier) > 0 {
				if token, ok := instanceTokenValue(member.InstanceID); ok {
					instances[token] = true
				}
			}
		}
	}
	deferred := [][]FrontierMember{cp.DeferredFrontier}
	if activeDeferred {
		deferred = active
	}
	static := make(map[NodeID]bool)
	for _, frame := range deferred {
		for _, member := range frame {
			if _, ok := g.deferredNodes[member.NodeID]; !ok {
				return fmt.Errorf("%w: node %q is not a deferred node", ErrCheckpointNotResumable, member.NodeID)
			}
			if member.InstanceID == "" {
				if static[member.NodeID] {
					return fmt.Errorf("%w: duplicate deferred node %q", ErrCheckpointNotResumable, member.NodeID)
				}
				static[member.NodeID] = true
				continue
			}
			token, ok := instanceTokenValue(member.InstanceID)
			if !ok || token == 0 || member.InstanceID != "i"+strconv.FormatUint(token, 36) {
				return fmt.Errorf("%w: invalid deferred instance %q", ErrCheckpointNotResumable, member.InstanceID)
			}
			if instances[token] {
				return fmt.Errorf("%w: duplicate deferred instance %q", ErrCheckpointNotResumable, member.InstanceID)
			}
			instances[token] = true
		}
	}
	return nil
}

func cloneJoinProgress(progress map[int][]NodeID) map[int][]NodeID {
	if len(progress) == 0 {
		return nil
	}
	cloned := make(map[int][]NodeID, len(progress))
	for i, ids := range progress {
		cloned[i] = slices.Clone(ids)
	}
	return cloned
}
