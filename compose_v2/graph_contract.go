package compose_v2

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
)

const checkpointIdentityPrefix = "graph-v1:"

// CheckpointIdentity 返回编译期计算的存档身份。身份包含图的逻辑标识、节点、出边、channel
// 合并契约、已声明的子图与显式版本。相同声明在进程重启后仍能匹配存档。
func (g *Graph) CheckpointIdentity() string { return g.checkpointIdentity }

// WithCheckpointRevision 声明业务语义版本。节点函数、条件闭包、工具或自定义 reducer 的行为
// 如果可能改变已挂起任务的含义，构建新版图时应修改该版本。重新编译同版本时须使用相同的值。
func (b *GraphBuilder) WithCheckpointRevision(revision string) *GraphBuilder {
	b.checkpointRevision = revision
	return b
}

// WithCheckpointOwner 返回绑定到另一个稳定逻辑所有者的图。原图保持不变，公开 [Graph.ID]
// 仍相同。用于同一编译图由不同包装器使用时隔离存档。
func (g *Graph) WithCheckpointOwner(owner string) *Graph {
	if g.checkpointOwner == owner {
		return g
	}
	owned := *g
	owned.checkpointOwner = owner
	owned.checkpointIdentity = owned.checkpointIdentityForVariant()
	return &owned
}

// WithCheckpointRevision 返回带包装层业务版本的图，保留构建时声明的版本和所有者。
// 空值沿用原图，重建同一包装层时须使用相同的值。
func (g *Graph) WithCheckpointRevision(revision string) *Graph {
	if revision == "" || g.checkpointRevision == revision {
		return g
	}
	versioned := *g
	versioned.checkpointRevision = revision
	versioned.checkpointIdentity = versioned.checkpointIdentityForVariant()
	return &versioned
}

// WithCheckpointContext 返回应用当前上下文存档版本的图，保留构建与包装层版本。
// 同一个上下文版本可重复应用，未声明版本的上下文清除先前应用的上下文版本。
func (g *Graph) WithCheckpointContext(ctx context.Context) *Graph {
	revision := CheckpointRevisionFromContext(ctx)
	if g.checkpointContextRevision == revision {
		return g
	}
	versioned := *g
	versioned.checkpointContextRevision = revision
	versioned.checkpointIdentity = versioned.checkpointIdentityForVariant()
	return &versioned
}

func (g *Graph) checkpointIdentityForVariant() string {
	encoded := checkpointEncoding(append([]byte(nil), g.checkpointDefinition[:]...))
	encoded.text(g.checkpointOwner)
	encoded.text(g.checkpointRevision)
	encoded.text(g.checkpointContextRevision)
	digest := sha256.Sum256(encoded)
	return checkpointIdentityPrefix + hex.EncodeToString(digest[:])
}

func (g *Graph) fingerprintCheckpoint(revision string, subgraphs map[NodeID][sha256.Size]byte) [sha256.Size]byte {
	var encoded checkpointEncoding
	encoded.text(g.name)
	encoded.text(g.identityPrefix)
	encoded.text(revision)

	nodes := make([]NodeID, 0, len(g.nodes))
	for id := range g.nodes {
		nodes = append(nodes, id)
	}
	slices.Sort(nodes)
	encoded.count(len(nodes))
	for _, id := range nodes {
		encoded.text(string(id))
		digest, subgraph := subgraphs[id]
		encoded.flag(subgraph)
		if subgraph {
			encoded = append(encoded, digest[:]...)
		}
	}

	sources := make([]NodeID, 0, len(g.adj))
	for src := range g.adj {
		sources = append(sources, src)
	}
	slices.Sort(sources)
	encoded.count(len(sources))
	for _, src := range sources {
		encoded.text(string(src))
		edges := g.adj[src]
		encoded.count(len(edges))
		// 同一来源的边按声明顺序执行，保留其顺序。
		for _, edge := range edges {
			encoded.flag(edge.cond != nil)
			if edge.cond != nil {
				encoded = binary.AppendVarint(encoded, int64(edge.condFailPolicy))
				continue
			}
			encoded.text(string(edge.to))
		}
	}

	channels := slices.Clone(g.channels)
	// 同名重复注册时首个 channel 生效，保留同名项的相对顺序。
	slices.SortStableFunc(channels, func(a, b anyChannel) int {
		return strings.Compare(a.name(), b.name())
	})
	encoded.channels(channels)
	if len(g.joinEdges) > 0 {
		encoded.text("join-edges")
		encoded.count(len(g.joinEdges))
		for _, edge := range g.joinEdges {
			encoded.text(string(edge.to))
			encoded.count(len(edge.from))
			for _, id := range edge.from {
				encoded.text(string(id))
			}
		}
	}
	if len(g.deferredNodes) > 0 {
		encoded.text("deferred-nodes")
		ids := slices.Sorted(maps.Keys(g.deferredNodes))
		encoded.count(len(ids))
		for _, id := range ids {
			encoded.text(string(id))
		}
	}
	return sha256.Sum256(encoded)
}

func (b *GraphBuilder) setCheckpointSubgraph(id NodeID, sub *Graph, cfg *subgraphConfig) {
	var encoded checkpointEncoding
	if sub != nil {
		encoded.text(sub.CheckpointIdentity())
	} else {
		encoded.text("")
	}
	encoded.channels(cfg.imports)
	encoded.channels(cfg.exports)
	encoded.flag(cfg.isolateResumeContext)
	if b.checkpointSubgraphs == nil {
		b.checkpointSubgraphs = make(map[NodeID][sha256.Size]byte)
	}
	b.checkpointSubgraphs[id] = sha256.Sum256(encoded)
}

type checkpointEncoding []byte

func (e *checkpointEncoding) count(n int) {
	*e = binary.AppendUvarint(*e, uint64(n))
}

func (e *checkpointEncoding) text(s string) {
	e.count(len(s))
	*e = append(*e, s...)
}

func (e *checkpointEncoding) flag(v bool) {
	if v {
		*e = append(*e, 1)
		return
	}
	*e = append(*e, 0)
}

func (e *checkpointEncoding) channels(channels []anyChannel) {
	e.count(len(channels))
	for _, channel := range channels {
		e.text(channel.name())
		e.text(channel.checkpointMergeKind())
		e.flag(channel.singleWriterPerStep())
		digest := checkpointTypeFingerprint(channel.typeRep())
		*e = append(*e, digest[:]...)
	}
}

// 类型指纹缓存随进程复用，避免每次重建 BasicAgent 的图时重复遍历消息等公共类型。
var checkpointTypeFingerprints sync.Map

func checkpointTypeFingerprint(t reflect.Type) [sha256.Size]byte {
	if digest, ok := checkpointTypeFingerprints.Load(t); ok {
		return digest.([sha256.Size]byte)
	}
	var encoded checkpointEncoding
	encoded.goType(t, make(map[reflect.Type]int))
	digest := sha256.Sum256(encoded)
	checkpointTypeFingerprints.Store(t, digest)
	return digest
}

// goType 编码完整包路径和类型结构。按遍历顺序编号，共用或递归的类型以编号引用。
func (e *checkpointEncoding) goType(t reflect.Type, references map[reflect.Type]int) {
	if index, seen := references[t]; seen {
		e.flag(true)
		e.count(index)
		return
	}
	e.flag(false)
	references[t] = len(references)
	e.text(t.PkgPath())
	e.text(t.Name())
	e.text(t.Kind().String())
	switch t.Kind() {
	case reflect.Array:
		e.count(t.Len())
		e.goType(t.Elem(), references)
	case reflect.Chan:
		e.text(t.ChanDir().String())
		e.goType(t.Elem(), references)
	case reflect.Pointer, reflect.Slice:
		e.goType(t.Elem(), references)
	case reflect.Map:
		e.goType(t.Key(), references)
		e.goType(t.Elem(), references)
	case reflect.Struct:
		e.count(t.NumField())
		for field := range t.Fields() {
			e.text(field.Name)
			e.text(field.PkgPath)
			e.text(string(field.Tag))
			e.flag(field.Anonymous)
			e.goType(field.Type, references)
		}
	case reflect.Interface:
		e.count(t.NumMethod())
		for method := range t.Methods() {
			e.text(method.Name)
			e.text(method.PkgPath)
			e.goType(method.Type, references)
		}
	case reflect.Func:
		e.flag(t.IsVariadic())
		e.count(t.NumIn())
		for in := range t.Ins() {
			e.goType(in, references)
		}
		e.count(t.NumOut())
		for out := range t.Outs() {
			e.goType(out, references)
		}
	}
}

// withCheckpointChannels 核对 ChannelManager 的实际注册。低阶注册的额外 channel 同样参与身份。
func (g *Graph) withCheckpointChannels(registered []anyChannel) (*Graph, error) {
	if len(registered) == 0 {
		return g, nil
	}
	declared := make(map[string]anyChannel, len(g.channels))
	for _, channel := range g.channels {
		if declared[channel.name()] == nil {
			declared[channel.name()] = channel
		}
	}
	var extra []anyChannel
	for _, channel := range registered {
		name := channel.name()
		if expected := declared[name]; expected != nil {
			if channel.typeRep() != expected.typeRep() || channel.checkpointMergeKind() != expected.checkpointMergeKind() {
				return nil, fmt.Errorf("%w: registered channel %q conflicts with graph %q channel contract", ErrGraphIDConflict, name, g.ID())
			}
			continue
		}
		extra = append(extra, channel)
		declared[name] = channel
	}
	if len(extra) == 0 {
		return g, nil
	}
	slices.SortFunc(extra, func(a, b anyChannel) int { return strings.Compare(a.name(), b.name()) })
	encoded := checkpointEncoding(append([]byte(nil), g.checkpointDefinition[:]...))
	encoded.channels(extra)
	effective := *g
	effective.checkpointDefinition = sha256.Sum256(encoded)
	effective.checkpointIdentity = effective.checkpointIdentityForVariant()
	return &effective, nil
}
