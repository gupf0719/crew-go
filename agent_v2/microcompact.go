package agent_v2

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

// MicroCompact 微压缩（ISSUE-66）
//
// 上下文压缩 4 层中的第 1 层——纯规则操作，BasicAgent makeModelFn 最早段触发
// （先于 AutoCompact，等价于"上轮 toolsFn 完成后清"的语义但实际跑在 modelFn）。
// 把白名单内的旧 tool 调用结果内容替换为占位符，**保留 tool_use / tool_result
// 块结构**（API 配对完整），但释放大块内容（Read 整文件 / Bash 输出 / WebFetch
// 网页）占用的 tokens。
//
// 算法：按 tool name 分组，每个 tool 各自保留最近 KeepRecent（默认 5）个；早于
// 该 tool 第 N 个的 tool_result 内容替换为 Placeholder；已带 cleared marker 的
// 不重复 clone（幂等保护）。
//
// 设计参考 Claude Code COMPACTABLE_TOOLS——只清"内容大且后续轮次大概率用不到原文"
// 的 tool（用户 / Agent / Skill / MCP / 自定义工具不在白名单——这些的输出可能
// 是策略输入或代理协作的关键素材，清掉风险高）。

// MicroCompactConfig 控制 MicroCompact 行为。
type MicroCompactConfig struct {
	// Enabled 默认 false——需 WithMicroCompact 显式开启。
	Enabled bool

	// KeepRecent 每个 tool 保留最近 N 次调用的完整 tool_result 内容；
	// 早于第 N 次的内容替换为占位符。<= 0 时使用默认值 5。
	KeepRecent int

	// CompactableTools 白名单——只清这些 tool 的旧 tool_result 内容。
	// 空时使用 DefaultCompactableTools。
	CompactableTools []string

	// Placeholder 替换内容的占位符；空时使用 defaultMicroCompactPlaceholder。
	Placeholder string

	// OffloadReadToolName 是业务注册的「按 key 读回卸载内容」那个工具的名字。
	// **配了才在占位符里拼一句「用 X 读回全文」，空则不拼。**
	//
	// **why 要有它**：占位符里本来只有 `[offloaded:key] preview: …`——模型看得见有东西
	// 被卸载了、也看得见 key，**但没人告诉它这东西能读回来**，于是它只会按残缺的预览作答。
	// 对照的实现每个模板都以「用 X 工具查看」结尾。
	//
	// **why 配了才拼**：这个工具是**业务自己注册**的，框架不铸它。没配却写一句
	// 「用 X 读回全文」等于给模型一条它执行不了的指令——比不写更糟。
	//
	// ⚠️ 同时开了 `Config.ToolResultMaxBytes` 时，卸载出去的**已经是截断版**
	// （闸在工具结果进消息流之前就截了），这句「全文」会名不副实。见 doc/code/todo.md。
	OffloadReadToolName string

	// OffloadEnabled 是否启用 offload-to-store 模式 (HIGH-2): 清前先把当时的 tool_result
	// 写入 store, messages 留 "[offloaded:key] preview: <head 200 char>" 占位符让 LLM
	// 看到预览 + 业务可通过 store.Get(NS, key) 取回做审计/重读.
	//
	// ⚠️ **取回来的不一定是「全文」**: 开了 `Config.ToolResultMaxBytes` 时闸在工具结果进
	// 消息流之前就截过了, 存进去的就是截断版; 且读回来是 `map[string]any`(store 按值的 JSON 形状存,
	// 每个后端都一样, 见 store.Item), 不是 `*schema.Message`, 要自己 decode.
	//
	// 默认 false——纯替占位符行为不变 (跟 OffloadEnabled=false / store 未注入 时
	// 等价). true 时:
	//   - 必须 ctx 内有 store.WithContext 注入的 store.Store
	//   - 默认 NS **按 sid 派生**：store.NS("_micro_offload", sid)；无 sid 才回落裸
	//     store.NS("_micro_offload")。业务可用 OffloadNS 覆盖。⚠️ 取回一律走
	//     OffloadNamespace(ctx)，别直接用裸 cfg.OffloadNS（见下方 audit 注释）
	//   - key 由存储的消息内容派生，读取时使用 MicroCompactOffloadKeyAnnotation
	//   - value = 卸载那一刻的 *schema.Message (含 Content + ContentBlocks; 开了
	//     ToolResultMaxBytes 时它已是截断版)
	//   - store.Put 失败 → slog.Warn + 退化为纯占位符 (不阻断主链路)
	//
	// **业务侧**: 想让 LLM 主动重读 offloaded 内容需注册类似 `read_offloaded_result`
	// tool (业务自管). 框架不自动 inject (业务责任).
	OffloadEnabled bool

	// OffloadNS 是 offload store key 的 namespace. 空时默认按 ctx sid 自动派生
	// per-sid 隔离 NS `store.NS("_micro_offload", sid)` (sid 取自
	// `compose_v2.GetSessionID(ctx)`); ctx 无 sid 时 fallback `store.NS("_micro_offload")`
	// 全局共享 (audit HIGH-B2 + LOW-A3 fix: 之前默认全局 NS 跨 sess 同 callID 撞 key,
	// 业务自定义 tool ID 测试场景 silent 覆盖前 sess 内容).
	//
	// **业务自管 cleanup 责任**: store offload data 不走 cp.DeleteSubtree 自动清,
	// 业务可走以下任一路径管 lifecycle:
	//   - 用 TTL-aware 后端 (Redis EXPIRE / mem0 内置 TTL), store.Put 加
	//     store.WithTTL(d) 让 backend 自动过期
	//   - cron sweeper: 业务定期 store.Search(NS("_micro_offload"))+逐个 Delete
	//   - per-sid Run 完成时业务调 store.Search(NS("_micro_offload", sid)) 逐个删
	//   - 业务 backend (mem0 / Pinecone) 走自己的清理 API
	OffloadNS store.Namespace
}

// MicroCompactOffloadedAnnotation 是 MicroCompact 卸载模式下 placeholder message
// 上打的 marker. 跟 MicroCompactClearedAnnotation 互斥 (cleared 是纯清, offloaded
// 是卸到 store). 业务可按此 marker 区分 "内容已丢" vs "内容可从 store 取回".
const MicroCompactOffloadedAnnotation = "microcompact_offloaded"

// MicroCompactOffloadKeyAnnotation 存 offload store key. 业务读 placeholder message
// Annotations 拿不透明的 key 后 store.Get(ctx, cfg.OffloadNamespace(ctx), key) 取回。
// ⚠️ **取回的不是 `*schema.Message`**：store 按值的 JSON 形状存（见 store.Item），每个后端读回都是
// `map[string]any`，要消息就 json.Marshal 再 json.Unmarshal 进 schema.Message。
// **用 cfg.OffloadNamespace(ctx) 拿 NS，别用裸 cfg.OffloadNS**——默认档 cfg.OffloadNS 空 ≠
// 实际写入 NS（按 sid 派生），裸用恢复会 miss（audit MED fix）。
const MicroCompactOffloadKeyAnnotation = "microcompact_offload_key"

// TruncatedOriginalBytesAnnotation 记「这条消息的正文在被截断之前有多少字节」（int）。
//
// **它是给下一刀看的**，不是给人看的：同一条正文可能被截两次（源头闸一次、紧急压缩
// 兜底一次），而第二刀手上的 `len(content)` 是第一刀之后的残长。没有这个槽的话，
// 第二刀报出来的「省略了多少」只算它自己删的那部分——实测偏小 100 倍，**方向还正相反**
// （报小了模型会以为内容基本完整）。
//
// 用结构化槽而不是去 parse 正文，跟 [MicroCompactOffloadKeyAnnotation] 是同一个理由
// （那处 godoc 写明「业务直接读取 key 不用 parse Content」）。
const TruncatedOriginalBytesAnnotation = "truncated_original_bytes"

// defaultMicroCompactOffloadNS 是 OffloadNS 字段未设 + ctx 无 sid 时的默认 namespace.
var defaultMicroCompactOffloadNS = store.NS("_micro_offload")

// OffloadNamespace 返回 offload store 的**实际** namespace——offload 写入与业务恢复共用此单点，
// 消除"业务按旧 godoc 用裸 cfg.OffloadNS 恢复、默认档却 miss"的 drift（audit MED）。
//
// 派生：cfg.OffloadNS 非空 → 用它；否则按 ctx sid per-sid 隔离 store.NS("_micro_offload", sid)，
// ctx 无 sid 时 fallback 全局 defaultMicroCompactOffloadNS。
//
// **业务取回 offloaded 内容**：占位符 message 的 Annotations[MicroCompactOffloadKeyAnnotation]
// 存 key；业务注册的 read_offloaded_result 类工具内 store.Get(ctx, cfg.OffloadNamespace(ctx), key)
// 取回——NS 走本方法，不必知道内部 "_micro_offload"/sid 派生规则。⚠️ 读回是 `map[string]any`
// 不是 `*schema.Message`（store 按值的 JSON 形状存），业务别写死类型断言。
func (cfg *MicroCompactConfig) OffloadNamespace(ctx context.Context) store.Namespace {
	if len(cfg.OffloadNS) > 0 {
		return cfg.OffloadNS
	}
	if sid := compose_v2.GetSessionID(ctx); sid != "" {
		return store.NS("_micro_offload", sid)
	}
	return defaultMicroCompactOffloadNS
}

// microCompactPreviewBytes 是 offload 占位符内 preview 头部字节数.
const microCompactPreviewBytes = 200

// DefaultCompactableTools 是默认白名单——内容大且后续轮次通常不需要原文的内置工具。
//
// 不在白名单：Agent / Skill / MCP / 用户自定义——这些的输出可能是策略输入或
// agent 协作关键素材，统一清掉风险高，需要用户显式 opt-in。
var DefaultCompactableTools = []string{
	"Read", "Bash", "Grep", "Glob",
	"WebSearch", "WebFetch", "Edit", "Write",
}

const defaultMicroCompactPlaceholder = "[Old tool result content cleared by microcompact]"

// MicroCompactClearedAnnotation 是 MicroCompact 在被清掉的 tool_result 上打的
// Annotations key——值固定为 true。下游（如 AutoCompact extractRecentFiles）
// 用此 marker 识别"内容已是占位符"，避免把占位符当成真实数据复用。
//
// 用 annotation 而非字符串匹配，保证用户自定义 Placeholder 时识别仍可靠。
const MicroCompactClearedAnnotation = "microcompact_cleared"

// microCompactToClear 按配对结果的位置选择可压缩消息，MicroCompact 和文件重注入共用判定。
func microCompactToClear(messages []*schema.Message, cfg *MicroCompactConfig) map[int]bool {
	keep := cfg.KeepRecent
	if keep <= 0 {
		keep = 5
	}
	whitelist := make(map[string]bool, len(cfg.CompactableTools))
	for _, t := range cfg.CompactableTools {
		whitelist[t] = true
	}
	if len(whitelist) == 0 {
		for _, t := range DefaultCompactableTools {
			whitelist[t] = true
		}
	}
	recent := make(map[string]int, len(whitelist))
	eligible := map[int]int{}
	for _, pair := range slices.Backward(pairToolResults(messages)) {
		name := pair.call.Function.Name
		if !whitelist[name] {
			continue
		}
		if recent[name] < keep {
			recent[name]++
			continue
		}
		if pair.result != nil {
			eligible[pair.resultAt]++
		}
	}
	toClear := make(map[int]bool, len(eligible))
	for i, count := range eligible {
		m := messages[i]
		// 合并结果共享正文和标注，全部结果块都符合时才压缩。
		if count != len(toolResultIDs(m)) {
			continue
		}
		cleared, _ := m.Annotations[MicroCompactClearedAnnotation].(bool)
		offloaded, _ := m.Annotations[MicroCompactOffloadedAnnotation].(bool)
		skillBody, _ := m.Annotations[SkillBodyAnnotation].(bool)
		if !cleared && !offloaded && !skillBody {
			toClear[i] = true
		}
	}
	return toClear
}

// MicroCompactRun 应用 MicroCompact 算法到 messages。
//
// 不变量：
//   - tool_use 块永不修改（保 LLM 看到的 call_id 列表完整）
//   - tool_result 块的元数据（tool_use_id / role）永不修改
//   - 仅 tool_result.Content + Toolresult.Content 被替换为占位符
//   - 不在 CompactableTools 白名单的 tool 完全跳过
//
// 算法：
//  1. 按出现顺序配对工具声明与结果。
//  2. 每个白名单工具保留最近 KeepRecent 次调用，按结果位置选择更早的配对结果。
//  3. 复制选中的结果并替换正文，合并消息在所有结果块可压缩时整体替换。
//
// **HIGH-1 改动 (audit⁵)**: 加 ctx 参数对齐 AutoCompactRun / ReactiveCompactRun 签名,
// 让 ctx 挂 EventSink (业务接 observability) 时 EmitCompactionEvent fire Before/After event.
// sink-gated 设计——不再单独 opt-in EmitEvents.
// 项目未上线无兼容包袱, 4 个 test 调用点同步改 ctx 传 context.Background().
func MicroCompactRun(ctx context.Context, messages []*schema.Message, cfg *MicroCompactConfig) []*schema.Message {
	if cfg == nil || !cfg.Enabled || len(messages) == 0 {
		return messages
	}
	// Sub-5d audit fix: 测 duration 让 After event 自带 DurationMs (OTel self-contained).
	startNanos := time.Now().UnixNano()
	// 有 sink 才构造事件。转存独立执行，编码结果用于派生 key 和统计写入大小。
	hasSink := hasEventSink(ctx)
	// emit Before event. MicroCompact 不计 token, InputTokens=0.
	// Trigger 字段空——MicroCompact 总跑无显式触发条件.
	if hasSink {
		EmitCompactionEvent(ctx, &CompactionAction{
			Layer: CompactionLayerMicro,
			Type:  CompactionActionBefore,
			Before: &CompactionBeforeAction{
				Messages: messages,
			},
		})
	}
	placeholder := cfg.Placeholder
	if placeholder == "" {
		placeholder = defaultMicroCompactPlaceholder
	}
	// 按配对结果的位置选择待压缩消息。
	toClear := microCompactToClear(messages, cfg)
	if len(toClear) == 0 {
		// 无 tool_result 待清——后处理 noop, After event Dropped=0/Kept=len(messages)
		if hasSink {
			EmitCompactionEvent(ctx, &CompactionAction{
				Layer: CompactionLayerMicro,
				Type:  CompactionActionAfter,
				After: &CompactionAfterAction{
					Messages:   messages,
					Dropped:    0,
					Kept:       len(messages),
					DurationMs: (time.Now().UnixNano() - startNanos) / 1e6,
				},
			})
		}
		return messages
	}

	var offloadStore store.Store
	offloadNS := cfg.OffloadNamespace(ctx)
	if cfg.OffloadEnabled {
		offloadStore = store.FromContext(ctx)
		if isFrameworkNil(offloadStore) {
			offloadStore = nil
			slog.WarnContext(ctx, "microcompact: offload enabled without a store, falling back to placeholder-only")
		}
	}

	out := slices.Clone(messages)
	for i, m := range messages {
		if !toClear[i] {
			continue
		}
		// 产生时已转存的结果只清预览，保留原取回入口。
		if sid, _ := m.Annotations[ToolResultSpillAnnotation].(string); sid != "" {
			out[i] = cloneToolResultWithPlaceholder(m, fmt.Sprintf(
				"[较早的工具输出已清理；全文仍可用 %s 取回，id=%q]", ReadToolOutputToolName, sid))
			continue
		}
		if offloadStore != nil {
			key, err := putMicroCompactOffload(ctx, offloadStore, offloadNS, m, hasSink)
			if err == nil {
				out[i] = cloneToolResultWithOffloadRef(m, key, microCompactPreviewBytes, cfg.OffloadReadToolName)
				continue
			}
			slog.WarnContext(ctx, "microcompact: offload failed, falling back to placeholder-only",
				"key", key, "ns", offloadNS.String(), "err", err)
		}
		out[i] = cloneToolResultWithPlaceholder(m, placeholder)
	}
	// emit After event (opt-in). Dropped = len(toClear), Kept = len(messages) - len(toClear).
	// (注: 这里 Dropped 是"被清的 tool_result 数", 不是真删 message 数.)
	if hasSink {
		EmitCompactionEvent(ctx, &CompactionAction{
			Layer: CompactionLayerMicro,
			Type:  CompactionActionAfter,
			After: &CompactionAfterAction{
				Messages:   out,
				Dropped:    len(toClear),
				Kept:       len(messages) - len(toClear),
				DurationMs: (time.Now().UnixNano() - startNanos) / 1e6,
			},
		})
	}
	return out
}

// cloneToolResultWithPlaceholder 浅拷贝 tool 消息并把 Content + Toolresult.Content
// 替换为占位符；同时打 MicroCompactClearedAnnotation 让下游识别。
// 其它字段（Metadata 等）保持指针共享，配对元数据完整。Annotations 是新建 map
// 拷贝（避免修改原 message 的 annotations 共享 map 影响调用方）。
func cloneToolResultWithPlaceholder(m *schema.Message, placeholder string) *schema.Message {
	cloned := *m
	cloned.Content = placeholder
	// 打 cleared marker——拷贝原 annotations 后 set，避免共享 map mutation
	cloned.Annotations = make(map[string]any, len(m.Annotations)+1)
	for k, v := range m.Annotations {
		cloned.Annotations[k] = v
	}
	cloned.Annotations[MicroCompactClearedAnnotation] = true
	if len(m.ContentBlocks) > 0 {
		cloned.ContentBlocks = make([]*schema.ContentBlock, len(m.ContentBlocks))
		for i, cb := range m.ContentBlocks {
			if cb == nil {
				continue
			}
			if cb.Toolresult != nil {
				cbCopy := *cb
				trCopy := *cb.Toolresult
				trCopy.Content = placeholder
				cbCopy.Toolresult = &trCopy
				cloned.ContentBlocks[i] = &cbCopy
				continue
			}
			cloned.ContentBlocks[i] = cb
		}
	}
	return &cloned
}

// cloneToolResultWithOffloadRef HIGH-2 offload 模式占位符——浅拷贝 tool 消息,
// Content + Toolresult.Content 替换为 "[offloaded:<key>] preview: <head N char>".
// LLM 看到 preview 知道有内容 + key, 业务侧可 store.Get(NS, key) 取回 (⚠️ 持久后端
// 读回是 map[string]any 不是 *schema.Message).
//
// preview 取原 Content 前 previewBytes 字节 (UTF-8 safe — 用 clampPrefixToUTF8
// 防中文截半截).
//
// 同时打 2 markers:
//   - MicroCompactOffloadedAnnotation = true (区分 cleared 纯清 vs offloaded 卸载)
//   - MicroCompactOffloadKeyAnnotation = key (业务直接读取 key 不用 parse Content)
func cloneToolResultWithOffloadRef(m *schema.Message, key string, previewBytes int, readToolName string) *schema.Message {
	cloned := *m
	// audit MED-1 fix: ContentBlocks-only message (m.Content=="") 时 fallback 取
	// ContentBlocks[*].Toolresult.Content 做 preview — 否则 preview 空 LLM 失上下文.
	previewSrc := m.Content
	if previewSrc == "" && len(m.ContentBlocks) > 0 {
		for _, cb := range m.ContentBlocks {
			if cb != nil && cb.Toolresult != nil && cb.Toolresult.Content != "" {
				previewSrc = cb.Toolresult.Content
				break
			}
		}
	}
	_, truncated := annotationInt(m, TruncatedOriginalBytesAnnotation)
	cloned.Content = offloadPlaceholder(key, clampPrefixToUTF8(previewSrc, previewBytes), readToolName, truncated)
	cloned.Annotations = make(map[string]any, len(m.Annotations)+2)
	for k, v := range m.Annotations {
		cloned.Annotations[k] = v
	}
	cloned.Annotations[MicroCompactOffloadedAnnotation] = true
	cloned.Annotations[MicroCompactOffloadKeyAnnotation] = key
	if len(m.ContentBlocks) > 0 {
		cloned.ContentBlocks = make([]*schema.ContentBlock, len(m.ContentBlocks))
		for i, cb := range m.ContentBlocks {
			if cb == nil {
				continue
			}
			if cb.Toolresult != nil {
				cbCopy := *cb
				trCopy := *cb.Toolresult
				trCopy.Content = offloadPlaceholder(key, clampPrefixToUTF8(cb.Toolresult.Content, previewBytes), readToolName, truncated)
				cbCopy.Toolresult = &trCopy
				cloned.ContentBlocks[i] = &cbCopy
				continue
			}
			cloned.ContentBlocks[i] = cb
		}
	}
	return &cloned
}

// putMicroCompactOffload 按实际写入的内容派生 key，只为真实写入记录 Store 事件。
func putMicroCompactOffload(ctx context.Context, st store.Store, ns store.Namespace, m *schema.Message, hasSink bool) (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode tool result: %w", err)
	}
	key := fmt.Sprintf("mco-%x", sha256.Sum256(data))
	start := time.Now()
	attempted, err := offloadDedupFrom(ctx).put(ctx, st, ns, key, m)
	if hasSink && attempted {
		action := &StorePutAction{
			NS:             ns.String(),
			Key:            key,
			ValueSizeBytes: len(data),
			DurationMs:     time.Since(start).Milliseconds(),
			Ok:             err == nil,
		}
		if err != nil {
			action.Err = err.Error()
		}
		EmitStoreEvent(ctx, &StoreAction{Type: StoreOperationPut, Put: action})
	}
	return key, err
}

type offloadDedupKey struct{}

// offloadDedup 保存本次运行的写入结论。并行调用等待相同地址的写入完成，失败后下一轮可重试。
type offloadDedup struct {
	mu     sync.Mutex
	writes map[offloadAddress]*offloadWrite
}

type offloadAddress struct {
	store     store.Store
	namespace string
	key       string
	owner     string
	member    bool
}

type offloadWrite struct {
	done chan struct{}
	err  error
}

var errOffloadIncomplete = errors.New("offload write did not complete")

func (d *offloadDedup) put(ctx context.Context, st store.Store, ns store.Namespace, key string, value *schema.Message) (bool, error) {
	if d == nil || !reflect.ValueOf(st).Comparable() || ns.Validate() != nil {
		return true, st.Put(ctx, ns, key, value)
	}
	addr := offloadAddress{store: st, namespace: ns.String(), key: key,
		owner: compose_v2.GetOwner(ctx), member: IsSubAgentStoreScope(ctx)}
	d.mu.Lock()
	if previous := d.writes[addr]; previous != nil {
		d.mu.Unlock()
		select {
		case <-previous.done:
			return false, previous.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	// Put 发生 panic 或 Goexit 时也要释放等待者，后续调用可以重试。
	current := &offloadWrite{done: make(chan struct{}), err: errOffloadIncomplete}
	d.writes[addr] = current
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if current.err != nil {
			delete(d.writes, addr)
		}
		close(current.done)
	}()
	current.err = st.Put(ctx, ns, key, value)
	return true, current.err
}

// withOffloadDedup 在 Run / Resume 入口挂一张空的写入记录，嵌套执行共享同一张。
func withOffloadDedup(ctx context.Context) context.Context {
	if ctx.Value(offloadDedupKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, offloadDedupKey{}, &offloadDedup{writes: make(map[offloadAddress]*offloadWrite)})
}

func offloadDedupFrom(ctx context.Context) *offloadDedup {
	d, _ := ctx.Value(offloadDedupKey{}).(*offloadDedup)
	return d
}

// offloadPlaceholder 拼卸载占位符——**正文只有这一个地方拼**。
//
// ⚠️ 它存在的理由就是「别有第二份」：`schema.ToolResultMessage` 造出来的消息把正文同时
// 存在 `Content` 和 `ContentBlocks[].Toolresult.Content` 里，这里原先是**两处各拼一遍**
// 同样的格式串——然后它就漂了：给占位符加「怎么读回来」那句提示时只加进了 `Content`，
// block 那份没加，于是同一条消息的两份正文说的话不一样。收敛成一个函数之后，
// 「两处保持同步」这个要求本身消失了。
//
// readToolName 为空时不拼读回提示——那个工具是业务自己注册的，没配却写一句
// 「用 X 读回全文」等于给模型一条它执行不了的指令。
func offloadPlaceholder(key, preview, readToolName string, truncated bool) string {
	s := fmt.Sprintf("[offloaded:%s] preview: %s", key, preview)
	if readToolName != "" {
		content := "全文"
		if truncated {
			content = "已保存的截断内容"
		}
		s += fmt.Sprintf("（用 %s 工具按上面这个 key 读回%s）", readToolName, content)
	} else if truncated {
		s += "（已保存的内容经过截断）"
	}
	return s
}
