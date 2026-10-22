package compose_v2

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"crew-go/internal/panicerr"
)

// ── 类型注册表：cp marshal/unmarshal 协议先决 ──
//
// 问题：channel value 是 `any`——序列化后反序列化时 JSON 不知道实际类型，
// `json.Unmarshal(b, &any)` 默认拿到 `map[string]interface{}` / `float64` 等
// 退化类型，cast 回原 T 失败。
//
// 解法：业务在 init() 注册 channel value 类型——marshal 时附 type tag，
// unmarshal 按 tag 查表用 reflect.New(T) 反序列化。
//
// 对照参考：现有 compose/serialization.go RegisterType[T any](name) 用相同模式。
//
// 我们走 JSON + type registry：
//   - 跨语言友好（spec 初衷）
//   - wire format 稳定（不像 gob 易碎）
//   - 类型擦除靠业务 init 注册补回

var (
	// **ISSUE-87 audit MED-1 fix**: 加 typeRegistryMu 保护跨 map 写入原子性——
	// sync.Map 单 op 原子, 但 typeRegistry + typeNameByType 跨两 map 序列不原子。
	// 之前 LoadOrStore 修法只解了单 map race, 跨 map race 仍存在 (G1 写 typeRegistry
	// 但还没写 typeNameByType 时 G2 抢先写 typeNameByType → canonical name 漂移)。
	// 完整修法用 mutex 保护两 map 一起写, sync.Map 在 read-mostly 路径仍快。
	typeRegistryMu sync.Mutex
	typeRegistry   sync.Map // typeName → reflect.Type (read 用 sync.Map 快)
	typeNameByType sync.Map // reflect.Type → typeName (read 用 sync.Map 快)
)

// RegisterValueType 注册 channel value 类型——cp 协议必须知道每个 channel 的
// value 实际类型才能正确反序列化。
//
// **调用约定**：业务在 `init()` 注册所有跨 cp 持久化的 value 类型。一次注册
// 全局可见。重复注册同名同类型幂等；同名异类型 panic（fail-fast）。
//
// 用法：
//
//	func init() {
//	    compose_v2.RegisterValueType[int]("int")
//	    compose_v2.RegisterValueType[[]string]("[]string")
//	    compose_v2.RegisterValueType[*MyMessage]("agent.MyMessage")
//	}
//
// 命名约定：尽量用 "包.类型" 形式避免冲突；纯基础类型可用裸名（int / string / bool）。
func RegisterValueType[T any](typeName string) {
	if typeName == "" {
		panic("RegisterValueType: typeName must not be empty")
	}
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		// T 是 interface 或 nil-able 类型 + zero=nil——用反射拿到 T 的类型
		t = reflect.TypeOf((*T)(nil)).Elem()
	}

	// typeName → type 必须 1:1（同 typeName 不能映射两个不同 type，否则 unmarshal
	// 时按 typeName 拿不到正确 type）
	//
	// **ISSUE-87 audit MED-1 fix**: 加 typeRegistryMu 保护跨 map 写入原子性——
	// 单 sync.Map LoadOrStore 是 op 原子, 但 typeRegistry + typeNameByType 两步
	// 跨 map 序列不原子, 并发同 type 不同 name 注册时 canonical name 漂移
	// (G1 写 typeRegistry["A"]=t 后还没写 typeNameByType[t]="A" 时, G2 抢先写
	// typeNameByType[t]="B" → canonical 不确定)。mutex 保护两 map 一起写让
	// canonical name first-write 严格确定。
	typeRegistryMu.Lock()
	defer typeRegistryMu.Unlock()

	if existing, loaded := typeRegistry.LoadOrStore(typeName, t); loaded {
		if existing.(reflect.Type) != t {
			panic(fmt.Sprintf("RegisterValueType: typeName %q already registered with type %v, cannot re-register with %v",
				typeName, existing, t))
		}
		// 幂等返——同 type 重复注册 OK, 不重复写 typeNameByType (保 first-write canonical name)
		return
	}
	// type → typeName 允许多对一（alias）——audit Round 8 Bug 1 修复：
	// schema.Message = common.Message 是 type alias，业务用 "[]*schema.Message"
	// 注册 + 历史上曾用 "[]*common.Message"（ISSUE-87 LOW-1 后已统一 schema 形态）注册时不应
	// panic。改为 silent alias：第一次写入 typeNameByType 后，后续同 type 不同
	// name 注册时 typeRegistry 仍存 alias→type 映射，typeNameByType 保 first-write
	// 决定 marshal 时的 canonical name。
	typeNameByType.LoadOrStore(t, typeName)
}

// taggedValue 是 cp 中单个 channel value 的 wire 格式：含类型 tag + JSON 编码值。
type taggedValue struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// marshalValue 把 any 编码为 taggedValue。
// 类型未注册返 error（fail-fast 比静默丢更好）。
func marshalValue(v any) (taggedValue, error) {
	if v == nil {
		return taggedValue{Type: "<nil>"}, nil
	}
	t := reflect.TypeOf(v)
	tn, ok := typeNameByType.Load(t)
	if !ok {
		return taggedValue{}, fmt.Errorf("marshalValue: type %v not registered (call compose_v2.RegisterValueType[T])", t)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return taggedValue{}, fmt.Errorf("marshalValue: json marshal type %v: %w", t, err)
	}
	return taggedValue{Type: tn.(string), Data: data}, nil
}

// unmarshalValue 把 taggedValue 解码为 any。
// 类型未注册返 error。
func unmarshalValue(tv taggedValue) (any, error) {
	if tv.Type == "<nil>" {
		return nil, nil
	}
	t, ok := typeRegistry.Load(tv.Type)
	if !ok {
		return nil, fmt.Errorf("unmarshalValue: type %q not registered", tv.Type)
	}
	rt := t.(reflect.Type)
	ptr := reflect.New(rt) // *T
	if err := json.Unmarshal(tv.Data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("unmarshalValue: json unmarshal type %s: %w", tv.Type, err)
	}
	return ptr.Elem().Interface(), nil
}

// ── 公开导出 wrappers（业务跨 cp 序列化自定义类型时用） ──

// LookupType 按 typeName 反查注册类型——caller 想动态构造同类型实例时用。
//
// 第二个返回值表示是否找到。
func LookupType(typeName string) (reflect.Type, bool) {
	v, ok := typeRegistry.Load(typeName)
	if !ok {
		return nil, false
	}
	return v.(reflect.Type), true
}

// LookupKey 按 reflect.Type 反查注册的 typeName——caller 想知道某 type 的 wire
// canonical name 时用。
//
// 第二个返回值表示是否找到。
func LookupKey(t reflect.Type) (string, bool) {
	original := t // **R32+ 视角 6 HIGH-2 fix**: 保留原 type 用于 fallback retry
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if v, ok := typeNameByType.Load(t); ok {
		return v.(string), true
	}
	// **R32+ 视角 6 HIGH-2 fix**: retry 原 type (不解引用)——typeNameByType 可能直接
	// 注册了 *T。原实现 `reflect.TypeOf(t)` 拿到的是 `*reflect.rtype` 元类型 (因为
	// t 已 deref 成 T), 业务 RegisterValueType[*MyType]("...") + LookupKey(reflect.TypeOf(&MyType{}))
	// 100% 失败。修法: 用 original 不是 reflect.TypeOf(t)。
	if v, ok := typeNameByType.Load(original); ok {
		return v.(string), true
	}
	return "", false
}

// MarshalTagged 把 v 编码为带类型 tag 的 JSON。v 必须已通过 RegisterValueType 注册。
//
// **wire 格式**：`{"type":"<typeName>","data":<v 的常规 JSON>}`。
// nil 输入返 JSON null。
//
// **vs 内部 marshalValue**：导出版本——业务想跨 cp 边界存自定义 channel value
// 类型 + 跨进程恢复时调此 API。
func MarshalTagged(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	tv, err := marshalValue(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(tv)
}

// UnmarshalTagged 反向解码 MarshalTagged 产物。返 *T 指针类型——caller 做一次类型断言。
//
// 输入是 JSON null 时返 (nil, nil)；type 未注册返 error。
func UnmarshalTagged(data []byte) (any, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var tv taggedValue
	if err := json.Unmarshal(data, &tv); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if tv.Type == "" {
		return nil, fmt.Errorf("compose_v2: tagged envelope missing type field")
	}
	return unmarshalValue(tv)
}

// ── ChannelManager 快照协议 ──
//
// **Phase 5.1.b.4 重构**：cp wire 格式合并到 Checkpoint 单一类型——之前
// CheckpointSnapshot（PoC 早期 wire 类型）+ Checkpoint（ISSUE-70 元数据增强）
// 二者并存，要桥接函数 checkpointToSnapshot/snapshotToCheckpoint 映射。
// Phase 5.1.b.4 删冗余 Snapshot 类型，Checkpoint 既是持久化模型又是 wire 格式。
//
// schema 演进（**历史记述**：下面每条记的是「那一版加了什么」，其中提到的字段可能已被
// 后续版本换掉——例如 v7/v8 的 `FrontierInstanceIDs` / `CompletedInstanceIDs` 已由 v10 的
// 身份内联取代。**引用已删符号在这里是正确的**，不是注释 drift；机械对账会反复把它们报成
// 失效符号，逐条读语境后判假阳即可）：
//   - v1-v3：CheckpointSnapshot wire（仅 Channels/Frontier/PendingInterrupts）
//   - v4：~~SubgraphSnapshots~~（被 Phase 5.1.a Step 4 否决删除）
//   - v5（Phase 5.1.a Step 4）：删 SubgraphSnapshots
//   - v6（Phase 5.1.b.4）：合并到 Checkpoint，含 ID/NS/ParentNS/Status/GraphID/
//     CreatedAt 元数据字段（cm.Snapshot 仅填业务 state，元数据由
//     SaveNS / caller 填）
//   - v7（ISSUE-96）：加 FrontierInstanceIDs——与 CurrentFrontier 一一对应的
//     branch 实例身份。**向后兼容**：并行数组、全空时省略，旧 cp（无此字段）
//   - v8（ISSUE-96 补漏）：加 CompletedInstanceIDs（与 CompletedFrontier 一一对应）
//     + SerializedSend 的 OwnerInstanceID / InstanceID。前者让重跑成员的旧记录能按
//     实例精确 purge，后者让 deferred send 重放出的实例保住各自身份。
//     Restore 后按"全静态成员"工作 = 本字段引入前的语义
//   - v9：加 Epoch——每次写 cp 换新的世代标记，供 `WithStaleWriteGuard` 判"我载入
//     的那一代是否已被别人推进"。**向后兼容**：不开守卫时只写不读；旧 cp 无此字段
//     时守卫读到空值即跳过比对（退化为引入前行为）
//   - v10：**frontier 成员身份内联**——`CurrentFrontier` / `CompletedFrontier` 从
//     `[]NodeID` 改成 `[]FrontierMember{node_id, instance_id}`；
//     `FrontierInstanceIDs` 已删（本版起不存在此字段）
//     `CompletedInstanceIDs` 已删（同上）——两条平行数组一起退役。
//     **wire 破坏性变更**（数组元素从字符串变对象），跨语言对端需同步。
//
//     换掉的是「两个数组按下标一一对应」这条**类型系统够不着的外部约束**：错位的
//     后果是实例互相冒充，且不报错。内联之后没有对齐可错。v7 选平行数组时给的理由
//     「缺字段天然退化成静态边语义」**内联后仍然成立**（缺 `instance_id` 键 → 空串
//     → 静态边语义），故这次只丢了约束、没丢那份宽容。
//   - v11：加 NodeStates——节点私有中断态（`map[nodeStateKey]taggedValue`，见
//     node_state.go）。让「一个节点内做了 N 件带副作用的事」在中断重跑时跳过已完成
//     的那几件（tools 节点一轮 N 个 tool_call、其中一个抛中断是典型）。**向后兼容**：
//     旧 cp 无此字段 ⟹ 节点读不到上次进度 ⟹ 全部重跑 = 引入前行为，安全侧退化。
//
//     ⚠️ **它是 cp 体积的主要增量**：存的是节点已完成部分的产出（tools 节点那侧是
//     整条 tool_result 消息），**每轮累积、每超步全量重写** ⟹ 写入字节数随轮数平方
//     增长。实测长 ReAct（6 轮 × 2 tool_call、800B 结果）下 node_states 约占整份 cp
//     的一半；20 轮合成循环从 4.4KB 涨到 78KB。用 sync/async durability + 真后端
//     （PG/Redis，注意 MySQL 默认 4MB max_allowed_packet）时按这个量级留预算。
//   - v12：`AskQuery` 加 `tool_call_id` / `tool_name`——标出这条挂起卡的是**哪一次工具
//     调用**（空 = 不指向任何待执行动作）。审批型挂起被放弃时要靠它给悬空的 tool_call
//     补一条能配对的合成结果；从 `InterruptID` 反查不出来（它用的是 tc **下标**维度）。
//     **向后兼容**：omitempty string，旧 cp 读回空串 = 「不指向动作」，语义正确。
//   - v13：加 TaskID——任务代号，新开时铸、此后随存档一路带下去，掺进中断寻址让同一 sid
//     下两个任务走到同一位置时 ID 不再相等。**向后兼容**：旧 cp 读回空串时续跑沿用空值、
//     不铸新（退化成纯位置派生 = 引入前行为，见 `Checkpoint.TaskID`）。
//   - v14：加 CompletedTargets——上面那些成功成员**当时算好的出边目标**。出边计算
//     从「提交之后的独立相位」挪进成员自己的执行之后，成功成员的出边在它成功那一刻
//     就定了，不能等 Resume 重算（重算读到的是它跑完之后的状态）。**向后兼容**：旧 cp
//     无此字段时该批成功成员的后继丢失——与 CompletedFrontier 漏序列化同款失效形态。
//   - v15：加 ConsumedAnswers——已兑付的答案位。**向后兼容**：旧 cp 无此字段时那道
//     「一个答案只能满足一个提问」的兜底跨 Run 失效（同 Run 内仍在），退化成引入前行为。
//   - v16：加 InputDigest——这份存档新开时那次调用交来的摘要，只用于续跑时比对（它是调用的
//     身份时对不上就终局失败，只作核对时告警），不决定捡不捡。**向后兼容**：旧 cp 无此字段时
//     不比。
//   - v17：加 Signals / CompletedSignals / RecipientKey——本图已提交的上行信号（转交 / 路由这类
//     交给上方的决定，见 signal.go）、部分失败超步里成功成员先记在一边的信号、铸收件人的密钥。
//     **不向后兼容**：旧 cp 无这几个字段时，停下之前已提交的决定续跑后丢失（转交没发生、路由落到
//     兜底）；而旧 cp 里工具账的 `effect` 与嵌套调用记录的 `effects`（引入前记决定的地方）也不再被
//     读取——跨这个版本续跑的话，比引入前更差，别跨版本续跑。
//   - v18：交出去的一段执行（[HandOver] 封的那一格与它下面的分域）的重做代际从交接那一层数起，那一格里
//     的可重做记录在 NodeStates 里改用 `:hredo` 标记；交接过来的主图的外层代际链不再清空。**不向后兼容**：
//     旧 cp 里那一格的可重做记录（`:redo`）续跑时读不到——被调方在图外按 Rerunnable 记下的东西会重新产出，
//     而那一格里的提问（没被否决过时）ID 不变，旧答案会兑付给重新产出的内容。别跨版本续跑在途任务。

// CheckpointSchemaVer 是 Checkpoint 当前 schema 版本号, 仅供观测 / 业务 debug
// 用 — 框架不在 Marshal/Unmarshal 路径校验.
//
// **设计决策** (Option D):
//   - Checkpoint 是 exported, 业务 ad-hoc `&Checkpoint{...}` 漏填 `SchemaVer`
//     是常态 (`example/nested_smoke` 等). 故**不在 Unmarshal 强制 fail-fast** —
//     业务零负担, framework Snapshot 路径仍填 framework 当前版本走主流路径,
//     ad-hoc 构造的 cp.SchemaVer=0 默认值也能正常 roundtrip.
//   - 取舍理由: 反序列化时做 version mismatch fail-fast 价值低、业务负担大
//     (旧 cp / ad-hoc cp 都得能加载).
//
// 未来真要做版本兼容 (v6 → v7 字段语义变) 时:
//   - 业务侧迁移逻辑负责按 cp.SchemaVer 分支决定 unmarshal 字段
//   - framework 加 `Migrate(v Old, vNew int) (*Checkpoint, error)` 类工具
//   - 不在 Unmarshal 路径加 fail-fast
const CheckpointSchemaVer = 18

// checkpointSchemaVer (private alias 保 framework 内部 reference 兼容).
const checkpointSchemaVer = CheckpointSchemaVer

// marshalSendDirectives 把 SendDirective 列表编码为 cp wire 形态（PA-H1）。
// Payload 类型未注册时返 error——与 cp.Channels 持久化同 fail-fast 协议
// （业务用 SendFrom 自定义 payload 类型时需 RegisterValueType，与 channel 值同款）。
func marshalSendDirectives(dirs []SendDirective) ([]SerializedSend, error) {
	if len(dirs) == 0 {
		return nil, nil
	}
	out := make([]SerializedSend, 0, len(dirs))
	for _, d := range dirs {
		sd := SerializedSend{Target: d.Target, Owner: d.owner, OwnerInstanceID: d.ownerInstanceID, InstanceID: d.instanceID}
		for _, w := range d.Payload {
			if w == nil {
				continue // 对齐 ApplyAll 对 nil write 的 skip 语义（panic 在 SaveNS defer 内会吞原 InterruptError）
			}
			tv, err := marshalValue(w.valueAny())
			if err != nil {
				return nil, fmt.Errorf("marshal deferred send payload (channel %q): %w", w.channelName(), err)
			}
			sd.Payload = append(sd.Payload, SerializedWrite{Channel: w.channelName(), Type: tv.Type, Data: tv.Data})
		}
		out = append(out, sd)
	}
	return out, nil
}

// marshalChannelWrites 把 ChannelWrite 列表编码为 cp wire 形态（F-B
// pendingSendWrites 用）。与 marshalSendDirectives 的 payload 同协议。
func marshalChannelWrites(writes []ChannelWrite) ([]SerializedWrite, error) {
	if len(writes) == 0 {
		return nil, nil
	}
	out := make([]SerializedWrite, 0, len(writes))
	for _, w := range writes {
		if w == nil {
			continue // 对齐 ApplyAll 对 nil write 的 skip 语义
		}
		tv, err := marshalValue(w.valueAny())
		if err != nil {
			return nil, fmt.Errorf("marshal pending send write (channel %q): %w", w.channelName(), err)
		}
		out = append(out, SerializedWrite{Channel: w.channelName(), Type: tv.Type, Data: tv.Data})
	}
	return out, nil
}

// unmarshalChannelWrites 把 cp wire 形态还原为 ChannelWrite 列表（F-B）。
// 重建为 rawChannelWrite——归并时走 ApplyAll 复用 reducer 协议。
func unmarshalChannelWrites(sws []SerializedWrite) ([]ChannelWrite, error) {
	if len(sws) == 0 {
		return nil, nil
	}
	out := make([]ChannelWrite, 0, len(sws))
	for _, sw := range sws {
		v, err := unmarshalValue(taggedValue{Type: sw.Type, Data: sw.Data})
		if err != nil {
			return nil, fmt.Errorf("unmarshal pending send write (channel %q): %w", sw.Channel, err)
		}
		out = append(out, &rawChannelWrite{name: sw.Channel, val: v})
	}
	return out, nil
}

// unmarshalSendDirectives 把 cp wire 形态还原为 SendDirective 列表（PA-H1）。
// Payload 重建为 rawChannelWrite——归并时走 ApplyAll 复用 reducer 协议。
func unmarshalSendDirectives(sds []SerializedSend) ([]SendDirective, error) {
	if len(sds) == 0 {
		return nil, nil
	}
	out := make([]SendDirective, 0, len(sds))
	for _, sd := range sds {
		d := SendDirective{Target: sd.Target, owner: sd.Owner, ownerInstanceID: sd.OwnerInstanceID, instanceID: sd.InstanceID}
		for _, sw := range sd.Payload {
			v, err := unmarshalValue(taggedValue{Type: sw.Type, Data: sw.Data})
			if err != nil {
				return nil, fmt.Errorf("unmarshal deferred send payload (channel %q): %w", sw.Channel, err)
			}
			d.Payload = append(d.Payload, &rawChannelWrite{name: sw.Channel, val: v})
		}
		out = append(out, d)
	}
	return out, nil
}

// Snapshot 把当前 ChannelManager 状态序列化为 *Checkpoint。
//
// 仅填**业务 state**：SchemaVer / Channels / CurrentFrontier / PendingInterrupts。
// **元数据字段**（ID/NS/ParentNS/Status/GraphID/CreatedAt）默认空，由
// caller 填后调 Checkpointer.SaveNS 持久化（SaveNS 自动填 ID/NS/ParentNS）。
//
// 类型未注册的 channel value 返 error——避免静默丢失。
//
// **PendingInterrupts 浅拷贝注意** (audit #5 F20 doc): cp.PendingInterrupts 复制
// 的是 *AskQuery 指针 slice (非深拷贝 AskQuery struct). 业务跨 Snapshot 边界
// mutate `cp.PendingInterrupts[i].Question` 等字段会**回污染 cm.pendingInterrupts**.
// 业务想改 cp 字段先 `*cp.PendingInterrupts[i] = ...struct copy...` 或调用
// `cm.PendingInterrupts()`（channel_manager.go 深拷贝版）. Restore 反向恢复已深拷贝
// （本文件 Restore 内 `q.clone()`）, Snapshot 浅拷贝是性能 / 一致性权衡——业务 cp 多数走
// `MarshalCheckpoint` 序列化 round-trip 隐式深拷贝, 直接 mutate 是反模式.
func (cm *ChannelManager) Snapshot() (*Checkpoint, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cp := &Checkpoint{
		SchemaVer: checkpointSchemaVer,
		Channels:  make(map[string]taggedValue, len(cm.values)),
		// 任务代号跟着快照走。**这里不填的话，跨进程 wire-push 那条路整条断**：
		// 那条路是 `SnapshotForGraph → Marshal → wire → Unmarshal → WithResumeFrom`，
		// 全程不经过写 cp 的那个入口（代号原先只在那儿盖），于是对端载入到空代号、
		// 铸一枚新的 ⟹ 中断 ID 变了 ⟹ 调用方拿着旧 ID 永远答不进去（实测连跑五轮不收敛）。
		//
		// ⚠️ 这里读的是 `cm.taskID` 裸字段不是 `cm.TaskID()`——调用方已持锁。
		TaskID: cm.taskID,
		// 输入摘要同理跟着快照走（续跑时比对用，见 Checkpoint.InputDigest）。
		InputDigest: cm.inputDigest,
	}
	for name, v := range cm.values {
		tv, err := marshalValue(v)
		if err != nil {
			return nil, fmt.Errorf("snapshot channel %q: %w", name, err)
		}
		cp.Channels[name] = tv
	}
	if len(cm.frontierAtExit) > 0 {
		// 成员身份内联，直接整段拷贝。**必须拷贝不能别名**——cp 要落盘，而 cm 的
		// frontier 在后续超步里还会被改写。
		cp.CurrentFrontier = make([]FrontierMember, len(cm.frontierAtExit))
		copy(cp.CurrentFrontier, cm.frontierAtExit)
		cp.FrontierStarted = cm.frontierStarted
	}
	if len(cm.pendingInterrupts) > 0 {
		cp.PendingInterrupts = make([]*AskQuery, len(cm.pendingInterrupts))
		copy(cp.PendingInterrupts, cm.pendingInterrupts)
	}
	// audit Round 3 Bug #1 修复：visitCount 持久化让跨 Run/Resume 的 InterruptID
	// 编入 visit 维度——同节点多次访问 ID 不串台
	if len(cm.visitCount) > 0 {
		cp.NodeVisits = make(map[NodeID]int, len(cm.visitCount))
		for k, v := range cm.visitCount {
			cp.NodeVisits[k] = v
		}
	}
	// PA-H1：部分失败超步的成功成员 + 其 SendDirectives 持久化——Resume 重跑
	// 失败成员成功后并入出边计算（不重跑），没它发散 fan-out 的成功分支后继永久丢。
	if len(cm.completedFrontier) > 0 {
		cp.CompletedFrontier = make([]FrontierMember, len(cm.completedFrontier))
		copy(cp.CompletedFrontier, cm.completedFrontier)
	}
	// 它们当时算好的出边目标——不存的话 Resume 只能重算，而重算就等于让「已成功已提交」
	// 的成员还能在出边上失败（见 Checkpoint.CompletedTargets godoc）。
	if len(cm.completedTargets) > 0 {
		cp.CompletedTargets = make([]NodeID, len(cm.completedTargets))
		copy(cp.CompletedTargets, cm.completedTargets)
	}
	// 已兑付的答案位——不持久化的话这道兜底只在单次 Run 内有效，而它要防的
	// 「累积传旧答案静默满足新提问」恰恰是跨 Run 的形态。
	// ⚠️ 读**裸字段**，与上面 `cm.taskID` 同款理由：调用方（本函数）已持 RLock，
	// 再调一个内部会 RLock 的方法就是**递归读锁**——Go 的 RWMutex 读锁不可重入，
	// 只要此刻有写者在排队，第二次 RLock 永远拿不到（实测三次独立复现，其中一次走的是
	// 框架自己的 async cp 写路径，不需要任何业务代码）。
	if len(cm.consumedAnswers) > 0 {
		cp.ConsumedAnswers = append([]string(nil), cm.consumedAnswers...)
	}
	// 已提交的上行信号——做出它们的那几步续跑时不再执行，图跑完时只能从这里交上去。
	// 读裸字段，理由同上（本函数已持 RLock）。
	if len(cm.signals) > 0 {
		cp.Signals = append([]SignalRecord(nil), cm.signals...)
	}
	if len(cm.completedSignals) > 0 {
		cp.CompletedSignals = append([]SignalRecord(nil), cm.completedSignals...)
	}
	if len(cm.recipientKey) > 0 {
		cp.RecipientKey = append([]byte(nil), cm.recipientKey...)
	}
	// 节点私有中断态（见 node_state.go）——**投影导出**：只带「从这份 cp 恢复时真的
	// 读得到」的那些键，不是整张表。
	//
	// # 为什么要投影
	//
	// cm 里那张表**只增不减**（键含 visit、成功后自然不可达，正确性上不需要清），而
	// cp 是**每超步全量重写**的 ⟹ 写入字节数随轮数**平方**增长。实测长 ReAct 下
	// node_states 约占整份 cp 一半、20 轮从 4.4KB 涨到 78KB；配 sync/async durability
	// + 真后端（MySQL 默认 max_allowed_packet 才 4MB）时这是持续写放大。
	//
	// # 投影谓词 = 读谓词，逐字相同
	//
	// 导出集合 = `{key(m.NodeID, m.InstanceID, visitCount[m.NodeID]) : m ∈ frontier}`。
	// 它跟节点侧真正会算出来的键（`withNodeStateAccessor` 在节点入口算的那个）用的是
	// **同一个函数、同一批输入**——不是近似、不是保守估计。
	//
	// 能这么写死，靠三条既有约束：
	//
	//  1. **memo 的唯一读点是点查**（`getNodeState`），全仓没有遍历整表的读；
	//  2. **Resume 恒从 `cp.CurrentFrontier` 起跑**（`WithResumeFrom` 的 godoc），
	//     我们**没有「从任意节点 Resume」的 API**——这正是投影在我们这儿可判、在带
	//     time-travel 的模型里不可判的原因；
	//  3. `visit` 只在成功路径递增 ⟹ 后续超步节点算出的键要么是它这次新写的（cp 里
	//     本来就没有），要么撞不上任何旧键。
	//
	// ⚠️ 上面「投影谓词 = 读谓词，逐字相同」那条对**派生实例**曾经不成立，靠 `nodeStateKey` 对非空 `InstanceID` 把 visit 置 0
	// 兜住：`visitCount` 是 per-NodeID **共享**的，混合 frontier（同 NodeID 既是静态边目标
	// 又是 `SendFrom` 目标）下静态那位成功会把它推高，于是「导出时读到的值」跟「节点入口
	// 读到的值」分叉——实测 memo 整条掉档、Resume 重放已完成的副作用。两处必须用同一条
	// 互斥规则（同 `InterruptID()`），哨兵见 node_state_mixed_frontier_test.go。
	//
	// ⚠️ **曾经有一个反例，现在没有了**：`SendFrom` 派生实例的 visit 由 `nodeStateKey` 置 0（**不是**它读到的值天生为 0），而出边还是提交
	// 之后的独立相位时，一个已成功的实例会被 cond 报错拉回来重跑、InstanceID 复用 ⟹ 键
	// 完全重合。那时靠一道按实例精确清 memo 的 purge 兜，于是「取 visit 的方式」在两处
	// 各写一遍、必须同步——正是本仓反复栽跟头的那种形状。
	//
	// 出边归成员之后（cond 排在提交之前、算不出来就是该成员失败），「已成功成员被拉回
	// 重跑」不可达 ⟹ 那道 purge 连同它依赖的整套机件一并删除，这里也就只剩一处取值。
	//
	// # 一个副作用：中途 cp 的 NodeStates 恒为空
	//
	// 每超步 durability cp 写在 `updateFrontier(nextFrontier)` **之后**，而 nextFrontier
	// 里的节点这一超步还没执行、上一超步执行过的已 visit++ ⟹ 交集为空。而 O(n²) 的字节
	// 正是这些中途 cp 贡献的——**投影不是把它削小，是把它归零**；只有真中断那份出口 cp
	// 带 memo，且只带中断成员那几条。
	//
	// 值仍是浅拷贝（不深拷 `taggedValue.Data`），靠的是 **`marshalValue` 每次都
	// `json.Marshal` 新分配一份、从不原地改写或复用 buffer**——这条隐式依赖别破：给它加
	// buffer pool（性能条款正会引向这个）之后，async durability 的后台 writer 会拿着已
	// 交出的 cp 读到被后续超步改掉的字节，静默写出撕裂的 cp。要加池就得在这里深拷 Data。
	// 哨兵见 `TestNodeState_MarshalledBytesNeverAliased`。
	//
	// 一次节点执行名下除了 [SaveNodeState] 那一条，还有派生的若干条——按步记下的
	// （[SaveStepResult]）、在分域里记的（tool_call、钩子相位与条件边、交出去的执行）、重做
	// 代际——键都是
	// `<节点执行键>:<…>`，**同一个成员判据、一起导出**（归属取第一个冒号之前那段，见
	// [nodeStateOwner]）：它们是这次节点执行的进度，跟那一条同生同灭；只导出前者的话，中断
	// 之后那些条目整条掉出存档，重跑时已经做成的步骤（已完成的嵌套调用）被再做一遍。
	if len(cm.nodeStates) > 0 && len(cm.frontierAtExit) > 0 {
		members := make(map[string]struct{}, len(cm.frontierAtExit))
		for _, m := range cm.frontierAtExit {
			members[nodeStateKey(m.NodeID, m.InstanceID, cm.visitCount[m.NodeID])] = struct{}{}
		}
		// 可重做的步骤记录：同一个分域里同一步（派生键去掉代际那一段相同）只有代际最大的那条
		// 可能读得到。记录总是按写入那一刻所在分域的有效代际记的，而有效代际只增不减，所以代际
		// 更小的那几条一定是更早写、已被推进过的——读不到。只留最大的那条，别的不导出（全留的话
		// 分域级否决每发生一次存档就多带一份整条产出，实测线性增长）。
		newest := map[string]int{}
		for k := range cm.nodeStates {
			if _, ok := members[nodeStateOwner(k)]; !ok {
				continue
			}
			if slot, gen, _, ok := redoSlotOf(k); ok {
				if g, seen := newest[slot]; !seen || gen > g {
					newest[slot] = gen
				}
			}
		}
		for k, tv := range cm.nodeStates {
			owner := nodeStateOwner(k)
			if _, ok := members[owner]; !ok {
				continue
			}
			// 普通的可重做记录比这次执行的执行级代际还小的一定过期（有效代际不小于执行级）。最大的那条
			// 也可能已经过期（所在分域在它之后又被推进过）——派生键里只有分域的哈希、拼不回分域链，这一点
			// 判不了，照样导出，读不到而已。宁可多带一条读不到的，不能丢一条读得到的。
			//
			// ⚠️ 交出去的一段执行（HandOver 封的那一格，键里是 :hredo）只按「同一步留最新一代」丢，不拿
			// 执行级代际比：那一格的代际从交接那一层数起、不算执行级（见 nodeStateAccessor.handedOver），
			// 读得到的记录代际可以比执行级小。拿执行级比的话，调用方被否决重做之后，被调方在图外按
			// Rerunnable 记下的方案掉出存档，续跑时重新产出一份，而提问的 ID 没变——对旧方案的批准兑付给了
			// 新方案（实测）。
			// 代价：调用方重做之后交来的输入变了、调用换了身份时，旧身份那一格最后那条读不到却照样导出——每否决
			// 一次多带一条（审计实测 3 次否决 3 条），这次节点执行成功之后整串一起不可达。精确回收要知道「这一格
			// 的调用已经完成」，见 doc/code/todo.md。
			if slot, gen, handed, ok := redoSlotOf(k); ok && (gen < newest[slot] || (!handed && gen < cm.rerunGenHeld(owner, ""))) {
				continue
			}
			if cp.NodeStates == nil {
				cp.NodeStates = make(map[string]taggedValue, len(members))
			}
			cp.NodeStates[k] = tv
		}
	}
	if len(cm.deferredSends) > 0 {
		sds, err := marshalSendDirectives(cm.deferredSends)
		if err != nil {
			return nil, fmt.Errorf("snapshot deferred sends: %w", err)
		}
		cp.DeferredSends = sds
	}
	// F-B：没能提交、待 Resume 补 apply 的 SendDirective payload——不存
	// 的话 target（已在 CurrentFrontier 里）Resume 后静默读到零值。
	if len(cm.pendingSendWrites) > 0 {
		sws, err := marshalChannelWrites(cm.pendingSendWrites)
		if err != nil {
			return nil, fmt.Errorf("snapshot pending send writes: %w", err)
		}
		cp.PendingSendWrites = sws
	}
	return cp, nil
}

// SnapshotForGraph 是 Snapshot 的 graph-aware 变体——业务跨进程 wire-push 推荐用。
//
// **audit Round 8 Bug 5 修复**：业务 wire-push 路径（cm.Snapshot →
// MarshalCheckpoint → wire → UnmarshalCheckpoint → WithResumeFrom）原本完全不
// 带 GraphID/Status，跨 graph mix cp 时 silent corruption（Bug 4 amplified）。
//
// 本方法在 Snapshot 基础上自动填：
//   - GraphID = g.ID()——让接收端 WithResumeFrom 能做冲突守卫（Bug 4 修复）
//   - Status  = **按内容算**：有 frontier 或有挂起提问才 [CPStatusPending]，否则留
//     [CPStatusUnset]（见下）
//
// # Status 为什么按内容算，而不是无条件盖 Pending
//
// 这个方法是 [ChannelManager] 上的公开方法，**业务任意时刻都能调**——包括在一次跑完
// 之后。它**结构上拿不到 runErr**，也就没有引擎出口那处判据（`shouldStayPending`）所需
// 的信息，替引擎盖章只会跟那处漂移。
//
// 而无条件盖 Pending 在三条路上分别是零收益、误报、有害：
//   - 它自己文档化的 wire-push 路径上**零收益**——接收端 `WithResumeFrom` 根本不看
//     Status，恢复 frontier 读的是 `cp.CurrentFrontier`；
//   - cp.load 事件里的 `WouldResume` 字段会**误报**；
//   - 业务把产物存进 checkpointer 时**有害**：一份「已经跑完」的快照被标成挂起，
//     下次同 sid 跑就被 auto-load 当未完成任务捡起来——实测表现是 Run 静默返 nil、
//     一个节点都没跑、存档还被当成功清掉。
//
// 留 [CPStatusUnset] 正是要的：「跑完了」和「没盖过戳」在 auto-load 侧行为一致（都不捡）。
//
// ⚠️ **代价（实测，别照着推理）**：Unset 的存档会绕过 Run 入口那道 auto-load 的
// GraphID 守卫——它整个包在 `Status == Pending` 里。业务把这种快照存进别人的
// (sid, ns) 时：入口不报 [ErrGraphIDConflict]、**静默走 fresh run**，而且这一跑跑成功
// 之后按 sid 所有权清整棵子树 ⟹ **别人那份存档被删掉且 err=nil**。
//
// **不要指望出口那道 `SaveNS` 守卫兜住**：成功路径上根本没有 SaveNS，只有
// `DeleteSubtree`，而它不看 GraphID。（[CPStatusFailed] 的存档今天也一样绕过入口守卫，
// 那是先于本改动就有的同形状缝。）
//
// ⟹ **别把跑完的快照存进 checkpointer**。要种 channel 初值用 [WithInitialWrites]，
// 要续跑用 [WithResumeFrom] 直接把 cp 递进去——两者都不经过那个槽。
//
// 业务跨进程典型用法：
//
//	cp, _ := cm.SnapshotForGraph(g)        // 自动填 GraphID + Status
//	data, _ := compose_v2.MarshalCheckpoint(cp)
//	// ... 跨进程：data → wire → 下游
//	cp2, _ := compose_v2.UnmarshalCheckpoint(data)
//	err := compose_v2.Run(ctx, sameOrSameID(g), cm2, compose_v2.WithResumeFrom(cp2))
//	// ↑ 接收端 cp.GraphID 不匹配会 fail-fast 报 ErrGraphIDConflict
//
// 不要 graph-aware 元数据时仍可用裸 Snapshot——单进程内部用 / 调试用 OK。
func (cm *ChannelManager) SnapshotForGraph(g *Graph) (*Checkpoint, error) {
	cp, err := cm.Snapshot()
	if err != nil {
		return nil, err
	}
	if g != nil {
		cp.GraphID = g.ID()
	}
	// 按内容算，不无条件盖——理由见 godoc「Status 为什么按内容算」。
	// 判据跟引擎出口那处「要不要存档」同源（`graph_loop.go` 的空 frontier + 空挂起
	// 早退），但这里读的是快照自身而不是 runErr——那个信息这个方法拿不到。
	if cp.HasResumableWork() {
		cp.Status = CPStatusPending
	}
	return cp, nil
}

// MarshalCheckpoint 把 cp 编码为 JSON bytes——用户存盘 / 网络传输用。
//
// **不 mutate 入参**: marshal 是纯函数, 业务持有的 cp 实例
// 字段不被改写 (避免 side-effect 误改业务对象).
func MarshalCheckpoint(cp *Checkpoint) ([]byte, error) {
	if cp == nil {
		return nil, fmt.Errorf("MarshalCheckpoint: nil checkpoint")
	}
	return json.Marshal(cp)
}

// UnmarshalCheckpoint 把 JSON bytes 解码为 *Checkpoint。
//
// **设计决策** (Option D): 不在反序列化时做 SchemaVer mismatch fail-fast —
// 业务 ad-hoc cp / 旧 cp 都能正常加载. 业务 ad-hoc `&Checkpoint{}` 漏填
// SchemaVer (默认 0) 直接走通; 跨版本 cp 业务自行按 cp.SchemaVer 分支兼容.
func UnmarshalCheckpoint(data []byte) (*Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("UnmarshalCheckpoint: %w", err)
	}
	return &cp, nil
}

// Restore 从 cp 恢复 ChannelManager 状态。
//
// **必须先 register 所有 channel**——Restore 只填充 values，channel 定义
// （reducer / zero）由 caller 通过 RegisterChannel / WithChannel 重建。
//
// cp.Channels 中存在但 cm 未注册的 channel：返 error（防止静默 drop）。
// cm 已注册但 cp 中无的 channel：保持原状（可能尚未写入）。
//
// **audit Round 10 Bug L1 修复**：业务自定义 channel value type 含 UnmarshalJSON
// 内部 panic（schema 校验 / enum 断言 panic）时之前 partial-restore 半烂状态
// （前 K-1 个 channel 已写新值，第 K 个 panic 冒泡，cm.values 留 mixed state）。
// 入口 snapshot 全 state，panic 时 LIFO 还原。（ApplyAll 当年是同模式，现已换成
// 归并/提交分离——它不再需要回滚，Restore 这条还需要：它是就地改多个字段。）
func (cm *ChannelManager) Restore(cp *Checkpoint) (err error) {
	if cp == nil {
		return fmt.Errorf("Restore: nil checkpoint")
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 入口 snapshot 三类 state 用于 panic 回滚——values / visitCount /
	// pendingInterrupts 都可能被本函数 mutate。audit Round 10 Bug L1 修复。
	prevValues := make(map[string]any, len(cm.values))
	for k, v := range cm.values {
		prevValues[k] = v
	}
	var prevVisit map[NodeID]int
	if cm.visitCount != nil {
		prevVisit = make(map[NodeID]int, len(cm.visitCount))
		for k, v := range cm.visitCount {
			prevVisit[k] = v
		}
	}
	prevPending := cm.pendingInterrupts
	prevCompleted := cm.completedFrontier
	prevCompletedTargets := cm.completedTargets
	prevConsumedAnswers := cm.consumedAnswers
	prevSignals := cm.signals
	prevCompletedSignals := cm.completedSignals
	prevRecipientKey := cm.recipientKey
	prevDeferred := cm.deferredSends
	prevFrontier := cm.frontierAtExit
	prevFrontierStarted := cm.frontierStarted
	prevPendingSends := cm.pendingSendWrites
	// nodeStates 的赋值在 DeferredSends / PendingSendWrites 两个会 rollback 的步骤**之前**
	// ——不快照它，那两步失败时它就留在半还原状态（旧 cm 上盖了新 cp 的 memo）。
	prevNodeStates := cm.nodeStates

	// 全量还原到 Restore 入口前的快照——**唯一一份清单**，panic 与 error 两条路都调它。
	//
	// ⚠️ 这里曾经是两份平行清单（panic 的 defer 一份、error 的 rollbackOnErr 一份），
	// 于是新增字段时补了一处漏另一处——`completedTargets` 与 `consumedAnswers` 两个都只
	// 补进了 panic 那份，error 路径静默留着被拒绝那份 cp 的内容。这正是「必须与 X 保持
	// 一致」的注释挡不住的那类漂移：**消灭拷贝，不是维护拷贝**。
	rollbackOnErr := func() {
		cm.values = prevValues
		cm.visitCount = prevVisit
		cm.pendingInterrupts = prevPending
		cm.completedFrontier = prevCompleted
		cm.completedTargets = prevCompletedTargets
		cm.consumedAnswers = prevConsumedAnswers
		cm.signals = prevSignals
		cm.completedSignals = prevCompletedSignals
		cm.recipientKey = prevRecipientKey
		cm.deferredSends = prevDeferred
		cm.frontierAtExit = prevFrontier
		cm.frontierStarted = prevFrontierStarted
		cm.pendingSendWrites = prevPendingSends
		cm.nodeStates = prevNodeStates
	}

	// **三条终止路径都要回滚，不只 panic 和 error**：`runtime.Goexit`（业务自定义类型的
	// `UnmarshalJSON` 里一句 logger.Fatal）也展开这条栈而 `recover()` 返 nil ⟹ 回滚跳过
	// ⟹ 调用方手上那个 cm 留在 **partial-restore** 半烂状态（前 K-1 个 channel 已是新值、
	// 其余是旧值）。判据同 ApplyAll：`restored` 只在正常走到末尾时置位。
	restored := false
	defer func() {
		if r := recover(); r != nil {
			rollbackOnErr()
			// audit Round 10 D-N3：保 error chain。持着 cm 写锁，日志不在这里记（handler 是业务代码），
			// 标成「待补记」由 Run 出口补记（见 panicerr.WrapPending）
			err = panicerr.WrapPending("Restore panic (rolled back)", panicerr.New(r))
			return
		}
		if !restored {
			rollbackOnErr() // Goexit：栈在坍塌、err 没人接，承重的是别留半烂的 cm
		}
	}()

	// **audit Round 10 Bug D-N4 修复**：error 路径同 panic 路径一样需要 atomic
	// rollback——unmarshalValue 返普通 error（如类型未注册）时之前直接 return，
	// 让 cm.values 留 partial-restore 半烂状态（前 K-1 个 channel 已写新值）。
	// 复用 entry-snapshot 在 error 路径手动还原（panic 路径走 defer 自动还原）。
	for name, tv := range cp.Channels {
		ch, ok := cm.channels[name]
		if !ok {
			restored = true // 本处自带回滚，别让 Goexit 兜底支重复回滚
			rollbackOnErr()
			return fmt.Errorf("Restore: channel %q in checkpoint not registered in this ChannelManager", name)
		}
		v, err := unmarshalValue(tv)
		if err != nil {
			restored = true // 本处自带回滚，别让 Goexit 兜底支重复回滚
			rollbackOnErr()
			return fmt.Errorf("Restore channel %q: %w", name, err)
		}
		// **R32+ 视角 6 HIGH-1 fix**: 跨进程 / 跨版本 cp restore type mismatch 校验。
		// 之前 cm.channels[name] 检查通过 + unmarshalValue 按 cp wire type 反序列化后
		// 直接写 cm.values, 后续 Get[T] cast 才 panic (access.go:34) — 错误延迟 + 难
		// 定位。修法: Restore 阶段 fail-fast 比对 v 类型与 channel 注册类型。typed-nil
		// 场景 (reflect.TypeOf(typedNil)==typeRep) 也 OK。
		//
		// **ISSUE-87 MED-2 fix**: untyped nil v (cp wire 含 `<nil>` tag, 业务直接调
		// MarshalTagged(nil) 或跨版本遗留) 跳过写 cm.values 让 channel zero value (ch.z)
		// 自动兜底——避免 cm.values[name] = untyped nil 后 Get[T] cast 失败 panic。
		// **已知 limitation (audit LOW)**: channel 值是 untyped nil(any) 时跨进程 round-trip
		// 后 Has() 由 true 退化 false（cm.values 不含该 key）。仅业务自定义 reducer 显式返
		// untyped nil 才命中——框架 reducer (LastValue/Append/Sum/OnceFlag) + typed nil 指针
		// (走下方 typeRep 校验保留) 均无此问题。业务勿依赖「Has()+nil 值」跨 Resume 稳定。
		if v == nil {
			continue // 跳过写, channel zero value 兜底
		}
		if expected := ch.typeRep(); expected != nil && reflect.TypeOf(v) != expected {
			restored = true // 本处自带回滚，别让 Goexit 兜底支重复回滚
			rollbackOnErr()
			return fmt.Errorf("Restore: channel %q type mismatch — cp wire type %T vs ChannelManager registered %v",
				name, v, expected)
		}
		cm.values[name] = v
	}
	// audit Round 3 Bug #1 修复：恢复 visitCount 让 Resume 后跨 Run 视角下
	// 同节点的 InterruptID 编入正确 visit 维度
	if len(cp.NodeVisits) > 0 {
		if cm.visitCount == nil {
			cm.visitCount = make(map[NodeID]int, len(cp.NodeVisits))
		}
		for k, v := range cp.NodeVisits {
			cm.visitCount[k] = v
		}
	}
	// **audit Round 8 Bug 2 修复**：恢复 pendingInterrupts 让跨进程 Restore
	// 后 cm.PendingInterrupts() API 一致——之前 Snapshot 写出 cp.PendingInterrupts
	// 但 Restore 漏读，跨进程业务"先查 pending 渲染 UI → 拿答案 → Resume"用法
	// 第一步通过 cm API 拿不到（必须绕开读 cp.PendingInterrupts 字段）。
	//
	// 单进程 Resume 路径 graph_loop.go:305 之后会 clearInterrupts，所以单进程
	// 不暴露此 bug；跨进程需要 caller 在 ResumeFrom 之前查询 pending 时必要。
	if len(cp.PendingInterrupts) > 0 {
		cm.pendingInterrupts = make([]*AskQuery, 0, len(cp.PendingInterrupts))
		for _, q := range cp.PendingInterrupts {
			if q == nil {
				continue
			}
			// 深拷贝避免 caller 后续 mutate cp.PendingInterrupts 串到 cm
			cm.pendingInterrupts = append(cm.pendingInterrupts, q.clone())
		}
	}
	// PA-H1：恢复部分失败超步的成功成员 + 其 SendDirectives——Resume 重跑失败
	// 成员成功后 peekCompletedSiblings 取出并入出边计算。unmarshal 失败走统一
	// rollback（与 channel 值同 fail-fast 协议）。
	if len(cp.CompletedFrontier) > 0 {
		cm.completedFrontier = make([]FrontierMember, len(cp.CompletedFrontier))
		copy(cm.completedFrontier, cp.CompletedFrontier)
	}
	if len(cp.CompletedTargets) > 0 {
		cm.completedTargets = make([]NodeID, len(cp.CompletedTargets))
		copy(cm.completedTargets, cp.CompletedTargets)
	}
	if len(cp.ConsumedAnswers) > 0 {
		cm.consumedAnswers = make([]string, len(cp.ConsumedAnswers))
		copy(cm.consumedAnswers, cp.ConsumedAnswers)
	}
	// 上行信号与收件人密钥**整体替换**，不是「存档里有才覆盖」：Restore 恢复的是存档那一刻的状态。
	// 只在有时才覆盖的话，复用的 cm 上残留的旧信号在这次成功时再交一次（实测：同一份不带信号的
	// 存档，新 cm 交出 ""、复用 cm 交出 "stale"）。
	cm.signals = append([]SignalRecord(nil), cp.Signals...)
	cm.completedSignals = append([]SignalRecord(nil), cp.CompletedSignals...)
	cm.recipientKey = append([]byte(nil), cp.RecipientKey...)
	// 节点私有中断态：整表拷回。**不校验里头的 tag 能不能 unmarshal**——那是节点自己
	// `GetNodeState` 时的事（类型不匹配就当没有上次、从头做，见其 godoc）；在 Restore
	// 期 fail-fast 会让「业务改了 state 类型」变成整个 Resume 起不来，代价不对等。
	if len(cp.NodeStates) > 0 {
		cm.nodeStates = make(map[string]taggedValue, len(cp.NodeStates))
		for k, v := range cp.NodeStates {
			cm.nodeStates[k] = v
		}
	}
	if len(cp.DeferredSends) > 0 {
		sends, derr := unmarshalSendDirectives(cp.DeferredSends)
		if derr != nil {
			restored = true // 本处自带回滚，别让 Goexit 兜底支重复回滚
			rollbackOnErr()
			return fmt.Errorf("Restore deferred sends: %w", derr)
		}
		cm.deferredSends = sends
	}
	// F-B：恢复待补 apply 的 SendDirective payload——主循环在跑任何节点之前
	// peekPendingSendWrites 补 apply（见 graph_loop.go）。
	if len(cp.PendingSendWrites) > 0 {
		pws, perr := unmarshalChannelWrites(cp.PendingSendWrites)
		if perr != nil {
			restored = true // 本处自带回滚，别让 Goexit 兜底支重复回滚
			rollbackOnErr()
			return fmt.Errorf("Restore pending send writes: %w", perr)
		}
		cm.pendingSendWrites = pws
	}
	// **round-trip 完备**（引擎深审：失败的 Resume 不能降级已存恢复点）：
	// Snapshot 读的**每个** cm 字段，Restore 都必须填回——否则那个字段在"Restore 之后、
	// 主循环填它之前"的窗口里是零值，而 Run 出口 defer 对**所有** runErr != nil 都
	// Snapshot+SaveNS（业务错也存档好让 Resume 重试，这是特性——档位标 [CPStatusFailed] 不是 [CPStatusPending]，但**存不存**与档位无关），窗口内任何
	// early return 都会拿零值覆盖掉原本好的 cp。
	//
	// `frontierAtExit` 曾是唯一漏的一个（只被 updateFrontier 写）。**实测后果**：HITL 存档
	// + Resume 时 import overlay 的 reducer panic → 原 `CurrentFrontier=[A]` 被空值覆盖 →
	// 业务修好 reducer 再 Resume 得 err=nil **静默成功、节点从没跑过、用户的问题永久丢**。
	//
	// **修在 Restore 而不是修那个 early return**：early return 是会长出来的（现在只有
	// resume import overlay 一处），补在这里让现在和将来的所有窗口一次性免疫。
	if len(cp.CurrentFrontier) > 0 {
		cm.frontierAtExit = make([]FrontierMember, len(cp.CurrentFrontier))
		copy(cm.frontierAtExit, cp.CurrentFrontier)
		cm.frontierStarted = cp.FrontierStarted
	}

	// **实例身份发号器要跨进程续上**（ISSUE-96）：instanceCounter 是进程级的，重启后
	// 从 0 重新发号，而这份 cp 里存着重启前发出去的 token —— 新号会**逐位撞上**旧号。
	//
	// 撞了不报错，是"两个互不相干的任务顶着同一个身份"：InterruptID 相同 ⟹ 第二个任务
	// 静默吃掉第一个任务的用户答案、自己从不提问、Run 还报成功（不报错、结果错，最坏的
	// 一档）；childNS 相同 ⟹ 它还会自动 load 走第一个任务的子 cp。
	//
	// 原 godoc 说的「跨进程 Resume 读的是存下来的值、不重新 mint」只对**被恢复的成员**
	// 成立 —— Resume 期间任何重跑节点调 SendFrom 都会 fresh mint，跟旧号共用同一个 ID
	// 空间。故 Restore 时把发号器抬过本 cp 出现过的最大值。
	bumpInstanceCounterFromCheckpoint(cp)
	restored = true
	return nil
}
