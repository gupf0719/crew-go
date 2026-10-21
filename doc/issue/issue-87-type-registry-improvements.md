# ISSUE-87 · type registry / channel reducer / cp marshal 改进

**优先级**：MED（HIGH 已修剩 3 MED + 3 LOW 主要是边缘 wire 漂移 + 文档/命名约定）
**状态**：✅ 已修（2026-05-16）— HIGH-1/2 commit 23d75cb + MED/LOW 全修
**触发**：R32+ 9 agent 并发审 视角 6 系统审 type registry / cp marshal / channel
reducer 兼容性

## 已修 (commit 23d75cb)
- HIGH-1: Restore 不校验 channel value 类型与本地注册类型一致 → 加 typeRep 比对
- HIGH-2: LookupKey 对指针类型完全失效 → 修 retry 用 original 不是 reflect.TypeOf(t)

## 待修 finding

### MED-1: RegisterValueType 并发注册有 TOCTOU race（弱原子）

**位置**: `compose_v2/serialization.go:62-81`

```go
if existing, ok := typeRegistry.Load(typeName); ok { ... }  // ← Load
typeRegistry.Store(typeName, t)                              // ← Store
// Load 与 Store 之间有窗口
```

业务用 DynamicChannel + 并发 spawn goroutine 分别 RegisterValueType 同 type 不同
name 时 wire format 漂移 (canonical name 不确定)。

**修法**: 用 `LoadOrStore` 原子操作。

### MED-2: marshalValue 写 untyped-nil tag (`<nil>`), Restore 后 Get panic

**位置**:
- 写: `compose_v2/serialization.go:92-95`
- 读: `:110-113`
- 触发 panic: `compose_v2/access.go:32-35`

业务直接 MarshalTagged(nil) + UnmarshalTagged 然后塞回 cm; 或跨版本 cp wire 中
有遗留 `{"type":"<nil>","data":""}` entry → cm.values[name] = nil → Get cast 失败 panic。

**修法**: 三选一
- (a) `marshalValue` 拒绝 nil 入参返 error (fail-fast)
- (b) `unmarshalValue` 在 `<nil>` 时返 channel zero value 而非 untyped nil
- (c) `Restore` 在 v==nil 时跳过写 cm.values (让 Get fall back 到 ch.z) — 最简

### MED-3: ParallelBranchResult.OrigErr error 接口字段 cp Restore 失败

**位置**: `compose_v2/parallel_agent.go:20-24`

```go
type ParallelBranchResult struct {
    Name    string
    Output  *schema.Message
    OrigErr error  // ← interface 类型, json.Unmarshal 不可逆
}
```

实测 round-trip:
```
marshal err: {"Name":"b1","OrigErr":{}}
unmarshal: cannot unmarshal object into Go struct field ParallelBranchResult.OrigErr of type error
```

当前生产 worker 不填 OrigErr 不踩, 但**任何未来 commit 让框架在 worker 失败
路径填 OrigErr 即破 cp Restore** — 定时炸弹。

**修法**: 三选一
- (a) 字段加 `json:"-"` tag (业务读它仅 in-process / not cross-Resume)
- (b) 改类型为 `OrigErrMsg string` (损失类型保真换 wire 安全)
- (c) 自定义 MarshalJSON / UnmarshalJSON 处理 error 接口

### LOW-1: `[]*schema.Message` 注册名跨包不一致 — first-write-wins 无确定性

**位置**:
- `agent_v2/basic_agent.go:669` 注册名 `"[]*common.Message"`
- `example/orchestration/channel_chat_demo/main.go:34` + `example/orchestration/channel_smoke/main.go:34`
  注册名 `"[]*schema.Message"`

`schema.Message = common.Message` 是 type alias, canonical name 取决于第一次写入
typeNameByType 的顺序 — 二进制 import 顺序变化 wire format 漂移。

**修法**: 项目应统一注册名 (建议 `"[]*schema.Message"` 与导出路径对齐), example
与 agent_v2 用同一字符串常量。

### LOW-2: RegisterValueType 对 interface 类型 T 注册 typeRep 返 nil

`reflect.TypeOf(zero_interface_value)` → nil。两个异 interface T (`Channel[Animal]`
vs `Channel[Vehicle]`) 同名注册全过 typeRep 比对。

当前生产没有 interface T 通道, godoc 没说明此限制。

**修法**: typeRep 用 `reflect.TypeOf((*T)(nil)).Elem()` 拿 interface T 自身,
不依赖零值。

### LOW-3: 自定义 reducer 闭包捕获 mutable state 不持久化语义陷阱

业务自定义 `Custom(name, reducer, zero)` reducer 闭包捕获 mutable state (如
`var counter atomic.Int64`) — `counter` 不在 cp 协议内, 跨 Resume 丢。

godoc 强调 reducer 必须是纯函数 + 关联律 + 交换律, 没显式提"闭包 state 跨
Resume 丢"。

**修法**: 在 `Reducer[T]` godoc 加一行"reducer 必须无副作用 + 闭包不持久化 —
所有跨 Resume 状态用 channel value 自身承载"。

## audit follow-up (2026-05-16)

R32+ 视角 6 修完后再 audit, 揭露 1 真 MED:

### audit MED-1: LoadOrStore 单 op 原子 但跨两 sync.Map 仍有 race window

视角 D 自审揭露 — `RegisterValueType` 改 LoadOrStore 后单 map (typeRegistry) 写已原子, 但 typeRegistry + typeNameByType **跨两 map** 仍有 race window:
- goroutine A: LoadOrStore typeRegistry → 成功
- goroutine B: 同 type 不同 name 之间 LoadOrStore typeRegistry → A.name 已存 typeRegistry 但 B 未到 typeNameByType 写
- A: LoadOrStore typeNameByType → A canonical name
- B: LoadOrStore typeNameByType → 因 typeNameByType 已有 A 的 canonical name, B 用了 LoadOrStore 不会覆盖 → B 不一致

实际 race 罕见 (业务通常 init 同 goroutine), 但**测试 -race 仍可能触发**报"WARN race".

**修法**: 加 `var typeRegistryMu sync.Mutex` 跨两 map 写时持锁, LoadOrStore 改 `mu.Lock` + `Load` + `Store` 简单序列化。LoadOrStore 优势是免锁 hot path read; 此处写路径罕见 (注册一次), mutex 不影响热路径性能。

### audit MED 改进:

- **ParallelBranchResult.OrigErr godoc** 加跨 Resume 失败信号丢失警示 — `json:"-"` 让 cp wire 安全, 但 in-process 业务 branch fail → cp save → Resume 时 OrigErr 字段丢 (业务靠 OrigErr 判 retry/skip 失效)

### audit LOW 改进:

- **issue87 测试 t.Errorf → t.Fatalf** — `TestISSUE87_LOW2_TypeRepInterfaceT` 在 `got == nil` 用 t.Errorf 后续访问 got.Kind() 会 SIGSEGV panic 让测试 message 丢失, 改 t.Fatalf 早退

## audit follow-up² (commit a8b42b9 audit, 2026-05-16)

commit 49d3065 audit² 揭露 commit 5afb8eb 加的 `TestISSUE87_AuditFollowup_HIGH1_CrossMapAtomicity`
**假绿** — 实测 sanity-revert 删 `typeRegistryMu.Lock/Unlock` 后 `-race -count 20` 仍 20/20
PASS, 测试**不能区分 fix 在 vs revert** (sync.Map 单 op atomic 让 canonical name 稳定,
mu 真效果是逻辑层 ordering 单元测试无法可靠验证).

**修法 (诚实降级)**: 测试改名 `*_CrossMapMutexNoDeadlock` + godoc 直接说明:
> 本测试不构成 mutex fix 的 sanity-revert 守护, 只验"持锁不死锁 + canonical name
> 视图一致"基础不变量. mutex 真验证依赖 stress test 多 instance / 业务长跑场景 ad-hoc 观察.

诚实文档化 > 假装能验. commit 49d3065 误以为本测试拆了 ISSUE-87 假绿 — audit² 视角 D
实测揭露仍是假绿. **修复完成 ≠ 修对** R29 教训三复证 (commit 5afb8eb IndexRef regression
→ commit 49d3065 ISSUE-87 假绿 → commit a8b42b9 isAgentNil 协议反向不对称 — 同模式三轮).

### audit follow-up³ (commit a8b42b9 audit, 2026-05-16) — 诚实降级标定

视角 2 实测确证: `*_CrossMapMutexNoDeadlock` 测试 sanity-revert 删 mu 后仍 PASS — 接受
单元测试无法可靠验证此类逻辑 ordering race, 留 stress test 补 (CI 跑 `-race -count 100`
也无法保证抓到 — sync.Map 内部 atomic 让 race detector 不 fire).

## 关联

- R32+ 视角 6 audit: type registry / channel reducer / cp marshal 完整 finding
- ISSUE-79: typed local state 跨 Resume 不持久化 (当时 state.go godoc 写明, 同款语义陷阱；该文件已随 typed-state 子系统删除, 语义记录见 doc/v2/migration-history.md)
- R32+ audit follow-up: LoadOrStore 跨两 map race window + ParallelBranchResult.OrigErr 跨 Resume 失败信号丢失警示
- R32+ audit follow-up² + ³: ISSUE-87 测试假绿诚实降级 (sync.Map 单 op atomic 让 mutex 真效果不可单元测试验)
