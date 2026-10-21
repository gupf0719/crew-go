# 主题：Address / NS 寻址模型

> **v2 baseline** (audit follow-up #6 全文同步):
> - **单维度 graphNS 协议**: `WithGraphNS / GetGraphNS` 一对 helper (audit #6 删 v1
>   时代 `currentNS` 双维度协议 + `EnterChildNS` 组合 helper —— 生产 0 caller)
> - `JoinNS / JoinFanoutNS / SplitNS / ParentNS / IsNSInSubtree / ValidateNodeIDForNS`
>   核心 API (`compose_v2/ns.go`)
> - **stable InterruptID**: `InterruptSite.InterruptID()`（TaskID / graphNS / nodeID / 实例身份⊻visit / tc 下标 / idx）
>   → fnv64a 16 hex (`compose_v2/interrupt.go`)
> - **resume 走 LangGraph 风格 map ID 命中** (无 ns 自检, v1 时代 `IsResumeTarget`
>   已删, audit follow-up #5)


## 触发场景

需要查阅本文档的设计决策：
- 改 ns 字符串格式 / 分隔符（`/` `#` 当前用法）
- 加新 NS 派生 / 解析 helper
- 决策是否引入 typed Address（替代 string ns）
- 设计 Address 段 Kind 区分（agent / node / tool）
- 实现新 ns 子系统时的命名 / 拼接策略

## 三方对照速查表（v2 baseline）

| 维度 | crew-go v2 | LangGraph | eino |
|---|---|---|---|
| **承载结构** | `string`（`"a/b#0/c"`）+ 出口 typed `[]NSSegment{Kind="node"\|"fanout"}` | `string`（`"a\|b\|c"`，可含 `:taskID`） | typed `[]AddressSegment{ID,Type,SubID}` 全程 |
| **分隔符** | `/`=父子，`#`=fan-out | `\|`=父子，`:`=task | `;`=父子，`:`=type 内 ID/SubID |
| **派生 API** | `JoinNS(parent, child)` 纯函数 + `JoinFanoutNS(parent, branch, idx)` 含 idx 防 retry 重入 (ISSUE-78) | `tuple append` + `"\|".join(...)` 边界 + `task_id` hash (含 step) | `AppendAddressSegment(ctx, type, id, subID)` 派生 ns + 注入 resume 状态合一 |
| **解析 API** | `SplitNS / ParentNS / IsNSInSubtree / ValidateNodeIDForNS` | `xxh3_128_hexdigest(ns)` 直接 hash 不解析 | `Equals` + slice prefix `addr[:len(parent)]` |
| **stable ID 协议** | `InterruptSite.InterruptID() → fnv64a 16hex`（各维度都跨 Resume 稳定） | `xxh3_128_hexdigest(ns)` 32 hex（含 task_id） | `Address.String() = "type:id:subID;..."`（直串无 hash） |
| **核心代码** | `compose_v2/ns.go` 402 行（代码 162 行、注释 216 行，2026-09-24 实数）+ `ns_ctx.go` ~45 行 + `interrupt.go` 的 `InterruptSite.InterruptID` 50 行（godoc 19 + 代码 31）; 合 ~497 行 | 字符串 + hash 工具 | ~338 行（`internal/core/address.go`） |
| **业务侧负担** | string，零负担 | string，零负担 | 全程构造 `AddressSegment{Type,ID,SubID}` |
| **编译期保护** | `ValidateNodeIDForNS` 一次性拦字符冲突 (`/` `#` 禁止) | hash 后无字符冲突可能 | typed 天然防 |
| **resume 自检** | **无** — 走 LangGraph 风格 map ID 命中 (audit follow-up #5 删 v1 时代 `IsResumeTarget` self/ancestor/sibling 三模式) | 同 — `namespace_hash in scratchpad.resume_map` 自然命中 | `internal/core/address.go:178-191` 业务 ns 自检 "self+descendant" 耦合在 `AppendAddressSegment` 76 行内 |

## crew-go 设计

### 数据结构

`compose_v2/ns.go::JoinNS / JoinFanoutNS` (line 软引用 — 按符号自查):
```go
// JoinNS 拼接 parent + child nodeID 形成嵌套 ns。
//   JoinNS("", "a")       = "a"
//   JoinNS("a", "b")      = "a/b"
//   JoinNS("a/b", "c")    = "a/b/c"
func JoinNS(parent, nodeID string) string {
	if parent == "" {
		return nodeID
	}
	return parent + nsSepParent + nodeID  // nsSepParent = "/"
}

// JoinFanoutNS 拼接 fan-out 分支 ns。
//   JoinFanoutNS("", "worker", 0)  = "worker#0"
//   JoinFanoutNS("a", "branch", 1) = "a/branch#1"
func JoinFanoutNS(parent, branchNodeID string, index int64) string {
	base := JoinNS(parent, branchNodeID)
	return base + nsSepFanout + strconv.FormatInt(index, 10)  // nsSepFanout = "#"
}
```

### 解析 API

`compose_v2/ns.go::SplitNS / ParentNS / IsNSInSubtree` (line 软引用 — 按符号自查):
```go
SplitNS("a/b#0/c") -> []NSSegment{
    {NodeID: "a", Kind: "node"},
    {NodeID: "b", Kind: "fanout", Index: 0},
    {NodeID: "c", Kind: "node"},
}

ParentNS("a/b#1/c") = "a/b#1"     // 普通子图剥 /c
ParentNS("a/b#1")   = "a/b"       // fan-out 分支剥 #1

IsNSInSubtree("", "a/b/c")   = true   // 空 root = 整个 session
IsNSInSubtree("a", "a/b/c")  = true
IsNSInSubtree("a", "ab")     = false  // 字符串前缀≠ns 子树（边界判断）
IsNSInSubtree("a", "a#0")    = false  // 同一个节点的另一次执行，是兄弟不是下级
```

### ctx 注入（`compose_v2/ns_ctx.go`）

v2 单维度协议——audit follow-up #6 删 currentNS 维度 + EnterChildNS / EnterFanoutBranchNS helper (生产 0 caller, godoc 谎报"业务 tool 算 interrupt_id 用 currentNS" 实际不用):

```go
WithGraphNS(ctx, "a")                                       // 当前 graph 整体 ns push
GetGraphNS(ctx)                                             // 取当前 graph ns (业务 / framework hot path)

// 嵌套 sub-agent push 子 ns (7 编排 wrapper 内置 push 模式 —
// Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan;
// BasicAgent ReAct 无 sub-agent / GraphAgent 业务自定义 NodeFunc 走 RunSubAgent helper):
ctx = compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), childName))
```

业务提问走 `compose_v2.Ask`（ID 在它内部按 `InterruptSite` 算完立刻用）。

### 校验

`compose_v2/ns.go::ValidateNodeIDForNS` 在 `GraphBuilder.Build / ParallelAgent.AddBranch / SequentialAgent.AddStep` 等编排注册时一次性拦截 nodeID 含 `/` `#` 的字符冲突。运行时 hot path 不再校验（性能敏感）。

## LangGraph 设计（v0.6.10）

### 数据结构

string 全程，分隔符 `NS_SEP="|"` 父子、`NS_END=":"` 拆 task_id（`langgraph/_internal/_constants.py:72-75`）：
```python
checkpoint_ns = "graph|subgraph|subsubgraph"            # 父子链
task_checkpoint_ns = "node:task_id|subnode:subtask_id"  # 含 task_id
```

### Hash 化处理

边界处用 xxh3 hash（`langgraph/types.py:185, 191`）：
```python
def from_ns(cls, ns: str) -> str:
    return xxh3_128_hexdigest(ns.encode())  # 32 字符 hex 字符串（128 bit）
```

ID 全程是 hash——避免长 ns 串 + 自动防字符冲突。**代价**：丢失人类可读性，调试需要反查 ns。

### 派生 API

无专门 helper，调用方手动 `tuple append + join`：
```python
parent_ns = configurable.get(CONFIG_KEY_CHECKPOINT_NS, "")
checkpoint_ns = f"{parent_ns}{NS_SEP}{name}" if parent_ns else name
task_checkpoint_ns = f"{checkpoint_ns}:{task_id}"
```

## eino 设计

### 数据结构

`internal/core/address.go:29-77`：
```go
type AddressSegment struct {
    ID    string
    Type  AddressSegmentType  // "node" / "agent" / "tool" 等
    SubID string              // tool call ID 等
}

type Address []AddressSegment

func (a Address) String() string {
    // "node:foo;agent:bar:subID"
}
```

### 派生（耦合 resume 状态注入）

`AppendAddressSegment` 是 eino 设计精华——一个 API 同时做 ns 派生 + resume 状态查找：
```go
func AppendAddressSegment(ctx context.Context, segType, segID, subID string) context.Context {
    parent := getAddrCtx(ctx)
    newAddr := append(parent.addr, AddressSegment{Type: segType, ID: segID, SubID: subID})

    // 同时扫 id2Addr 找前缀匹配的 pending interrupt
    var interruptState core.InterruptState
    var resumeData any
    var isResumeTarget bool
    for id, candidate := range parent.id2Addr {
        if newAddr.Equals(candidate) {
            interruptState = parent.id2State[id]
            resumeData = parent.id2ResumeData[id]
            isResumeTarget = true
            break
        }
        // 还检查 candidate 是 newAddr 的后代——让 conduit 节点知道要 drill-down
    }

    return setAddrCtx(ctx, &addrCtx{
        addr: newAddr, interruptState, resumeData, isResumeTarget,
    })
}
```

### 优势

- 业务侧 typed 编辑器自动补全
- 编译期防字符串拼接错
- 派生 + resume 状态注入合一 API（少调一次 helper）

### 缺陷

- 全程构造 `AddressSegment{Type,ID,SubID}` 三件套
- 核心代码 ~338 行（vs 我们 ~497 行 — `compose_v2/ns.go` 402（其中注释 216）+ `ns_ctx.go` 45 + `interrupt.go` 的 ID 派生 50（godoc 19 + 代码 31）；早先写的「~302 行、实际相当」是 ns.go 还只有 232 行时的数，总行数现在已经不相当，多出来的大头是 ns.go 里的设计注释。关键差异仍在业务侧 — 我们 string 路线零负担 vs eino typed `AddressSegment{Type,ID,SubID}` 三件套构造）
- 解析 API 简陋（无 ParentAddress 函数，业务直接 slice）

## 我们的优势（v2 保留不动）

1. **string + helper 平衡**：业务零负担（直接传 nodeID），框架内部用 typed `NSSegment` 解析时按需转换
2. **`IsNSInSubtree` 严格语义**：`IsNSInSubtree("a", "ab") = false`（按 ns 边界，不是字符串前缀）；`IsNSInSubtree("a", "a#0") = false`（`#`/`@` 是同一个节点的兄弟执行，`DeleteSubtree` 不跨它们删）—— eino 用 `Equals` 是精确相等，不支持祖先链查询；LangGraph hash 后完全丢拓扑信息
3. **fan-out 分支 `#idx`** vs eino 嵌套层数有限制（递归构造 typed segment 慢）：我们 string 拼接零开销
4. **fan-out 物理分离协议** (audit follow-up #6 更新)：`JoinFanoutNS(parent, name, hash(身份))` 派 childNS（`agent_v2/run_subagent.go` 的 `nestedCallNS`，AgentAsTool / skill fork 也经它）；身份 = 位置（调用方所在的节点与相位 + tool_call 维 tc.ID + 批内下标 + tools 节点访问序号，或实例维 / visit）+ 输入内容（同一份输入再分第几次）或 key（全历史输入的 AgentAsTool 与 skill fork 只看位置），fnv64a 取低 63 位、`#` 下标是 int64，与平台字长无关. eino `AppendAddressSegment` 76 行函数耦合 ns 派生 + resume 状态注入 + isResumeTarget **自身/后代 二态**判断（eino 无 sibling 模式；sibling 是 crew-go v1 `IsResumeTarget` 的扩展、v2 已删）（`address.go:118-194`）——crew-go v2 物理分离 cp + map ID 命中 + business 简单 hash 拼接零 cognitive overhead
5. **stable InterruptID 协议**：`InterruptSite.InterruptID() → fnv-64a 16 hex` (`compose_v2/interrupt.go`) ——LangGraph hash `"|".join(ns)` 输入维度隐含 ns 派生里；eino `Address.String()` 直串无 hash. 我们坐标是显式 struct 字段、业务调试可追溯 + `slog.LogAttrs`（DEBUG level）输出各维度 (audit follow-up #5)

## 我们的不足（已评估不抄）

> InterruptID 长度问题已不存在 —— 当前用 fnv-64 hash → 16 字符 hex 定长（`compose_v2/interrupt.go` 的 `InterruptSite.InterruptID`），与 LangGraph xxh3 同属定长 hash 化（我们 fnv-64 16 hex / LangGraph xxh3-128 32 hex），ns 多深都不膨胀.
>
> **eino `AppendAddressSegment` 自检** (业务 ns 自检 "self+descendant"): crew-go v2 走 LangGraph 风格 map ID 命中, 完全无 ns 自检. 业务想完全隔离子 agent 走 opt-in `StripResumeContext` / `WithSubgraphIsolatedResumeContext` — 单包 compose_v2 + agent_v2 用例足够, 不抄 eino "76 行耦合派生+resume" 复杂度.

## 我们已偏离三方但更优（保留不动）

1. **`Kind` 区分 fan-out 分支 vs 普通子图**：通过 `#idx` 后缀编码到 string ns，`SplitNS` 出口产 `NSSegment.Kind="fanout"`；eino 全是 `AddressSegment.Type=string`，业务自定义
2. **v2 走 LangGraph 风格纯 map ID 命中 + opt-in ns 隔离**：graphNS 编入 hash 让父子节点 ID 天然分离；业务需要完全隔离用 `StripResumeContext` / `WithSubgraphIsolatedResumeContext()` opt-in. eino 把"自身或后代"判断耦合在 `AppendAddressSegment` (76 行函数), crew-go v2 完全删自检走 map lookup. ⚠️ v1 时代曾有 `IsResumeTarget` self/ancestor/sibling 三模式守门 helper, v2 演化已删 (grep `func IsResumeTarget` 返 0 hits)

## 决策记录

| 时间 | 决策 | 借鉴 | 不全抄理由 |
|---|---|---|---|
| ISSUE-70 Phase 1 | 加 `Checkpoint.NS` / `ParentNS` 字段 | LangGraph ns 派生 | 不抄 hash——保人类可读 |
| ISSUE-70 Phase 2 | string ns + `JoinNS / JoinFanoutNS / SplitNS` helper | LangGraph string 路线 + eino kind 概念 | 不抄 typed Address——业务零负担 |
| ISSUE-70 Phase 2 波 1.1 | (v1 时代) `IsResumeTarget` 三态分立 — **v2 已删此 helper, 走 map ID 命中** | eino "自身或后代" + 我们扩兄弟 | (v1 决策, v2 重新选择走 LangGraph 风格 map 命中删自检, 见 2026-05 audit follow-up #5) |
| ISSUE-70 Phase 3 | **保留 string ns + helper，不引入 typed Address** | （决策记录） | typed 多 50% 代码 + 业务负担, 红利已通过 string ns 拆分获得 |
| 2026-05 audit (#6 三方对照) | (1) 删 `WithCurrentNS / GetCurrentNS / EnterChildNS` 单维度协议——godoc 谎报"业务 tool 算 interrupt_id 用 currentNS" 实际 0 production caller; (2) 文档 stale 路径全更新到 v2 + 删 `EnterFanoutBranchNS` (已 R32+ 删) / `crossNSPendingInterrupts` (已重构为 `Checkpointer.ListPending`) doc 引用 | — | v2 走 LangGraph 风格 map ID 命中 + graphNS 单维度协议. 业务提问走 `Ask`, 不需要单独 currentNS 维度 |

## 分隔符族的组织形态（2026-08-31 调研）

**起因**：我们的 ns 有三种角色不同的段分隔符（`/` 层级 / `#` fan-out 实例 / `@` 重访序数），
此前散在 6 个函数里各枚举一遍。加第三种（`@`）时实测「只改几处，全套测试含三个 backend 的
conformance 全绿，而 `ParentNS` 已经开始把每一份存档的父链写错」。我据此把它收成一张带
kind 的表，**但当时没做三方调研**——本节是补的账。

### 三方形态

| | 分隔符 | 组织方式 | 自洽性保障 |
|---|---|---|---|
| **LangGraph** | `NS_SEP="\|"`（层级）+ `NS_END=":"`（段内 task_id）+ **纯数字段**（同节点多子图实例，复用 `NS_SEP` 不引入新字符）| 两个裸常量，无表无枚举 | **0 条测试**（全仓无测试引用这两个常量）|
| **eino** | `:` / `;` 只出现在 `Address.String()` 一处 | **结构化 `[]AddressSegment`**，kind 与实例维都是字段不是分隔符；全仓无解析函数 | 不需要——没东西要解析回来 |
| **MAF** | 无层级 ns 串（uuid + `previous_checkpoint_id` 链）| N/A | N/A |

### 关键实证：LangGraph 现在就在漂，且有过 13 个月的静默 bug

- 同一个「ns + `NS_END` + task_id」拼接在 `pregel/_algo.py` 出现 4 次，**其中 3 处硬编码 `":"`**
  而非用常量（`:843` / `:1002` / `:1153` vs 用了常量的 `:624`）
- 第三个角色的判据 `isdigit()` 复制 3 次、跨 2 文件（`_config.py:48` / `_retry.py:559,567`）
- 单字节假设无文档无守卫（`main.py:331` 的 `namespace[len(name)+1:]`）
- **时间线**：2024-08 引入 2 个角色 → 2025-01 加第三个角色（数字段）→ **2026-02 才修**
  父 ns 计算不认识第三个角色（commit `f178eb821`，message 自陈会「produce a malformed parent
  graph identifier」）。**加维度 → 父链静默写错 → 线上 13 个月。**

### 我们的决策

| 项 | 决策 | 理由 |
|---|---|---|
| 分隔符族的组织 | **保留带 kind 的表** | 「退回裸常量」被 LangGraph 用 20 个月实证否掉——同样的形态产出了同样症状的 bug |
| `instance bool` 字段 | **删**，`kind == ""` 即层级分隔符 | 与 `kind != ""` 完全同义反复，而当时靠两条测试断言守它俩一致——需要测试保证两字段同步就是该合并的证据 |
| eino 的结构化 Address | **不抄** | 前提是 checkpoint 单键 blob（eino `Get/Set(id)`、MAF uuid 链）；我们是 `(sid, ns)` 二级键、`DeleteSubtree` 要在 backend 侧做子树匹配 ⟹ ns 必须可解析。不是他们设计更好，是问题不同 |
| LangGraph 的「不加字符、靠段内容谓词」 | **不抄** | 我们可以这么做（`@1` 本可写成独立数字段），但它换来 `isdigit()` 三处拷贝 + 那个 13 个月的 bug，不明显更好 |
| 「为将来加第四种」当理由 | **删掉这个论证** | 无证据：LangGraph 在 3 个角色上稳了 20 个月；我们现有 3 个已覆盖它的全部语义空间。表真正防的是**刚加完第三种时某个消费者没跟上** |

### 功劳归属（别记混）

加 `@` 时真正救场的是 **`NSSegmentSeparators()` 导出 + `checkpointer_conformance` 里对 backend
手抄拷贝的扫描**——治的是**跨语言 backend 漏跟**（Lua 脚本没法调 Go 函数）。那张表治的是
**包内 6 个函数漏改**。两件正交的事，LangGraph 只有后一个问题（它没有多语言 backend）。
记混了下次精简会砍错那个。

**2026-09-23 更新**：删除范围收窄成「自身 + 经 `/` 往下的后代」（`#`/`@` 是兄弟执行，不删）
之后，backend 只需要**层级分隔符这一个**——`NSSegmentSeparators()` 换成 `NSSubtreeSeparator()`，
redis Lua 从它注入，sql 后端直接调 `IsNSInSubtree`。新增一种实例段不再需要 backend 跟进，
「跨语言 backend 漏跟」那类漂移在结构上没了，那条「同一行列出 ≥2 个分隔符」的源码扫描随之
删除；回归由 conformance 的 `DeleteSubtree_Sibling_Executions_Survive` 在各后端实跑兜住。

## 开放问题

1. **NSKind 是否升 typed 常量**？
   - 当前 `compose_v2/ns.go::NSSegment.Kind` 是 string（"node" / "fanout"）
   - 候选：升 `type NSKind string` + 常量 `NSKindNode / NSKindFanout`
   - 收益：业务用 const 编辑器补全；改名时编译期挂
   - 当前没业务直接读 `NSSegment.Kind`，暂不做；将来需要再改
2. **跨子图 ns prefix 查询性能**？
   - `IsNSInSubtree` 和 `Checkpointer.ListPending(sid) → []NSCheckpoint` (v2 8 方法接口) 当前 O(N)
   - 大子树时性能瓶颈
   - Phase 4 PG/Redis 后端加索引时一并优化
