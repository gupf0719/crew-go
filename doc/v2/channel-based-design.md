# Channel-based 设计文档

> 阅读前提：先读 `../description/three-way-comparison/{state-model,error-handling}.md`

> ⚠️ **本文是 channel-based PoC 期设计稿（2026-05），保留作「设计思路 + 三大难点应对 + 决策溯源」**。文中 API 名是 PoC 草案，**多处与最终实现不符**——`DefineChannel[T]` → 实际 `LastValue`/`Append`/`Sum`/`OnceFlag` + `RegisterChannel`；`WithChannelImport`/`WithChannelExport` → `WithImport`/`WithExport`；`RegisterChannelDynamic` → `RegisterChannel`。**最终 API 以 [channel-engine.md](channel-engine.md)（引擎设计）+ [usage-guide.md](usage-guide.md)（业务用法）为准**；本文仅供理解"为什么 channel-based + 三大难点怎么解"。PoC 验证结论见 [poc-report.md](poc-report.md)，演化史 + v1↔v2 收益见 [why-channel-based.md](why-channel-based.md)。

## 0. 背景

`plan_chat_web` demo 命中"已完成 graph 被 ctx 拦下"bug，根因追查发现：我们 frontier-based 数据流模型缺少 LangGraph/eino 的 channel-based 模型那种"全图 channel merge → 自然得到 isEnd"的统一同步点。三方对照分析后明确：

- **抛开成本，channel-based 是更优设计**（state-model.md 第五章决策）
- **唯一阻挡因素**：Go 泛型实现 channel API 的设计可行性——eino 的 `field_mapping` 是 Go 类型擦除妥协的反面教材，难用
- **核心命题**：Go 泛型能否做出**比 eino 干净**的 channel API（PoC 已验证通过，见 [poc-report.md](poc-report.md) §1）

## 1. 三大难点的设计应对

> 下方代码块的 API 名是 PoC 草案（见文档头警示），设计**思路**仍是现役实现的依据。

### 1.1 难点 1：异构 fan-in 的类型保护

**难点描述**：节点要读 N 个不同类型的 channel——`Channel[int]` / `Channel[string]` / `Channel[[]Message]`。Go 泛型限制：`Channel[T]` 是不同类型，不能放进 `[]Channel[T]` 或 `map[string]Channel[T]`，必须 type erasure。

**eino 妥协方案**：`field_mapping{from, to}` 字符串字段映射，编译期不类型安全。

**我们的方案**：**接口抽象 + 泛型读取方法**

```go
// 内部类型擦除接口（隐藏在 ChannelManager 内）
type anyChannel interface {
    name() string
    reducer() reducer
    zero() any
    typeRep() reflect.Type
}

// 公开的泛型类型
type Channel[T any] struct {
    n    string
    fn   func(prev, next T) T
    zero T
}

// Channels 是节点函数收到的 read API
type Channels struct {
    cm *ChannelManager  // 不暴露
}

// **泛型读取方法**：调用点实例化 T，编译期类型保护
// 注：Go 泛型方法限制——方法不能引入新类型参数，所以用包级泛型函数
func Get[T any](chs Channels, ch *Channel[T]) (T, bool) {
    raw, ok := chs.cm.read(ch.n)
    if !ok {
        return ch.zero, false
    }
    // 类型 cast：raw 实际类型与 T 不一致 → panic（开发期立即发现）
    return raw.(T), true
}

func Has[T any](chs Channels, ch *Channel[T]) bool {
    _, ok := chs.cm.read(ch.n)
    return ok
}
```

**关键决策**：
- 节点拿 `*Channel[T]` 引用（在 Builder 阶段定义并复用）→ 编译期保证类型一致
- `Get[T]` 包级泛型函数（不是方法），调用点 `compose.Get(chs, tokenUsageCh)` 编译期类型推断
- 类型 cast 失败 panic（不返 error）——这种错误在测试期就该暴露，不应运行时静默失败

**与 eino 对比**：eino 用 `compileOption.fields` 字符串映射 + `inputZeroValue` 反射构造 zero，用户写 builder 时要敲 channel 名字符串；我们的 `*Channel[T]` 引用用变量名替代字符串，IDE 重命名安全。

### 1.2 难点 2：动态创建 channel

**难点描述**：业务运行时决定要哪些 channel——例如根据用户 query 类型动态注册不同的 metric channel。编译期类型注册不够。

**LangGraph 方案**：channels 是 StateGraph 实例属性，运行时可 add（Python 灵活）。
**eino 方案**：channel 在 GraphBuilder 阶段静态声明，无运行时 register。

**我们的方案**：**Hybrid—Builder 静态优先 + 运行时动态注册兜底**

```go
// 90% 场景：Builder 阶段静态声明（首选，类型安全）
var (
    TokenUsageCh = compose.Sum("token_usage")              // 返 *Channel[int]（Sum 非泛型）
    MessagesCh   = compose.Append[*schema.Message]("messages")  // 返 *Channel[[]*schema.Message]
)

builder.WithChannel(TokenUsageCh).WithChannel(MessagesCh)

// 10% 场景：运行时注册（少用，类型用 reflect.Type 标记）
//   内部存到 map[string]anyChannel，类型是 reflect.TypeOf(typeOf)
//   节点用 GetByName(name) 取出，需要显式 type assert
```

**关键决策**：
- **不强求所有场景都 Builder 静态声明**——保留 escape hatch
- 静态声明 90% 场景享受编译期类型保护
- 动态声明 10% 场景退化到 reflect 但保留运行时类型检查（类型 mismatch panic）
- API 命名差异化让用户看到"动态版本是 escape hatch"

### 1.3 难点 3：嵌套子图 channel 作用域

**难点描述**：父图 / 子图各自的 channel 命名空间——子图能不能读写父图 channel？怎么避免命名冲突？

**LangGraph 方案**：subgraph 默认 channel space 隔离，通过 input/output mapping 显式跨边界。
**eino 方案**：channel 隔离 + 通过 `compileOption.fields` 声明跨边界字段。

**我们的方案**：**默认完全隔离 + 显式声明 import/export**

```go
// 子图作头等节点
builder.AddGraphNode("subgraph_x", subGraph,
    compose.WithSubgraphInput(adapter),
    compose.WithSubgraphOutput(adapter),
    // channel 跨边界声明（PoC 草案名 WithChannelImport/Export → 实际 WithImport/WithExport）
    compose.WithImport(TokenUsageCh, RouterDecisionCh),  // 子图内能读父图这两个 channel
    compose.WithExport(SubgraphResultCh),                 // 子图内写这个 channel 会冒泡到父图
)
```

**关键决策**：
- **默认完全隔离**：不 import / export 的 channel 子图看不见、写不到——避免隐式耦合
- **显式声明 import/export**：跨边界 channel 必须在 AddGraphNode 选项里列出，所见即所得
- **类型保护跨边界**：import/export 用 `*Channel[T]` 引用（不是字符串），父子图必须共享同一 channel 定义变量
- **不抄 LangGraph 自动跨边界**：Python 因 TypedDict 灵活可以做隐式跨边界，Go 强类型场景显式更安全

## 2. 决策记录

| 日期 | 决策 | 理由 |
|---|---|---|
| 2026-05-07 | 启动 channel-based PoC（本文档）| `plan_chat_web` bug 溯因发现 frontier 模型缺统一同步点；三方对照确认 channel-based 抛开成本是更优；需 PoC 验证 Go 泛型实现可行性 |
| 2026-05-07 | A 方案临时止血合入 main（commit `0bfd568`）| PoC 周期 6-9 天，期间 main 仍可用；A 方案 1 行改 + 测试 + sanity-revert 验证通过 |
| 2026-05 | **PoC 通过（5/5 达标）**，channel-based 落地为 compose_v2 | Go 泛型 `Channel[T]` + 包级 `Get[T]` 比 eino `field_mapping` 干净；详见 [poc-report.md](poc-report.md) §1 |

## 3. 参考

- `doc/description/three-way-comparison/state-model.md` — 三方状态模型对照
- `doc/description/three-way-comparison/error-handling.md` — 三方错误处理对照
- LangGraph channels 实现：`/Library/Python/3.9/site-packages/langgraph/channels/`
- eino channel 实现：`/Users/gupengfei/program/go/src/eino/compose/graph_run.go`（calculateNextTasks）
- main bug 修复 commit：`0bfd568 fix(compose): graphLoop done 检查放在 ctx 检查之前`
