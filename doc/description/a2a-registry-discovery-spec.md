# A2A 多语言注册发现协议规范

本文定义 crew-go A2A 服务注册发现的多语言协议规范。Java、Go、Python 或其他语言 SDK
只要按本文注册服务实例，就可以被其它语言实现发现，并通过 A2A Agent Card、JSON-RPC 和
SSE streaming 完成跨语言调用。

本规范描述的是 **A2A Agent Card 服务目录协议**，不是 A2A 任务协议本身，也不是任何语言 SDK
的内部 API。

当前服务目录面向 **A2A 协议版本 `1.0`**。A2A 历史上存在 `0.3` 版本，但当前 Java SDK
注册、发现、Agent Card 解析和服务端暴露均只支持 A2A `1.0`，不兼容 A2A `0.3` 的
Agent Card 或 transport 目录格式。payload 中的 `schemaVersion: 1` 是服务目录 JSON
schema 编号；`version` 字段则表示服务或 Agent 自身版本，三者含义不同。

## 1. 范围与互通目标

A2A 服务目录只保存“如何找到一个 A2A Agent 服务”的最小信息：

- 逻辑服务名。
- 实例 ID。
- Agent Card 地址。
- 可选 baseUrl。
- transport、状态、权重、版本和非敏感路由元数据。

发现端选中实例后，必须通过 `agentCardUrl` 拉取 Agent Card，再按照 Agent Card 和 A2A
协议发起 `message/send` 或 `message/stream` 调用。跨语言互通依赖共同的服务目录 payload、
Agent Card 和 A2A transport，而不是 Java、Go、Python 之间的语言运行时互调。

典型互通场景：

- Java SDK 注册的 `research-agent` 可以被 Go SDK 发现并调用。
- Go SDK 注册的 `research-agent` 可以被 Java `DiscoveredA2aAgent` 发现并调用。
- Python SDK 只要写入同一格式的注册节点，也可以被 Java 或 Go SDK 发现。

## 2. 术语

- **Service Directory**：服务目录，保存 A2A Agent Card 指针和路由元数据。
- **Service Name**：逻辑服务名，例如 `research-agent`。调用方按该名称发现实例。
- **Instance ID**：同一服务名下的实例唯一标识，例如 `10.0.0.12:8080`。
- **Agent Card URL**：可拉取 A2A Agent Card 的绝对 URL，通常是
  `http://host:port/.well-known/agent-card.json`。
- **Transport**：远端 A2A 调用传输方式。当前 Java 默认使用 JSON-RPC。
- **Binding**：服务目录在某个注册中心上的落地协议。本文定义 ZooKeeper Binding v1。
- **Malformed Node**：无法按本规范解析或校验失败的实例节点。

## 3. 通用服务实例模型

服务实例 payload 必须是 UTF-8 JSON object。当前 schema 版本为 `1`：

```json
{
  "schemaVersion": 1,
  "serviceName": "commerce-service",
  "agentName": "research",
  "instanceId": "10.0.0.12:8080",
  "baseUrl": "http://10.0.0.12:8080",
  "agentCardUrl": "http://10.0.0.12:8080/.well-known/agent-card.json",
  "transport": "JSONRPC",
  "version": "1.0.0",
  "status": "SERVING",
  "weight": 100,
  "metadata": {
    "zone": "az1",
    "language": "java"
  }
}
```

### 3.1 字段定义

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `schemaVersion` | number | 是 | 无 | 当前固定为 `1`。发现端遇到未知版本必须跳过该节点。 |
| `serviceName` | string | 是 | 无 | 逻辑服务名。ZooKeeper Binding 中还必须满足路径字符约束。 |
| `agentName` | string | 否 | `serviceName` | 区分同一 `serviceName` 下的多个 Agent Card。未显式设置时默认等于 `serviceName`（单 Agent 服务把 `agentName` 与 `serviceName` 用同一值）。ZooKeeper Binding 中还必须满足路径字符约束。 |
| `instanceId` | string | 是 | 无 | 同一 `serviceName` + `agentName` 下唯一实例 ID。可包含 `:`、`/`、IPv6 或 URL 字符；作为路径段前必须编码。 |
| `baseUrl` | string | 否 | 无 | 服务基础地址。提供时必须是绝对 URI。 |
| `agentCardUrl` | string | 是 | 无 | Agent Card 绝对 URI。发现端必须使用该地址拉取 Agent Card。 |
| `transport` | string | 否 | `JSONRPC` | A2A transport 类型，写入官方 A2A transport 字符串。该字段是服务目录 hint，便于发现端做预筛选或路由；最终调用 transport 和 endpoint 必须以 Agent Card 中的 `preferredTransport` / `supportedInterfaces` 为准。 |
| `version` | string | 否 | 无 | 服务或 Agent 版本。 |
| `status` | string | 否 | `SERVING` | 实例服务状态。 |
| `weight` | integer | 否 | `100` | 非负整数路由权重。等于 `0` 的实例不参与默认选择。 |
| `metadata` | object | 否 | `{}` | 字符串到字符串的非敏感元数据。 |

### 3.2 枚举值

`transport` 当前取值：

- `JSONRPC`
- `GRPC`
- `HTTP+JSON`
- `CUSTOM`：crew-go 扩展值，用于目录中只能表达自定义 transport 类型而无法写入具体 vendor binding 的场景。


`status` 当前取值：

- `SERVING`：可接收新请求。
- `DRAINING`：实例正在下线或排空，不参与默认路由。
- `OUT_OF_SERVICE`：实例不可服务，不参与默认路由。

### 3.3 校验规则

注册端写入 payload 前必须满足：

- `schemaVersion` 为数字 `1`。
- `serviceName`、`instanceId`、`agentCardUrl` 为非空字符串。
- `agentName` 缺省（注册端补 = `serviceName`）或为满足路径字符约束的非空字符串。
- `agentCardUrl` 是绝对 URI。
- `baseUrl` 缺省或为绝对 URI。
- `transport` 缺省或为已知协议值。
- `status` 缺省或为已知枚举值。
- `weight` 缺省或为大于等于 `0` 的整数。
- `metadata` 缺省或为 string-to-string map；key 必须为非空字符串，value 必须为非 null 字符串。

发现端读取 payload 时必须满足：

- 未知 `schemaVersion`：跳过该节点。
- 缺失必填字段：跳过该节点。
- 相对 `agentCardUrl` 或相对 `baseUrl`：跳过该节点。
- 未知 `transport` 或未知 `status`：跳过该节点。
- `metadata` 缺失、为 `null` 或不是 object：按空 map 处理。
- `metadata` 是 object 但包含非字符串 value：跳过该节点。
- 单个 malformed node 不得导致同一 `serviceName` 下的其它健康实例发现失败。

## 4. ZooKeeper Binding v1

当前官方注册中心 binding 使用 ZooKeeper。ZooKeeper 在本规范中只作为 Agent Card 服务目录，
不保存 A2A task、session、memory、请求正文或鉴权密钥。

### 4.1 路径

实例节点路径固定为：

```text
/crew/a2a/{namespace}/{serviceName}/{agentName}/{encodedInstanceId}
```

路径约束：

- `namespace` 缺省时必须使用 `default`。所有语言 SDK 如果没有显式传入 namespace，都必须使用该默认值，避免同一服务被写入不同目录。
- `namespace` 只允许字母、数字、`.`、`_`、`-`。
- `serviceName` 只允许字母、数字、`.`、`_`、`-`。
- `agentName` 只允许字母、数字、`.`、`_`、`-`；未显式设置时默认等于 `serviceName`（单 Agent 服务路径退化为 `.../{serviceName}/{serviceName}/{encodedInstanceId}`，仍是 4 层）。
- `encodedInstanceId` 是 `instanceId` 的 UTF-8 字节经过 Base64 URL-safe no padding 编码后的结果。

路径与 payload 必须一致：

- 路径中的 `{serviceName}` 必须等于 payload 中的 `serviceName`。
- 路径中的 `{agentName}` 必须等于 payload 中的 `agentName`。
- 路径中的 `{encodedInstanceId}` 必须由 payload 中的 `instanceId` 按本文编码规则生成。
- 发现端如果发现路径和 payload 不一致，必须把该节点视为 malformed node 并跳过。

编码规则：

```text
encodedInstanceId = base64url_no_padding(utf8(instanceId))
```

示例：

```text
serviceName = commerce-service
agentName   = research
instanceId  = 10.0.0.12:8080
path        = /crew/a2a/prod/commerce-service/research/MTAuMC4wLjEyOjgwODA
```

不同语言必须使用等价的 Base64 URL-safe no padding 实现：

- Java：`Base64.getUrlEncoder().withoutPadding()`
- Go：`base64.RawURLEncoding`
- Python：`base64.urlsafe_b64encode(...).rstrip(b"=")`

### 4.2 节点类型与内容

注册节点必须使用 ephemeral node。服务进程退出、ZooKeeper session 丢失或显式反注册后，该实例节点应消失。

节点内容必须是通用服务实例模型的 UTF-8 JSON。当前必须包含 `schemaVersion: 1`。

父路径可使用 persistent node，例如：

```text
/crew
/crew/a2a
/crew/a2a/prod
/crew/a2a/prod/commerce-service
/crew/a2a/prod/commerce-service/research
```

`serviceName` 与 `agentName` 中间层都是 persistent node；只有实例叶子节点必须使用 ephemeral node。

### 4.3 重复注册

同一个服务进程重复注册相同 `namespace + serviceName + agentName + instanceId`（4 元组唯一标识
一个实例节点）时，可以先关闭旧 handle 再写入新 payload。若目标实例路径已经由其它进程或其它注册器
持有，注册端不得删除该节点，应返回注册失败。注意：同 `serviceName` + 同 `instanceId` 但不同
`agentName` 是不同节点（路径 `agentName` 段不同），可共存，不算重复。

## 5. 注册流程

注册端必须执行以下流程：

1. 构造服务实例 payload。
2. 校验字段和 URI。
3. 计算 ZooKeeper 实例路径。
4. 确保父路径存在。
5. 创建 ephemeral instance node，节点内容为 UTF-8 JSON。
6. 对外暴露 Agent Card，使 `agentCardUrl` 可被其它服务访问。
7. 服务排空时将 `status` 更新为 `DRAINING`，不再希望被路由时更新为 `OUT_OF_SERVICE` 或显式反注册。

注册端不得把以下内容写入 payload：

- API Key
- Authorization header
- Cookie
- 模型密钥
- 租户敏感信息
- 用户请求正文
- Agent task、session、memory 或运行态

认证信息必须由调用方通过本语言 SDK 的远端调用配置显式提供，例如 HTTP headers、凭证 provider
或自定义 A2A client factory。

## 6. 发现与选择流程

发现端按 `namespace + serviceName` 读取服务路径，遍历其下**所有 `agentName` 中间层**收集实例
（聚合该 serviceName 下所有 Agent Card 的实例）：

```text
/crew/a2a/{namespace}/{serviceName}                                  # service 路径
/crew/a2a/{namespace}/{serviceName}/{agentName}                      # agentName 中间层（持久）
/crew/a2a/{namespace}/{serviceName}/{agentName}/{encodedInstanceId}  # 实例叶子（ephemeral）
```

处理规则：

1. 服务路径不存在时返回空实例列表。
2. 遍历 service 路径下每个 `agentName` 中间层，再遍历各中间层下的实例叶子读取 payload。
3. 如果 ephemeral child 在列举后、读取前消失，应跳过该 child。
4. 校验 payload 的 `serviceName` 与服务路径段一致、`agentName` 与所属中间层节点名一致。
5. 校验实例叶子节点名与 payload `instanceId` 编码结果一致。
6. 如果单个节点 malformed（含 `agentName` 与中间层节点名不符），应跳过该节点。
7. 服务路径整体读取失败可向调用方返回 discovery error；单个 `agentName` 中间层读取失败应 best-effort 跳过，不影响其它中间层。
8. 返回通过校验的实例快照（该 serviceName 下所有 agentName 的实例聚合）。

如需只发现某个特定 `agentName`，在返回的快照上按 `agentName` 过滤即可（Go SDK 用 `a2a.WithDiscoveryAgentName(name)`）。

默认选择规则：

- 只选择 `status=SERVING` 的实例。
- 只选择 `weight>0` 的实例。
- 可以叠加语言 SDK 自己的 instance filter，例如按 `metadata.zone` 或 `metadata.language` 过滤。
- 选择器可以实现 first available、round-robin、random 或 weighted-random。
- 同一次 failover 尝试中，选择器不得重复选择已经尝试过的实例（去重身份 = `agentName + instanceId`，即 §4.3 4 元组的后两段；同 instanceId 不同 agentName 是不同实例，不能误判已试）。

`DRAINING` 和 `OUT_OF_SERVICE` 实例可以被 discovery 返回，便于观察和管理，但默认不得参与新请求路由。

## 7. 跨语言调用流程

发现端选中实例后，必须按下面流程进入标准 A2A 调用：

1. 使用实例中的 `agentCardUrl` 拉取 Agent Card。
2. 读取 Agent Card 中声明的 endpoint、能力和 transport 信息。
3. 根据 Agent Card 的 `preferredTransport` 和 `supportedInterfaces` 选择最终 transport 与 endpoint。
4. 同步请求使用 A2A `message/send`。
5. 流式请求使用 A2A `message/stream`，并消费 SSE streaming events。
6. 将远端 message、task、artifact update、status update、input-required 和 error 映射回本语言 SDK 的 Agent 事件模型。

目录 payload 的 `transport` 不得覆盖 Agent Card。若二者不一致，发现端应把目录字段视为过期或
路由 hint，只能在 Agent Card 确认支持相同 transport 时使用；否则必须按 Agent Card 的声明创建
client，或者在本 SDK 尚不支持该 Card transport 时返回 unsupported transport 错误。

跨语言示例：

```text
Java Agent Server
  -> 写入 /crew/a2a/prod/commerce-service/research/{encodedInstanceId}
  -> Go SDK discovery 按 serviceName 发现实例（遍历 agentName 中间层聚合）
  -> Go SDK 拉取 Java 服务的 Agent Card
  -> Go SDK 使用 A2A JSON-RPC message/send 或 message/stream 调用 Java Agent
```

```text
Go Agent Server
  -> 写入相同规范的 ZooKeeper 节点
  -> Java DiscoveredA2aAgent 按 serviceName 发现实例
  -> Java RemoteA2aAgent 拉取 Go 服务的 Agent Card
  -> Java 使用 A2A JSON-RPC message/send 或 message/stream 调用 Go Agent
```

如果远端服务也是 crew-go 实现，可以通过 A2A message/status metadata 透传
`crew.nodeName`、`crew.nodeScope` 等 UI 分组信息。非 crew-go 实现可以忽略这些扩展字段。
目录节点的 `metadata` 只用于发现和路由，不用于承载运行时流事件。

## 8. Failover 契约

默认 failover 只允许发生在：

- discovery 读取失败或返回空结果。
- selector 选中不可用实例。
- Agent Card 拉取失败。

一旦 `message/send` 已发出，发现端不应自动切换到另一个实例重试。一旦 `message/stream`
已建立，发现端不应自动切换到另一个实例续流。

原因是 A2A 调用可能已经在远端创建 task 或执行带副作用的操作。自动切换实例会带来重复执行、
重复扣费、状态分叉或用户可见的重复输出。

如果业务确实需要请求级重试，应由上层调用方基于幂等 key、task 状态查询或业务确认机制显式实现。

## 9. 安全边界

ZooKeeper 节点和其它服务目录 payload 中禁止保存：

- API Key
- Authorization header
- Cookie
- OAuth token
- 模型密钥
- 数据库连接串
- 租户敏感信息
- 用户输入、模型输出或工具调用正文
- Agent task、session、memory、checkpoint 或运行态

推荐 metadata 只保存非敏感路由信息，例如：

```json
{
  "zone": "az1",
  "language": "go",
  "team": "search-platform",
  "capability": "web-research"
}
```

如果某个 metadata 字段可能被用于鉴权、审计或租户隔离，不应放入服务目录，应通过调用端配置或专门的安全系统处理。

## 10. 兼容性规则

为了让不同语言 SDK 能长期互通，实现方必须遵守以下兼容性规则：

- 写入端必须写 `schemaVersion: 1`。
- 读取端遇到未知 `schemaVersion` 必须跳过节点。
- 读取端必须忽略自己不认识的额外字段。
- 新增字段必须保持向后兼容，不得改变现有字段语义。
- 未知枚举值必须视为 malformed node，并跳过该实例。
- 缺失可选字段时必须使用本文定义的默认值。
- 缺省 `namespace` 时必须使用 `default`。
- ZooKeeper 路径中的 `serviceName`、`agentName` 和 `encodedInstanceId` 必须与 payload 中的 `serviceName`、`agentName` 和 `instanceId` 一致。
- 单个 malformed node 不得影响同一服务下其它节点。
- 字符编码必须使用 UTF-8。
- ZooKeeper path encoding 必须使用 Base64 URL-safe no padding。

## 11. 最小实现清单

一个新的语言 SDK 若要实现 A2A 注册发现，至少需要完成：

- 能构造并校验服务实例 payload。
- 能按 ZooKeeper Binding v1 写入 ephemeral instance node。
- 能显式反注册并删除 instance node。
- 能更新 `status`，至少支持 `SERVING`、`DRAINING`、`OUT_OF_SERVICE`。
- 能在未显式传入 namespace 时使用默认值 `default`。
- 能按 `namespace + serviceName` 发现实例。
- 能跳过 malformed node 和读取中消失的 ephemeral child。
- 能校验 ZooKeeper 路径和 payload 的 `serviceName` / `agentName` / `instanceId` 一致性。
- 能过滤 `status=SERVING` 且 `weight>0` 的实例。
- 能从选中实例拉取 `agentCardUrl`。
- 能按 Agent Card 创建 A2A client。
- 能发起 `message/send`，并在支持流式时发起 `message/stream`。
- 能保证目录 payload 不包含密钥、请求正文或运行态。

完成以上清单后，该语言 SDK 注册的 Agent 就可以被其它遵守本规范的 SDK 发现；该语言 SDK
也可以发现其它语言 SDK 注册的 Agent，并通过标准 A2A 协议调用。
