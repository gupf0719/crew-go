# MCP 接入（`mcp` 包）

> 代码：`mcp/client.go`（Client 接口、ToolSpec / ToolAnnotations、CanonicalName）、`mcp/hub.go`（Hub 与 Hub 级选项）、
> `mcp/server.go`（每个 server 的连接、掉线判定、重连；server 级选项、ServerStatus、ErrCodeServerUnavailable）、
> `mcp/tool_adapter.go`（RemoteTool）、`mcp/jsonrpc.go`（RPCError、ErrNotSent）、`mcp/stdio.go` / `mcp/http.go`（两种传输）。
> 用例：`mcp/degrade_test.go`。2026-10-22 起（提交 `f740711`、`b95a0cb`、`afd1590`、`f83bf31`、`5945ab0`）。

## 1. 组成

| 类型 | 作用 |
|---|---|
| `Client` | 一个 MCP server 的连接：`Initialize` → `ListTools` / `CallTool`（可并发）→ `Close`。`Close` 之后不能再用。内置 `StdioClient`（`NewStdioClient`，子进程，按行 JSON-RPC）与 `StreamableHTTPClient`（`NewStreamableHTTPClient`，每次调用一个 POST） |
| `Hub` | 管多个 server：`Start` 连接并拉清单、把工具注册进一个 `*tool.Toolkit`；`Toolkit()` 交给 agent；`Status()` 查在线状态；`Close()` |
| `RemoteTool` | 一个远端工具适配成 `tool.Tool`；`Call` 经所属 server 的当前连接调 `CallTool` |

## 2. 三条不变量

1. **工具集只在 `Start` 时定一次，之后不变。** 名字、描述、参数 schema、顺序（按 server 注册顺序，再按 server 报的
   顺序）都不变——server 掉线、重连、重连后报的清单变了也一样。发给模型的工具段在消息之前，它一变，按前缀命中的
   prompt cache 与要求前缀逐字节不变的 thinking 回放（Anthropic）都从头失效。server 的变化只体现在调用结果上。
2. **调用失败交给模型，不让 Run 失败。** `RemoteTool.Call` 返回 error 只有一种情况：**调用方的 ctx 已结束**，而这次
   调用正在排队或 `CallTool` 出了错——那是控制流，原样往上交（错误链上带着 ctx 的错）。其余失败一律是 `tool.Failure`：
   BasicAgent 交给模型的是 `[<code>] <说明>` 文本，Anthropic 适配器另外置上 tool_result 的 `is_error`（openai 系的
   适配器没有这个字段，模型只看文本）。BasicAgent 把工具返回的 error 当成整轮失败——原来传输错误直接返回 error，
   一个 server 出错就拖垮整个 Run。离线、已关闭、清单里已没有的工具直接返回失败结果，不看 ctx。
3. **掉线后快速失败、后台重连（`WithClientFactory` 注册的 server）。** 一次调用撞上连接错误后 server 标成掉线，
   之后它的工具直接返回 `server_unavailable`、不再发出（不用每次干等超时）；后台按退避新建连接重连，连上后恢复。
   `WithClient` 给的固定实例不标掉线（它没法新建连接），出错后下一次调用照常发给它。

## 3. 用法

```go
hub := mcp.NewHub(
    mcp.WithPartialStart(),                         // 个别 server 连不上不让 Start 失败
    mcp.WithToolNamer(func(server, tool string) string { return server + "__" + tool }),
    mcp.WithConnectTimeout(10*time.Second),
    mcp.WithStatusHook(func(st mcp.ServerStatus) { /* 记日志；在线时把 st.Tools 存成下次的快照 */ }),
    mcp.WithClientFactory("devbox", func(context.Context) (mcp.Client, error) {
        return mcp.NewStreamableHTTPClient("devbox", url,
            mcp.WithHTTPClient(&http.Client{}),     // 时限交给 WithCallTimeout（默认 http.Client 自带 20s）
            mcp.WithHTTPHeader("Authorization", "Bearer "+token))
    }, mcp.WithCallTimeout(120*time.Second), mcp.WithToolSnapshot(lastSpecs)),
    mcp.WithClientFactory("camera", newCameraClient, mcp.WithSerialCalls(), mcp.WithCallTimeout(10*time.Second)),
)
if err := hub.Start(ctx); err != nil { ... }
defer hub.Close()
agent := agent_v2.NewBasicAgent(agent_v2.WithToolkit(hub.Toolkit()), ...)
```

`WithClient(c, opts...)` 也能用：这个实例在 Start 失败的回滚与 `Close` 时被关；开了 `WithPartialStart` 而它 Start 时连不上，
后台用同一个实例重试 `Initialize` / `ListTools`；中途出错不重建连接、不标掉线（`Client` 关了不能再用）。要掉线后自动重连
（stdio 子进程退了只能新起一个）用 `WithClientFactory`：每次重连先关旧实例、再用工厂建新的；工厂建出的 `Client` 的
`Name()` 必须等于登记的名字。

## 4. `Start` 的语义

| | 默认（全有或全无） | `WithPartialStart` |
|---|---|---|
| 某个 server 连不上 | 整体失败，关掉 `WithClient` 的实例与工厂已建的连接 | 不失败。给了 `WithToolSnapshot` 就按快照注册它的工具（工具集跟它在线时一样），没给就不注册（本会话没有它的工具，连上后也不补）；它离线，后台重连 |
| 连接尝试的时限（建实例 + `Initialize` + `ListTools`） | 不设（只受调用方 ctx 约束），除非显式 `WithConnectTimeout`——首次运行要下载东西的 stdio server 起得慢 | 默认 30s（`WithConnectTimeout` 可改）；stdio 连接失败后关子进程最多再等 3s |
| 调用方 ctx 取消 / 到期 | 失败 | 同样失败（视为放弃启动） |
| server 名重复、两个工具生成同一个名字（含 `WithToolNamer` 的结果为空） | 失败（同名的两个工具在 `Toolkit` 里会互相顶掉，原来静默顶掉） | 同样失败 |
| server 报了重名或没名字的工具 | 失败 | 算这个 server 连不上 |
| 要用的快照（server 连不上时）里有重名或没名字的工具 | — | 失败 |
| 失败后重试 | 可以，但只有 `WithClientFactory` 的 server 会新建连接；`WithClient` 的实例已被回滚关掉（stdio 关了就不能再用），要重试就换新实例重建 Hub | 同左 |

成功后重复调用幂等；并发 `Start` 串行；`Close` 打断进行中的 `Start`（连接尝试被取消，`Start` 回滚并返回错误）；`Close`
之后调用返回错误。

## 5. 调用结果

「掉线」只对 `WithClientFactory` 注册的 server 发生（见 §2 第 3 条）。

| 情形 | 结果（`tool.Result.Error.Code`） | 模型看到的说明 | server 状态 |
|---|---|---|---|
| 调用方 ctx 已结束（排队中，或 `CallTool` 出错时） | 返回 error（链上带着 ctx 的错） | — | 不变；`WithSerialCalls` 的、请求已发出时掉线、断开重连（同下面的超时） |
| Hub 已 `Close` | `server_unavailable` | 该能力暂时不可用（X 已关闭），这次调用没有发出 | — |
| server 离线 | `server_unavailable` | 该能力暂时不可用（X offline），这次调用没有发出 | 不变 |
| 重连后 server 上已没有这个工具 | `tool_not_found` | X 上已经没有工具 "t"……这次调用没有发出 | 不变 |
| 串行排队超过 `WithCallTimeout`（排队与执行各有一份时限） | `timeout` | 等 X 上一个调用结束超时……这次调用没有发出 | 不变 |
| 超过 `WithCallTimeout`、请求已发出 | `timeout` | 工具调用超时（超过 d 未返回），已放弃等待；X 那边可能仍在执行 | 不变；`WithSerialCalls` 的掉线、断开重连，说明改为「已断开 X 重连（它那边的执行可能被打断）」 |
| 请求没发出（`Client` 包上 `ErrNotSent`：连不上、没写出去、连接早已关闭、握手没过；含被时限打断的） | `server_unavailable` | 该能力暂时不可用（X 连不上），这次调用没有发出 | 掉线 |
| server 答了 JSON-RPC 错误（`*RPCError`，在 2xx 或非 2xx 响应体里） | `internal_error` | `mcp rpc error <code>: <message>` | 不变 |
| HTTP 400 / 413 / 422 / 429（不是 JSON-RPC 错误体） | `internal_error` | X 拒绝了这次调用（HTTP n），没有执行 | 不变 |
| HTTP 409 | `internal_error` | X 拒绝了这次调用（HTTP 409）（不断言没执行） | 不变 |
| HTTP 408（代理的空闲超时也回它，请求可能已转给 server） | `timeout` | X 那边超时（HTTP 408），不确定是否已执行 | 不变 |
| HTTP 401 / 403 / 404（哪怕响应体是 JSON-RPC 错误） | `server_unavailable` | 该能力暂时不可用（X 拒绝了请求：HTTP n），这次调用没有执行 | 掉线（重连会新建会话） |
| server 答了但结果解析不了（不是 JSON、坏的 base64、超过 10MB） | `internal_error` | X 返回的结果解析不了（原因），这次调用可能已执行 | 不变 |
| 其余连接错误（写出之后断开、HTTP 5xx、stdio 子进程中途退出……） | `server_unavailable` | 该能力暂时不可用（X 连接出错），这次调用没拿到结果，不确定是否已执行 | 掉线 |
| server 返回 `isError` | `internal_error` | 第一个非空文本块（没有则 "mcp tool call failed"） | 不变 |
| `CallTool` 返回 `(nil, nil)` | `internal_error` | X 返回了空结果 | 不变 |
| 成功 | — | 有 `structuredContent` 时 `Output` 是解出来的值（BasicAgent 用 `tool.OutputText` 编成 JSON 交给模型），否则各内容块按原顺序整理成文本、块间空一行：text 原样；嵌入资源（resource）取出正文、前面标 `[资源 URI]`；资源链接写成 `[资源链接 名字 URI]`；图片、音频、二进制资源只写一行说明（类型、MIME、字节数、URI），字节不交给模型 | — |

「没有发出」只认 `ErrNotSent`，不从错误形状推断：推断错了，模型会把一个已执行的写操作当成没执行、再做一遍。
`StreamableHTTPClient` 用 `httptrace` 判：trace 生效了（`GetConn` 到过）而请求头、请求体都没写出才算没发出（请求 ctx
结束时 `net/http` 交出的是 ctx 的错，错误链上没有 dial 错误；HTTP/2 在 ctx 结束时 `Do` 立刻返回、`WroteRequest` 可能晚到，
只看它会把一个已完整发出、已执行的请求说成没发出——`TestStreamableHTTP_H2_SlowBodyWrite_NeverClaimsNotSent`）；拿不准的
一律按「不确定」。`StdioClient` 在写管道失败、连接已关、握手没过时包上它。

交给模型的 JSON-RPC 错误信息与 HTTP 错误里带的响应体都会先抹掉自己发出的秘密（请求头的值、endpoint 里的 userinfo、
query 值与长路径段），响应体再截到 512 字节（不切断多字节字符）。原始错误（地址、端口）不进模型看到的文本；把 server 标掉线的那次记一条 warn 日志并写进 `ServerStatus.Err`（非串行的
超时、`WithClient` 的连接错误不标掉线，也就不记）。有的 server 在错误里回显 `Authorization`，有的服务把 token 放在 URL 里、404 时回显路径。

自定义 `Client` 的契约（`Client.CallTool` godoc）：server 答的 JSON-RPC 错误交出 `*RPCError`，确定没发出时包上
`ErrNotSent`，不要返回控制流信号（中断等）。`CallTool` panic 按 agent_v2 对工具 panic 的统一处理（整轮失败），Hub 不兜。

⚠️ `StreamableHTTPClient` 默认的 `*http.Client` 自带 20s 超时，比它长的 `WithCallTimeout` 要配 `WithHTTPClient` 换掉，
否则 20s 时 HTTP 层先报错，按连接出错处理（标掉线）。

## 6. 掉线与重连

- 掉线判定只认**当前这一代连接**：每换一个连接代数加一。并发的几个调用撞上同一次掉线时只有第一个生效；重连上之后，
  卡在旧连接上的调用迟到的连接错误不会把新连接标掉（`TestHub_LateErrorFromOldConnection_DoesNotKnockNewOne`）。
- 重连循环：先关掉线的旧连接（stdio 要等子进程退出，至多 3s，不放在调用路径上），然后按 `WithReconnectBackoff` 退避
  （默认 1s 起、每次翻倍、上限 60s）反复连，连上为止；每次尝试受 `WithConnectTimeout` 约束（不给时 30s）。
- `WithSerialCalls`：排队与执行各自最多 `WithCallTimeout`（共用一份的话排在后面的调用执行时只剩一点时间）；排队的调用
  拿到信号量后按当时的状态再准入（不发给已判掉线的旧连接）；请求发出之后没等到结果（超时、调用方取消）时 server 那边
  可能还在执行，`WithClientFactory` 的串行 server 这时掉线、关旧连接（stdio 旧子进程随之退出）再重连，排队的调用不会跟它
  重叠。管不到的：HTTP server 端还在执行的调用（要它自己串行）、stdio 命令是包装器（npx 之类）时留下的真正 server 进程、
  `WithClient` 的固定实例（不重连）。
- 重连后 server 报的清单跟本会话注册的不一样时：去掉的工具调用返回 `tool_not_found`，新加的这次不注册，改了定义的
  仍按启动时的定义发给模型；日志里列出差异。下次启动按新清单注册。
- 超时（非串行）不算掉线（server 可能只是忙）。
- `Close`：先打断进行中的 `Start` 与重连尝试，等重连循环退出，再并发关闭所有连接；之后调用返回「已关闭」。并发的
  `Close` 等第一个做完、返回同一个结果。最坏要等两轮 stdio 关闭（重连循环里关旧连接、再关当前连接，各至多 3s）。

## 7. 状态与快照

`Hub.Status()` 返回各 server 的 `ServerStatus`（注册顺序；`Start` 成功之前为 nil；不拿 Hub 的锁，`Start` 进行中也不挡）：

| 字段 | 含义 |
|---|---|
| `Name` / `State`（`ServerOnline` / `ServerOffline`）/ `Since` | 名字、在线状态、进入这个状态的时刻 |
| `Err` | 离线的原因；在线时为 nil；`Close` 之后是 Hub 已关闭 |
| `Tools` | 最近一次连上时 server 报的清单（含 `Annotations`）；从没连上过为 nil |
| `Registered` | 本会话为它注册的工具（server 原始名，注册顺序），整个会话不变 |
| `FromSnapshot` | 本会话的工具来自快照（Start 时它连不上） |

`WithStatusHook(fn)`：`Start` 成功后每个 server 回调一次初始状态，之后每次上线 / 掉线回调一次；**按状态变化的先后
顺序**、在 Hub 自己的一个 goroutine 里逐个调用（入队在 server 的锁里做，回调不持任何锁）。回调里调 `Status`、调工具、
调 `Close` 都可以；panic 只记日志。`Close` 之后不再开始新的回调（至多还有一个恰好在那一刻开始的），也不等正在执行的那个
返回；`Close` 时还没送出的状态
变化不再回调——要持久化最后的清单，`Close` 之前读一次 `Status`。

要让下次启动时连不上的 server 也有工具，就把在线状态的 `Tools` 持久化，下次作为 `WithToolSnapshot` 传回（deskbot 的
`core/internal/mcpglue` 存成 JSON：JSON Schema 经 JSON 往返，发给模型的工具定义逐字节不变）。`WithToolSnapshot` 与
`Status` 都深拷贝 JSON 形状的值（`map[string]any`、`[]any`、`[]string`；自定义 Client 放进 schema 的其他 Go 类型是共享的），
调用方改自己手里的那份不影响已注册的定义。

`ToolSpec.Annotations` 是 tools/list 的 `annotations`（`readOnlyHint` 等，没给为 nil），不发给模型，给审批之类的策略用；
它是 server 的自述，只该用来放宽「不知道」的情况，不能推翻显式的确认规则。

## 8. 其他选项

| 选项 | 作用 |
|---|---|
| `WithCallTimeout(d)`（server 级） | 每次调用的时限，见 §5 |
| `WithSerialCalls()`（server 级） | 这个 server 的调用串行（硬件互斥），见 §6：BasicAgent 一轮里的多个工具调用是并发执行的。stdio server 的命令别用包装器（npx / uvx），断开重连时杀不到真正的 server 进程 |
| `WithToolNamer(fn)` | 框架内的工具名，默认 `CanonicalName`（`mcp_<server>_<tool>`）；结果要符合 provider 的要求（Anthropic / OpenAI：`^[a-zA-Z0-9_-]{1,64}$`） |
| `WithHTTPAllowTools` / `WithStdioAllowTools`（Client 级） | 按原始名过滤 server 报的清单；快照存的是过滤后的 |

## 9. 已知缺口

见 [`doc/code/todo.md`](../code/todo.md)：

- `StreamableHTTPClient` 只实现了 streamable HTTP 的子集：只解 JSON 响应、`Accept` 只写 `application/json`、不处理
  `Mcp-Session-Id`、不发 `notifications/initialized`。实测（2026-10-22）官方 MCP Go SDK v1.8.0 的 server 四种配置全在
  `initialize` 处 400；加 `WithHTTPHeader("Accept", "application/json, text/event-stream")` 之后只有
  `StreamableHTTPOptions{Stateless: true, JSONResponse: true}` 能用（另三种败在 SSE 响应或缺 initialized 通知）。
- 图片、音频不交给模型（只有一行说明）：要原样交，得让 tool_result 带图片块（Anthropic 支持），牵涉 schema 与各家适配器，没做。
- stdio 子进程继承主进程的全部环境变量（`WithStdioEnv` 只能追加）。
