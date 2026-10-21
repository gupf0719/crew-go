# Code Review #5（2026-04-15）

> ⚠️ **历史 Code Review 记录**：v1 时代审查结果。Stage 7 后 v1 (`agent/` + `compose/`) 已删，文中 file:line 引用是 v1 路径。审查发现的设计问题 / bug 类别在 v2 中已等价覆盖。


> 范围：ISSUE-25 MCP 集成新增代码（`mcp/` 包 6 文件）
> 重点：协议合规性、并发安全、错误处理、资源生命周期

---

## MCP-01 · rpcResponse.ID 仅支持 int64，不兼容 string id（高）

**文件**：`mcp/jsonrpc.go:21-26`

```go
type rpcResponse struct {
    ID int64 `json:"id"`
    ...
}
```

JSON-RPC 2.0 规范：`id` MUST contain a `String, Number, or NULL`。MCP 官方示例 server（如 TypeScript SDK 默认）常用自增 number，但 Python SDK、部分自研 server 会用 `"uuid-string"` 作 id。

**影响**：对端若返回 string id，`json.Unmarshal(line, &env)` 在 stdio 的 `readLoop` 被吞 err（`continue`），响应永久丢失 → 所有 rpc 调用 ctx 超时返回 `timeout`。HTTP 路径直接 `decode envelope` 失败 → `mcp initialize` 报错。

**修复**：
```go
type rpcResponse struct {
    ID json.RawMessage `json:"id"` // 协议允许 string | number | null
    ...
}
```
`rpcRequest.ID` 可保持 int64（我方只发 number），但 `rpcResponse.ID` 必须用 RawMessage；stdio readLoop 按 raw bytes 与 pending map 的 key（以 `strconv.AppendInt` 生成的 string）比较。pending map 同步改为 `map[string]chan *rpcResponse`。

---

## MCP-02 · Initialize / ListTools 的 double-checked locking 有 race（高）

**文件**：`mcp/http.go:86-103`、`mcp/stdio.go:103-128`、两处 `ListTools` 缓存读取

```go
c.mu.Lock()
if c.initialized { c.mu.Unlock(); return nil }
c.mu.Unlock()
// ← 两个 goroutine 可同时在此
_, err := c.rpc(ctx, "initialize", ...)
c.mu.Lock()
c.initialized = true
c.mu.Unlock()
```

两个并发 Initialize 调用都会通过第一次检查、都执行 rpc 握手 → 重复 initialize 通知到 server（部分 server 拒绝二次 initialize，返回 error）。ListTools 的 cachedTools 检查同理，会并发触发两次 `tools/list`。

**影响**：Hub.Start 自身顺序调用 Initialize/ListTools 不会触发；但用户直接并发调用 Client API（或多个 agent 共享同一 Client 并发 CallTool 时首次自动 Initialize）就会出问题。

**修复**：改用 `sync.Once` + 错误记忆；或加 `initMu sync.Mutex` 在 rpc 外圈独占。推荐：
```go
var initOnce sync.Once
var initErr error
func (c *StreamableHTTPClient) Initialize(ctx context.Context) error {
    c.initOnce.Do(func() {
        _, c.initErr = c.rpc(ctx, "initialize", ...)
    })
    return c.initErr
}
```
注意 `sync.Once` 对 ctx 的不友好（首次 ctx 超时后 initErr 锁死）；若介意，用 mutex + flag 实现单飞。ListTools 同理。

---

## MCP-03 · Block 结构体无 JSON tag，序列化字段名不合协议（高）

**文件**：`mcp/client.go:53-58`

```go
type Block struct {
    Type string
    Text string
    Data []byte
    Mime string
}
```

`RemoteTool.Call` 在多 block 场景下 `tool.Success(res.Content)` 把 `[]Block` 直接作为输出。下游 reactLoop 用 `json.Marshal` 送给 LLM 的 ToolResult，输出变成：
```json
[{"Type":"text","Text":"hello"}, {"Type":"image","Data":"...","Mime":"image/png"}]
```

LLM 看到大写字段名，且 `Mime` 不是 MCP 的 `mimeType`。

**修复**：加 JSON tag 对齐 MCP 协议命名：
```go
type Block struct {
    Type string `json:"type"`
    Text string `json:"text,omitempty"`
    Data []byte `json:"data,omitempty"`
    Mime string `json:"mimeType,omitempty"`
}
```

---

## MCP-04 · CallTool 传入 nil args 时 JSON 编码为 null（中）

**文件**：`mcp/http.go:132`、`mcp/stdio.go:160`

```go
params := map[string]any{
    "name":      name,
    "arguments": args, // args 为 nil → JSON "arguments": null
}
```

MCP 协议 `tools/call.arguments` 要求 object（即使为空应为 `{}`）。部分 server（Python SDK 默认）对 `null` 抛 validation error。agentdock-spec 的实现用 `cloneMap(arguments)` 天然兜底为 `{}`。

**修复**：在 Client 层统一兜底：
```go
if args == nil {
    args = map[string]any{}
}
```
两个 transport 各加一行或提取到 helper。

---

## MCP-05 · Hub.Close / Start 失败回滚丢失错误（中）

**文件**：`mcp/hub.go:77-82`（Start 回滚）、`hub.go:117-125`（Close）

Start 失败时只返回 `results` 里**第一个** err（按 client 声明顺序，不是时序顺序）：
```go
for _, r := range results {
    if r.err != nil { ...; return r.err }
}
```
若多个 client 都失败，后续错误被丢弃，回滚 Close 的错误也被 `_ = c.Close()` 吞了。Close 同理返回首个 err。

**影响**：调试困难 —— 用户看到 "client-A: initialize: boom"，但真正难产的可能是 client-B/C。

**修复**：用 `errors.Join`（Go 1.20+）聚合：
```go
var errs []error
for _, r := range results { if r.err != nil { errs = append(errs, r.err) } }
if len(errs) > 0 {
    for _, c := range h.clients { _ = c.Close() }
    return errors.Join(errs...)
}
```
Close 同样改。

---

## MCP-06 · Hub.Start 失败后不可安全重试（中）

**文件**：`mcp/hub.go:47-97`、`mcp/stdio.go:104`

`StdioClient.Initialize` 用 `startOnce sync.Once` 启动子进程；Start 失败回滚 Close 后，再调 `h.Start()`：
- `startOnce.Do` 不再执行 → `startErr` 为 nil，进入 rpc 路径
- 但 `c.stdin` 已在 Close 中置 nil → rpc 返回 `"client closed"`
- Start 再次失败，此 client 永久死锁

**影响**：典型场景 —— Hub 启动时某 MCP server 还没就绪，用户想重试 Start，实际只能重建整个 Hub + 所有 Client。

**修复选项**：
- A：`startOnce` 改为 `startMu sync.Mutex + started bool`，Close 时重置 started = false，允许重启
- B：文档化 "Client 一次性，失败需重建"，并让 Hub 的 `WithClient` 接受构造函数 `func() Client` 以便重试时重建

当前优先文档化（B），代码改动小；彻底修需 A。

---

## MCP-07 · stdio readLoop 忽略所有通知与反向请求（中）

**文件**：`mcp/stdio.go:246-273`

```go
if env.ID == 0 {
    // 通知或未匹配响应，跳过。
    continue
}
```

MCP 协议中 server→client 方向的通知包括：
- `notifications/tools/list_changed` — 工具列表变化，应使 cachedTools 失效
- `notifications/progress` — 长耗时工具的进度回推
- `sampling/createMessage` 反向请求 — server 要求 client 代为调 LLM

当前全部丢弃。`list_changed` 被忽略会导致 cachedTools 陈旧到进程生命周期结束；progress 丢失对流式 tool 影响较大。

**修复**：V1 可先只处理 `list_changed` → 清空 cachedTools，注册一个回调让 Hub 重新 ListTools。progress 和 sampling 留到 V2 随 StreamableTool 一起做。记录为已知限制也可接受。

---

## MCP-08 · CanonicalName 无长度上限（低）

**文件**：`mcp/client.go:71-73`

OpenAI/Qwen tool_call 的 `function.name` 限制 64 字符，`[a-zA-Z0-9_-]`。`mcp_<client>_<source>` 拼接无截断；若 server 给出 50+ 字符长名、client 再用描述性长名，拼起来超限 → LLM 端报 invalid tool name。

**修复**：超 64 字符时对后半做 hash 截断：
```go
name := "mcp_" + sanitize(clientName) + "_" + sanitize(sourceName)
if len(name) > 64 {
    sum := sha1.Sum([]byte(name))
    name = name[:56] + hex.EncodeToString(sum[:4]) // 56 + 8 = 64
}
```

---

## MCP-09 · sanitize 对 CJK 字符全部替换为 `_`，多工具易碰撞（低）

**文件**：`mcp/client.go:79-93`

非 ASCII 字符统一替换为 `_`，则 `搜索` → `___`、`翻译` → `___`，两个 CJK-only 工具 canonical name 完全相同，Toolkit.Register 后注册覆盖前注册。

**修复**：非 ASCII 字符用 `%x` 保留原信息：
```go
default:
    buf = append(buf, []byte(fmt.Sprintf("_%x", ch))...)
```

---

## MCP-10 · Close 后不能再用，缺文档说明（低）

**文件**：`mcp/stdio.go:171`（注释）、`mcp/http.go:146`（Close）

stdio 的 Close 是终态；http 的 Close 实际允许 Initialize 再起（因为它仅清 flag），但调用方不知道两者行为不一致。

**修复**：Client 接口 godoc 明确声明 "Close 后 Client 不可再用；如需重连请新建实例"，统一语义。http.Close 保持清 flag 也可，但加注释说明这是为了测试便利而非稳定 API。

---

## MCP-11 · RemoteTool 多 block fallback 返回 `[]Block`（低）

**文件**：`mcp/tool_adapter.go:59-63`

```go
if len(res.Content) == 1 && res.Content[0].Type == "text" {
    return tool.Success(res.Content[0].Text), nil
}
return tool.Success(res.Content), nil
```

多 block 或非纯 text 场景直接把 `[]Block` 作为 Output，依赖 MCP-03 加 JSON tag 后字段名才正确；但对 LLM 友好度仍差（原始 JSON 数组）。

**修复**：配合 MCP-03 修完后，再考虑对多 text block 做 `strings.Join(texts, "\n\n")`，非 text block 保留结构化：
```go
var texts []string
var others []Block
for _, b := range res.Content {
    if b.Type == "text" { texts = append(texts, b.Text); continue }
    others = append(others, b)
}
if len(others) == 0 { return tool.Success(strings.Join(texts, "\n\n")), nil }
return tool.Success(map[string]any{"text": strings.Join(texts,"\n\n"), "blocks": others}), nil
```

---

## 总结

| ID | 严重度 | 文件 | 问题 | 状态 |
|----|--------|------|------|------|
| MCP-01 | 高 | jsonrpc.go | rpcResponse.ID 不兼容 string id，协议合规性 | 待修复 |
| MCP-02 | 高 | http.go / stdio.go | Initialize/ListTools DCL race，重复握手 | 待修复 |
| MCP-03 | 高 | client.go | Block 缺 JSON tag，LLM 侧字段名错误 | 待修复 |
| MCP-04 | 中 | http.go / stdio.go | nil args 序列化为 null，违反协议 | 待修复 |
| MCP-05 | 中 | hub.go | Start/Close 错误聚合缺失 | 待修复 |
| MCP-06 | 中 | stdio.go / hub.go | Start 失败后不可安全重试 | 文档化或重构 |
| MCP-07 | 中 | stdio.go | 忽略 list_changed / progress 通知 | 记录为已知限制 |
| MCP-08 | 低 | client.go | CanonicalName 无 64 字符上限 | 待修复 |
| MCP-09 | 低 | client.go | sanitize CJK 碰撞 | 待修复 |
| MCP-10 | 低 | client.go (godoc) | Close 后语义未文档化 | 待补充 |
| MCP-11 | 低 | tool_adapter.go | 多 block 输出对 LLM 不友好 | 依赖 MCP-03 |

**建议优先修复顺序**：MCP-03（1 分钟，影响明显）→ MCP-04（1 分钟，协议合规）→ MCP-01（半小时，协议合规）→ MCP-02（半小时，并发正确性）→ MCP-05（快速重构）。

MCP-06/07 的"记录为已知限制 + 文档化"方案可在 ISSUE-25 里追加一节。
