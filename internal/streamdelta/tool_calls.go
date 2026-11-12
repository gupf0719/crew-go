// Package streamdelta 定义模型适配器与增量聚合器之间的元数据。
package streamdelta

// ToolCallIndices 对应 map[int]int，将片段中的内容块位置映射到 provider 的工具调用索引。
const ToolCallIndices = "crew-go.internal.stream.tool_call_indices"
