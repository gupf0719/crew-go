package common

// DataContentBlock 承载结构化 JSON（A2A 新版 data part 对齐）。spec 原 ContentBlock union
// 无此类型——crew-go 为 A2A 各语言互通新增（A2A Part 的 data content，见 a2a/convert.go
// 双向映射）。Data 是任意 JSON 值（对象 / 数组 / 标量）；LLM 不直接消费原生 JSON，由下游
// 适配器 / tool 决定用法（A2A wire 层无损往返是本类型的核心职责）。
type DataContentBlock struct {
	Type ContentBlockType `json:"type"`
	Data any              `json:"data"`
}
