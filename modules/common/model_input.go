package common

// ModelInput 是一次模型调用的入参视图——`hook.Hooks.OnModelStart` 拿到的就是它。
//
// **Preamble 与 Messages 是两条并列的流，不是一条**（相对 spec 的自有设计，理由见下）。
type ModelInput struct {
	// Preamble 是「这个 agent 是什么」——框架每轮按配置重新摆上去的那一段：
	// 提示词模板的渲染结果（含 few-shot）、system prompt（含 skill 提示与 skill 正文）。
	//
	// **它不进上下文压缩**：能由配置重算的东西不该占着被压缩的名额，更不该被压缩层丢掉。
	// 曾经它跟 Messages 混在一条列表里，压缩层靠「前导位置」把它认出来加以保护——
	// 而模板一旦渲染出 few-shot，真正的 system prompt 就排在 few-shot 之后、判据当场失效，
	// 实测被静默丢掉（`err == nil`）。分成两条之后**判据消失**，不是变准。
	//
	// **[消费] 档**：hook 改写它生效——改内容、删条目、整段换掉都行，框架用改写后的值发请求。
	//
	// ⚠️ **顺序语义**：Preamble 恒排在 Messages 之前。想在 agent 的前言**之前**放东西，
	// 往 Preamble 头部插——往 Messages 头部插只能排到前言**之后**。这跟前言还在
	// 消息列表里的时候不同（那时往列表头插就真在最前），是分成两条流的直接结果。
	//
	// ⚠️ **别把它跟 Messages 合并再拆开**。试过（marker 打标 + 合并 + 劈回），两处静默错：
	// 业务插在前言**之前**的消息劈完跑到了它**之后**（跨半的相对顺序在分区时丢掉）；
	// 业务造一条新 system 想顶掉前言时**两条都发出去**。分成两个字段之后两者都不成立——
	// 想换前言就写 Preamble，想动对话就写 Messages，是两个不会互相误伤的动作。
	Preamble []*Message `json:"preamble,omitempty"`
	// Messages 是会被上下文压缩处置的那条对话流：长期记忆注入、历史、本轮输入、
	// 以及本次调用里累积的模型产出与工具结果。
	//
	// ⚠️ 它**不是**最终发给模型的那一份——四层压缩在 `OnModelStart` **之后**才跑。
	Messages []*Message  `json:"messages"`
	Tools    []*ToolInfo `json:"tools"`
	// Config 是本次请求的模型名与按次参数（温度、max_tokens、top_p），给钩子看：BasicAgent 用 model.RequestConfig
	// 算——模型名取按次的 model.WithModel 覆盖，没有时取客户端实现的 model.ModelNamer。**[只读] 档**：发请求不读它，
	// 改了不影响调用。客户端构造时配的默认参数不在内；什么都不知道时为 nil。
	Config *ModelConfig   `json:"config"`
	Extra  map[string]any `json:"extra"`
}
