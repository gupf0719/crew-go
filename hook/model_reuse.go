package hook

// ModelResponseReusedKey 标「这一遍处理的是之前已经返回过的模型答复」：框架放在 OnModelStart 的 ModelInput.Extra 与
// OnModelEnd 的 ModelOutput.Extra 上，值为 true。
//
// BasicAgent 的一轮在 OnModelEnd 里因为等人或被叫停（中断、取消）停下时，续跑沿用那次的答复、不再调模型，
// OnModelStart / OnModelEnd 再走一遍把这一轮处理完（问过人的钩子这一遍拿到答案）。这一遍没有模型调用：OnModelStart
// 对输入的改写不生效，ModelOutput.Usage 为 nil（那次的用量已经交给过停下之前跑过的钩子）。
//
// 标记的意思是「你在这一遍之前已经收到过这次调用」，这只对排在会停下的钩子**前面**的钩子成立——那次它们跑过了。所以：
//
//   - 按调用计数、计时、计费、把答复往外发的钩子见到标记就不再记一次、不再发一遍，**它们必须排在会在 OnModelEnd 里
//     停下的钩子（问人、暂停）前面**。agui 适配层这样做；框架自带的观测把业务钩子包在里面（Install / AgentHooks(inner...)），里面的钩子停下时它照样按这次调用的结局收尾，效果相同。排在后面的话
//     那次没跑到、这一遍又跳过，那次调用一次也没记上（审计实测：示例把观测排在暂停钩子后面，那次模型调用的耗时与用量
//     一次都没记）。
//   - 会停下的那个钩子自己见到标记照常处理：它这一遍要拿到答案、把这一轮处理完。
//
// 链里每个钩子在一次调用上恰好处理一次、不依赖顺序，要框架记下链停在第几个、从那里接着跑；钩子链对框架不透明（业务
// 能自己写组合钩子），这里不做。
const ModelResponseReusedKey = "crew_model_response_reused"

// IsModelResponseReused 报 extra（ModelInput.Extra 或 ModelOutput.Extra）上是否标着 [ModelResponseReusedKey]。
func IsModelResponseReused(extra map[string]any) bool {
	reused, _ := extra[ModelResponseReusedKey].(bool)
	return reused
}

// ModelRunIDKey 是这次模型调用的 run_id（框架放在 OnModelEnd 的 ModelOutput.Extra 上，string），与它流式片段的 run_id
// 同一个。要知道「这份答复出自哪次调用」读它、别读 output.Message 上的标注：排在前面的钩子可能换掉了 Message，新消息不带
// 框架的标注（曾经适配层排在改写钩子后面时按消息标注取不到 run_id，把改写后的答复当成另一条消息发出，审计实测）。
const ModelRunIDKey = "crew_model_run_id"

// ModelRunIDOf 取 extra（ModelOutput.Extra）上的 [ModelRunIDKey]；没有时返回空串。
func ModelRunIDOf(extra map[string]any) string {
	id, _ := extra[ModelRunIDKey].(string)
	return id
}
