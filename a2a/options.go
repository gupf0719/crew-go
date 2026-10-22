package a2a

import (
	"reflect"

	"crew-go/hook"
	"crew-go/observability"

	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// remoteConfig 是 NewRemoteAgent / NewRemoteAgentTool 的内部配置。
type remoteConfig struct {
	name        string                       // override agent/tool 名（默认从 AgentCard.Name 转合规名）
	description string                       // override 描述（默认 AgentCard.Description）
	clientOpts  []a2aclient.FactoryOption    // 透传 a2aclient.NewFromCard（auth interceptor / transport）
	humanGate   bool                         // input-required 抛 InterruptError 走真人 HITL（默认返问题让 host LLM 自动补）
	hooks       hook.Hooks                   // 业务级 hook（OnAgentStart/OnAgentEnd 对，对齐 8 编排观测范式）
	obs         *observability.Observability // a2a 跨进程 trace：client span + 把 trace context 注入出站 metadata
	// discoverAgentName 仅 NewRemoteAgentFromService / NewRemoteAgentToolFromService 用：限定只
	// 发现该 agentName 的实例（空 = 该 serviceName 下所有 agentName 聚合）。对直接 NewRemoteAgent 无效。
	discoverAgentName string
}

// RemoteOption 配置远程 A2A agent 接入。
type RemoteOption func(*remoteConfig)

// WithName override agent/tool 名（默认从远程 AgentCard.Name 转 OpenAI 合规工具名）。
func WithName(name string) RemoteOption {
	return func(c *remoteConfig) { c.name = name }
}

// WithDescription override 描述（默认用远程 AgentCard.Description——能力发现）。
func WithDescription(desc string) RemoteOption {
	return func(c *remoteConfig) { c.description = desc }
}

// WithClientOptions 透传 a2aclient FactoryOption——auth / transport 等。
//
// **认证**（解决跨网络调用安全）：远程 AgentCard 的 securitySchemes 声明所需凭证；
// 业务用 a2aclient.WithCallInterceptors(&a2aclient.AuthInterceptor{...}) 注入凭证，
// 或自定义 *http.Client（带 auth-injecting RoundTripper）+ a2aclient.WithJSONRPCTransport(client)。
//
// 跨进程 trace 串联走 WithObservability（W3C trace context 写进 A2A Message.metadata，传输层不是
// HTTP 时也能串上）。想要 HTTP 层的 span 可以另外用 otelhttp 包 transport，两者不冲突。
func WithClientOptions(opts ...a2aclient.FactoryOption) RemoteOption {
	return func(c *remoteConfig) { c.clientOpts = append(c.clientOpts, opts...) }
}

// WithObservability 注入 *observability.Observability 启用 a2a 跨进程 trace：client 发请求前建
// invoke_agent CLIENT span，把它的 W3C trace context（traceparent / tracestate / baggage）写进出站
// Message.metadata，远端（server 侧配 WithExecutorObservability）的 span 树挂在它下面、同一条 trace。
// 不注入时没有 a2a trace（只有 WithHooks 的业务观测）。typed-nil 防御同 WithHooks。
func WithObservability(obs *observability.Observability) RemoteOption {
	return func(c *remoteConfig) {
		if isNilIface(obs) {
			return
		}
		c.obs = obs
	}
}

// WithHumanGate 让远程返 input-required 时抛 InterruptError 触发真人 HITL（骑 host
// 编排 / BasicAgent 的 cp，复用 ask_user 中断-恢复机制）。
//
// **远端的审批请求不归它管，一律交给真人**：挂起清单里标了审批的问题（crew-go 服务端的工具审批）、任务状态
// AUTH_REQUIRED（A2A 规范给「需要人批准 / 授权」的标准信号），不管开没开都走下面「开启」那条路径——宿主模型不能替人
// 批准远端的操作。曾经默认模式把审批问句当普通追问交给宿主模型，模型回一个 yes，远端的工具就执行了（端到端探针
// 实测）。WithHumanGate 只决定**普通追问**交给模型还是交给人。
//
// **AUTH_REQUIRED 的凭证走带外通道**（规范 §7.6）：交给真人的那一问按审批展示（批准 / 拒绝），远端说明要去哪里授权。
// 人没有拒绝（批准、「已授权」这类话都算）就不给远端发消息，重新挂上那个任务取后续：先轮询到它离开 AUTH_REQUIRED
// （最多等 30 秒，到时仍停在那儿就再问一次人——授权可能没有成），之后已经结束的取终态，还在跑的支持流式就订阅、
// 不支持就接着轮询；人拒绝时取消远端任务、这次调用报错（不是终局失败）。曾经把人的答复作为消息发过去：
// 执行器还在跑的服务端回「任务还在执行」、已经跑完的回 invalid params，远端的结果拿不回来（实测）。⚠️ 识别不了的审批：别家服务端把审批写成纯文本的
// input-required（比如 "Confirm delete? yes/no"）时我们分不出它是审批，默认模式下仍会交给宿主模型；对端不是
// crew-go 时，要真人把关就开 WithHumanGate。
//
// **默认（不开）**：普通追问（input-required）当普通输出返回——host BasicAgent 的 LLM 看到远程
// 追问后在 ReAct 循环里自动补信息再调一次（续同一远程 task，零中断零 cp）。适合"host
// 自己能答"。**前提是有 LLM host**：默认模式返的
// "call this tool again" 追问需一个 ReAct LLM 看到后再调。若把**非 humanGate** 的
// RemoteAgent 嵌进**无 LLM 的确定性编排**（Sequential/Parallel/Loop），追问会变成
// 该 step 的输出、无人再调 → 远程 task 搁置在 input-required，编排却报"成功"。
// **无 LLM 编排里必须开 WithHumanGate**（让追问抛 InterruptError 找真人补）。
//
// **开启**：input-required → InterruptError 冒泡到人类。适合远程要的是**人类确认**
// （如付款确认）host LLM 不该替人决定，或 agent 形态嵌入无 LLM 的确定性编排
// （Sequential/Parallel）需真人补输入。同一个远端任务答完一问又来要人时是下一问，再问一次人。
//
// **一问一答，答案只续它答复的那个远端任务**。等人答复的那一问（远端 taskID、会话线程、这个任务里的第
// 几问、问题）按调用位置记进 host 节点的私有态，那一问的身份是「调用位置 + 远端 taskID + 第几问」：
//
//   - 续跑时有这条记录、也拿到了人对这一问的答复：带着答复续那个远端任务。
//   - 有记录但没拿到答复（没给、给的是 nil、并行几问只答了一部分）：不联系远端，原样再问一次（同一个
//     ID）。曾经把这次调用的原始输入当成答复发出去，远端照样续上、做完，`err == nil`（实测）。
//   - 远端任务结束（完成 / 失败 / 被拒 / 取消）之后记录清掉，再跑——续跑重试失败的那一步、或编排按
//     retryCfg 重试——发原始请求；新任务再来要人是新问题，换一个 ID 问人。曾经人的回答被当成新任务的第一条
//     消息发出去、原始请求丢了（实测）。
//   - 发送在网络层失败（远端任务还在等人）时记录留着，重跑带同一个答复续同一个任务、不重问。
//   - 远端答完这一问之后改问一个交给宿主模型的普通追问（默认模式）：记录同样清掉，人的答复不会再发给远端。曾经
//     记录留着，节点再跑时把人的批准又发了一遍、被当成那个追问的答复（实测）。
//   - 记录按调用位置分开：节点里先后几次调用、同一批几个 tool_call、几个并行分支各续各的。曾经整个任务
//     共用 session 上的一个句柄，一个调用续掉另一个调用挂着的远端任务（实测）。
//
// **在节点执行里被调起时，这次调用在它自己的相位里问**，不占调用它的那段代码的提问序号——调用方在节点体、工具体、
// 框架交接的相位还是钩子里都一样：调用方按进度跳过这次已完成的调用时，后面的提问不挪位。曾经占调用方一个序号，
// 跳过它之后后面那一问挪到它的位置上、拿到别的问题的答案（节点体与被交接的业务壳里各实测一次）。相位名决定续跑时
// 认不认得出同一次调用：
//
//   - 节点体、工具函数体里：按「哪个远端 + 发出去的请求内容 + 同样的请求在这次执行里第几次」定。内容完全相同的几次
//     只能按次序区分，别按进度单独跳过其中一次（同 agent_v2.RunSubAgent）。⚠️ 请求在续跑之间变了（带请求号、
//     时间戳、节点里现调模型拼的）就是另一次调用：原始请求作为新任务重发、远端要人时再问一次——把拼好的请求记下来
//     （compose_v2.SaveStepResult），或经 agent_v2.RunSubAgent + WithCallKey 调。
//   - 别人给的相位里（框架交接给它或包着它的业务壳、图与工具的钩子）：按「外层相位名 + 哪个远端 + 在这个相位里第几次」
//     定，不看请求内容——输入在续跑之间变了（全历史输入、带 call key）也找得回挂着的那一问。同一个相位里对同一个
//     远端的几次调用按次序区分，别按进度单独跳过其中一次。
//
// **续跑不依赖原 session**：记录随 host 的存档走，Resume 不带 `agent_v2.WithResumeSession(sess)`、甚至换了
// RemoteAgent 与 host 实例也续得上。仍建议带上：会话线程（contextID）的默认值在 session 上，换了 session 时
// 之后新开的远端任务不在原来的线程里。
//
// 以上都以「在有提问位置的地方调」为前提：节点体、工具（含工具钩子）、编排成员、节点体里的 RunSubAgent。
// 另外两处：
//
//   - 引擎的钩子与条件边里（OnNodeStart / OnNodeEnd / OnNodeEvent、嵌套图的 OnGraphStart / OnGraphEnd、条件边，
//     以及从这些地方发起的调用）：没有提问位置，抛的中断接不住（被拒或被丢掉），跟 compose_v2.Ask 在钩子里直接拒
//     是同一条规矩。gate 在这里不续任何任务，远端要人时返回终局错误（`compose_v2.ErrRunAborted`：调用放错了
//     位置，重跑必然一样），提示把调用搬到上面那几处，并尽力取消远端那个没人会去续的任务（默认模式下远端的审批
//     请求同样如此）。曾经这里用 session 上的句柄：下一次调用把上一次在等的
//     那一问当成已答，远端照着执行，`err == nil`（实测）。
//   - 不在任何节点执行里（顶层直接调、DetachTask 开的独立任务）：没有存档接得住中断（中断的 ID 为空）。句柄
//     记在 session 上，业务拿同一个 session 带上人的答复再 Run 一次就续刚才那个远端任务，跟默认模式一样。
func WithHumanGate() RemoteOption {
	return func(c *remoteConfig) { c.humanGate = true }
}

// WithHooks 注入业务级 hook（对齐 8 编排观测范式）：Run 在 top-level fire
// OnAgentStart（input 可改写，[消费]）+ defer fire OnAgentEnd（含 InterruptError /
// panic 路径，agent_v2.WithEndError 协议让 observability 识别中断）——RemoteAgent 作
// **agent 形态**嵌编排时由此获得 agent span + agent.run metric（tool 形态本就有 host
// 的 tool span，无需此 option）。OnModel*/OnTool* 不 fire（远程是黑盒，无本地 LLM/tool）。
func WithHooks(h hook.Hooks) RemoteOption {
	return func(c *remoteConfig) {
		if isNilIface(h) {
			return
		}
		c.hooks = h
	}
}

// WithDiscoveryAgentName 限定 NewRemoteAgentFromService / NewRemoteAgentToolFromService 只发现
// serviceName 下指定 agentName 的实例（同一 serviceName 注册了多个 Agent Card 时精确路由，对齐
// Java DiscoveredA2aAgent.agentName(...)）。不传 = 发现该 serviceName 下**所有 agentName** 的实例
// （聚合，selector 从全部里选）。仅作用于服务发现过滤，对直接 NewRemoteAgent / NewRemoteAgentFromCard 无效。
func WithDiscoveryAgentName(agentName string) RemoteOption {
	return func(c *remoteConfig) { c.discoverAgentName = agentName }
}

// isNilIface 检 untyped nil + typed-nil interface（对齐 agent_v2 isFrameworkNil 协议的
// 全 kind 覆盖，跨包各持一份）：业务 `var h *MyHooks; WithHooks(h)` / `var d *MyDirectory;
// NewRemoteAgentFromService(d,...)` 时入参是 typed nil interface（== nil 为 false），存下/
// 调用后远 stack panic 难定位——入口处过滤。
func isNilIface(h any) bool {
	if h == nil {
		return true
	}
	v := reflect.ValueOf(h)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
