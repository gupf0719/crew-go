package server

import (
	"net/http"
	"reflect"
	"strings"

	"crew-go/a2a/registry"
	"crew-go/agent_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// Card 从 agent 构造最小 AgentCard——name/description 取自 agent，interface URL 用 baseURL。
// Streaming=true：Executor 把 agent 的 LLM chunk 实时桥接为 A2A artifact 事件（见 Executor godoc）。
func Card(agent agent_v2.Agent, baseURL string) *a2atypes.AgentCard {
	mustAgent(agent)
	baseURL = strings.TrimRight(baseURL, "/") // W7 fix: 防尾斜杠拼出 //invoke 等畸形 URL（跨语言消费端未必容错）
	return &a2atypes.AgentCard{
		Name:        agent.Name(),
		Description: agent.Description(),
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(baseURL+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: true},
		Skills:             []a2atypes.AgentSkill{}, // A2A 1.0 schema 要求 skills 为数组（非 null）
	}
}

// Handler 构造 agent 的 A2A server http.Handler（JSON-RPC /invoke + AgentCard well-known 路径）。
// baseURL 是本 server 对外可达的基地址（如 "http://10.0.0.12:8080"）——写进 card 的 interface URL，
// 必须与发现端拉 card 时用的地址一致。业务用 http.Server{Handler: server.Handler(ag, baseURL)} 起服务。
// opts 透传 ExecutorOption——跨语言 a2a trace 用 WithExecutorObservability(obs)。
//
// # 这是唯一带守卫的装配
//
// 归属检查由两件东西撑着，**都只长在这个函数里**：task store 的归属包装
// （ownerScopedTaskStore）和跨协议方法的 conformanceInterceptor。**自己拼 a2asrv 栈
// （`a2asrv.NewHandler(server.NewExecutor(...), ...)`）两道都不会有**——[WithOwnerResolver]
// 照样必配、照样被调用，但没人拿它的结果去分区，跨租户读原样成立（实测：自拼 local 栈
// 与自拼 cluster 栈都能让租户 B 经 tasks/get 读到 A 的完整历史）。
//
// 本函数**有意不接 `a2asrv.RequestHandlerOption`**：那类 option 里有几个
// （`a2asrv.WithClusterMode` / `WithTaskStore`）跟我们写的是同一个 SDK 字段，谁后落谁生效，而
// `a2asrv.RequestHandlerOption` 是不透明函数值——框架无法知道业务在里面设了什么，也就无法保证
// 自己的包装层裹的是业务真正想用的那个 store。透传等于把「守卫有没有生效」变成一个
// 取决于参数顺序、且失败时静默的属性。需要那些能力就自拼栈并自负归属（见包 godoc「局限」）。
func Handler(agent agent_v2.Agent, baseURL string, opts ...ExecutorOption) http.Handler {
	mux := http.NewServeMux()
	// 同时挂 /invoke 与 /invoke/：Go ServeMux 精确匹配，客户端带尾斜杠（官方 a2a-tck 即如此）
	// 命中不到会 404，body 非 JSON 令对端解析失败。两路径共用同一 handler。
	exec := NewExecutor(agent, opts...)
	// task store 包一层归属检查——**SDK 的 Get / Update 不做**，而 `tasks/get` 这个协议
	// 方法直接走 handler → taskStore.Get → 返回完整 Task 含 History，整条路不经过
	// AgentExecutor，我们在那层的分区拦不住它。实测过：租户 B 拿 A 的 taskId 调
	// tasks/get，读到 A 的原始请求和 agent 向 A 提的问题。见 ownerScopedTaskStore。
	// **总是包一层**——业务经 WithTaskStore 换后端时，包装不由业务负责。守卫要是靠
	// 调用方记得自己包，那它就不是守卫。
	inner := exec.taskStore
	if inner == nil {
		// SDK 自带的进程内后端**真的按 user 下推**（它的 List 用 Authenticator 读
		// CallInterceptor 注入的身份先过滤再分页），但它不认识我们的
		// [OwnerPartitionedTaskStore]——不套这层声明的话包装层不信任它，会把它正确的
		// per-owner TotalSize 降级成本页条数。实测：B 有 12 条、pageSize=5 时默认装配
		// 给 5，裸后端给 12。
		inner = declaredPartitioned{taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
			Authenticator: a2asrv.NewTaskStoreAuthenticator(),
		})}
	}
	store := newOwnerScopedTaskStore(inner)
	jsonrpcHandler := a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec,
		a2asrv.WithTaskStore(store),
		// interceptor 与 Executor 共用同一个 owner resolver——SDK taskstore 的 User 分区
		// 跟我们的挂起 task 表必须按同一维切。
		a2asrv.WithCallInterceptors(conformanceInterceptor{ownerResolver: exec.ownerResolver, store: store})))
	mux.Handle("/invoke", jsonrpcHandler)
	mux.Handle("/invoke/", jsonrpcHandler)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(agent, baseURL)))
	return contentTypeGuard(mux) // 解析前校验请求 Content-Type（A2A 1.0 §5.4）
}

// mustAgent 构造期 fail-fast nil / typed-nil agent（对齐 agent_v2 isAgentNil 协议——
// 否则 agent.Name() 在远 stack nil deref 难定位）。
func mustAgent(agent agent_v2.Agent) {
	if agent == nil {
		panic("a2a/server: agent must be non-nil")
	}
	if v := reflect.ValueOf(agent); (v.Kind() == reflect.Pointer || v.Kind() == reflect.Map ||
		v.Kind() == reflect.Slice || v.Kind() == reflect.Func || v.Kind() == reflect.Chan ||
		v.Kind() == reflect.Interface || v.Kind() == reflect.UnsafePointer) && v.IsNil() {
		panic("a2a/server: agent must be non-nil (typed nil)")
	}
}

// Instance 构造指向本 server 的 registry.ServiceInstance——agentCardUrl = baseURL + well-known 路径。
// serviceName 是注册的逻辑服务名（发现端按它找），agentName 区分同一 serviceName 下的多个 Agent
// Card（单 agent 服务传 "" → Register WithDefaults 补 = serviceName），instanceID 是实例唯一标识
// （如 "host:port"）。业务拿它 dir.Register 进服务目录（4 层路径 service/{agentName}/{encodedInstanceId}）。
func Instance(serviceName, agentName, instanceID, baseURL string) registry.ServiceInstance {
	baseURL = strings.TrimRight(baseURL, "/") // W7 fix: 同 Card，规整尾斜杠
	return registry.ServiceInstance{
		SchemaVersion: registry.CurrentSchemaVersion,
		ServiceName:   serviceName,
		AgentName:     agentName,
		InstanceID:    instanceID,
		BaseURL:       baseURL,
		AgentCardURL:  baseURL + a2asrv.WellKnownAgentCardPath,
		Transport:     registry.TransportJSONRPC,
		Status:        registry.StatusServing,
		// Weight 留 nil → Register 时 WithDefaults 补默认 100。要显式权重用 registry.WeightPtr。
	}
}

// declaredPartitioned 给一个**已知按 owner 下推**的后端补上
// [OwnerPartitionedTaskStore] 声明。只用在默认装配上——业务经 [WithTaskStore] 换后端时
// 自己实现那个方法（跑 `taskstore_conformance` 会检查）。
type declaredPartitioned struct{ taskstore.Store }

func (declaredPartitioned) PartitionsByOwner() bool { return true }
