package server

import (
	"crew-go/compose_v2"
	"log/slog"
	"reflect"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// ---- CallInterceptor：注入默认 User + 校验 A2A-Version ----

// conformanceInterceptor 补齐 A2A server 端两处 1.0 合规行为——SDK 提供机制、需业务接（不接则
// 缺省行为不合规）：
//
//  1. 注入已认证 User：SDK 的 taskstore 按 User 隔离（ListTasks 只返当前 User 的 task），
//     无 User 时内建 NewTaskStoreAuthenticator 返 ErrUnauthenticated。User.Name 取自
//     WithOwnerResolver 的结果——**跟 Executor 挂起 task 表用同一个身份源**，让 SDK 侧
//     taskstore 的分区跟我们自己那张表按同一维切（曾经写死 "default"，把所有调用方压成
//     同一个 owner，SDK 的分区形同虚设）。resolver 返空（匿名）时回落 "default" 保持
//     SDK 侧「必须有 User」的要求。业务可再叠自己的 CallInterceptor 覆盖。
//  2. 校验 A2A-Version：客户端 A2A-Version header 与本 server 协议版本不符时返
//     ErrVersionNotSupported（SDK server 端无内建版本校验，走 CallInterceptor 机制补）。
//
// 另外两件事在 Before / After 里做：**归属检查**（所有指向已有 task 的请求，一处拦下）
// 和**「不存在」错误正文归一**（防存在性预言机）——各见那两个方法。
//
// 经 a2asrv.WithCallInterceptors 挂到 Handler（见 Handler）。
type conformanceInterceptor struct {
	a2asrv.PassthroughCallInterceptor // Before / After 都自己实现，嵌它只为将来补方法时不破编译

	// ownerResolver 与 Executor 共用同一个——SDK taskstore 的 User 分区跟我们自己那张
	// 挂起 task 表必须按同一个维度切，否则一边隔离一边不隔离。
	ownerResolver compose_v2.OwnerResolver

	// store 是包了归属检查的 task store。**归属检查放在这一层而不是各方法各写一遍**：
	// 带 taskID 的协议方法有七八个，走的下游各不相同（有的过 taskStore、有的直接查
	// 执行管理器的内存表、push-config 那组走另一个 store），逐个补必然漏。
	store *ownerScopedTaskStore
}

// requestTaskID 从请求负载里抽出它要操作的 taskID。
//
// 第二个返回值 false = 这个请求不针对某个已有 task（新建消息、列表、拿 agent card），
// 不做归属检查。
//
// ⚠️ **新增协议方法时这里要跟着加**——漏了不会报错，只会让那个方法绕过归属检查。
// 判据很简单：请求类型里有没有一个指向已有 task 的 ID 字段。
// taskIDLookup 是 [requestTaskID] 的三态结果。
//
// ⚠️ **「不认识」必须跟「认识但不指向 task」分开**，一个 bool 装不下。混在一起的代价两边
// 都有：当成「不认识」⟹ 每次新建会话（空 taskID 的 SendMessage，最常见的请求）打一条
// ERROR，真警报被淹没（实测过）；当成「不用查」⟹ SDK 加了新的指向 task 的负载时归属检查
// 静默不跑，跨租户读写照常发生。
//
// 对齐 k8s 授权器的 `authorizer.DecisionNoOpinion`：不认识是一等状态，不是 false 的一个分身。
type taskIDLookup int

const (
	// taskIDFound 这个负载指向一个已有 task，要查归属。
	taskIDFound taskIDLookup = iota
	// taskIDNone 认识这个负载，它不指向已有 task（新建会话 / 取 card / 列任务）。
	taskIDNone
	// taskIDUnknown 不认识 —— 失败开放，调用方必须发信号。
	taskIDUnknown
)

// requestTaskID 从入站负载里取出它指向的 task。
//
// **这个 switch 就是「协议入站负载全集」那张表**，没有第二份豁免清单——不指向 task 的类型
// 同样在这儿列出并返 [taskIDNone]，理由写在各自的 case 上。机械对账测试
// `TestRequestTaskID_CoversEveryInboundPayload` 拿 SDK 的
// `a2asrv.RequestHandler` 方法入参跟它比，少一个就红。
func requestTaskID(payload any) (a2atypes.TaskID, taskIDLookup) {
	found := func(id a2atypes.TaskID) (a2atypes.TaskID, taskIDLookup) {
		if id == "" {
			return "", taskIDNone
		}
		return id, taskIDFound
	}
	switch v := payload.(type) {
	case *a2atypes.GetTaskRequest:
		return found(v.ID)
	case *a2atypes.CancelTaskRequest:
		return found(v.ID)
	case *a2atypes.SubscribeToTaskRequest:
		return found(v.ID)
	case *a2atypes.GetTaskPushConfigRequest:
		return found(v.TaskID)
	case *a2atypes.ListTaskPushConfigRequest:
		return found(v.TaskID)
	case *a2atypes.DeleteTaskPushConfigRequest:
		return found(v.TaskID)
	case *a2atypes.PushConfig: // CreateTaskPushConfig 的负载
		return found(v.TaskID)
	case *a2atypes.SendMessageRequest:
		// 续发（带已有 taskId）才检查；新建（taskId 空）是 taskIDNone 不是「不认识」。
		if v.Message == nil {
			return "", taskIDNone
		}
		return found(v.Message.TaskID)
	case *a2atypes.GetExtendedAgentCardRequest:
		// 取 agent card，跟任何 task 无关（也没有 TaskID 字段）。
		return "", taskIDNone
	case *a2atypes.ListTasksRequest:
		// 没有 TaskID 字段——它按 contextId/tenant 列，归属靠 ownerScopedTaskStore.List
		// 逐条过滤 + TotalSize 不透传，不是单点校验能覆盖的。
		return "", taskIDNone
	}
	return "", taskIDUnknown
}

// Before 在每个 method 调用前跑：先校验版本（不符早退拒绝，整个请求返错），再注入默认 User。
// callCtx 是指针，改 User 在下游 taskstore Authenticator（读 callCtx.User.Name）处生效。
func (ci conformanceInterceptor) Before(ctx context.Context, callCtx *a2asrv.CallContext, req *a2asrv.Request) (context.Context, any, error) {
	// A2A-Version 校验：ServiceParams 由 HTTP header 构造，key 已小写。客户端声明了版本且与本
	// server 不符 → 拒绝（返 wrap 的 ErrVersionNotSupported，SDK 映射成对应 JSON-RPC 错误码）。
	// 未带版本 header（空）视为不声明、放行。
	if sp := callCtx.ServiceParams(); sp != nil {
		if vs, ok := sp.Get("a2a-version"); ok && len(vs) > 0 && vs[0] != "" && vs[0] != string(a2atypes.Version) {
			return ctx, nil, fmt.Errorf("client A2A-Version %q not supported (server speaks %s): %w",
				vs[0], a2atypes.Version, a2atypes.ErrVersionNotSupported)
		}
	}
	// 注入已认证 User——身份取自 owner resolver（跟 Executor 挂起 task 表同一个源），
	// 让按 User 隔离的 taskstore 操作（ListTasks / Get）按真实调用方分区。
	name := "default"
	owner := ""
	if ci.ownerResolver != nil {
		owner = ci.ownerResolver(ctx)
		if owner != "" {
			name = owner
		}
	}
	callCtx.User = &a2asrv.User{Name: name, Authenticated: true}
	// **owner 必须在这里 push，不能只在 Executor 里 push。**
	//
	// task store 的 `Create` 跑在 SDK handler 的链上、**早于** AgentExecutor.Execute；
	// `tasks/get` 那条路更是整条不经过 Execute。只在 Execute 里 push 的话，store 包装层
	// 拿到的 owner 恒为空 ⟹ 记的归属是空、校验时两边都空 ⟹ 放行 ⟹ 跨租户读原样发生
	// （实测过：改成在这里 push 之前，租户 B 照样读到 A 的完整历史）。
	//
	// CallInterceptor.Before 是唯一一个**能改整条调用链 ctx** 的位置（它的返回值就是
	// 下游用的 ctx），所以身份注入必须落在这儿。
	ctx = compose_v2.WithOwner(ctx, owner)

	// **归属检查：所有指向已有 task 的请求，一处拦下。**
	//
	// 为什么不能只靠 task store 的包装层：`tasks/resubscribe` 走的是执行管理器的
	// `taskexec.Manager.Resubscribe`，它只查一张**按 taskID 单键**的在途执行表、
	// **碰都不碰 store**
	// （同一 SDK 的 cluster 版第一件事就是 store.Get，是 local 版单独漏了这道）。
	// 实测后果：租户 B 只凭 A 的 taskId 就订到了 A 的实时 artifact 流和完整最终答复——
	// 快照那半被 store 挡住了，直播那半整个在守卫之外。
	//
	// push-config 那组同理走另一个 store（现在因为没开启而够不着，但那是「没开」不是
	// 「挡住了」——业务加一行 option 就成立）。
	//
	// 拒绝一律返 ErrTaskNotFound：跟「不存在」同一个错，不泄漏归属（正文由 After 归一，
	// 见那里）。
	if req != nil {
		switch tid, look := requestTaskID(req.Payload); look {
		case taskIDFound:
			if ci.store != nil {
				if err := ci.store.CheckAccess(ctx, tid); err != nil {
					return ctx, nil, err
				}
			}
		case taskIDUnknown:
			// **「不认识」是一等状态，不是 false 的一个分身。** 上面那个 switch 的
			// default 是失败开放：SDK 加了新的指向 task 的负载而这里没跟上时，归属检查
			// 静默不跑。有一道机械对账测试盯着，但**测试只在有人跑测试时说话**——
			// `go get -u` 之后直接上线的部署需要运行期也有信号。
			slog.ErrorContext(ctx, "a2a/server: 未知的入站负载类型，归属检查未执行",
				"type", payloadTypeName(req.Payload),
				"fix", "在 requestTaskID 的 switch 里给它加一个 case")
		}
	}
	return ctx, nil, nil
}

// After 把所有「task 不存在」类错误的**正文**归一成同一句话。
//
// 不归一的话，拒绝和真的不存在在 wire 上分得开：我们这一层拒绝返的是裸
// [a2atypes.ErrTaskNotFound]（"task not found"），而下游报的是各自 wrap 过的
// （`tasks/get` 是 "failed to get task: task not found"）——**实测两句话逐字不同**，
// 于是「这个 taskId 存不存在」变成一个 1 bit 预言机。taskId 是 UUID 猜不到，但客户端
// **可以自带 taskId**，taskId 派生自共享业务键的部署里这个预言机是有用的。
//
// 归一放在这一层而不是「让拒绝跟着 wrap 同样的前缀」：前缀是**每个方法各一份**的
// （get / cancel / resubscribe 各不相同），照抄就是立一份注定要漂的拷贝，而且新增方法
// 时漏了不报错。这里做的是「凡是这个哨兵，正文一律换成哨兵本身」，跟方法数无关。
//
// ⚠️ Before 早退的错误**不经过 After**（SDK 的 doCall 直接返 earlyErr）——所以 Before
// 那边必须返裸哨兵，两条路才对得上。
func (ci conformanceInterceptor) After(_ context.Context, _ *a2asrv.CallContext, resp *a2asrv.Response) error {
	if resp != nil && resp.Err != nil && errors.Is(resp.Err, a2atypes.ErrTaskNotFound) {
		resp.Err = a2atypes.ErrTaskNotFound
	}
	return nil
}

// ---- HTTP middleware：校验请求 Content-Type ----

// contentTypeNotSupportedCode 是 A2A ContentTypeNotSupportedError 的 JSON-RPC 错误码（spec §5.4）。
// 硬编码而非从 SDK 取：SDK 的 error→code 映射与写入（marshalJSONRPCError / writeJSONRPCError）
// 均未导出，此 middleware 在 SDK handler 之前拦、自行成响应。A2A 错误码是 spec 稳定常量。
const contentTypeNotSupportedCode = -32005

// contentTypeGuard 在 SDK 解析请求前拦 Content-Type：POST 且 Content-Type 非空非 application/json
// → 返 ContentTypeNotSupportedError（-32005）。SDK server 端不校验请求 content-type（合法 JSON body
// 配错 content-type 会被当 ParseError -32700），此 middleware 补 A2A 1.0 §5.4。仅查 POST（GET
// well-known card 放行）；SSE 响应 content-type 与此无关（这里查请求方 header）。
func contentTypeGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			ct := r.Header.Get("Content-Type")
			if i := strings.IndexByte(ct, ';'); i >= 0 { // 去掉 charset 等参数
				ct = ct[:i]
			}
			if ct = strings.TrimSpace(ct); ct != "" && !strings.EqualFold(ct, "application/json") {
				id := jsonRPCRequestID(r.Body) // 解析前取 id 让错误响应 id 对齐请求
				rw.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(rw, `{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":"incompatible content types"}}`,
					id, contentTypeNotSupportedCode)
				return
			}
		}
		next.ServeHTTP(rw, r)
	})
}

// jsonRPCRequestID 从请求 body 提原始 JSON-RPC id（number/string/null 原样）让错误响应 id 对齐
// 请求；提取失败（非法 JSON / 无 id）返 "null"（JSON-RPC 允许 parse 前错误 id 为 null）。
func jsonRPCRequestID(body io.Reader) string {
	var req struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.NewDecoder(body).Decode(&req); err != nil || len(req.ID) == 0 {
		return "null"
	}
	return string(req.ID)
}

// payloadTypeName 取负载的具体类型名（去掉指针与包名），给上面那条「不认识」告警用。
func payloadTypeName(payload any) string {
	if payload == nil {
		return ""
	}
	t := reflect.TypeOf(payload)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}
	return t.Name()
}
