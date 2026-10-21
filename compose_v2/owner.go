package compose_v2

import "context"

// ownerKey 是 ctx-value key。
type ownerKey struct{}

// WithOwner 声明「这次调用属于谁」——多租户隔离用的**主体身份**。
//
// # 它跟 ThreadID 是两件事
//
// [WithThreadIDCtx] 的 ConvID 是**续接标识**：「我要接着哪个会话说话」。它从 wire 来，
// 任何人都能给，而且**就该**任何人都能给——前端刷新页面重连，拿的正是同一个 ID。
//
// owner 是**授权主体**：「谁在说话」。它必须由服务端从已认证的凭据里解析出来，
// 不能取自请求体里的任何字段。
//
// 两者对「要不要验证来源」的答案相反，所以不能共用一个量。共用的后果是：任何知道
// threadId 的人都能续别人的会话——读到里面挂起的提问、替对方回答、继承已经转交出去
// 的控制权。（实测过一个具体形态：两个租户递同一个 threadID 会撞同一个转交槽，
// 租户 B 的全新对话被租户 A 的转交状态直接送进了另一个候选。）
//
// # 为什么是「进存储键」而不是「加一道授权闸」
//
// 一道 `authorize(caller, threadID) bool` 的闸要求调用方回答「这个 thread 之前归谁」
// ——那是框架的私有状态，调用方只能自己再存一份归属表，那就是又一份会漂的判据拷贝。
// 而 owner 进键之后：合法刷新（同 owner + 同 threadID）照常命中；攻击者猜中 threadID
// 但 owner 不同，读到的是**空**，不是错误。**根本不存在 deny 决策**，所以没有任何
// 合法场景会被打断。
//
// 会话历史层早一步立过同一条规矩，形状也一样（`WithHistoryOwnerFunc`）。
//
// 空串 = 不设（单租户默认），此时行为跟没有这一维时完全一致。
func WithOwner(ctx context.Context, owner string) context.Context {
	if owner == "" {
		return ctx
	}
	return context.WithValue(ctx, ownerKey{}, owner)
}

// GetOwner 取当前调用的主体身份。没设返空串。
func GetOwner(ctx context.Context) string {
	if o, ok := ctx.Value(ownerKey{}).(string); ok {
		return o
	}
	return ""
}

// OwnerResolver 从请求 ctx 里解析主体身份。
//
// **入参是 ctx 而不是解析后的请求体**——身份要从已认证的凭据里取（HTTP 中间件放进
// ctx 的那个），而不是从客户端能填的字段里取。拿得到请求体的 resolver 会诱使实现者
// 去读 `headers["X-Tenant"]` 之类客户端可控的东西，那等于没做。
//
// 返空串 = 这次调用没有主体（单租户 / 匿名）。
type OwnerResolver func(context.Context) string
