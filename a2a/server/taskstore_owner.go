package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"

	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// taskOwnerMetadataKey 是归属戳在 [a2atypes.Task.Metadata] 里的键。
//
// 带包名前缀是因为这张 map 是 A2A 的**扩展位**——业务和其它扩展也往里写，键空间共享。
//
// 值是 owner 的**摘要**不是 owner 本身，见 [ownerDigest]。
const taskOwnerMetadataKey = "crew.task.ownerDigest"

// ownerDigest 把 owner 映射成一个只用来比相等的摘要。
//
// **这一层需要的全部能力就是「比相等」，那就没有理由把身份本身发出去。** 而戳会跟着
// task 走得很远：`tasks/get` 的响应带 Metadata（回到客户端）、业务的持久后端、推送
// 通知的 body。owner 常常是租户 ID / 用户 ID / 邮箱这类内部标识，明文写进去等于把它
// 们撒到这些地方——即使每个 task 的主人只看得到自己那份，暴露面也没有必要扩大。
//
// **它挡的是「明文撒出去」，不是「反查不出来」**：owner 常常是低熵的（`tenant-A`、
// 一个邮箱），字典跑一轮就还原了。选摘要是因为这一层只需要「比相等」，没理由把身份
// 本身发出去；不是因为它做到了匿名。
//
// 不加盐是权衡：盐必须跨重启、跨进程稳定，否则所有旧戳一起失配——那是个静默把每个人
// 都锁在自己 task 外面的故障模式，换来的只是挡住一个二阶泄漏（拿到别人的 task 才谈得上
// 反查它的主人，而拿得到就已经是更大的问题了）。
//
// 想审计「这个 task 属于谁」，去问你的身份系统，别从一个会上 wire 的字段里读。
func ownerDigest(owner string) string {
	sum := sha256.Sum256([]byte(owner))
	return hex.EncodeToString(sum[:])
}

// ownerPartitionAuthenticator 是默认装配给 SDK 内存后端的 Authenticator：按 owner 分区，分区键跟 owner 一一对应。
//
// 不用 `a2asrv.NewTaskStoreAuthenticator`（读 CallContext 的 User.Name）：拦截器给匿名调用方把 User.Name 回落成
// "default"，跟真叫 "default" 的租户撞进同一个分区，而包装层信任这个后端按 owner 分区、原样透传它的 TotalSize ⟹
// 匿名调用方拿到对方的条数（审计实测）。空 owner 也映射成非空值：后端对空 user 的 List 报 ErrUnauthenticated。
func ownerPartitionAuthenticator(ctx context.Context) (string, error) {
	return "owner:" + compose_v2.GetOwner(ctx), nil
}

// ownerScopedTaskStore 给底层 task store 包一层归属检查。
//
// # 为什么必须包这一层
//
// 后端自己做不做归属检查靠不住：SDK 的 store 接口没有这个要求，业务经 `WithTaskStore` 换的后端未必做（SDK 自带的
// 内存后端早先只在 `List` 时按 user 过滤、`Get` / `Update` 不看，v2.7.0 起才都看）。而 `tasks/get` 这个
// 协议方法的处理链是「handler 直接 taskStore.Get → 返回完整 Task 含 History」——
// **整条路不经过 AgentExecutor**，所以我们在 Executor 那层按 (owner, taskID) 分区的
// 挂起表拦不住它。
//
// 实测过后果：租户 A 发起一个任务并停在 input-required，租户 B 拿着同一个 taskId 调
// `tasks/get`，拿到 A 的完整历史——A 的原始请求、agent 向 A 提的问题，一字不差。
// taskId 出现在 A 的每一个响应里，拿到它不需要任何攻击技巧。
//
// # 归属记在 task 自己身上
//
// 归属戳写进 [a2atypes.Task.Metadata]（键 [taskOwnerMetadataKey]），**跟着 task 一起
// 落进后端**，而不是另立一张进程内的表。这一条是这层唯一的设计决定，其余都是它的推论：
//
//   - **换持久后端 + 多实例横向扩容照样对**。归属跟 task 同生共死，任何实例读到同一个
//     task 就读到同一个归属。旁表版本在这里静默失效过：实例 1 建的 task，实例 2 上
//     另一个租户读**直接放行**（实例 2 的表里没这条记录，而无记录是放行的）——而多实例
//     正是换持久后端的主要动机。
//   - **`List` 过滤得动**。旁表只有「我这个进程记得的那些 ID」，回答不了「这一页里哪些
//     是你的」；戳在 task 上就是逐条读一下的事。
//   - **没有「建到一半」的窗口**。归属是 task 的一个字段，跟 task 同一次写入落盘，不存在
//     「已经建好但还没记归属」的中间态。旁表版本靠「先记后建 + 失败回滚」绕，那是在
//     补一个本不该存在的窗口。
//   - **不会无界增长**。旁表只增不删（`taskstore.Store` 接口没有 Delete），进程活多久
//     它涨多久。
//
// 戳**对 task 的主人可见**（`tasks/get` / `tasks/list` 返回的 Task 里就带着它）。想在读
// 出口抹掉它之前先想清楚抹的是谁的对象：内层返的很可能是它**内部持有的结构**（SDK 自带
// 的 InMemory 每次深拷贝，业务后端没这个义务），抹一次就把后端里那条记录永久去了戳。
// 而戳留在返回值上还有用：后续 `Update` 靠它认归属，不必再读一次后端。
//
// # 没有归属戳的 task 一律拒（fail-closed）
//
// 经 [Handler] 进来的 task **一定**带戳（`Create` 无条件盖，单租户/匿名盖的是空串，
// 那也是一个值）。所以「读到一个没戳的 task」只有两种可能：后端里的数据不是经这台
// server 写进去的，或者**业务后端把 `Task.Metadata` 丢了**（自己挑列存、序列化漏字段）。
// 两种都不该放行——放行就是把归属判断降级成「谁问都行」。
//
// 后者的症状是「刚建好的 task 自己也读不到」，第一次冒烟就撞上，且 `Get` 会 warn 一句
// 指名原因；比起静默放行泄漏，这是好得多的失败方式。
//
// # 空 owner
//
// resolver 返空（单租户 / 匿名）时所有 task 的戳都是空串，比对恒相等——行为跟没有这一层
// 完全一致。那是「没有身份」的诚实语义，不是保护失效。
type ownerScopedTaskStore struct {
	inner taskstore.Store
}

func newOwnerScopedTaskStore(inner taskstore.Store) *ownerScopedTaskStore {
	return &ownerScopedTaskStore{inner: inner}
}

// stampOwner 在 task 上盖归属戳。
//
// **原地改**不是疏忽：SDK 的 task 更新管理器会把这个对象留作 `taskupdate.Manager.lastStored`，
// 后续每次 `Update` 的入参都从它派生——盖在这个对象上，戳才跟着往下传。传进来的 task 本身
// 已经是 SDK 给事件做的深拷贝（`taskupdate.Manager.Process`），改它不会影响发给客户端的事件。
//
// **戳已经是这个值时不写**：SDK 有一条路（`taskupdate.Manager.SetTaskFailed`）传进来的
// 是 task 的**浅拷贝**，它的 metadata map 跟管理器留着的那份是同一张——那条路上原地写
// 就是在写一张可能有别人正在读的 map。而那条路上戳本来就已经对了，跳过写既省事又躲开
// 这个别名。
func stampOwner(task *a2atypes.Task, owner string) {
	if task == nil {
		return
	}
	if cur, ok := ownerOf(task); ok && cur == ownerDigest(owner) {
		return
	}
	if task.Metadata == nil {
		task.Metadata = make(map[string]any, 1)
	}
	task.Metadata[taskOwnerMetadataKey] = ownerDigest(owner)
}

// ownerOf 读出 task 上的归属戳。第二个返回值 false = 没盖过戳（**不是**「戳是空串」）。
func ownerOf(task *a2atypes.Task) (string, bool) {
	if task == nil || task.Metadata == nil {
		return "", false
	}
	v, ok := task.Metadata[taskOwnerMetadataKey]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// Create 盖归属戳后透传。
func (s *ownerScopedTaskStore) Create(ctx context.Context, task *a2atypes.Task) (taskstore.TaskVersion, error) {
	stampOwner(task, compose_v2.GetOwner(ctx))
	return s.inner.Create(ctx, task)
}

// Update 校验归属后重盖戳、再透传。
//
// 这一道是纵深防御：走到 Update 的请求已经过了 CallInterceptor 的 [ownerScopedTaskStore.CheckAccess]
// 和载入执行上下文那次 `Get`（两处都校验过）。它挡的是「那两处哪天漏了」。
//
// 校验取入参上带的戳（不额外读一次后端）——那个戳只可能来自我们自己的 `Create` 或一次
// 已校验的 `Get`。**戳不在时回落读后端**：那种情况正常流程下到不了（到了说明戳在链路某处
// 掉了），此时宁可多读一次也不能当作「没记录 ⟹ 放行」。
//
// 校验通过后**无条件重盖**：SDK 更新状态时会把事件的 metadata 合并进 task metadata
// （`taskupdate.Manager.updateStatus` 的 `maps.Copy`），重盖让那条路径盖不掉归属。
func (s *ownerScopedTaskStore) Update(ctx context.Context, req *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	if req == nil || req.Task == nil {
		return s.inner.Update(ctx, req)
	}
	owner := compose_v2.GetOwner(ctx)
	if carried, ok := ownerOf(req.Task); ok {
		if carried != ownerDigest(owner) {
			return taskstore.TaskVersionMissing, a2atypes.ErrTaskNotFound
		}
	} else if err := s.CheckAccess(ctx, req.Task.ID); err != nil {
		return taskstore.TaskVersionMissing, err
	}
	stampOwner(req.Task, owner)
	return s.inner.Update(ctx, req)
}

// Get 透传后校验归属——不是你的就返 [a2atypes.ErrTaskNotFound]，跟「不存在」同一个错。
func (s *ownerScopedTaskStore) Get(ctx context.Context, taskID a2atypes.TaskID) (*taskstore.StoredTask, error) {
	stored, err := s.inner.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !belongsToCaller(ctx, stored, taskID) {
		return nil, a2atypes.ErrTaskNotFound
	}
	return stored, nil
}

// belongsToCaller 判断这个已读出的 task 归不归当前调用方。**归属判据只此一处**——
// [ownerScopedTaskStore.Get] 和 [ownerScopedTaskStore.CheckAccess] 共用，两边不各写一遍。
//
// stored 为 nil / Task 为 nil 时返 true：那不是「归属不符」，是后端返了个空壳，
// 交给调用方按自己的语义处置（Get 原样返；CheckAccess 在调它之前就把空结果当成没有、拒掉）。
func belongsToCaller(ctx context.Context, stored *taskstore.StoredTask, taskID a2atypes.TaskID) bool {
	if stored == nil || stored.Task == nil {
		return true
	}
	owner, stamped := ownerOf(stored.Task)
	if !stamped {
		// 没戳的 task 一律拒（见类型 godoc）。这一句只在没戳时 warn、不在「戳对不上」时
		// warn——后者是外部可触发的，谁都能拿它刷日志；没戳则是部署侧的配置问题，
		// 值得吵醒运维。
		slog.WarnContext(ctx, "a2a server: task 没有归属戳，按「读不到」处理"+
			"（后端把 Task.Metadata 丢了？换持久后端时它必须跟着一起存）",
			"task", string(taskID))
		return false
	}
	return owner == ownerDigest(compose_v2.GetOwner(ctx))
}

// List 透传后按归属过滤。
//
// **不能裸透传**：「底层按 owner 过滤」只在 SDK 自带的 InMemory 上成立（它有
// Authenticator），而 [WithTaskStore] 的整个用途就是换掉它。实测过：换一个不认识 owner
// 的朴素后端，租户 B 的 `tasks/list` 返回租户 A 的 task，含完整 history。
//
// # 分页
//
// `a2atypes.ListTasksResponse.NextPageToken` **原样透传**——它是内层自己坐标系里的
// 游标，语义是「从这儿往后接着取」，把页内某几条滤掉不影响它继续往后走。代价是**页
// 可能变薄甚至变空**（内层给了一页、里面没一条是你的），所以翻页要按「token 空了才
// 算完」判，不能按「这页不满就算完」判——后者本来也不是这个字段的契约。
//
// `a2atypes.ListTasksResponse.TotalSize` 的处理：**默认改成本页你自己的条数**，只有内层
// 自报 [OwnerPartitionedTaskStore] 时才透传它的。
//
// 一度是用「这一页有没有滤掉东西」推断「内层分不分区」：滤掉过就改写、一条没滤就透传。
// 那是拿一个量去回答另一个问题，两者恰好在**没东西可滤**时分叉，而那是最白送的一档——
//
//   - **空页**（内层返 0 条但 TotalSize 非 0，合法协议状态：翻过尾页、或过滤命中 0 条而
//     总数非 0）⟹ `len(kept)==len(resp.Tasks)==0` 恒成立 ⟹ 直接透传。拿别人的 contextId
//     查一把就读到对方的任务条数，**零成本**。
//   - **整页恰好全是自己的** ⟹ 同样透传，而那个数是跨租户总数。
//
// 「内层分不分区」只有内层自己知道，猜不出来，所以让它自报；不自报按不分区处理
// （fail-closed）。代价是**不分区的后端上 TotalSize 退化成「本页条数」**，跟它协议上
// 「分页前总数」的语义不符——但那个语义在不分区的后端上本来就只能是跨租户总数，给不出来。
// 要精确的 per-owner 总数，让后端自己按 owner 分区并自报。
func (s *ownerScopedTaskStore) List(ctx context.Context, req *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	resp, err := s.inner.List(ctx, req)
	if err != nil || resp == nil {
		return resp, err
	}
	owner := compose_v2.GetOwner(ctx)
	var kept []*a2atypes.Task
	for _, task := range resp.Tasks {
		if o, stamped := ownerOf(task); stamped && o == ownerDigest(owner) {
			kept = append(kept, task)
		}
	}
	if p, ok := s.inner.(OwnerPartitionedTaskStore); ok && p.PartitionsByOwner() {
		// 内层自己按 owner 分区 ⟹ 它那个 TotalSize 就是本 owner 的跨页总数，比我们能算的
		// 准（我们只看得见这一页）。过滤仍然照做——纵深防御，分区的后端上它本来也滤不掉东西。
		if len(kept) == len(resp.Tasks) {
			return resp, nil
		}
		out := *resp
		out.Tasks = kept
		return &out, nil
	}
	// 拷一份再改：内层返的对象未必是它现造的（业务后端可能返内部持有的结构）。
	out := *resp
	out.Tasks = kept
	out.TotalSize = len(kept)
	return &out, nil
}

// OwnerPartitionedTaskStore 让 task store **声明它遵守了 owner 分区契约**（A2A 1.0 §13.1：
// 授权检查必须发生在任何可能泄漏资源存在性的查询之前）。
//
// ⚠️ **这不是「可选能力」，是「有没有做到必须做的事」**。分区本身是 [WithTaskStore] 对
// 后端的要求，不是加分项——外面这层包装挡得住内容、挡不住计数（实测：不分区的后端上，
// 沿 NextPageToken 翻 6 页，每页缺口累加就是别的租户的 task 条数）。声明合规请先跑
// `a2a/server/taskstore_conformance.RunTaskStoreTests`。
//
// 形状（方法而非类型断言）同 [store.TTLAware] / [store.RelevanceRanker]——同一个实现可能
// 按配置切换（接了多租户 schema 才分区）。
//
// 不实现视为「没做到」：[ownerScopedTaskStore.List] 于是不透传 `a2atypes.ListTasksResponse.TotalSize`，改给本页条数
// ——一个偏小但不跨租户的数。
type OwnerPartitionedTaskStore interface {
	PartitionsByOwner() bool
}

// CheckAccess 校验当前调用方够不够得着这个 task——给**够不着 store 的那些协议方法**用
// （`tasks/resubscribe` 走在途执行表、push-config 走另一个 store），由 CallInterceptor
// 统一调，见 conformanceInterceptor。
//
// 返回：
//   - nil：store 确认这个 task 归当前调用方。
//   - [a2atypes.ErrTaskNotFound]：不是你的，或 store 里没有它——两种一律拒。「没有」不能放下去让下游自己报：内层 store
//     可能把「不是你的」也报成没有（规范 §3.3.2 要求这样伪装，a2a-go 的内存 store 自 v2.7.0 起照做），而 `tasks/resubscribe`
//     的下游根本不查 store、只看在途执行表——「没有就放行」时，租户 B 凭 A 的 taskId 订到了 A 在途的实时输出
//     （`TestCrossTenant_Resubscribe_CrossTenant`，升级到 v2.7.0 时实测）。拒绝用的哨兵跟下游「不存在」相同，
//     conformanceInterceptor 的 After 把下游的错误正文归一成它（见那里的注释），wire 上分不出两种。
//   - 其它：后端读失败，原样上抛。
//
// ⚠️ 它**读一次后端**。归属不再有进程内缓存（那正是多实例下失效的那张表），所以
// `tasks/get` 这类本来就要读 store 的方法会读两次。这是不缓存归属的价钱：多一次读，
// 换「哪个实例上问都是同一个答案」。
func (s *ownerScopedTaskStore) CheckAccess(ctx context.Context, taskID a2atypes.TaskID) error {
	stored, err := s.inner.Get(ctx, taskID)
	if errors.Is(err, a2atypes.ErrTaskNotFound) {
		return a2atypes.ErrTaskNotFound // 可能是「不是你的」被伪装成了没有，拒（见上）
	}
	if err != nil {
		return err
	}
	if stored == nil || stored.Task == nil {
		// 后端用 `(nil, nil)` 表示没有（Go 常见写法；store 接口对 Get 只说 SHOULD 返 ErrTaskNotFound）：同样拒。
		// 放行的话 resubscribe 照样泄漏——SDK 的订阅遇到空快照直接跳过、接着推在途事件（审计实测）
		return a2atypes.ErrTaskNotFound
	}
	if !belongsToCaller(ctx, stored, taskID) {
		return a2atypes.ErrTaskNotFound
	}
	return nil
}

var _ taskstore.Store = (*ownerScopedTaskStore)(nil)
