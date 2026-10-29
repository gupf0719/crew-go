package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// TestOwnerScopedStore_UpdateIsGuarded 钉住 `Update` 的归属检查。
//
// ⚠️ 这一半此前**零覆盖**：删掉它全仓静默绿，整个跨租户场景里它触发 0 次——因为
// 那些场景都在更早的 `Get` 上就被拒了。而 godoc 明写「Get / Update 都校验」，
// 没有用例走到的承诺就是下一次重构会被顺手删掉的那一条。
//
// 写侧被攻破的后果比读侧重：改别人 task 的状态/历史。
func TestOwnerScopedStore_UpdateIsGuarded(t *testing.T) {
	st := newOwnerScopedTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	}))
	ctxA := compose_v2.WithOwner(context.Background(), "tenant-A")
	ctxB := compose_v2.WithOwner(context.Background(), "tenant-B")

	task := &a2atypes.Task{ID: "t1", ContextID: "c1"}
	if _, err := st.Create(ctxA, task); err != nil {
		t.Fatalf("A create: %v", err)
	}

	// B 试图改 A 的 task。
	if _, err := st.Update(ctxB, &taskstore.UpdateRequest{Task: task}); err == nil {
		t.Error("B 改到了 A 的 task——Update 没做归属检查")
	}
	// 正向：A 自己改得动（否则「Update 对所有人拒」也能让上面那条绿）。
	if _, err := st.Update(ctxA, &taskstore.UpdateRequest{
		Task: task, PrevTask: task,
	}); err != nil {
		t.Errorf("A 改自己的 task 失败了：%v", err)
	}
}

// TestOwnerScopedStore_UpdateWithoutCarriedStampConsultsBackend 钉住 `Update` 的回落路径。
//
// 入参上没带归属戳时**必须去后端问**，不能当成「没记录 ⟹ 放行」——那正是旁表版本
// 的洞（无记录放行），换了载体不能把它一起搬过来。
//
// 正常流程走不到这条（戳从 Create / Get 一路带下来），所以它天然零覆盖，只能这样直接
// 构造：手搓一个不带戳的 UpdateRequest。
func TestOwnerScopedStore_UpdateWithoutCarriedStampConsultsBackend(t *testing.T) {
	st := newOwnerScopedTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	}))
	ctxA := compose_v2.WithOwner(context.Background(), "tenant-A")
	ctxB := compose_v2.WithOwner(context.Background(), "tenant-B")

	if _, err := st.Create(ctxA, &a2atypes.Task{ID: "t1", ContextID: "c1"}); err != nil {
		t.Fatalf("A create: %v", err)
	}

	// B 手搓一个不带戳的更新（模拟「戳在链路某处掉了」）。
	bare := &a2atypes.Task{ID: "t1", ContextID: "c1"}
	if _, err := st.Update(ctxB, &taskstore.UpdateRequest{Task: bare}); err == nil {
		t.Error("入参没带戳时 B 改到了 A 的 task——回落没去后端问，退化成了「没记录就放行」")
	}
	// 正向：A 自己用同样不带戳的入参改得动（否则「没戳就一律拒」也能让上面那条绿）。
	bareA := &a2atypes.Task{ID: "t1", ContextID: "c1"}
	if _, err := st.Update(ctxA, &taskstore.UpdateRequest{Task: bareA}); err != nil {
		t.Errorf("A 用不带戳的入参改自己的 task 失败了：%v", err)
	}
	// 且回落之后戳要被重新盖上——否则下一次 Update 又得回落读一遍后端。
	if o, ok := ownerOf(bareA); !ok || o != ownerDigest("tenant-A") {
		t.Errorf("回落放行后没重新盖戳：ownerOf=%q ok=%v", o, ok)
	}
}

// TestOwnerScopedStore_CreateStampsBeforeInner 钉住归属戳**跟 task 同一次写入落盘**。
//
// 前身是「先记后建 + 失败回滚」那套旁表逻辑要防的窗口：两步之间 task 已存在但没有归属
// 记录，而无记录的 ID 是放行的 ⟹ 谁在那个窗口里问就答谁（自旋轮询实测命中率 94.3%）。
// 戳搬进 task 之后那个窗口在结构上就不存在了——这条用例钉的就是「不存在」：inner.Create
// 拿到手的那个 task 上，戳已经在了。
func TestOwnerScopedStore_CreateStampsBeforeInner(t *testing.T) {
	probe := &createProbeStore{inner: taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	})}
	st := newOwnerScopedTaskStore(probe)
	seen := 0
	probe.onCreate = func(task *a2atypes.Task) {
		seen++
		o, ok := ownerOf(task)
		if !ok {
			t.Error("inner.Create 拿到的 task 上没有归属戳——存进去的就是一条谁都读得到的记录")
			return
		}
		if o != ownerDigest("A") {
			t.Errorf("归属戳是 %q，期望 %q 的摘要", o, "A")
		}
	}
	if _, err := st.Create(compose_v2.WithOwner(context.Background(), "A"),
		&a2atypes.Task{ID: "t1", ContextID: "c1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if seen != 1 {
		t.Fatalf("探针没被调到（seen=%d）——上面那些断言一条都没跑", seen)
	}
}

// TestOwnerScopedStore_UnstampedTaskIsRejected 钉住 fail-closed：后端里没有归属戳的
// task **谁都读不到**。
//
// 反面（没戳就放行）是旁表版本的默认行为，而它在「业务后端把 Task.Metadata 丢了」时
// 会把整个后端降级成公开可读。
func TestOwnerScopedStore_UnstampedTaskIsRejected(t *testing.T) {
	inner := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	})
	// 绕过包装层直接塞一条没戳的进去（= 后端里本来就有的数据 / 后端丢了 metadata）。
	if _, err := inner.Create(context.Background(), &a2atypes.Task{ID: "t1", ContextID: "c1"}); err != nil {
		t.Fatalf("inner create: %v", err)
	}
	st := newOwnerScopedTaskStore(inner)

	for _, owner := range []string{"", "tenant-A", "tenant-B"} {
		ctx := compose_v2.WithOwner(context.Background(), owner)
		if _, err := st.Get(ctx, "t1"); err == nil {
			t.Errorf("owner=%q 读到了没有归属戳的 task", owner)
		}
		if err := st.CheckAccess(ctx, "t1"); err == nil {
			t.Errorf("owner=%q 的 CheckAccess 放行了没有归属戳的 task", owner)
		}
	}
	// 正向：同一个包装层上，正经建的 task 读得到（否则「Get 对所有人拒」也能让上面那条绿）。
	ctxA := compose_v2.WithOwner(context.Background(), "tenant-A")
	if _, err := st.Create(ctxA, &a2atypes.Task{ID: "t2", ContextID: "c2"}); err != nil {
		t.Fatalf("A create: %v", err)
	}
	if _, err := st.Get(ctxA, "t2"); err != nil {
		t.Errorf("A 读自己正经建的 task 失败了：%v", err)
	}
}

// TestOwnerScopedStore_CheckAccessDeniesMissing 钉住 CheckAccess 对 store 里**没有**的 taskID 拒，跟「不是你的」同一个哨兵。
//
// 「没有就放行、让下游报」不成立：内层 store 可能把「不是你的」也报成没有（a2a-go 的内存 store 自 v2.7.0 起照规范这么伪装），
// 而 tasks/resubscribe 的下游不查 store——放行时租户 B 订到了 A 在途的实时输出（`TestCrossTenant_Resubscribe_CrossTenant`）。
// 两种拒绝在 wire 上分不出来由 `TestCrossTenant_NoExistenceOracle` 钉。
func TestOwnerScopedStore_CheckAccessDeniesMissing(t *testing.T) {
	st := newOwnerScopedTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	}))
	ctx := compose_v2.WithOwner(context.Background(), "tenant-A")
	if err := st.CheckAccess(ctx, "never-existed"); !errors.Is(err, a2atypes.ErrTaskNotFound) {
		t.Errorf("store 里没有的 taskID 应拒（ErrTaskNotFound），实际 %v", err)
	}
	// 对照：存在但不是你的 → 拒。
	if _, err := st.Create(compose_v2.WithOwner(context.Background(), "tenant-B"),
		&a2atypes.Task{ID: "t1", ContextID: "c1"}); err != nil {
		t.Fatalf("B create: %v", err)
	}
	if err := st.CheckAccess(ctx, "t1"); err == nil {
		t.Error("A 的 CheckAccess 放行了 B 的 task")
	}
}

// TestOwnerScopedStore_AnonymousSingleTenant 钉住单租户 / 匿名（owner 恒空）照常工作。
//
// 空 owner 盖的戳是**空串**，不是「不盖」——盖了才能跟「后端里那些没经过我们的数据」
// 分得开，也才不会被 fail-closed 一起拒掉。这是最常见的部署形态，拒错了就是全挂。
func TestOwnerScopedStore_AnonymousSingleTenant(t *testing.T) {
	st := newOwnerScopedTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "default", nil },
	}))
	ctx := context.Background() // 没 push 过 owner ⟹ GetOwner 返空

	task := &a2atypes.Task{ID: "t1", ContextID: "c1"}
	if _, err := st.Create(ctx, task); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 空 owner 也要**盖戳**（盖的是空串的摘要，不是"不盖"）——「没盖戳」是 fail-closed
	// 的触发条件，匿名单租户不该落进那一档。
	if o, ok := ownerOf(task); !ok || o != ownerDigest("") {
		t.Errorf("空 owner 应盖空串的摘要（而不是不盖）：ownerOf=%q ok=%v", o, ok)
	}
	if _, err := st.Get(ctx, "t1"); err != nil {
		t.Errorf("单租户读自己的 task 失败了：%v", err)
	}
	if err := st.CheckAccess(ctx, "t1"); err != nil {
		t.Errorf("单租户 CheckAccess 被拒：%v", err)
	}
	resp, err := st.List(ctx, &a2atypes.ListTasksRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.Tasks) != 1 {
		t.Errorf("单租户 list 应看到自己那 1 个，实际 %d", len(resp.Tasks))
	}
}

// ---- List：过滤 + 分页语义 ----

// listStub 是一个「按调用次序吐预设页」的内层，用来精确钉 List 的分页处理：
// 真后端的 taskID 是 UUID，页内构成不可控，钉不住「滤掉之后 token / TotalSize 怎么办」。
type listStub struct {
	pages []*a2atypes.ListTasksResponse
	calls int
}

func (l *listStub) Create(context.Context, *a2atypes.Task) (taskstore.TaskVersion, error) {
	return 0, nil
}
func (l *listStub) Update(context.Context, *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	return 0, nil
}
func (l *listStub) Get(context.Context, a2atypes.TaskID) (*taskstore.StoredTask, error) {
	return nil, a2atypes.ErrTaskNotFound
}
func (l *listStub) List(context.Context, *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	p := l.pages[l.calls]
	l.calls++
	return p, nil
}

func stampedTask(id, owner string) *a2atypes.Task {
	t := &a2atypes.Task{ID: a2atypes.TaskID(id), ContextID: "c"}
	stampOwner(t, owner)
	return t
}

// TestOwnerScopedStore_ListPagination 钉住三件事：
//
//  1. 滤掉别人的（缺口 1 本身）；
//  2. `NextPageToken` **原样透传**——它是内层坐标系里的游标，滤掉页内几条不影响它继续
//     往后走。翻页要按「token 空了才算完」判，所以**页可以是空的**、也必须继续翻；
//  3. `TotalSize` 恒改成本页自己的条数——除非内层自报按 owner 分区。「有没有滤掉东西」
//     推断不出「内层分不分区」，见 TestOwnerScopedStore_ListTotalSizeIsNotACountOracle。
func TestOwnerScopedStore_ListPagination(t *testing.T) {
	stub := &listStub{pages: []*a2atypes.ListTasksResponse{
		// 第 1 页：混着别人的
		{Tasks: []*a2atypes.Task{stampedTask("a1", "A"), stampedTask("b1", "B"), stampedTask("a2", "A")},
			TotalSize: 9, PageSize: 3, NextPageToken: "cursor-1"},
		// 第 2 页：一条都不是我的
		{Tasks: []*a2atypes.Task{stampedTask("b2", "B"), stampedTask("b3", "B")},
			TotalSize: 9, PageSize: 3, NextPageToken: "cursor-2"},
		// 第 3 页：全是我的（末页）
		{Tasks: []*a2atypes.Task{stampedTask("a3", "A")},
			TotalSize: 9, PageSize: 3, NextPageToken: ""},
	}}
	st := newOwnerScopedTaskStore(stub)
	ctx := compose_v2.WithOwner(context.Background(), "A")

	ids := func(resp *a2atypes.ListTasksResponse) []string {
		var out []string
		for _, task := range resp.Tasks {
			out = append(out, string(task.ID))
		}
		return out
	}

	// 第 1 页
	p1, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3})
	if err != nil {
		t.Fatalf("list p1: %v", err)
	}
	if got := ids(p1); len(got) != 2 || got[0] != "a1" || got[1] != "a2" {
		t.Errorf("第 1 页应只剩 [a1 a2]，实际 %v", got)
	}
	if p1.NextPageToken != "cursor-1" {
		t.Errorf("游标被动过了：%q（滤掉页内几条不该影响它）", p1.NextPageToken)
	}
	if p1.TotalSize != 2 {
		t.Errorf("滤掉过东西还透传内层 TotalSize=%d——那是跨租户计数", p1.TotalSize)
	}

	// 第 2 页：空页也得带着游标，否则客户端就停在这儿、后面属于它的再也拿不到
	p2, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3, PageToken: p1.NextPageToken})
	if err != nil {
		t.Fatalf("list p2: %v", err)
	}
	if len(p2.Tasks) != 0 {
		t.Errorf("第 2 页应该是空的，实际 %v", ids(p2))
	}
	if p2.NextPageToken != "cursor-2" {
		t.Errorf("空页把游标弄丢了：%q——客户端会以为翻完了", p2.NextPageToken)
	}

	// 第 3 页：一条都没滤掉。**这里以前断言透传内层的 9**——理由写的是「默认后端本来就
	// 按 owner 分区」，可这个 stub 恰恰不分区（前两页混着 B 的就是证据）。那条断言把一个
	// 跨租户计数泄漏钉成了期望行为，见
	// TestOwnerScopedStore_ListTotalSizeIsNotACountOracle。现在：内层不自报分区 ⟹ 一律
	// 改成本页条数。
	p3, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3, PageToken: p2.NextPageToken})
	if err != nil {
		t.Fatalf("list p3: %v", err)
	}
	if got := ids(p3); len(got) != 1 || got[0] != "a3" {
		t.Errorf("第 3 页应是 [a3]，实际 %v", got)
	}
	if p3.TotalSize != 1 {
		t.Errorf("内层没自报分区，TotalSize 该是本页条数 1，实际 %d——透传的话就是跨租户总数", p3.TotalSize)
	}
	if p3.NextPageToken != "" {
		t.Errorf("末页不该有游标：%q", p3.NextPageToken)
	}
	if stub.calls != 3 {
		t.Errorf("内层被调了 %d 次（期望 3）", stub.calls)
	}
}

// ---- 后端读失败不能被当成「放行」 ----

type failingGetStore struct {
	taskstore.Store
	err error
}

func (f *failingGetStore) Get(context.Context, a2atypes.TaskID) (*taskstore.StoredTask, error) {
	return nil, f.err
}

// TestOwnerScopedStore_BackendErrorIsNotAllow 钉住：后端读挂了要上抛，不能当成
// 「问不出归属 ⟹ 放行」。
//
// 归属检查现在要读一次后端（戳在 task 上，没有进程内缓存了），于是多出一条以前没有的
// 分支：后端报错。把它吞成放行，等于给了一条「打挂后端就能读别人 task」的路。
func TestOwnerScopedStore_BackendErrorIsNotAllow(t *testing.T) {
	boom := errors.New("backend down")
	st := newOwnerScopedTaskStore(&failingGetStore{err: boom})
	ctx := compose_v2.WithOwner(context.Background(), "A")

	if err := st.CheckAccess(ctx, "t1"); !errors.Is(err, boom) {
		t.Errorf("CheckAccess 把后端错误吞了：%v", err)
	}
	if _, err := st.Get(ctx, "t1"); !errors.Is(err, boom) {
		t.Errorf("Get 把后端错误吞了：%v", err)
	}
}

// TestOwnerScopedStore_ConcurrentStampIsRaceFree 拿 -race 压一遍并发读写。
//
// 盖戳是**原地改 task 的 metadata map**（不这么做戳传不下去），所以「有没有别人同时在
// 读这张 map」是这次改动唯一的新并发面。用 -race 跑才算数，不能靠读代码推。
func TestOwnerScopedStore_ConcurrentStampIsRaceFree(t *testing.T) {
	st := newOwnerScopedTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "svc", nil },
	}))
	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{}) // 屏障：让 n 条真并发起跑，别被调度器串行化掉
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := compose_v2.WithOwner(context.Background(), fmt.Sprintf("tenant-%d", i%3))
			id := a2atypes.TaskID(fmt.Sprintf("t%d", i))
			<-start
			task := &a2atypes.Task{ID: id, ContextID: "c"}
			if _, err := st.Create(ctx, task); err != nil {
				t.Errorf("create %s: %v", id, err)
				return
			}
			for r := 0; r < 5; r++ {
				if _, err := st.Get(ctx, id); err != nil {
					t.Errorf("get %s: %v", id, err)
				}
				if _, err := st.Update(ctx, &taskstore.UpdateRequest{Task: task}); err != nil {
					t.Errorf("update %s: %v", id, err)
				}
				if _, err := st.List(ctx, &a2atypes.ListTasksRequest{}); err != nil {
					t.Errorf("list: %v", err)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}

type createProbeStore struct {
	inner    taskstore.Store
	onCreate func(*a2atypes.Task)
}

func (p *createProbeStore) Create(ctx context.Context, task *a2atypes.Task) (taskstore.TaskVersion, error) {
	if p.onCreate != nil {
		p.onCreate(task)
	}
	return p.inner.Create(ctx, task)
}
func (p *createProbeStore) Update(ctx context.Context, r *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	return p.inner.Update(ctx, r)
}
func (p *createProbeStore) Get(ctx context.Context, id a2atypes.TaskID) (*taskstore.StoredTask, error) {
	return p.inner.Get(ctx, id)
}
func (p *createProbeStore) List(ctx context.Context, r *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	return p.inner.List(ctx, r)
}

// TestOwnerScopedStore_ListTotalSizeIsNotACountOracle 钉住 `TotalSize` **不泄漏跨租户计数**。
//
// 曾经用「这一页有没有滤掉东西」去推断「内层分不分区」：滤掉过就改写 TotalSize、一条没滤
// 就透传。两者恰好在**没东西可滤**时分叉，而那正是最白送的一档——
//
//   - **空页**（内层返 0 条、TotalSize 非 0，是合法协议状态：翻到尾后一页、或过滤条件命中
//     0 条但总数非 0）⟹ `len(kept)==len(resp.Tasks)==0` 恒成立 ⟹ 直接透传。租户 B 拿
//     别人的 contextId 查一把、翻过尾页，就读到了对方的任务条数。
//   - **整页恰好全是自己的** ⟹ 同样透传，而那个数是跨租户总数。
//
// 按本仓「借来的量」判别式：「有没有滤掉东西」这个名字里没有「内层分不分区」的意思。
// 分区与否只有后端自己知道 ⟹ 让它自报（[OwnerPartitionedTaskStore]），不自报就按不分区
// 处理（fail-closed）。
func TestOwnerScopedStore_ListTotalSizeIsNotACountOracle(t *testing.T) {
	ctx := compose_v2.WithOwner(context.Background(), "B")

	t.Run("空页不得透传内层计数", func(t *testing.T) {
		stub := &listStub{pages: []*a2atypes.ListTasksResponse{
			// 内层一条没返，但报「总共有 7 条」——那 7 条是别人的
			{Tasks: nil, TotalSize: 7, PageSize: 3, NextPageToken: ""},
		}}
		st := newOwnerScopedTaskStore(stub)
		resp, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(resp.Tasks) != 0 {
			t.Fatalf("fixture 没搭对：这一页应该是空的，实际 %d 条", len(resp.Tasks))
		}
		if resp.TotalSize != 0 {
			t.Errorf("空页透传了内层 TotalSize=%d——租户 B 白拿一个跨租户条数预言机", resp.TotalSize)
		}
	})

	t.Run("整页全是自己的也不得透传", func(t *testing.T) {
		stub := &listStub{pages: []*a2atypes.ListTasksResponse{
			{Tasks: []*a2atypes.Task{stampedTask("b1", "B"), stampedTask("b2", "B")},
				TotalSize: 7, PageSize: 3, NextPageToken: ""},
		}}
		st := newOwnerScopedTaskStore(stub)
		resp, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(resp.Tasks) != 2 {
			t.Fatalf("fixture 没搭对：两条都该留下，实际 %d", len(resp.Tasks))
		}
		if resp.TotalSize != 2 {
			t.Errorf("一条没滤掉就透传内层 TotalSize=%d——那是跨租户总数", resp.TotalSize)
		}
	})

	t.Run("后端自报按owner分区时才透传", func(t *testing.T) {
		stub := &partitionedListStub{listStub: listStub{pages: []*a2atypes.ListTasksResponse{
			{Tasks: []*a2atypes.Task{stampedTask("b1", "B")}, TotalSize: 7, PageSize: 3},
		}}}
		st := newOwnerScopedTaskStore(stub)
		resp, err := st.List(ctx, &a2atypes.ListTasksRequest{PageSize: 3})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if resp.TotalSize != 7 {
			t.Errorf("后端自报分区了，它那个 7 就是 B 自己的跨页总数，不该改成 %d", resp.TotalSize)
		}
	})
}

// partitionedListStub 自报「我按 owner 分区」——用来验能力接口那条路。
type partitionedListStub struct{ listStub }

func (partitionedListStub) PartitionsByOwner() bool { return true }

// TestDefaultAssembly_TotalSizeNotDegraded 钉住**默认装配不被自己的降级误伤**。
//
// 包装层在后端不声明分区时把 TotalSize 降级成本页条数（那是对不合规后端的安全兜底）。
// 而 SDK 自带的进程内后端**真的按 user 下推**——它只是不认识我们的
// [OwnerPartitionedTaskStore]。不给它补上声明的话，默认装配下每个人拿到的 TotalSize 都是
// 本页条数：实测 B 有 12 条、pageSize=5 时给 5，而裸后端直接查是 12。
//
// 断言选在「TotalSize 等于跨页总数」而不是「等于本页条数」——后者在两个值恰好相同的
// fixture 上恒真（PageSize 必须小于 B 的条数，这条 conformance 里踩过）。
func TestDefaultAssembly_TotalSizeNotDegraded(t *testing.T) {
	inner := declaredPartitioned{taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: ownerPartitionAuthenticator, // 跟默认装配同一个（曾经这里另写一个，测不到生产装配的分区撞车）
	})}
	st := newOwnerScopedTaskStore(inner)
	ctxB := compose_v2.WithOwner(context.Background(), "tenant-B")
	for i := 0; i < 12; i++ {
		task := &a2atypes.Task{ID: a2atypes.TaskID(fmt.Sprintf("b%02d", i)), ContextID: "c"}
		if _, err := st.Create(ctxB, task); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	resp, err := st.List(ctxB, &a2atypes.ListTasksRequest{PageSize: 5})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.Tasks) != 5 {
		t.Fatalf("fixture 没搭对：pageSize=5 该返 5 条，实际 %d", len(resp.Tasks))
	}
	if resp.TotalSize != 12 {
		t.Errorf("TotalSize=%d，B 有 12 条——默认装配被自己对不合规后端的降级误伤了", resp.TotalSize)
	}
}
