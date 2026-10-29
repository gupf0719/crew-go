// Package taskstore_conformance 验证业务自实现的 A2A task store **按调用方身份分区**。
//
// # 为什么这套必须跑
//
// A2A 1.0 §13.1 把它定为 MUST：授权检查必须发生在**任何可能泄漏资源存在性的查询之前**。
// crew-go 在 SDK 的 task store 外面包了一层归属过滤作为纵深防御，但那层是「先无差别
// 查、再逐条滤」——正是 §13.1 禁止的形状。它挡得住**内容**，挡不住**计数**：
//
//	实测：租户 A 有 30 个 task、租户 B 有 3 个。B 以 pageSize=10 沿 NextPageToken 翻页，
//	每页的 (PageSize - len(Tasks)) 累加**精确等于** A 的条数。4 次请求，无需任何权限。
//
// 这个通道从外面关不上（补页要自己合成游标，且仍不满足「查询之前」的字面要求）。**唯一
// 的关法是后端自己把 owner 下推成查询谓词。** 这套 conformance 就是验这件事。
//
// # 用法
//
//	import tsconf "crew-go/a2a/server/taskstore_conformance"
//
//	func TestMyPGTaskStore(t *testing.T) {
//	    tsconf.RunTaskStoreTests(t, func() taskstore.Store {
//	        st := mybackend.NewPGTaskStore("postgres://...")
//	        t.Cleanup(func() { _ = st.Close() })
//	        return st
//	    })
//	}
//
// factory 每个子测试调一次，必须返回**干净**的实例。
//
// # 这套只验「只有后端能修」的那一面
//
// 单条读（`tasks/get` / `tasks/resubscribe` 等）的跨租户访问由 crew-go 的包装层真正
// 关上了——它比对归属戳再决定返不返，并把拒绝和「不存在」的错误正文归一（否则两者在
// wire 上分得开，就是个存在性预言机）。**所以这套不要求后端自己做单条读的分区**：默认装配
// 不依赖后端做（SDK 自带的进程内后端早先 Get 不看 user，v2.7.0 起才看）。
//
// 关不上的是**计数面**：过滤发生在分页之后 ⟹ 页会变薄 ⟹ 缺口就是别人在这个窗口里的
// 条数。这个只有把 owner 下推成查询谓词才能消掉，所以这套验的是它。
//
// # 后端怎么拿到 owner
//
// `compose_v2.GetOwner(ctx)`——crew-go 的 CallInterceptor 在整条调用链最前面 push 它，
// 所有 store 方法都拿得到。别从 `a2atypes.ListTasksRequest.Tenant` 取：那是**客户端可控**的字段。
package taskstore_conformance

import (
	"context"
	"fmt"
	"testing"

	"crew-go/a2a/server"
	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// TaskStoreFactory 造一个干净的待测实例。
type TaskStoreFactory func() taskstore.Store

// Check 是一条检查：拿一个干净的 store，返 nil 表示通过。
//
// ⚠️ **抽成返 error 的纯函数是有原因的**：这样 [RunTaskStoreTests] 和本包的自检消费的是
// **同一份断言**。曾经不是——自检把翻页循环重抄了一遍，于是把 checkNoThinPages 的断言
// **整块删掉**，两条自检照样全绿（变异实测）。一个「守护断言」的测试守护的是自己的拷贝，
// 那就等于没有。
type Check struct {
	Name string
	Fn   func(taskstore.Store) error
}

// Checks 是全套检查，按声明序。业务一般用 [RunTaskStoreTests]；直接拿它是给「想自己控制
// 报告方式」或「想断言某条必须红」的场景。
func Checks() []Check {
	return []Check{
		{"CrossOwner_List_NotVisible", checkCrossOwnerList},
		{"FullPage_DoesNotThinOut", checkNoThinPages},
		{"TotalSize_IsPerOwner", checkTotalSizePerOwner},
		{"DeclaresOwnerPartitioning", checkDeclaresPartitioning},
	}
}

// RunTaskStoreTests 跑全套。
func RunTaskStoreTests(t *testing.T, factory TaskStoreFactory) {
	for _, c := range Checks() {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Fn(factory()); err != nil {
				t.Error(err)
			}
		})
	}
}

func ctxOf(owner string) context.Context {
	return compose_v2.WithOwner(context.Background(), owner)
}

func seed(st taskstore.Store, owner, prefix string, n int) error {
	ctx := ctxOf(owner)
	for i := 0; i < n; i++ {
		task := &a2atypes.Task{ID: a2atypes.TaskID(fmt.Sprintf("%s%03d", prefix, i)), ContextID: "shared"}
		if _, err := st.Create(ctx, task); err != nil {
			return fmt.Errorf("Create(%s): %w", owner, err)
		}
	}
	return nil
}

// testCrossOwnerList：列不到别人的。
func checkCrossOwnerList(st taskstore.Store) error {
	if err := seed(st, "tenant-A", "a", 5); err != nil {
		return err
	}
	if err := seed(st, "tenant-B", "b", 2); err != nil {
		return err
	}
	resp, err := st.List(ctxOf("tenant-B"), &a2atypes.ListTasksRequest{PageSize: 50})
	if err != nil {
		return fmt.Errorf("List: %w", err)
	}
	for _, task := range resp.Tasks {
		if len(task.ID) > 0 && task.ID[0] == 'a' {
			return fmt.Errorf("租户 B 列到了 A 的 task %q", task.ID)
		}
	}
	if len(resp.Tasks) != 2 {
		return fmt.Errorf("B 自己有 2 个 task，实际列到 %d 个", len(resp.Tasks))
	}
	return nil
}

// testNoThinPages 是这套里**最承重**的一条：满页不能因为过滤而变薄。
//
// 「变薄」本身就是信道——每页缺多少，就是这个窗口里别人有多少条。它跟「内容有没有泄漏」
// 是两件事：内容全挡住了，计数照样一条不落地漏出去（实测 4 次请求还原出精确条数）。
//
// 后端把 owner 下推成查询谓词时这条自动成立：分页发生在过滤**之后**，页要么满、要么是
// 最后一页。反过来，任何「先查后滤」的实现都过不了。
func checkNoThinPages(st taskstore.Store) error {
	if err := seed(st, "tenant-A", "a", 30); err != nil {
		return err
	}
	if err := seed(st, "tenant-B", "b", 12); err != nil {
		return err
	}

	const pageSize = 5
	token := ""
	page := 0
	for {
		resp, err := st.List(ctxOf("tenant-B"), &a2atypes.ListTasksRequest{
			PageSize: pageSize, PageToken: token,
		})
		if err != nil {
			return fmt.Errorf("List page %d: %w", page+1, err)
		}
		page++
		// 还有下一页 ⟹ 这一页必须是满的。不满 = 缺口 = 别人在这个窗口里有几条。
		if resp.NextPageToken != "" && len(resp.Tasks) < pageSize {
			return fmt.Errorf("第 %d 页只有 %d 条（PageSize=%d）却还有下一页——"+
				"缺的 %d 条就是别的租户在这个窗口里的条数，沿游标累加即得对方的 task 总数",
				page, len(resp.Tasks), pageSize, pageSize-len(resp.Tasks))
		}
		token = resp.NextPageToken
		if token == "" {
			break
		}
		if page > 20 {
			return fmt.Errorf("翻了 20 页还没到底，游标可能没推进")
		}
	}
	// B 有 12 条、每页 5 条 ⟹ 至少 3 页。**12 故意不是 5 的整数倍**：整数倍时最后一页
	// 恰好满，「先查后滤」的后端在某些排序下每页都可能是满的 ⟹ 断言漏报。
	if page < 3 {
		return fmt.Errorf("fixture 没搭对：12 条 / 每页 5 条应至少 3 页，实际 %d 页——"+
			"后端可能忽略了 PageSize", page)
	}
	return nil
}

// checkTotalSizePerOwner：TotalSize 是**自己的**总数，不是全局的、也不是本页条数。
//
// 它协议定义是「分页前的总数」。不分区的后端只能给出跨租户总数——那是个一次请求就到手的
// 计数预言机。
//
// ⚠️ **PageSize 必须小于 B 的条数**：曾经用 PageSize=50 / B=3，于是「分页前总数」和
// 「本页条数」数值恒相同 ⟹ 一个把 TotalSize 填成本页条数的后端照样过（stub 实测）。
// 而那正是我们自己的包装层在后端不声明分区时做的降级——这条断言对最可能出现的错误形态
// 是瞎的。现在 B=12 / PageSize=5，两者分得开。
func checkTotalSizePerOwner(st taskstore.Store) error {
	if err := seed(st, "tenant-A", "a", 30); err != nil {
		return err
	}
	if err := seed(st, "tenant-B", "b", 12); err != nil {
		return err
	}
	resp, err := st.List(ctxOf("tenant-B"), &a2atypes.ListTasksRequest{PageSize: 5})
	if err != nil {
		return fmt.Errorf("List: %w", err)
	}
	if resp.TotalSize != 12 {
		return fmt.Errorf("TotalSize=%d，B 自己有 12 个 task（本页只 %d 条）——"+
			"它该是**分页前**的总数，且只数自己的", resp.TotalSize, len(resp.Tasks))
	}
	return nil
}

// checkDeclaresPartitioning：合规的后端必须**说出来**。
//
// ⚠️ 没有这条时契约链是断的：业务写了个完全合规的后端、跑完这套全绿、上线，而
// `ownerScopedTaskStore.List` 仍然把它正确的 TotalSize 改写成本页条数——因为信任开关是
// [server.OwnerPartitionedTaskStore]，而「跑 conformance」和「包装层信任你」原本是两件事。
// 实测过：默认装配（SDK 自带后端，它真的按 user 下推）上 B 有 12 条、第一页 TotalSize=5，
// 裸后端直接查是 12。
func checkDeclaresPartitioning(st taskstore.Store) error {
	p, ok := st.(server.OwnerPartitionedTaskStore)
	if !ok {
		return fmt.Errorf("没有实现 server.OwnerPartitionedTaskStore——" +
			"前几条说明你已经按 owner 分区了，但包装层无从知道，会把你正确的 TotalSize " +
			"降级成本页条数。加一个 `func (s *YourStore) PartitionsByOwner() bool { return true }`")
	}
	if !p.PartitionsByOwner() {
		return fmt.Errorf("PartitionsByOwner() 返 false，但前几条显示你确实分区了——" +
			"返 false 会让包装层不信任你")
	}
	return nil
}
