package taskstore_conformance

import (
	"context"
	"sort"
	"strings"
	"testing"

	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// ---- 四种后端形态，用来验四条检查各自有独占的靶子 ----

// compliantStore：完全合规——owner 下推成查询谓词，分页发生在过滤之后。
func compliantStore() taskstore.Store {
	return declaring{taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(ctx context.Context) (string, error) {
			return compose_v2.GetOwner(ctx), nil
		},
	})}
}

type declaring struct{ taskstore.Store }

func (declaring) PartitionsByOwner() bool { return true }

// naiveStore：完全不分区（Authenticator 返常量）——内容和计数全泄漏。
func naiveStore() taskstore.Store {
	return taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "everyone", nil },
	})
}

// filterAfterPaginateStore：**业务最可能交出来的错误形态**——先无差别查、分页，再逐条滤。
// 内容不泄漏（滤干净了）、TotalSize 也对，但页会变薄。
//
// ⚠️ 自检必须量这个**裸后端**，不是「不分区后端 + 我们的包装层」：conformance 跑的是
// factory 直接给的对象，业务提交的也是这个。曾经量错了对象，于是自检声称守护的那条断言
// 在业务的形态上恒真。
type filterAfterPaginateStore struct {
	taskstore.Store
	all taskstore.Store // 不分区的全量视图
}

func newFilterAfterPaginate() taskstore.Store {
	all := naiveStore()
	return &filterAfterPaginateStore{Store: all, all: all}
}

func (s *filterAfterPaginateStore) List(ctx context.Context, req *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	resp, err := s.all.List(ctx, req) // 先无差别查 + 分页
	if err != nil || resp == nil {
		return resp, err
	}
	owner := compose_v2.GetOwner(ctx)
	var kept []*a2atypes.Task
	for _, task := range resp.Tasks { // 再逐条滤 ⟹ 页变薄
		if strings.HasPrefix(string(task.ID), ownerPrefix(owner)) {
			kept = append(kept, task)
		}
	}
	out := *resp
	out.Tasks = kept
	out.TotalSize = ownerTotal(s.all, owner)
	return &out, nil
}

func (s *filterAfterPaginateStore) PartitionsByOwner() bool { return true }

// pageCountTotalStore：分页/过滤全对，只有 TotalSize 填成**本页条数**——正是我们自己的
// 包装层在后端不声明分区时做的降级。
type pageCountTotalStore struct{ taskstore.Store }

func newPageCountTotal() taskstore.Store { return &pageCountTotalStore{compliantStore()} }

func (s *pageCountTotalStore) List(ctx context.Context, req *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	resp, err := s.Store.List(ctx, req)
	if err != nil || resp == nil {
		return resp, err
	}
	out := *resp
	out.TotalSize = len(resp.Tasks)
	return &out, nil
}

func (s *pageCountTotalStore) PartitionsByOwner() bool { return true }

func ownerPrefix(owner string) string {
	if owner == "tenant-A" {
		return "a"
	}
	return "b"
}

func ownerTotal(st taskstore.Store, owner string) int {
	resp, err := st.List(ctxOf(owner), &a2atypes.ListTasksRequest{PageSize: 100})
	if err != nil || resp == nil {
		return 0
	}
	n := 0
	for _, task := range resp.Tasks {
		if strings.HasPrefix(string(task.ID), ownerPrefix(owner)) {
			n++
		}
	}
	return n
}

// TestChecks_EachHasItsOwnTarget 验四条检查**各自有独占的靶子**，且自检消费的是
// [Checks] 里那份**真断言**（不是拷贝）。
//
// ⚠️ 这条替换了一版重抄翻页循环的自检。那一版把 checkNoThinPages 的断言**整块删掉**之后
// 照样全绿——它守护的是自己的拷贝。判别式：**自检有没有真的调用被测的那个函数？**
func TestChecks_EachHasItsOwnTarget(t *testing.T) {
	// 期望矩阵：形态 → 应该红的那些检查
	want := map[string][]string{
		"合规":             {},
		"完全不分区":          {"CrossOwner_List_NotVisible", "TotalSize_IsPerOwner", "DeclaresOwnerPartitioning"},
		"先查后滤":           {"FullPage_DoesNotThinOut"},
		"TotalSize给本页条数": {"TotalSize_IsPerOwner"},
	}
	factories := map[string]TaskStoreFactory{
		"合规":             compliantStore,
		"完全不分区":          naiveStore,
		"先查后滤":           newFilterAfterPaginate,
		"TotalSize给本页条数": newPageCountTotal,
	}

	for shape, factory := range factories {
		var failed []string
		for _, c := range Checks() { // ← 消费真断言
			if err := c.Fn(factory()); err != nil {
				failed = append(failed, c.Name)
			}
		}
		sort.Strings(failed)
		expect := append([]string(nil), want[shape]...)
		sort.Strings(expect)
		if strings.Join(failed, ",") != strings.Join(expect, ",") {
			t.Errorf("【%s】红的检查 = %v，期望 %v", shape, failed, expect)
		}
	}
}
