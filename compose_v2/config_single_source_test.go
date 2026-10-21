package compose_v2_test

// 持久化配置单源化哨兵（graph_loop.go Run 入口的「RunOption 缺省回落 ctx」）。
//
// 钉的缺陷形态：根层只认 RunOption、子图层读 ctx ⟹ 业务只用 ctx 形态（
// WithCheckpointerCtx / WithSessionIDCtx）调裸 Run 时，根不存档、子图照存——
// 半持久化树。后果链：根槽恒空 ⟹ Run2 新铸任务代号 ⟹ 子图挂起档旧代号对不上 ⟹
// 用户已答的问题被丢弃、同一问题换个 ID 再问（实测 gotAnswers=0、asked=2）。
//
// sanity-revert：删掉 graph_loop.go 的回落块，TestConfigSingleSource_CtxOnlyRun
// 在「Run1 后根 cp 必须存在」处红，直接指向半持久化树。

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// buildParentWithAskingSub 造「父图（tick→sub→End）+ 子图（ask 节点 Ask）」的最小
// 嵌套 fixture。返回父图与观测点。
func buildParentWithAskingSub(t *testing.T) (g *compose_v2.Graph, asked *[]string, gotAnswers *[]string) {
	t.Helper()
	askedIDs := []string{}
	answers := []string{}

	subBld := compose_v2.NewBuilder()
	subBld.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗?"})
		if err != nil {
			if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
				askedIDs = append(askedIDs, ie.Query.InterruptID)
			}
			return nil, err
		}
		answers = append(answers, ans.Content)
		return nil, nil
	})
	subBld.AddEdge(compose_v2.StartNode, "ask")
	sub, err := subBld.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}

	bld := compose_v2.NewBuilder()
	tick := compose_v2.LastValue[int]("tick")
	bld = compose_v2.WithChannel(bld, tick)
	bld.AddNode("tick", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(tick, 1)}, nil
	})
	bld.AddSubgraphNode("sub", sub)
	bld.AddEdge(compose_v2.StartNode, "tick")
	bld.AddEdge("tick", "sub")
	g, err = bld.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}
	return g, &askedIDs, &answers
}

// TestConfigSingleSource_CtxOnlyRun：checkpointer 与 sid 都只经 ctx 给（业务文档化
// 的 caller-facing 形态），裸 compose_v2.Run 一个 RunOption 都不传。
//
// 要求：根层必须与子图层看到同一份持久化配置——Run1 中断后**根 cp 必须存在**且与
// 子档同任务代号；Run2 凭 ctx 配置 auto-load 续跑，答案**恰好兑付一次**、不重问。
func TestConfigSingleSource_CtxOnlyRun(t *testing.T) {
	g, asked, gotAnswers := buildParentWithAskingSub(t)

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "cfg-single-source-sid"
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithSessionIDCtx(ctx, sid)

	e1 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	ie, ok := compose_v2.IsInterrupt(e1)
	if !ok {
		t.Fatalf("Run1 该中断, got %v", e1)
	}
	if len(*asked) != 1 {
		t.Fatalf("Run1 该恰好提问 1 次, got %d (%v)", len(*asked), *asked)
	}

	// ── 半持久化树哨兵：根 cp 必须存在（回落缺失时这里恒 nil）──
	rootCp, err := cper.LoadNS(ctx, sid, "")
	if err != nil {
		t.Fatalf("LoadNS root: %v", err)
	}
	if rootCp == nil {
		t.Fatalf("Run1 中断后根 cp 必须存在——根层没看到 ctx 里的 checkpointer/sid，半持久化树")
	}
	if !rootCp.HasResumableWork() {
		t.Fatalf("根 cp 该有可续内容, got frontier=%d pending=%d", len(rootCp.CurrentFrontier), len(rootCp.PendingInterrupts))
	}
	if rootCp.TaskID == "" {
		t.Fatalf("根 cp 该带任务代号")
	}
	subCp, err := cper.LoadNS(ctx, sid, "sub")
	if err != nil {
		t.Fatalf("LoadNS sub: %v", err)
	}
	if subCp == nil {
		t.Fatalf("子图 cp 该存在")
	}
	if subCp.TaskID != rootCp.TaskID {
		t.Fatalf("根/子任务代号该一致: root=%q sub=%q", rootCp.TaskID, subCp.TaskID)
	}

	// ── Run2：仍然 ctx-only，凭 auto-load 续跑并兑付答案 ──
	e2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("批准"),
		}))
	if e2 != nil {
		t.Fatalf("Run2 该成功续跑, got %v", e2)
	}
	if len(*gotAnswers) != 1 || (*gotAnswers)[0] != "批准" {
		t.Fatalf("答案该恰好兑付一次且内容对上, got %v", *gotAnswers)
	}
	if len(*asked) != 1 {
		t.Fatalf("Run2 不该重问（重问 = 身份对不上、旧答案被丢弃）, asked=%v", *asked)
	}
	pending, err := cper.ListPending(ctx, sid)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("成功收尾后不该有残留挂起档, got %d", len(pending))
	}
}

// TestConfigSingleSource_GraphNSFallback：ns 也在回落之列——ctx push 了 ns 而
// RunOption 没给时，根档必须落在 ctx ns 下（否则根落 ""、子落 "ctxNS/..."，
// 根不是子的 ns 祖先，树自身错位）。
func TestConfigSingleSource_GraphNSFallback(t *testing.T) {
	g, asked, _ := buildParentWithAskingSub(t)

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "cfg-ns-fallback-sid"
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithSessionIDCtx(ctx, sid)
	ctx = compose_v2.WithGraphNS(ctx, "tenant-a")

	e1 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	if _, ok := compose_v2.IsInterrupt(e1); !ok {
		t.Fatalf("Run1 该中断, got %v", e1)
	}
	if len(*asked) != 1 {
		t.Fatalf("该恰好提问 1 次, got %d", len(*asked))
	}

	rootCp, err := cper.LoadNS(ctx, sid, "tenant-a")
	if err != nil {
		t.Fatalf("LoadNS tenant-a: %v", err)
	}
	if rootCp == nil {
		t.Fatalf("根档该落在 ctx push 的 ns 下（tenant-a）——ns 没回落")
	}
	// 子档在根 ns 之下（树形一致），且不落在裸 "" 树里
	all, err := cper.LoadAllInSession(ctx, sid)
	if err != nil {
		t.Fatalf("LoadAllInSession: %v", err)
	}
	var subUnderTenant bool
	for _, c := range all {
		if c.NS == "" {
			t.Fatalf("不该有档落在裸 \"\" ns（根/子分家）, got ns=%q", c.NS)
		}
		if c.NS != "tenant-a" && strings.HasPrefix(c.NS, "tenant-a") {
			subUnderTenant = true
		}
	}
	if !subUnderTenant {
		t.Fatalf("子档该落在 tenant-a 子树下, got %d 份档", len(all))
	}
}
