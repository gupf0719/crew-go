package agent_v2

import (
	"context"
	"crew-go/compose_v2"
	"fmt"
	"sort"

	"crew-go/hook"
	"crew-go/schema"
)

// ParallelStepFn 是 ParallelAgent 编排的单分支节点函数。
//
// 入参：原始 input（所有分支拿到同一 input）
// 输出：本分支产出 message + name 标识（Merger 区分各分支用）
//
// **idempotent 约定**（同 BasicAgentModelFn）：业务 NodeFn 必须可重入。
type ParallelStepFn func(ctx context.Context, input *schema.Message) (*schema.Message, error)

// ParallelBranchResult 单分支结果——按 branch name 区分，Merger 汇聚用。
//
// **ISSUE-87 MED-3 fix**: OrigErr 加 `json:"-"` tag 不进 cp wire——之前 error
// 是 interface 类型, json.Unmarshal 不可逆 (实测 round-trip:
// `{"OrigErr":{}}` → "cannot unmarshal object into Go struct field
// ParallelBranchResult.OrigErr of type error"). 当前生产 worker 不填 OrigErr
// 不踩, 但任何未来 commit 让框架在 worker 失败路径填 OrigErr 即破 cp Restore
// (定时炸弹). 加 json:"-" 让 OrigErr 仅 in-process 透传, 不入 cp。
//
// **⚠️ ISSUE-87 audit MED-2 警示 — 跨 Resume 失败信号丢失**: OrigErr 加
// `json:"-"` 后 cp Restore 后**永远 nil**, 业务消费者 (typical: AggregatorToMerger
// `if r.OrigErr != nil { continue }` 跳过失败分支) 在 Resume 后**看不到失败**
// 信号 → silent 业务错。**业务想跨 Resume 保 失败信号必须把 error 编码到 Output
// (如 schema.AssistantMessage("[ERROR] ...")) 自己解析**, 不能依赖 OrigErr。
// 框架未来加 OrigErrMsg string 字段保 wire 安全 + 信号不丢 (待 ISSUE-87 长期评估)。
type ParallelBranchResult struct {
	Name    string          // 分支名
	Output  *schema.Message // 分支产出
	OrigErr error           `json:"-"` // 分支错（business err，InterruptError 由 framework 自动 cp save）— ISSUE-87 MED-3: error 接口 json 不可逆, 不入 cp; cp Restore 后永远 nil 见 godoc 警示
}

// ParallelMerger 把 N 个分支结果合并成 1 个 final message。
//
// 实现示例：
//
//	merger := func(ctx context.Context, results []ParallelBranchResult) (*schema.Message, error) {
//	    var sb strings.Builder
//	    for _, r := range results {
//	        if r.Output != nil { sb.WriteString(r.Output.Content + "\n") }
//	    }
//	    return schema.AssistantMessage(sb.String()), nil
//	}
type ParallelMerger func(ctx context.Context, results []ParallelBranchResult) (*schema.Message, error)

// parallelBranchDef 是 ParallelAgentBuilder 注册的分支定义。
type parallelBranchDef struct {
	Name string
	Fn   ParallelStepFn
}

// ParallelAgentBuilder 是 ParallelAgent 编排：N 分支并发跑同 input，结果合并。
//
// 拓扑：
//
//	compose_v2.StartNode → split → compose_v2.SendFrom([branch_b1, branch_b2, ...]) → 各 worker 并发
//	                                                          → finalize → compose_v2.EndNode
//
// 每分支拿到同 input（caller 通过 WithInitialWrites 写 parAgentOriginalCh）；
// 所有 worker 完成后 finalize 节点读 parAgentResultsCh slice 调 merger 出 final。
//
// **vs Sequential**：Sequential 串行 chain，Parallel 并行 fan-out。共享底层
// compose_v2.SendFrom + 超步 fan-out 原语。
//
// **vs PoC NewSequential 简单 helper**：本编排自带 hooks + 内置 channels + 自动
// Merger 调用，与 NewSequentialAgent 命名对称。
type ParallelAgentBuilder struct {
	name     string
	branches []parallelBranchDef
	merger   ParallelMerger
	hooks    hook.Hooks
}

// 内部 channels (ParallelAgent 编排专用)
var (
	parAgentResultsCh  = compose_v2.Append[ParallelBranchResult]("__par_agent_results__") // 各分支 append 结果
	parAgentOriginalCh = compose_v2.LastValue[*schema.Message]("__par_agent_original__")  // 原始 input（所有分支共享）
	parAgentFinalCh    = compose_v2.LastValue[*schema.Message]("__par_agent_final__")     // merger 产出 final
)

// NewParallelAgentBuilder 创建 ParallelAgentBuilder。
func NewParallelAgentBuilder() *ParallelAgentBuilder {
	return &ParallelAgentBuilder{}
}

// WithName 设 ParallelAgent 名。
func (b *ParallelAgentBuilder) WithName(name string) *ParallelAgentBuilder {
	b.name = name
	return b
}

// AddBranch 追加一个并发分支。name 作 cp ns + log 标识。
//
// **同名 branch 静默丢失防护**（R31 audit 视角 A HIGH-1 fix）：name 在 branches
// 内重复时 panic——Compile 时 `AddNode(compose_v2.NodeID("worker_"+name), fn)` map 写入会
// 静默覆盖前一个 fn，导致前一个 branch 永远不跑；同时 agent_v2 上层 branchFn 用
// `JoinNS(parentNS, name)` push graphNS，同名 branch 共用同 childNS 让 cp 互覆。
// fail-fast 比 silent loss 安全。
func (b *ParallelAgentBuilder) AddBranch(name string, fn ParallelStepFn) *ParallelAgentBuilder {
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		panic(fmt.Sprintf("ParallelAgentBuilder.AddBranch: name %q invalid for ns: %v", name, err))
	}
	if fn == nil {
		panic(fmt.Sprintf("ParallelAgentBuilder.AddBranch: name %q fn must not be nil", name))
	}
	for _, existing := range b.branches {
		if existing.Name == name {
			panic(fmt.Sprintf("ParallelAgentBuilder.AddBranch: duplicate branch name %q (会让前一个分支静默被 worker_xxx map 覆盖 + cp ns 互覆)", name))
		}
	}
	b.branches = append(b.branches, parallelBranchDef{Name: name, Fn: fn})
	return b
}

// WithMerger 设 Merger 函数——把 N 分支结果合并成 1 final message。
//
// 不设时框架默认拼装 "branch_name: content\n..." 简单文本聚合。
func (b *ParallelAgentBuilder) WithMerger(m ParallelMerger) *ParallelAgentBuilder {
	b.merger = m
	return b
}

// WithAgentHooks 设业务 hook。
func (b *ParallelAgentBuilder) WithAgentHooks(h hook.Hooks) *ParallelAgentBuilder {
	b.hooks = h
	return b
}

// Compile 编译为 compose_v2.Graph。
//
// 拓扑：
//  1. split 节点 compose_v2.SendFrom N 个 worker target
//  2. 每 worker 节点跑业务 ParallelStepFn → append 到 parAgentResultsCh
//  3. 全部 worker 完成后 finalize 节点读 parAgentResultsCh → 调 Merger → 写 final
//
// **业务 hook (hook.Hooks) 协议** (8-method + ISSUE-82 更新):
//   - OnAgentStart 整个调用 1 次, 由 agent_v2.ParallelAgent.Run/Resume top-level fire
//     (不再在 split 节点); OnAgentEnd 由 Run/Resume defer fire 1 次 (单一所有权)
//   - per-branch agent-level hook 不支持 (agent-level 是整个调用语义, 非分支语义)
//   - 业务想统计 N branch 实际跑数: 用 OrchHooks.OnNodeStart filter `worker_*` 节点
func (b *ParallelAgentBuilder) Compile() (*compose_v2.Graph, error) {
	if len(b.branches) == 0 {
		return nil, fmt.Errorf("parallel_agent: no branches; call AddBranch")
	}

	hooks := b.hooks
	// nil 防护: 业务直接用 NewParallelAgentBuilder() 不调 WithAgentHooks 时,
	// hooks 是 typed nil (Hooks interface 持非 nil type tag + nil value)。下游 workerFn /
	// finalizeFn 业务 error 路径会调 `hooks.OnError(ctx, info, err)`——不防护则 nil deref
	// panic 整个 Run。与 7 编排 (BasicAgent / Sequential / Loop / Routing / Reflect /
	// Handoff / Parallel) 统一 isFrameworkNil 协议.
	if isFrameworkNil(hooks) {
		hooks = hook.NoOpHooks{}
	}
	name := b.name
	if name == "" {
		name = "parallel_agent"
	}
	merger := b.merger
	// 默认 merger 是**纯拼接**（见 defaultParallelMerger）：产出的每个成分都已经以分支
	// 自己的归属进了历史，聚合那条是可重算的。不标的话历史里成分和聚合并存，模型读回时
	// 同一份内容看到两遍。
	//
	// **只标默认的**：自定义 merger 可能是真合成（比如拿 LLM 把各分支结论总结成新结论），
	// 那种不可重算、必须留档。业务想标自己在 merger 里调 `MarkDerivedInput`。
	mergerIsDefault := merger == nil
	if merger == nil {
		merger = defaultParallelMerger
	}

	bld := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(bld, parAgentResultsCh)
	compose_v2.WithChannel(bld, parAgentOriginalCh)
	compose_v2.WithChannel(bld, parAgentFinalCh)

	// split 节点：compose_v2.SendFrom N 个 worker target（OnAgentStart 已移到 wrapper top-level，
	// 8-method 协议下整个调用 1 次，本节点不再触发 hook）。
	splitFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		dirs := make([]compose_v2.SendDirective, 0, len(b.branches))
		for _, br := range b.branches {
			dirs = append(dirs, compose_v2.SendDirective{Target: compose_v2.NodeID("worker_" + br.Name)})
		}
		compose_v2.SendFrom(ctx, dirs...)
		return nil, nil
	}
	bld.AddNode("split", splitFn)
	bld.AddEdge(compose_v2.StartNode, "split")

	// N 个 worker 节点
	for _, br := range b.branches {
		br := br
		workerFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			input, _ := compose_v2.Get(chs, parAgentOriginalCh)
			if input == nil {
				return nil, fmt.Errorf("parallel_agent: branch %q: 没拿到原始 input", br.Name)
			}
			out, err := br.Fn(ctx, input)
			if err != nil {
				// InterruptError 是控制流不上报 OnError (R7 Bug 3)
				// **R25 audit MED-4 fix**：ctx.Err() (cancel/deadline) 同属控制流——
				// 业务 br.Fn 内 `select { case <-ctx.Done(): return ctx.Err() }` 是
				// Go 标准范式，原代码上报 "branch_error" 让监控误判 ctx cancel = 业务错。
				// 与 finalize merger 路径（line 200，R24 LOW-2 已加双守卫）对称。
				if _, isInt := IsInterrupt(err); !isInt && ctx.Err() == nil {
					info := &hook.AgentInfo{Name: name, Type: "parallel", Round: 0}
					hooks.OnError(ctx, info, &schema.CommError{
						Code: "branch_error", Message: err.Error(),
					})
				}
				return nil, fmt.Errorf("branch %q: %w", br.Name, err)
			}
			return []compose_v2.ChannelWrite{
				compose_v2.Write(parAgentResultsCh, []ParallelBranchResult{
					{Name: br.Name, Output: out},
				}),
			}, nil
		}
		bld.AddNode(compose_v2.NodeID("worker_"+br.Name), workerFn)
		bld.AddEdge(compose_v2.NodeID("worker_"+br.Name), "finalize")
	}

	// finalize 节点：读全部 results，调 merger
	finalizeFn := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		results, _ := compose_v2.Get(chs, parAgentResultsCh)
		// 按分支 Name 稳定排序——消除"部分失败 Resume 只重跑失败分支"导致的 results
		// 顺序抖动（成功分支先 commit、失败分支推迟 append）。对所有 merger 生效（含自定义），
		// 让 merge 输入顺序确定。按分支 Name 排序的处理只在 merge 阶段做、不下沉 compose_v2.Append
		// channel（保 channel 通用性 + per-branch delta 优势，
		// 详见 three-way-comparison/state-model.md 决策 6）。
		sort.SliceStable(results, func(i, j int) bool { return results[i].Name < results[j].Name })
		final, err := merger(ctx, results)
		if err == nil && final != nil && mergerIsDefault {
			final = MarkDerivedInput(final) // ⚠️ 返拷贝不原地改，丢返回值就是 no-op
		}
		if err != nil {
			// **R24 audit LOW-2 fix**：对称 worker 路径（line 170-176）——
			// InterruptError 是控制流不上报 OnError；ctx.Err 同理（cancel/deadline）。
			// 业务 ParallelMerger / Aggregator 完全可能抛 InterruptError 做"merge 前
			// 用户审批最终聚合内容"等 HITL 场景，原代码无差别上报 OnError 让监控误判
			// 业务错；并 wrap "parallel_agent merger:" 让 caller 解析需 errors.As 多
			// 一层（虽 IsInterrupt 已透传 wrap chain，但语义不清晰）。
			if _, isInt := IsInterrupt(err); !isInt && ctx.Err() == nil {
				info := &hook.AgentInfo{Name: name, Type: "parallel", Round: 0}
				hooks.OnError(ctx, info, &schema.CommError{
					Code: "merger_error", Message: err.Error(),
				})
			}
			return nil, fmt.Errorf("parallel_agent merger: %w", err)
		}
		// 四编排 review PA-M1 fix：业务 merger 返 (nil, nil) 归一化为空 assistant
		//（对齐 defaultParallelMerger 空 results 行为）。否则 final=nil → 上层
		// ParallelAgent 不 emit 自己的 ExitEvent，嵌套消费方 FinalMessage(events)
		// 反向扫会捞到**最后提交分支**的 ExitEvent（lane commit 后进共享主线），
		// 把单分支输出静默当聚合结果向下游流。
		if final == nil {
			final = schema.AssistantMessage("")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(parAgentFinalCh, final)}, nil
	}
	bld.AddNode("finalize", finalizeFn)
	bld.AddEdge("finalize", compose_v2.EndNode)

	return bld.Compile()
}

// defaultParallelMerger 默认按 branch name 拼接文本。
func defaultParallelMerger(_ context.Context, results []ParallelBranchResult) (*schema.Message, error) {
	if len(results) == 0 {
		return schema.AssistantMessage(""), nil
	}
	out := ""
	for _, r := range results {
		if r.Output != nil {
			if out != "" {
				out += "\n\n"
			}
			out += "[" + r.Name + "] " + r.Output.Content
		}
	}
	return schema.AssistantMessage(out), nil
}

// ParAgentOriginalChannel 暴露内部 parAgentOriginalCh 让 caller 写入原始 input。
func ParAgentOriginalChannel() *compose_v2.Channel[*schema.Message] {
	return parAgentOriginalCh
}

// ParallelAgentFinalMessage 从 cm 拿 merger 产出的 final message。
func ParallelAgentFinalMessage(chs compose_v2.Channels) *schema.Message {
	out, _ := compose_v2.Get(chs, parAgentFinalCh)
	return out
}
