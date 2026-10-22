package agent_v2

import "sort"

// 各编排交出自己的直接子执行体，供 startHistoryForOrchestration 借历史配置。借配置的那一趟（borrowHistoryConfig）
// 遇到子执行体也是编排时接着往下走，所以整棵静态子树都算配置来源。
//
// # 为什么单独抽出来
//
// 每个编排的 `Run` 和 `Resume` **都要**建写入器，两处必须看到同一份子列表。原来只有 `Run`
// 里有这段（内联的），`Resume` 一处都没有——中断之后 Resume 那一段的历史整段丢：图重跑到
// 某一步时边界补记去 ctx 里取写入器，取到 nil，直接 return。实测 `Sequential(外部, 本地)`
// 里外部执行体抛一次中断，无中断基线 3 条，中断 + Resume 后只剩 1 条，用户原话和外部产出
// 双双永久缺席。抽成方法是为了让「两个入口对称」这件事有一个单一来源，不靠复制粘贴维持。
//
// # 被摘除的内部决策 LLM 也算配置来源
//
// RoutingAgent 的 classifier、ReflectAgent 的 evaluator / reflectorAgent 都跑在
// `WithoutHistoryCtx` 下——它们**自己的产出**进不了历史。但这里问的不是「谁会写」，
// 是「从谁那儿借配置」：`WithHistory(store, owner)` 声明的是**这场会话属于哪个
// store / 哪个 owner**，跟声明者自己写不写无关。
//
// 收进来有两个实际作用：(1) 编排里只有内部决策 LLM 是 BasicAgent 时（actor 是外部
// 执行体），它是唯一的配置来源，不收就建不起写入器、边界补记全程 no-op；(2) 配置
// 冲突检查覆盖到它们——不收的话业务把 reflectorAgent 配到另一个租户是**静默**的，
// 而配到 evaluator 上会报错，同类成员两种行为。
//
// # 顺序必须确定
//
// HandoffAgent 和 PlanAgent 的子是 `map[string]Agent`，`for range` 出来的顺序每次都不同。
// 而 `startHistoryForOrchestration` 取的是**第一个配了历史的**。配置真的冲突时它直接报错
//（不静默挑一个），所以排序不再是「防落错租户」——那道防线在报错那儿。排序留下是为两件事：
//
//  1. **报错信息跨次稳定**：冲突时报的是「谁跟谁冲突」，迭代顺序随机会让同一份配置每次
//     报不同的两个 owner，日志和告警对不上。
//  2. **兜住比不出来的那一档**：`sameHistoryStore` 对不可比较的 store 值类型只能按类型
//     判「算同一个」，那种情况下仍是取迭代到的第一个——排序让它至少确定。

func (s *SequentialAgent) historySubs() []Agent {
	subs := make([]Agent, 0, len(s.steps))
	for _, st := range s.steps {
		subs = append(subs, st.Agent)
	}
	return subs
}

func (p *ParallelAgent) historySubs() []Agent {
	subs := make([]Agent, 0, len(p.branches))
	for _, br := range p.branches {
		subs = append(subs, br.Agent)
	}
	return subs
}

func (r *RoutingAgent) historySubs() []Agent {
	subs := make([]Agent, 0, len(r.branches)+2)
	for _, br := range r.branches {
		subs = append(subs, br.Agent)
	}
	if r.fallback != nil {
		subs = append(subs, r.fallback)
	}
	if r.classifier != nil {
		subs = append(subs, r.classifier) // 内部决策 LLM，见文件头「被摘除的也算配置来源」
	}
	return subs
}

func (l *LoopAgent) historySubs() []Agent {
	if l.body == nil {
		return nil
	}
	return []Agent{l.body}
}

func (h *HandoffAgent) historySubs() []Agent {
	names := make([]string, 0, len(h.agents))
	for n := range h.agents {
		names = append(names, n)
	}
	sort.Strings(names) // map 迭代顺序随机——见文件头
	subs := make([]Agent, 0, len(names))
	for _, n := range names {
		subs = append(subs, h.agents[n])
	}
	return subs
}

func (p *PlanAgent) historySubs() []Agent {
	names := make([]string, 0, len(p.subAgents))
	for n := range p.subAgents {
		names = append(names, n)
	}
	sort.Strings(names) // map 迭代顺序随机——见文件头
	subs := make([]Agent, 0, len(names)+1)
	for _, n := range names {
		subs = append(subs, p.subAgents[n])
	}
	if p.executor != nil {
		subs = append(subs, p.executor)
	}
	return subs
}

func (r *ReflectAgent) historySubs() []Agent {
	subs := make([]Agent, 0, 3)
	if r.actor != nil {
		subs = append(subs, r.actor)
	}
	// evaluator / reflectorAgent 是内部决策 LLM，产出被 WithoutHistoryCtx 摘在历史外，
	// 但配置照算——见文件头「被摘除的也算配置来源」。
	if r.evaluator != nil {
		subs = append(subs, r.evaluator)
	}
	if r.reflectorAgent != nil {
		subs = append(subs, r.reflectorAgent)
	}
	return subs
}
