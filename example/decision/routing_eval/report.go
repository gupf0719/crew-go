package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// runResult 是一个「系统 × 条件 × 轮次」的全部判定，Records 按样本 ID 索引。
type runResult struct {
	System  string
	Cond    string
	Pass    int
	Records map[string]record
}

func (r runResult) label() string {
	s := r.System + " / " + r.Cond
	if r.Pass > 1 {
		s += fmt.Sprintf(" 第%d轮", r.Pass)
	}
	return s
}

// roles 标出报告里要做配对比较的几个系统；没跑的为空。
type roles struct {
	jev     string
	bailian string
	qwen    string
	ref     string
}

// deciders 是跑了的决策模型系统，按 Jev、百炼的顺序，带报告里的称呼。
func (rl roles) deciders() []struct{ key, title string } {
	var out []struct{ key, title string }
	if rl.jev != "" {
		out = append(out, struct{ key, title string }{rl.jev, "Jev"})
	}
	if rl.bailian != "" {
		out = append(out, struct{ key, title string }{rl.bailian, "百炼"})
	}
	return out
}

type reporter struct {
	cases []evalCase
	byID  map[string]evalCase
	runs  []runResult
	roles roles
	b     strings.Builder
}

// renderReport 按子集分别出报告：三个子集的难度与形态不同，混在一起算准确率会掩盖差别。
func renderReport(cases []evalCase, runs []runResult, rl roles, review bool, preface string) string {
	var b strings.Builder
	b.WriteString(preface)
	for _, t := range tiers {
		sub := make([]evalCase, 0, len(cases))
		for _, c := range cases {
			if c.Tier == t.ID {
				sub = append(sub, c)
			}
		}
		if len(sub) == 0 {
			continue
		}
		b.WriteString(newReporter(sub, runs, rl).render(t, review))
	}
	return b.String()
}

// newReporter 只保留在这批样本上有结果的执行单元，例如真实会话集没有英文，英文条件
// 的列就不出现在它的报告里。
func newReporter(cases []evalCase, runs []runResult, rl roles) *reporter {
	byID := make(map[string]evalCase, len(cases))
	for _, c := range cases {
		byID[c.ID] = c
	}
	kept := make([]runResult, 0, len(runs))
	for _, run := range runs {
		for _, c := range cases {
			if _, ok := run.Records[c.ID]; ok {
				kept = append(kept, run)
				break
			}
		}
	}
	return &reporter{cases: cases, byID: byID, runs: kept, roles: rl}
}

func (r *reporter) find(system, cond string, pass int) (runResult, bool) {
	for _, run := range r.runs {
		if run.System == system && run.Cond == cond && run.Pass == pass {
			return run, true
		}
	}
	return runResult{}, false
}

// correct 报一条样本判对没有；ok=false 表示没有结果或是基础设施错误，不参与统计。
func (r *reporter) correct(run runResult, id string) (right bool, ok bool) {
	rec, found := run.Records[id]
	if !found || rec.Err != "" {
		return false, false
	}
	return rec.Pred == r.byID[id].Gold, true
}

func (r *reporter) p(format string, args ...any) {
	fmt.Fprintf(&r.b, format, args...)
}

func (r *reporter) render(t tier, review bool) string {
	r.p("# 决策模型中文路由实测：%s\n\n", t.Title)
	r.p("%s。样本 %d 条：", t.Note, len(r.cases))
	parts := make([]string, 0, 4)
	for _, sc := range r.scenariosPresent() {
		n := 0
		for _, c := range r.cases {
			if c.Scenario == sc.ID {
				n++
			}
		}
		parts = append(parts, fmt.Sprintf("%s %d 个分支 %d 条", sc.Title, len(sc.Routes), n))
	}
	r.p("%s。\n\n", strings.Join(parts, "，"))
	r.overview()
	r.pairs()
	r.byScenario()
	r.byTag()
	r.confidence()
	r.hybrid()
	r.consistency()
	for _, d := range r.roles.deciders() {
		r.mistakes(d.key, d.title)
	}
	r.mistakes(r.roles.qwen, "qwen 基线")
	if review && r.roles.ref != "" {
		r.labelReview()
	}
	return r.b.String()
}

func (r *reporter) overview() {
	r.p("## 概况\n\n")
	r.p("| 系统 / 条件 | 有效样本 | 基础设施错误 | 准确率 | 95%% 置信区间 | 决策延迟 p50 / p90 | 端到端 p50 / p90 | 平均输入 token | 实际模型 |\n")
	r.p("|---|---|---|---|---|---|---|---|---|\n")
	for _, run := range r.runs {
		var k, n, errs, inTok int
		decide := make([]float64, 0, len(r.cases))
		total := make([]float64, 0, len(r.cases))
		models := map[string]int{}
		for _, c := range r.cases {
			rec, found := run.Records[c.ID]
			if !found {
				continue
			}
			if rec.Err != "" {
				errs++
				continue
			}
			n++
			if rec.Pred == c.Gold {
				k++
			}
			inTok += rec.InTokens
			decide = append(decide, rec.DecideMS)
			total = append(total, rec.TotalMS)
			if rec.Model != "" {
				models[rec.Model]++
			}
		}
		lo, hi := wilson(k, n)
		r.p("| %s | %d | %d | %s | %.1f%% – %.1f%% | %s / %s ms | %s / %s ms | %s | %s |\n",
			run.label(), n, errs, pct(k, n), lo*100, hi*100,
			fmtMS(percentile(decide, 50)), fmtMS(percentile(decide, 90)),
			fmtMS(percentile(total, 50)), fmtMS(percentile(total, 90)),
			fmtMean(inTok, n), joinCounts(models))
	}
	r.p("\n决策延迟：决策模型（Jev、百炼）是那次 HTTP 调用（含重试），qwen 是 classifier 的模型调用。端到端是整个 RoutingAgent.Run。\n\n")
}

func (r *reporter) pairs() {
	type pair struct {
		a, b   [2]string
		reason string
	}
	j, b, q, f := r.roles.jev, r.roles.bailian, r.roles.qwen, r.roles.ref
	candidates := []pair{
		{[2]string{j, "zh"}, [2]string{q, "zh"}, "主比较：中文场景下 Jev 与 qwen 基线"},
		{[2]string{b, "zh"}, [2]string{q, "zh"}, "主比较：中文场景下百炼与 qwen 基线"},
		{[2]string{b, "zh"}, [2]string{j, "zh"}, "两家决策模型：百炼与 Jev"},
		{[2]string{j, "zh"}, [2]string{j, "en"}, "Jev 的语言效应：同一批样本的中文与英文"},
		{[2]string{b, "zh"}, [2]string{b, "en"}, "百炼的语言效应：同一批样本的中文与英文"},
		{[2]string{j, "zh-en"}, [2]string{j, "zh"}, "Jev 的缓解手段：中文原文配英文指令与描述"},
		{[2]string{b, "zh-en"}, [2]string{b, "zh"}, "百炼的缓解手段：中文原文配英文指令与描述"},
		{[2]string{j, "zh-default"}, [2]string{j, "zh"}, "Jev 不定制：decision 包默认的英文问题描述配中文分支描述"},
		{[2]string{b, "zh-default"}, [2]string{b, "zh"}, "百炼不定制：decision 包默认的英文问题描述配中文分支描述"},
		{[2]string{q, "zh"}, [2]string{q, "en"}, "qwen 的语言效应，也检验英文译文难度是否相当"},
		{[2]string{j, "zh"}, [2]string{f, "zh"}, "Jev 与参考大模型"},
		{[2]string{b, "zh"}, [2]string{f, "zh"}, "百炼与参考大模型"},
		{[2]string{q, "zh"}, [2]string{f, "zh"}, "qwen 基线与参考大模型"},
	}
	rows := make([]string, 0, len(candidates))
	for _, pc := range candidates {
		ra, okA := r.find(pc.a[0], pc.a[1], 1)
		rb, okB := r.find(pc.b[0], pc.b[1], 1)
		if !okA || !okB || pc.a[0] == "" || pc.b[0] == "" {
			continue
		}
		var n, ka, kb, onlyA, onlyB int
		for _, c := range r.cases {
			ca, oka := r.correct(ra, c.ID)
			cb, okb := r.correct(rb, c.ID)
			if !oka || !okb {
				continue
			}
			n++
			if ca {
				ka++
			}
			if cb {
				kb++
			}
			if ca && !cb {
				onlyA++
			}
			if cb && !ca {
				onlyB++
			}
		}
		rows = append(rows, fmt.Sprintf("| %s | %s | %d | %s | %s | %d | %d | %.3g | %s |\n",
			ra.label(), rb.label(), n, pct(ka, n), pct(kb, n), onlyA, onlyB, mcnemarExact(onlyA, onlyB), pc.reason))
	}
	if len(rows) == 0 {
		return
	}
	r.p("## 配对比较\n\n")
	r.p("同一批样本上两两比较，只看两边都有有效结果的样本。p 值是精确 McNemar 检验，只用两边判定不一致的样本。\n\n")
	r.p("| A | B | 共同样本 | A 准确率 | B 准确率 | A 对 B 错 | A 错 B 对 | p 值 | 看什么 |\n")
	r.p("|---|---|---|---|---|---|---|---|---|\n")
	for _, row := range rows {
		r.p("%s", row)
	}
	r.p("\n")
}

// scenariosPresent 按定义顺序返回这批样本涉及的场景。
func (r *reporter) scenariosPresent() []scenario {
	present := make(map[string]bool, len(scenarios))
	for _, c := range r.cases {
		present[c.Scenario] = true
	}
	out := make([]scenario, 0, len(present))
	for _, sc := range scenarios {
		if present[sc.ID] {
			out = append(out, sc)
		}
	}
	return out
}

func (r *reporter) firstPassRuns() []runResult {
	out := make([]runResult, 0, len(r.runs))
	for _, run := range r.runs {
		if run.Pass == 1 {
			out = append(out, run)
		}
	}
	return out
}

func (r *reporter) byScenario() {
	runs := r.firstPassRuns()
	r.p("## 按场景\n\n")
	r.p("| 场景 |")
	for _, run := range runs {
		r.p(" %s |", run.label())
	}
	r.p("\n|---|%s\n", strings.Repeat("---|", len(runs)))
	for _, sc := range r.scenariosPresent() {
		r.p("| %s |", sc.Title)
		for _, run := range runs {
			var k, n int
			for _, c := range r.cases {
				if c.Scenario != sc.ID {
					continue
				}
				right, ok := r.correct(run, c.ID)
				if !ok {
					continue
				}
				n++
				if right {
					k++
				}
			}
			r.p(" %s |", pct(k, n))
		}
		r.p("\n")
	}
	r.p("\n")
}

func (r *reporter) byTag() {
	runs := r.firstPassRuns()
	tags := make([]string, 0, len(knownTags))
	for t := range knownTags {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	r.p("## 按语言现象\n\n")
	r.p("一条样本可带多个标签。typo、trad、mixed、dialect、slang 是中文特有现象，英文译文里保留不了，看英文条件时忽略这几行。\n\n")
	r.p("| 标签 | 含义 | 样本 |")
	for _, run := range runs {
		r.p(" %s |", run.label())
	}
	r.p("\n|---|---|---|%s\n", strings.Repeat("---|", len(runs)))
	for _, t := range tags {
		ids := make([]string, 0, 32)
		for _, c := range r.cases {
			for _, ct := range c.Tags {
				if ct == t {
					ids = append(ids, c.ID)
					break
				}
			}
		}
		if len(ids) == 0 {
			continue
		}
		r.p("| %s | %s | %d |", t, knownTags[t], len(ids))
		for _, run := range runs {
			var k, n int
			for _, id := range ids {
				right, ok := r.correct(run, id)
				if !ok {
					continue
				}
				n++
				if right {
					k++
				}
			}
			r.p(" %s |", pct(k, n))
		}
		r.p("\n")
	}
	r.p("\n")
}

// confidence 分析决策模型的置信度能不能用来分档：高置信的是不是真的更准。
func (r *reporter) confidence() {
	var deciderRuns []runResult
	for _, run := range r.firstPassRuns() {
		for _, d := range r.roles.deciders() {
			if run.System == d.key {
				deciderRuns = append(deciderRuns, run)
			}
		}
	}
	if len(deciderRuns) == 0 {
		return
	}
	r.p("## 决策模型置信度\n\n")
	r.p("分档阈值取官方置信度路由文档的建议：低于 0.6 转人工，0.6 到 0.85 需确认，0.85 以上自动执行。")
	r.p("没判出分支的样本（弃权、给了非候选目标、置信度越界）不带置信度，不在此表，所以这里的样本数可能少于概况里的有效样本数。\n\n")
	r.p("| 系统 / 条件 | 置信度区间 | 样本 | 占比 | 准确率 |\n|---|---|---|---|---|\n")
	type band struct {
		name   string
		lo, hi float64
	}
	bands := []band{{"< 0.6", -1, 0.6}, {"0.6 – 0.85", 0.6, 0.85}, {"≥ 0.85", 0.85, 2}}
	for _, run := range deciderRuns {
		var n int
		cnt := make([]int, len(bands))
		right := make([]int, len(bands))
		for _, c := range r.cases {
			rec, found := run.Records[c.ID]
			if !found || rec.Err != "" || rec.Confidence == nil {
				continue
			}
			n++
			for i, bd := range bands {
				if *rec.Confidence >= bd.lo && *rec.Confidence < bd.hi {
					cnt[i]++
					if rec.Pred == c.Gold {
						right[i]++
					}
				}
			}
		}
		for i, bd := range bands {
			r.p("| %s | %s | %d | %s | %s |\n", run.label(), bd.name, cnt[i], pct(cnt[i], n), pct(right[i], cnt[i]))
		}
	}
	r.p("\n")
	// 高置信却判错的样本，是自动放行时真正会出事的那部分。
	for _, run := range deciderRuns {
		var wrongHigh, wrong int
		for _, c := range r.cases {
			rec, found := run.Records[c.ID]
			if !found || rec.Err != "" || rec.Confidence == nil || rec.Pred == c.Gold {
				continue
			}
			wrong++
			if *rec.Confidence >= 0.85 {
				wrongHigh++
			}
		}
		r.p("- %s：判错 %d 条，其中置信度 ≥ 0.85 的有 %d 条。\n", run.label(), wrong, wrongHigh)
	}
	r.p("\n")
}

// hybrid 对每家决策模型模拟「置信度达到阈值就用它的判定，否则回落 qwen」的混合方案。
func (r *reporter) hybrid() {
	for _, d := range r.roles.deciders() {
		r.hybridWith(d.key, d.title)
	}
}

func (r *reporter) hybridWith(decider, title string) {
	jr, okJ := r.find(decider, "zh", 1)
	qr, okQ := r.find(r.roles.qwen, "zh", 1)
	if !okJ || !okQ || r.roles.qwen == "" {
		return
	}
	r.p("## 混合方案模拟（中文）：%s\n\n", title)
	r.p("%s 置信度达到阈值时直接用它的判定，否则回落 qwen classifier。只统计两边都有有效结果的样本。\n\n", title)
	r.p("| 阈值 | %s 承接占比 | 混合准确率 | 省掉的 LLM 调用 |\n|---|---|---|---|\n", title)
	var n, qRight int
	for _, c := range r.cases {
		_, okA := r.correct(jr, c.ID)
		qc, okB := r.correct(qr, c.ID)
		if okA && okB {
			n++
			if qc {
				qRight++
			}
		}
	}
	for _, t := range []float64{0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99} {
		var handled, right int
		for _, c := range r.cases {
			jc, okA := r.correct(jr, c.ID)
			qc, okB := r.correct(qr, c.ID)
			if !okA || !okB {
				continue
			}
			rec := jr.Records[c.ID]
			if rec.Confidence != nil && *rec.Confidence >= t {
				handled++
				if jc {
					right++
				}
				continue
			}
			if qc {
				right++
			}
		}
		r.p("| %.2f | %s | %s | %s |\n", t, pct(handled, n), pct(right, n), pct(handled, n))
	}
	r.p("| 只用 qwen | 0%% | %s | 0%% |\n\n", pct(qRight, n))
}

// consistency 比较同一系统两轮判定的一致率。
func (r *reporter) consistency() {
	rows := make([]string, 0, 4)
	for _, run := range r.runs {
		if run.Pass != 2 {
			continue
		}
		first, ok := r.find(run.System, run.Cond, 1)
		if !ok {
			continue
		}
		var n, same int
		var confDiff float64
		var confN int
		for _, c := range r.cases {
			a, okA := first.Records[c.ID]
			b, okB := run.Records[c.ID]
			if !okA || !okB || a.Err != "" || b.Err != "" {
				continue
			}
			n++
			if a.Pred == b.Pred {
				same++
			}
			if a.Confidence != nil && b.Confidence != nil {
				confDiff += math.Abs(*a.Confidence - *b.Confidence)
				confN++
			}
		}
		extra := ""
		if confN > 0 {
			extra = fmt.Sprintf("，置信度平均绝对差 %.3f", confDiff/float64(confN))
		}
		rows = append(rows, fmt.Sprintf("- %s / %s：两轮判定一致 %s（%d/%d）%s。\n", run.System, run.Cond, pct(same, n), same, n, extra))
	}
	if len(rows) == 0 {
		return
	}
	r.p("## 重复一致性\n\n同一输入跑两轮，看判定会不会变。\n\n")
	for _, row := range rows {
		r.p("%s", row)
	}
	r.p("\n")
}

func (r *reporter) mistakes(system, title string) {
	run, ok := r.find(system, "zh", 1)
	if !ok || system == "" {
		return
	}
	// 决策模型的错例对照 qwen 基线；qwen 的错例对照第一家跑了的决策模型。
	other := r.roles.qwen
	if system == r.roles.qwen {
		other = ""
		if ds := r.roles.deciders(); len(ds) > 0 {
			other = ds[0].key
		}
	}
	otherRun, hasOther := r.find(other, "zh", 1)
	r.p("## %s 错例（中文条件）\n\n", title)
	r.p("| 样本 | 标签 | 原文 | 正确分支 | 判定 | 置信度 | 另一方判定 | 模型原始输出 |\n|---|---|---|---|---|---|---|---|\n")
	count := 0
	for _, c := range r.cases {
		rec, found := run.Records[c.ID]
		if !found || rec.Err != "" || rec.Pred == c.Gold {
			continue
		}
		count++
		conf := "-"
		if rec.Confidence != nil {
			conf = fmt.Sprintf("%.2f", *rec.Confidence)
		}
		otherPred := "-"
		if hasOther {
			if o, ok := otherRun.Records[c.ID]; ok && o.Err == "" {
				otherPred = o.Pred
			}
		}
		r.p("| %s | %s | %s | %s | %s | %s | %s | %s |\n", c.ID, strings.Join(c.Tags, ","), mdCell(c.ZH), c.Gold, rec.Pred, conf, otherPred, mdCell(rec.Raw))
	}
	if count == 0 {
		r.p("| 无 | | | | | | | |\n")
	}
	r.p("\n")
}

// labelReview 列出参考模型与标注不一致的样本，供人工复核标注本身有没有问题。
func (r *reporter) labelReview() {
	run, ok := r.find(r.roles.ref, "zh", 1)
	if !ok {
		return
	}
	r.p("## 标注复核清单\n\n参考模型与标注不一致的样本。逐条读原文判断：标注错了就改，有歧义就删，不能照模型的答案改。\n\n")
	r.p("| 样本 | 原文 | 标注 | 参考模型 |\n|---|---|---|---|\n")
	for _, c := range r.cases {
		rec, found := run.Records[c.ID]
		if !found || rec.Err != "" || rec.Pred == c.Gold {
			continue
		}
		r.p("| %s | %s | %s | %s |\n", c.ID, mdCell(c.ZH), c.Gold, rec.Pred)
	}
	r.p("\n")
}

// ---------------------------------------------------------------------------
// 统计工具

// wilson 返回二项比例的 Wilson 95% 置信区间。
func wilson(k, n int) (lo, hi float64) {
	if n == 0 {
		return 0, 0
	}
	const z = 1.959963984540054
	p := float64(k) / float64(n)
	nn := float64(n)
	den := 1 + z*z/nn
	center := (p + z*z/(2*nn)) / den
	half := z * math.Sqrt(p*(1-p)/nn+z*z/(4*nn*nn)) / den
	return math.Max(0, center-half), math.Min(1, center+half)
}

// mcnemarExact 返回双侧精确 McNemar p 值。b、c 是两个系统判定不一致的两类样本数。
func mcnemarExact(b, c int) float64 {
	n := b + c
	if n == 0 {
		return 1
	}
	k := min(b, c)
	sum := 0.0
	for i := 0; i <= k; i++ {
		sum += math.Exp(logChoose(n, i) - float64(n)*math.Ln2)
	}
	return math.Min(1, 2*sum)
}

func logChoose(n, k int) float64 {
	// Lgamma 的第二个返回值是符号，参数都为正时恒为 +1，不需要。
	a, _ := math.Lgamma(float64(n + 1))
	b, _ := math.Lgamma(float64(k + 1))
	c, _ := math.Lgamma(float64(n - k + 1))
	return a - b - c
}

// percentile 用线性插值取分位数；空切片返回 NaN。
func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	pos := p / 100 * float64(len(s)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return s[lo]
	}
	return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
}

func pct(k, n int) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", float64(k)*100/float64(n))
}

func fmtMS(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmt.Sprintf("%.0f", v)
}

func fmtMean(sum, n int) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f", float64(sum)/float64(n))
}

func joinCounts(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s×%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// mdCell 让任意文本能安全放进 markdown 表格的一个单元格。
func mdCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
