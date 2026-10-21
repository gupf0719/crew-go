// routing_eval 用中文路由场景实测决策模型当 RoutingAgent 路由器的准确率、置信度与延迟，
// 以框架自带的 qwen classifier 路径为基线。决策模型有两家：TypeSafe 的 Jev 与阿里云百炼的
// decision-model-preview。
//
// 被测路径都跑真实的 RoutingAgent，分支是不调模型的桩：
//
//   - 决策模型（jev、bailian）：decision.NewRouteDecider 经 WithRouteDecider 挂上去，门槛 0 即
//     不因置信度升级，测的是决策模型自己的判断；置信度与概率从路由决策事件里取。
//   - qwen：WithRoutingClassifier + WithRouteTool + WithRoutingDescriptions，即业务
//     今天的默认做法。模型固定一个快照、温度 0，不走 fallback 链。
//
// 样本分三个子集，报告分别出：
//
//   - 标准集 cases.jsonl：单条消息，3 套场景各 7 个粗粒度分支，189 条，中英对照。
//   - 困难集 cases_hard.jsonl：单条消息，电商 20 个、运维 16 个细粒度意图（另各有一个
//     「其他」），叠加方言、黑话、双重否定等，180 条，中英对照。
//   - 真实会话集 cases_real.jsonl：用困难集的两套意图，输入按生产常见模板拼成
//     「会话信息 + 历史对话 + 用户最新消息」，含话题切换、语音转写、提示词注入与超
//     范围请求，89 条，只有中文。
//
// 语言条件：zh（中文原文 + 中文指令与描述）、zh-en（中文原文 + 英文指令与描述，
// 只对决策模型跑）、en（英文译文 + 英文指令与描述，真实会话集没有英文，自动跳过）、
// zh-default（中文原文 + 中文描述 + decision 包默认的英文问题描述，只对决策模型跑）。
//
// 跑（jev 需要 TYPESAFE_API_KEY；bailian 需要百炼的 API key——DASHSCOPE_API_KEY，没有就取
// QWEN_API_KEY——与 -bailian-workspace；qwen 需要 QWEN_API_KEY；全量十分钟左右）：
//
//	go run ./example/decision/routing_eval -repeat -review -qwen-ref qwen3.8-max-0902
//	go run ./example/decision/routing_eval -systems bailian,qwen,cascade-bailian -repeat -bailian-workspace llm-xxxx
//
// 缺 key 的系统整个跳过，它的缓存也不进报告。
//
// 结果按「系统 × 条件」缓存在 -out 目录，命中输入指纹的样本不重跑；报告打印到
// 标准输出，同时写入 -out 目录下的 report.md。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"crew-go/decision/bailian"
	"crew-go/decision/jev"
	"crew-go/example/envloader"
	"crew-go/model/qwen"
)

// job 是一个「系统 × 条件 × 轮次」的执行单元。
type job struct {
	sys  system
	cond condition
	pass int
}

func main() {
	envloader.Load()
	var (
		systemsFlag = flag.String("systems", "jev,qwen", "要跑的系统，逗号分隔：jev、bailian、qwen、cascade（Jev 拿不准交给 qwen 的升级链）、cascade-bailian（百炼拿不准交给 qwen）")
		cascadeMin  = flag.Float64("cascade-threshold", 0.85, "升级链里决策模型那一档的门槛")
		condsFlag   = flag.String("conds", "zh,zh-en,en,zh-default", "要跑的语言条件，逗号分隔；qwen 不跑 zh-en 与 zh-default")
		setsFlag    = flag.String("set", "standard,hard,real", "要跑的样本子集，逗号分隔：standard、hard、real")
		qwenModel   = flag.String("qwen-model", qwen.TestModelCandidates[0], "qwen 基线模型快照")
		cascadeQwen = flag.String("cascade-qwen-model", "", "升级链里 classifier 用的 qwen 模型，留空同 -qwen-model；基线快照额度用完时换一个，基线仍可用缓存")
		qwenRef     = flag.String("qwen-ref", "", "可选的 qwen 参考模型，只跑 zh，用作上限参照与标注复核")
		jevModel    = flag.String("jev-model", jev.Model, "Jev 模型名")
		jevBaseURL  = flag.String("jev-base-url", jev.BaseURL, "TypeSafe API 地址")
		blWorkspace = flag.String("bailian-workspace", os.Getenv("BAILIAN_WORKSPACE_ID"), "百炼业务空间 ID，形如 llm-xxxx")
		blRegion    = flag.String("bailian-region", string(bailian.Beijing), "百炼地域：cn-beijing 或 ap-southeast-1")
		blModel     = flag.String("bailian-model", bailian.Model, "百炼决策模型名")
		concurrency = flag.Int("concurrency", 4, "每个「系统 × 条件」的并发请求数")
		limit       = flag.Int("limit", 0, "只跑均匀抽取的 N 条样本，0 表示全部")
		repeat      = flag.Bool("repeat", false, "zh 条件额外再跑一轮，测同一输入的判定一致性")
		rerun       = flag.Bool("rerun", false, "忽略缓存全部重跑")
		outDir      = flag.String("out", defaultOutDir(), "结果缓存与报告目录")
		review      = flag.Bool("review", false, "报告附上参考模型与标注不一致的样本，用于复核标注")
	)
	flag.Parse()

	if err := run(options{
		systems: splitList(*systemsFlag), conds: splitList(*condsFlag), sets: splitList(*setsFlag),
		qwenModel: *qwenModel, qwenRef: *qwenRef, jevModel: *jevModel, jevBaseURL: *jevBaseURL,
		bailianWorkspace: *blWorkspace, bailianRegion: *blRegion, bailianModel: *blModel,
		concurrency: *concurrency, limit: *limit, repeat: *repeat, rerun: *rerun, cascadeMin: *cascadeMin,
		cascadeQwenModel: *cascadeQwen,
		outDir:           *outDir, review: *review,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "routing_eval:", err)
		os.Exit(1)
	}
}

type options struct {
	systems, conds, sets                     []string
	qwenModel, qwenRef, jevModel, jevBaseURL string
	bailianWorkspace, bailianRegion          string
	bailianModel                             string
	concurrency, limit                       int
	cascadeMin                               float64
	cascadeQwenModel                         string
	repeat, rerun, review                    bool
	outDir                                   string
}

func run(opt options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	all, err := loadCases()
	if err != nil {
		return err
	}
	cases, err := selectTiers(all, opt.sets)
	if err != nil {
		return err
	}
	cases = sample(cases, opt.limit)

	conds := make([]condition, 0, len(opt.conds))
	for _, name := range opt.conds {
		c, ok := conditionByName(name)
		if !ok {
			return fmt.Errorf("unknown condition %q", name)
		}
		conds = append(conds, c)
	}

	jobs, rl, err := planJobs(opt, conds)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return fmt.Errorf("nothing to run: check -systems and the API keys")
	}
	if err := os.MkdirAll(opt.outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	runs := make([]runResult, 0, len(jobs))
	for _, jb := range jobs {
		res, err := runJob(ctx, jb, cases, opt)
		if err != nil {
			return err
		}
		runs = append(runs, res)
	}

	report := renderReport(cases, runs, rl, opt.review, reportPreface(jobs, opt))
	fmt.Println(report)
	path := filepath.Join(opt.outDir, "report.md")
	if err := os.WriteFile(path, []byte(report), 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	fmt.Fprintln(os.Stderr, "report written to", path)
	return nil
}

// planJobs 按参数与可用的 key 排出要跑的「系统 × 条件 × 轮次」。缺 key 的系统跳过并提示。
func planJobs(opt options, conds []condition) ([]job, roles, error) {
	var jobs []job
	var rl roles
	// reference 为 true 的系统只跑 zh 且不重复：它只用作上限参照与标注复核。
	add := func(s system, cond condition, reference bool) {
		if (cond.Name == "zh-en" || cond.DefaultInstructions) && !isDecider(s) {
			return
		}
		if reference && cond.Name != "zh" {
			return
		}
		jobs = append(jobs, job{sys: s, cond: cond, pass: 1})
		if opt.repeat && !reference && cond.Name == "zh" {
			jobs = append(jobs, job{sys: s, cond: cond, pass: 2})
		}
	}
	for _, name := range opt.systems {
		switch name {
		case "jev":
			s, ok := jevDecider(opt)
			if !ok {
				continue
			}
			rl.jev = s.key()
			for _, c := range conds {
				add(s, c, false)
			}
		case "bailian":
			s, ok := bailianDecider(opt)
			if !ok {
				continue
			}
			rl.bailian = s.key()
			for _, c := range conds {
				add(s, c, false)
			}
		case "qwen":
			if os.Getenv("QWEN_API_KEY") == "" {
				fmt.Fprintln(os.Stderr, "skip qwen: QWEN_API_KEY is not set")
				continue
			}
			base := newQwenSystem(opt.qwenModel)
			rl.qwen = base.key()
			for _, c := range conds {
				add(base, c, false)
			}
			if opt.qwenRef != "" {
				ref := newQwenSystem(opt.qwenRef)
				rl.ref = ref.key()
				for _, c := range conds {
					add(ref, c, true)
				}
			}
		case "cascade", "cascade-bailian":
			if os.Getenv("QWEN_API_KEY") == "" {
				fmt.Fprintf(os.Stderr, "skip %s: QWEN_API_KEY is not set\n", name)
				continue
			}
			pick := jevDecider
			if name == "cascade-bailian" {
				pick = bailianDecider
			}
			d, ok := pick(opt)
			if !ok {
				continue
			}
			classifierModel := opt.qwenModel
			if opt.cascadeQwenModel != "" {
				classifierModel = opt.cascadeQwenModel
			}
			s := &cascadeSystem{decider: d, qwen: newQwenSystem(classifierModel), threshold: opt.cascadeMin}
			for _, c := range conds {
				// 升级链只在中文原文、中文指令下测，这是它面向的部署形态。
				if c.Name == "zh" {
					jobs = append(jobs, job{sys: s, cond: c, pass: 1})
				}
			}
		default:
			return nil, roles{}, fmt.Errorf("unknown system %q", name)
		}
	}
	return jobs, rl, nil
}

// jevDecider 构造 Jev 系统；缺 key 时提示并返回 false。
func jevDecider(opt options) (*deciderSystem, bool) {
	key := os.Getenv("TYPESAFE_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "skip jev: TYPESAFE_API_KEY is not set")
		return nil, false
	}
	return newJevSystem(key, opt.jevBaseURL, opt.jevModel, opt.concurrency), true
}

// bailianDecider 构造百炼系统；key 取 DASHSCOPE_API_KEY，没有就取 QWEN_API_KEY（同一种百炼 key）。
// 缺 key 或业务空间 ID 时提示并返回 false。
func bailianDecider(opt options) (*deciderSystem, bool) {
	key := os.Getenv("DASHSCOPE_API_KEY")
	if key == "" {
		key = os.Getenv("QWEN_API_KEY")
	}
	switch {
	case key == "":
		fmt.Fprintln(os.Stderr, "skip bailian: neither DASHSCOPE_API_KEY nor QWEN_API_KEY is set")
		return nil, false
	case opt.bailianWorkspace == "":
		fmt.Fprintln(os.Stderr, "skip bailian: -bailian-workspace (or BAILIAN_WORKSPACE_ID) is not set")
		return nil, false
	}
	return newBailianSystem(key, bailian.Region(opt.bailianRegion), opt.bailianWorkspace, opt.bailianModel, opt.concurrency), true
}

// isDecider 报告 s 是不是决策模型系统：zh-en 与 zh-default 两个条件只对它们跑。
func isDecider(s system) bool {
	_, ok := s.(*deciderSystem)
	return ok
}

// runJob 跑一个执行单元：先读缓存，只对缺失、指纹不符或上次出错的样本发请求，
// 跑完合并写回缓存。
func runJob(ctx context.Context, jb job, cases []evalCase, opt options) (runResult, error) {
	path := cachePath(opt.outDir, jb)
	cached, err := loadCache(path)
	if err != nil {
		return runResult{}, err
	}
	applicable := make([]evalCase, 0, len(cases))
	for _, c := range cases {
		if strings.TrimSpace(c.text(jb.cond.StateLang)) != "" {
			applicable = append(applicable, c)
		}
	}
	todo := make([]evalCase, 0, len(applicable))
	for _, c := range applicable {
		sc, _ := scenarioByID(c.Scenario) // loadCases 已校验场景存在
		rec, ok := cached[c.ID]
		if opt.rerun || !ok || rec.Err != "" || rec.InputHash != inputHash(jb.sys, sc, c, jb.cond) {
			todo = append(todo, c)
		}
	}
	res := runResult{System: jb.sys.key(), Cond: jb.cond.Name, Pass: jb.pass, Records: cached}
	label := res.label()
	if len(todo) == 0 {
		fmt.Fprintf(os.Stderr, "[%s] all %d cached\n", label, len(applicable))
		return res, nil
	}
	fmt.Fprintf(os.Stderr, "[%s] running %d of %d (%d cached)\n", label, len(todo), len(applicable), len(applicable)-len(todo))

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		done     int
		failures int
	)
	queue := make(chan evalCase)
	workers := max(1, opt.concurrency)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range queue {
				rec := decideWithRetry(ctx, jb, c)
				mu.Lock()
				res.Records[c.ID] = rec
				done++
				if rec.Err != "" {
					failures++
				}
				if done%20 == 0 || done == len(todo) {
					fmt.Fprintf(os.Stderr, "[%s] %d/%d done, %d errors\n", label, done, len(todo), failures)
				}
				mu.Unlock()
			}
		}()
	}
	for _, c := range todo {
		select {
		case queue <- c:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(queue)
	wg.Wait()

	if err := saveCache(path, cases, res.Records); err != nil {
		return runResult{}, err
	}
	if ctx.Err() != nil {
		return runResult{}, fmt.Errorf("interrupted: %w", ctx.Err())
	}
	return res, nil
}

// decideWithRetry 对单条样本做判定；基础设施错误（网络、限流、模型调用失败）重试，
// 路由判错不重试。
func decideWithRetry(ctx context.Context, jb job, c evalCase) record {
	sc, _ := scenarioByID(c.Scenario) // loadCases 已校验场景存在
	var rec record
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ictx, cancel := context.WithTimeout(ctx, 90*time.Second)
		rec = jb.sys.decide(ictx, sc, c, jb.cond)
		cancel()
		rec.Attempts = attempt
		if rec.Err == "" || ctx.Err() != nil || attempt == maxAttempts {
			break
		}
		select {
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		case <-ctx.Done():
		}
	}
	rec.ID = c.ID
	rec.InputHash = inputHash(jb.sys, sc, c, jb.cond)
	rec.At = time.Now().Format(time.RFC3339)
	return rec
}

func cachePath(dir string, jb job) string {
	name := strings.NewReplacer(":", "_", "/", "_", " ", "_").Replace(jb.sys.key()) + "__" + jb.cond.Name
	if jb.pass > 1 {
		name += fmt.Sprintf("__pass%d", jb.pass)
	}
	return filepath.Join(dir, name+".jsonl")
}

// loadCache 读一个执行单元的缓存；文件不存在时返回空表。
func loadCache(path string) (map[string]record, error) {
	out := make(map[string]record, 200)
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cache %s: %w", path, err)
	}
	for i, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("cache %s line %d: %w", path, i+1, err)
		}
		out[rec.ID] = rec
	}
	return out, nil
}

// saveCache 按样本顺序写回缓存，先写临时文件再改名，中途失败不会留下半截文件。
// 不在本次样本集里的旧记录原样保留。
func saveCache(path string, cases []evalCase, recs map[string]record) error {
	var b strings.Builder
	written := make(map[string]bool, len(recs))
	writeRec := func(rec record) error {
		line, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal record %s: %w", rec.ID, err)
		}
		b.Write(line)
		b.WriteByte('\n')
		written[rec.ID] = true
		return nil
	}
	for _, c := range cases {
		if rec, ok := recs[c.ID]; ok {
			if err := writeRec(rec); err != nil {
				return err
			}
		}
	}
	for id, rec := range recs {
		if written[id] {
			continue
		}
		if err := writeRec(rec); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write cache: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename cache: %w", err)
	}
	return nil
}

// reportPreface 是报告开头的来历说明：生成时间、被测系统与并发数，让存档的报告
// 离开这次运行也能读懂。
func reportPreface(jobs []job, opt options) string {
	seen := make(map[string]bool, len(jobs))
	keys := make([]string, 0, len(jobs))
	for _, jb := range jobs {
		if k := jb.sys.key(); !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return fmt.Sprintf("> 生成于 %s。被测系统：%s。qwen 温度 0。每个「系统 × 条件」并发 %d。延迟含本机到各服务的网络往返。\n\n",
		time.Now().Format("2006-01-02 15:04 MST"), strings.Join(keys, "、"), opt.concurrency)
}

// selectTiers 按子集名过滤样本，子集名不认识时报错。
func selectTiers(cases []evalCase, sets []string) ([]evalCase, error) {
	want := make(map[string]bool, len(sets))
	for _, id := range sets {
		if _, ok := tierByID(id); !ok {
			return nil, fmt.Errorf("unknown set %q", id)
		}
		want[id] = true
	}
	out := make([]evalCase, 0, len(cases))
	for _, c := range cases {
		if want[c.Tier] {
			out = append(out, c)
		}
	}
	return out, nil
}

// sample 从样本里均匀抽 n 条，n <= 0 或不小于总数时原样返回。
func sample(cases []evalCase, n int) []evalCase {
	if n <= 0 || n >= len(cases) {
		return cases
	}
	out := make([]evalCase, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, cases[i*len(cases)/n])
	}
	return out
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// defaultOutDir 是本源码目录下的 results 子目录。
func defaultOutDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "routing_eval_results"
	}
	return filepath.Join(filepath.Dir(file), "results")
}
