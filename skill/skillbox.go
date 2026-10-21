package skill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// SkillBox 定义 skill 容器契约，负责发现、匹配和资源读取。
//
// **业务实现契约（R23 audit LOW-2/3）**：
//
//  1. **并发安全**：业务自定义 SkillBox 实现的 ListSkills / MatchSkills /
//     ActivateSkill 等所有方法**必须线程安全**——典型场景 ParallelAgent 多 branch
//     并发调 ListSkills + lazy-load 实现会 race；DefaultSkillBox 一次性 scan +
//     运行时只读天然安全。
//
//  2. **MatchSkills 幂等性**：同一 query 重复调用应返一致结果（顺序+score）——
//     Resume 路径 toolsNode 重跑会再次调 box.MatchSkills，非确定性实现（如基于
//     时间戳排序 / 随机 tie-break）会让 selectSkillAdaptive fallback_top1 选不同
//     skill → Resume 行为漂移业务感知失忆。
type SkillBox interface {
	// Name 返回 skill box 名称。
	Name() string
	// RootDir 返回本地 skill 根目录。
	RootDir() string
	// ListSkills 列出当前 box 中成功加载的 skill。
	ListSkills() []SkillDescriptor
	// MatchSkills 根据自然语言查询匹配 skill，返回按分数降序排列的结果。
	MatchSkills(ctx context.Context, query string) []SkillMatchResult
	// ListSkillResources 列出 skill 可读取的资源文件清单。
	ListSkillResources(name string) []SkillResource
	// ReadSkillResource 读取 skill 内的资源文件内容。
	ReadSkillResource(name, path string) string
	// ReadSkillPrompt 读取 skill playbook 原文（SKILL.md body）。
	ReadSkillPrompt(name string) string
	// ListSkillActions 返回 skill 暴露的可执行 action 清单。
	ListSkillActions(name string) []SkillActionDescriptor
	// ActivateSkill 激活一个 skill，返回其展开视图。
	//
	// **HITL 支持**：业务自定义 SkillBox 实现可抛 `compose_v2.InterruptError` 做
	// "敏感 skill 激活前用户审批"等 HITL gate——按 BasicAgent skill 模式分两种触发位置：
	//   - **adaptive / preMatch 模式**（ISSUE-74 修后）：跑在 graph 首段 preamble 节点内，
	//     框架自动 SaveNS cp + Resume 重跑业务取 ResumeContext 答案续跑
	//   - **toolCall 模式（默认）**（R24 audit HIGH-1 fix 后）：跑在 toolsNode 内（LLM
	//     调 activate_skill 工具触发本方法），框架按 tools 节点协议自动 SaveNS cp + Resume
	//     重跑 toolsNode 续跑（**业务工具自负幂等性**——见 `doc/v2/hitl-resume.md` §6.3）
	//
	// **idempotent 契约**：Resume 时本方法被再次调用，业务实现必须支持多次调用返
	// 同样结果（除非业务自己读 ResumeContext 主动改写）。详见
	// `compose_v2.BasicAgentPreambleFn` godoc + `doc/v2/hitl-resume.md` §12。
	ActivateSkill(ctx context.Context, name string) (*ActiveSkill, error)
	// RunSkillAction 运行 skill 的命名 action（go runtime 进程内函数，经 WithSkillActionFunc 注册）。
	// 找不到 action 返 ErrSkillActionNotFound——LLM 可能报出清单里没有的名字。
	// 自定义 SkillBox 不提供 action 时 ListSkillActions 返空、本方法照样返 ErrSkillActionNotFound：
	// 框架只在至少一个可被 LLM 调的 skill 列出了 action 时才挂 run_skill_action 工具，不会调到这里。
	RunSkillAction(ctx context.Context, skillName, actionName string, arguments map[string]any) (*SkillExecutionResult, error)
}

// ErrSkillActionNotFound 表示 RunSkillAction 找不到指定 skill/action（或 go action 未注册实现）。
var ErrSkillActionNotFound = errors.New("skill: action not found")

// DefaultSkillBox 是基于文件系统的 SkillBox 实现。
// 扫描 rootDir 的一级子目录，加载每个目录下的 SKILL.md。
// 容错设计：目录不存在或单个 SKILL.md 解析失败均不报错，静默跳过。
type DefaultSkillBox struct {
	name      string
	rootDir   string
	skills    []loadedSkill
	allowList map[string]bool // nil = 允许所有

	actionFuncs    map[string]ActionFunc              // key = actionKey(skill, action)，go runtime 进程内实现
	pendingActions map[string][]SkillActionDescriptor // 选项期注册、scan 后并入对应 skill.actions
}

// loadedSkill 是已加载 skill 的内部表示。
type loadedSkill struct {
	descriptor SkillDescriptor
	dir        string // skill 目录的绝对路径
	prompt     string // SKILL.md body 内容
	matchText  string // 用于关键词匹配的拼接文本
	resources  []SkillResource
	actions    []SkillActionDescriptor
}

// SkillBoxOption 是 DefaultSkillBox 的构造选项。
type SkillBoxOption func(*DefaultSkillBox)

// WithSkillBoxName 设置 skill box 名称。
func WithSkillBoxName(name string) SkillBoxOption {
	return func(b *DefaultSkillBox) { b.name = name }
}

// WithAllowList 只加载指定名称的 skill，nil 或空列表表示允许所有。
func WithAllowList(names []string) SkillBoxOption {
	return func(b *DefaultSkillBox) {
		if len(names) == 0 {
			return
		}
		b.allowList = make(map[string]bool, len(names))
		for _, n := range names {
			b.allowList[n] = true
		}
	}
}

// actionKey 拼 (skillName, actionName) 为 actionFuncs map 的 key。
func actionKey(skillName, actionName string) string { return skillName + "\x00" + actionName }

// WithSkillActionFunc 注册一个 go runtime 的进程内 action（descriptor 元数据 + Go 实现）。
// desc.Runtime 留空默认 "go"。同 (skillName, desc.Name) 重复注册后者覆盖实现、descriptor 不重复加。
// fn 为 nil 或 desc.Name 为空时静默忽略。
//
// **skillName 必须对应一个已加载的 skill**（有 SKILL.md 且过 allow-list）——注册到未加载 skill
// 的 action 静默不生效（ListSkillActions / RunSkillAction 都看不到，typo skill 名时无运行时报错）。
func WithSkillActionFunc(skillName string, desc SkillActionDescriptor, fn ActionFunc) SkillBoxOption {
	return func(b *DefaultSkillBox) {
		if fn == nil || desc.Name == "" {
			return
		}
		if desc.Runtime == "" {
			desc.Runtime = string(RuntimeGo)
		}
		if b.actionFuncs == nil {
			b.actionFuncs = make(map[string]ActionFunc)
		}
		if b.pendingActions == nil {
			b.pendingActions = make(map[string][]SkillActionDescriptor)
		}
		key := actionKey(skillName, desc.Name)
		if _, dup := b.actionFuncs[key]; !dup {
			b.pendingActions[skillName] = append(b.pendingActions[skillName], desc)
		}
		b.actionFuncs[key] = fn
	}
}

// NewDefaultSkillBox 创建并初始化基于文件系统的 SkillBox。
// 构造时立即扫描 rootDir，失败的单个 skill 被跳过。
func NewDefaultSkillBox(rootDir string, opts ...SkillBoxOption) (*DefaultSkillBox, error) {
	abs, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("skill box: resolve path: %w", err)
	}
	b := &DefaultSkillBox{
		name:    "default",
		rootDir: abs,
	}
	for _, opt := range opts {
		opt(b)
	}
	if err := b.scan(); err != nil {
		return nil, err
	}
	b.mergePendingActions()
	return b, nil
}

func (b *DefaultSkillBox) Name() string    { return b.name }
func (b *DefaultSkillBox) RootDir() string { return b.rootDir }

// ListSkills 返回所有已加载 skill 的元数据。
func (b *DefaultSkillBox) ListSkills() []SkillDescriptor {
	out := make([]SkillDescriptor, 0, len(b.skills))
	for _, s := range b.skills {
		out = append(out, s.descriptor)
	}
	return out
}

// MatchSkills 用关键词匹配返回按分数降序排列的结果。
func (b *DefaultSkillBox) MatchSkills(_ context.Context, query string) []SkillMatchResult {
	return matchSkills(b.skills, query)
}

// ListSkillResources 列出指定 skill 可读取的资源文件清单。
func (b *DefaultSkillBox) ListSkillResources(name string) []SkillResource {
	for _, s := range b.skills {
		if s.descriptor.Name == name {
			return s.resources
		}
	}
	return nil
}

// ReadSkillResource 读取 skill 内指定路径的资源文件内容，失败时返回空字符串。
func (b *DefaultSkillBox) ReadSkillResource(name, path string) string {
	for _, s := range b.skills {
		if s.descriptor.Name != name {
			continue
		}
		// 防止路径穿越：path 必须在 skill 目录下
		full := filepath.Join(s.dir, path)
		if !strings.HasPrefix(full, s.dir+string(filepath.Separator)) && full != s.dir {
			return ""
		}
		data, err := os.ReadFile(full)
		if err != nil {
			return ""
		}
		return string(data)
	}
	return ""
}

// ReadSkillPrompt 读取 skill playbook 原文（SKILL.md body）。
func (b *DefaultSkillBox) ReadSkillPrompt(name string) string {
	for _, s := range b.skills {
		if s.descriptor.Name == name {
			return s.prompt
		}
	}
	return ""
}

// ListSkillActions 返回 skill 暴露的可执行 action 清单。
func (b *DefaultSkillBox) ListSkillActions(name string) []SkillActionDescriptor {
	for _, s := range b.skills {
		if s.descriptor.Name == name {
			return s.actions
		}
	}
	return nil
}

// ActivateSkill 按名称激活 skill，返回展开视图。
func (b *DefaultSkillBox) ActivateSkill(_ context.Context, name string) (*ActiveSkill, error) {
	for _, s := range b.skills {
		if s.descriptor.Name == name {
			return &ActiveSkill{
				Skill:     s.descriptor,
				RootDir:   s.dir,
				Prompt:    s.prompt,
				Resources: s.resources,
				Actions:   s.actions,
			}, nil
		}
	}
	return nil, fmt.Errorf("skill %q not found", name)
}

// RunSkillAction 运行 skill 的命名 action（go runtime 进程内函数，经 WithSkillActionFunc 注册）。
// 找不到 action 返 ErrSkillActionNotFound；命中但 runtime 非 go 也返 ErrSkillActionNotFound
// （SkillActionDescriptor.Runtime 当前仅支持 "go"）。
func (b *DefaultSkillBox) RunSkillAction(ctx context.Context, skillName, actionName string, arguments map[string]any) (*SkillExecutionResult, error) {
	desc := b.findAction(skillName, actionName)
	if desc == nil {
		return nil, fmt.Errorf("%w: %s/%s", ErrSkillActionNotFound, skillName, actionName)
	}
	switch ActionRuntime(desc.Runtime) {
	case RuntimeGo, "":
		fn := b.actionFuncs[actionKey(skillName, actionName)]
		if fn == nil {
			return nil, fmt.Errorf("%w: go action %s/%s has no registered func (use WithSkillActionFunc)", ErrSkillActionNotFound, skillName, actionName)
		}
		return fn(ctx, arguments)
	default:
		// 仅支持 go runtime——非法 runtime（不应出现，go-func 注册恒置 "go"）wrap 哨兵让业务 errors.Is 归类。
		return nil, fmt.Errorf("%w: %s/%s unsupported runtime %q", ErrSkillActionNotFound, skillName, actionName, desc.Runtime)
	}
}

// findAction 在指定 skill 的 actions 里按名找 descriptor，未找到返 nil。
func (b *DefaultSkillBox) findAction(skillName, actionName string) *SkillActionDescriptor {
	for i := range b.skills {
		if b.skills[i].descriptor.Name != skillName {
			continue
		}
		for j := range b.skills[i].actions {
			if b.skills[i].actions[j].Name == actionName {
				return &b.skills[i].actions[j]
			}
		}
	}
	return nil
}

// mergePendingActions 把选项期 WithSkillActionFunc 注册的 action 并入对应 skill 的 actions
// （让 ListSkillActions / ActiveSkill.Actions 含程序化 action）；scan 之后调用一次。
//
// **同名去重**：同 (skill, action) 重复时后注册覆盖（防 listing 出现重复项）。
func (b *DefaultSkillBox) mergePendingActions() {
	if len(b.pendingActions) == 0 {
		return
	}
	for i := range b.skills {
		for _, pa := range b.pendingActions[b.skills[i].descriptor.Name] {
			replaced := false
			for j := range b.skills[i].actions {
				if b.skills[i].actions[j].Name == pa.Name {
					b.skills[i].actions[j] = pa // 覆盖同名 action
					replaced = true
					break
				}
			}
			if !replaced {
				b.skills[i].actions = append(b.skills[i].actions, pa)
			}
		}
	}
}

// scan 扫描 rootDir 一级子目录，加载每个 SKILL.md 及同目录资源文件。
func (b *DefaultSkillBox) scan() error {
	entries, err := os.ReadDir(b.rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("skill box: scan %q: %w", b.rootDir, err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(b.rootDir, e.Name())
		skillFile := filepath.Join(dir, "SKILL.md")
		data, err := os.ReadFile(skillFile)
		if err != nil {
			continue
		}
		desc, body, err := ParseSkillMD(data)
		if err != nil {
			// **出声再跳过。** 静默 continue 的效果是 skill 凭空消失——业务在 listing
			// 里找不到它，而不知道是自己 SKILL.md 写错了。这比「静默降级」还难查：
			// 降级至少还有个东西在跑。
			slog.Warn("skill: SKILL.md 解析失败，跳过", "dir", dir, "err", err)
			continue
		}
		if desc.Name == "" {
			slog.Warn("skill: SKILL.md 缺 name，跳过", "dir", dir)
			continue
		}
		if b.allowList != nil && !b.allowList[desc.Name] {
			continue
		}
		resources := scanResources(dir)
		ls := loadedSkill{
			descriptor: desc,
			dir:        dir,
			prompt:     body,
			matchText:  buildMatchText(desc),
			resources:  resources,
			// actions 由 mergePendingActions 从 WithSkillActionFunc 程序化注册填充（frontmatter 声明不了 go action）
		}
		b.skills = append(b.skills, ls)
	}
	return nil
}

// scanResources 扫描 skill 目录下所有非 SKILL.md 的文件，构建资源清单。
func scanResources(dir string) []SkillResource {
	var resources []SkillResource
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return nil
		}
		if rel == "SKILL.md" {
			return nil
		}
		rt := resourceType(rel)
		resources = append(resources, SkillResource{
			Path:         rel,
			ResourceType: rt,
		})
		return nil
	})
	return resources
}

// resourceType 根据文件扩展名推断资源类型。
func resourceType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".md", ".txt":
		return "reference"
	case ".py":
		return "script"
	case ".sh", ".bash":
		return "script"
	case ".js", ".ts":
		return "script"
	case ".j2", ".tmpl", ".tpl":
		return "template"
	default:
		return "asset"
	}
}

// buildMatchText 拼接所有可用于关键词匹配的文本。
func buildMatchText(d SkillDescriptor) string {
	parts := make([]string, 0, 4+len(d.Tags)+len(d.Examples))
	parts = append(parts, d.Name, d.Description, d.WhenToUse)
	parts = append(parts, d.Tags...)
	parts = append(parts, d.Examples...)
	result := ""
	for _, p := range parts {
		if p != "" {
			result += " " + p
		}
	}
	return result
}

var _ SkillBox = (*DefaultSkillBox)(nil)
