package skill

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// SkillBox 定义 skill 容器契约，负责发现、匹配和资源读取。
//
// **业务实现契约（R23 audit LOW-2/3）**：
//
//  1. **并发安全**：业务自定义 SkillBox 实现的 ListSkills / MatchSkills /
//     ActivateSkill 等所有方法**必须线程安全**——典型场景 ParallelAgent 多 branch
//     并发调 ListSkills + lazy-load 实现会 race；DefaultSkillBox 发布不可变的目录快照。
//
//  2. **MatchSkills 幂等性**：同一目录版本内，同一 query 重复调用应返一致结果（顺序+score）——
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

// DefaultSkillBox 从文件系统或预解析的内存内容加载 skill。
// 可并发读取。列表、匹配和激活各自返回独立的元数据快照。
// 文件型目录可用 [WithHotReload] 启用原子重载。
type DefaultSkillBox struct {
	name      string
	rootDir   string
	allowList map[string]bool // nil = 允许所有
	recursive bool
	hotReload bool
	reloadMu  sync.Mutex
	catalog   atomic.Pointer[skillCatalog]

	actionFuncs    map[string]ActionFunc              // key = actionKey(skill, action)，go runtime 进程内实现
	pendingActions map[string][]SkillActionDescriptor // 选项期注册、scan 后并入对应 skill.actions
}

type skillCatalog struct {
	skills   []loadedSkill
	revision string
}

// loadedSkill 是已加载 skill 的内部表示。
type loadedSkill struct {
	descriptor SkillDescriptor
	dir        string // skill 目录的绝对路径
	dirInfo    fs.FileInfo
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

// WithRecursiveDiscovery 允许在分类目录内继续查找 SKILL.md。
// 含有 SKILL.md 的目录是一个 skill 包，其内部文件按资源处理。默认只扫描一级子目录。
func WithRecursiveDiscovery(enabled bool) SkillBoxOption {
	return func(b *DefaultSkillBox) { b.recursive = enabled }
}

// WithHotReload 允许对文件型 SkillBox 显式调用 Reload。
// 同时为快照生成内容版本，供 Agent 在续跑时核对目录。默认关闭。
func WithHotReload(enabled bool) SkillBoxOption {
	return func(b *DefaultSkillBox) { b.hotReload = enabled }
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
// 创建选项时保存 descriptor 的独立快照。
// desc.Runtime 留空默认 "go"。同 (skillName, desc.Name) 重复注册后者覆盖实现、descriptor 不重复加。
// fn 为 nil 或 desc.Name 为空时静默忽略。
//
// **skillName 必须对应一个已加载的 skill**（有 SKILL.md 且过 allow-list）——注册到未加载 skill
// 的 action 静默不生效（ListSkillActions / RunSkillAction 都看不到，typo skill 名时无运行时报错）。
func WithSkillActionFunc(skillName string, desc SkillActionDescriptor, fn ActionFunc) SkillBoxOption {
	if fn == nil || desc.Name == "" {
		return func(*DefaultSkillBox) {}
	}
	if desc.Runtime == "" {
		desc.Runtime = string(RuntimeGo)
	}
	desc.ArgumentsSchema = snapshotActionSchema(desc.ArgumentsSchema)
	desc.Examples = slices.Clone(desc.Examples)
	return func(b *DefaultSkillBox) {
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
	if err := b.loadCatalog(true); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *DefaultSkillBox) Name() string    { return b.name }
func (b *DefaultSkillBox) RootDir() string { return b.rootDir }

// Reload 重新扫描文件型 SkillBox。扫描成功后一次性发布目录，失败时保留原快照。
// 调用方在文件更新完成后触发。仅启用了 [WithHotReload] 的文件型 SkillBox 支持重载。
func (b *DefaultSkillBox) Reload() error {
	if b.rootDir == "" || !b.hotReload {
		return errors.New("skill box: reload requires a filesystem box with WithHotReload enabled")
	}
	b.reloadMu.Lock()
	defer b.reloadMu.Unlock()
	return b.loadCatalog(false)
}

// Snapshot 返回固定在当前目录版本的只读 SkillBox 及用于续跑校验的版本。
// 资源目录和元数据固定，资源文件内容仍在读取时取得。静态 SkillBox 的版本为空。
func (b *DefaultSkillBox) Snapshot() (*DefaultSkillBox, string) {
	catalog := b.catalog.Load()
	if catalog == nil {
		return b, ""
	}
	if !b.hotReload {
		return b, catalog.revision
	}
	view := &DefaultSkillBox{name: b.name, rootDir: b.rootDir, actionFuncs: b.actionFuncs}
	view.catalog.Store(catalog)
	return view, catalog.revision
}

func (b *DefaultSkillBox) loadedSkills() []loadedSkill {
	if catalog := b.catalog.Load(); catalog != nil {
		return catalog.skills
	}
	return nil
}

// ListSkills 返回所有已加载 skill 的元数据。
func (b *DefaultSkillBox) ListSkills() []SkillDescriptor {
	skills := b.loadedSkills()
	out := make([]SkillDescriptor, 0, len(skills))
	for _, s := range skills {
		out = append(out, copySkillDescriptor(s.descriptor))
	}
	return out
}

// MatchSkills 用关键词匹配返回按分数降序排列的结果。
func (b *DefaultSkillBox) MatchSkills(_ context.Context, query string) []SkillMatchResult {
	return matchSkills(b.loadedSkills(), query)
}

// ListSkillResources 列出指定 skill 可读取的资源文件清单。
func (b *DefaultSkillBox) ListSkillResources(name string) []SkillResource {
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name == name {
			return copySkillResources(s.resources)
		}
	}
	return nil
}

func copySkillResources(src []SkillResource) []SkillResource {
	resources := slices.Clone(src)
	for i := range resources {
		resources[i].Topics = slices.Clone(resources[i].Topics)
	}
	return resources
}

// ReadSkillResource 读取 skill 内指定路径的资源文件内容，失败时返回空字符串。
func (b *DefaultSkillBox) ReadSkillResource(name, path string) string {
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name != name {
			continue
		}
		root, err := s.openResourceRoot()
		if err != nil {
			return ""
		}
		defer root.Close() // 只读目录句柄。
		data, err := root.ReadFile(path)
		if err != nil {
			return ""
		}
		return string(data)
	}
	return ""
}

// ReadSkillPrompt 读取 skill playbook 原文（SKILL.md body）。
func (b *DefaultSkillBox) ReadSkillPrompt(name string) string {
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name == name {
			return s.prompt
		}
	}
	return ""
}

// ListSkillActions 返回 skill 暴露的可执行 action 清单。
func (b *DefaultSkillBox) ListSkillActions(name string) []SkillActionDescriptor {
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name == name {
			return copySkillActions(s.actions)
		}
	}
	return nil
}

// ActivateSkill 按名称激活 skill，返回展开视图。
func (b *DefaultSkillBox) ActivateSkill(_ context.Context, name string) (*ActiveSkill, error) {
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name == name {
			return &ActiveSkill{
				Skill:     copySkillDescriptor(s.descriptor),
				RootDir:   s.dir,
				Prompt:    s.prompt,
				Resources: copySkillResources(s.resources),
				Actions:   copySkillActions(s.actions),
			}, nil
		}
	}
	return nil, fmt.Errorf("skill %q not found", name)
}

func copySkillDescriptor(desc SkillDescriptor) SkillDescriptor {
	desc.Tags = slices.Clone(desc.Tags)
	desc.Examples = slices.Clone(desc.Examples)
	desc.AllowedTools = slices.Clone(desc.AllowedTools)
	return desc
}

func copySkillActions(src []SkillActionDescriptor) []SkillActionDescriptor {
	out := slices.Clone(src)
	for i := range out {
		out[i].ArgumentsSchema = copyActionSchema(out[i].ArgumentsSchema)
		out[i].Examples = slices.Clone(out[i].Examples)
	}
	return out
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
	for _, s := range b.loadedSkills() {
		if s.descriptor.Name != skillName {
			continue
		}
		for j := range s.actions {
			if s.actions[j].Name == actionName {
				return &s.actions[j]
			}
		}
	}
	return nil
}

func (b *DefaultSkillBox) loadCatalog(startup bool) error {
	skills, err := b.scan(startup)
	if err != nil {
		return err
	}
	catalog := &skillCatalog{skills: skills}
	if b.hotReload {
		// 使用稳定的内容版本，重启后仍能识别同一个目录。函数实现的部署版本由业务声明。
		type entry struct {
			Dir        string
			Descriptor SkillDescriptor
			Prompt     string
			Resources  []SkillResource
			Actions    []SkillActionDescriptor
		}
		entries := make([]entry, 0, len(skills))
		for _, s := range skills {
			entries = append(entries, entry{s.dir, s.descriptor, s.prompt, s.resources, s.actions})
		}
		data, err := json.Marshal(entries)
		if err != nil {
			return fmt.Errorf("skill box: encode catalog revision: %w", err)
		}
		catalog.revision = fmt.Sprintf("skill-catalog-v1:%x", sha256.Sum256(data))
	}
	b.catalog.Store(catalog)
	return nil
}

// scan 以稳定顺序遍历分类目录。遇到 skill 包后，它的子树交给资源扫描。
func (b *DefaultSkillBox) scan(startup bool) ([]loadedSkill, error) {
	root, err := os.OpenRoot(b.rootDir)
	if err != nil {
		if startup && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("skill box: scan %q: %w", b.rootDir, err)
	}
	defer root.Close() // 只读目录句柄。
	var skills []loadedSkill
	seen := make(map[string]bool)
	err = fs.WalkDir(root.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == "." || !d.IsDir() {
			return err
		}
		s, found, err := b.loadSkill(root, filepath.FromSlash(path))
		if err == nil && s.descriptor.Name != "" {
			name := s.descriptor.Name
			if (b.recursive || b.hotReload) && seen[name] {
				err = fmt.Errorf("duplicate skill name %q", name)
			} else {
				skills = append(skills, s)
				seen[name] = true
			}
		}
		if err != nil {
			if !startup {
				return fmt.Errorf("%s: %w", path, err)
			}
			slog.Warn("skill: 跳过无法加载的目录", "dir", path, "err", err)
		}
		if found || !b.recursive {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("skill box: scan %q: %w", b.rootDir, err)
	}
	return skills, nil
}

func (b *DefaultSkillBox) loadSkill(root *os.Root, path string) (loadedSkill, bool, error) {
	dir, err := root.OpenRoot(path)
	if err != nil {
		return loadedSkill{}, false, err
	}
	defer dir.Close() // 只读目录句柄。
	const manifest = "SKILL.md"
	entry, err := dir.Lstat(manifest)
	if errors.Is(err, fs.ErrNotExist) {
		return loadedSkill{}, false, nil
	}
	if err != nil {
		return loadedSkill{}, true, err
	}
	if entry.Mode()&fs.ModeSymlink != 0 {
		entry, err = dir.Stat(manifest)
		if err != nil {
			return loadedSkill{}, true, err
		}
	}
	if !entry.Mode().IsRegular() {
		return loadedSkill{}, true, errors.New("SKILL.md must be a regular file")
	}
	data, err := dir.ReadFile(manifest)
	if err != nil {
		return loadedSkill{}, true, err
	}
	desc, body, err := ParseSkillMD(data)
	if err != nil {
		return loadedSkill{}, true, err
	}
	if desc.Name == "" {
		return loadedSkill{}, true, errors.New("SKILL.md name is required")
	}
	if b.allowList != nil && !b.allowList[desc.Name] {
		return loadedSkill{}, true, nil
	}
	info, err := dir.Stat(".")
	if err != nil {
		return loadedSkill{}, true, err
	}
	resources, err := scanResources(dir)
	if err != nil {
		return loadedSkill{}, true, err
	}
	return loadedSkill{
		descriptor: desc,
		dir:        filepath.Join(b.rootDir, path),
		dirInfo:    info,
		prompt:     body,
		matchText:  buildMatchText(desc),
		resources:  resources,
		actions:    b.pendingActions[desc.Name],
	}, true, nil
}

// openResourceRoot 确认资源仍来自扫描时的目录。内存来源的 Dir 仅用于标记来源。
func (s loadedSkill) openResourceRoot() (*os.Root, error) {
	if s.dirInfo == nil {
		return nil, fs.ErrNotExist
	}
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(s.dirInfo, info) {
		_ = root.Close() // 目录未通过检查，保留原错误。
		if err != nil {
			return nil, err
		}
		return nil, errors.New("skill resource directory changed")
	}
	return root, nil
}

// scanResources 扫描 skill 目录下所有非 SKILL.md 的文件，构建资源清单。
func scanResources(root *os.Root) ([]SkillResource, error) {
	var resources []SkillResource
	err := fs.WalkDir(root.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() || path == "SKILL.md" {
			return err
		}
		rel := filepath.FromSlash(path)
		rt := resourceType(rel)
		resource := SkillResource{
			Path:         rel,
			ResourceType: rt,
		}
		if rt == "reference" {
			loadReferenceMetadata(root, &resource)
		}
		resources = append(resources, resource)
		return nil
	})
	return resources, err
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
