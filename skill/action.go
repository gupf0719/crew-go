// Package skill — action.go 定义 skill action 执行的类型。
package skill

import (
	"context"
)

// ActionRuntime 定义 action 执行环境。
type ActionRuntime string

const (
	// RuntimeGo 进程内 Go 函数执行环境——业务经 WithSkillActionFunc 注册，框架直接调用不起子进程。
	RuntimeGo ActionRuntime = "go"
)

// ActionFunc 是 runtime="go" 的进程内 action 实现——业务经 WithSkillActionFunc 注册。
// RunSkillAction 命中 go runtime 时框架直接调用（无子进程），适合「skill 提供引导 body +
// 确定性 Go 执行」二合一（如 code-review skill：body 给评审准则、action 跑 linter）。
//
// **可重入契约**：框架可能并发调用同一 ActionFunc（同轮多 action / 多 branch）——业务实现需
// 可重入 / 线程安全（共享字段加锁，类比 tool.Tool.Call 约定）。
type ActionFunc func(ctx context.Context, args map[string]any) (*SkillExecutionResult, error)
