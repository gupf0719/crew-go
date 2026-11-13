// Package skill — action.go 定义 skill action 执行的类型。
package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

// snapshotActionSchema 固定调用方的 JSON 表示，包含自定义编码值，保留数字精度。
// 无法编码的 schema 原本也不会出现在工具说明中。action 仍可注册和执行。
func snapshotActionSchema(schema map[string]any) (snapshot map[string]any) {
	if schema == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			// 自定义 JSON 编码器的 panic 视同无法编码。
			snapshot = nil
		}
	}()
	data, err := json.Marshal(schema)
	if err != nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&snapshot); err != nil {
		slog.Debug("skill: unable to decode action schema", "err", err)
		return nil
	}
	return snapshot
}

func copyActionSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	out := make(map[string]any, len(schema))
	for k, v := range schema {
		out[k] = cloneActionSchema(v)
	}
	return out
}

// cloneActionSchema 只接收 snapshotActionSchema 产出的 JSON 树。
func cloneActionSchema(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return copyActionSchema(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneActionSchema(item)
		}
		return out
	default:
		return v
	}
}
