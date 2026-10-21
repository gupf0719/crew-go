package agent_v2

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
)

// loadAgentsMD 读取 AGENTS.md 文件列表按序拼接为 harness 指令文本，返空串表示无内容
// （caller 跳过注入）。缺文件静默跳过（业务可配多候选路径）；其他错误（权限 / IO）warn
// 但不中断后续文件。
//
// transient 语义：每次 model 调用时读取（每次 model invocation 都重新读文件），
// 支持业务热改 AGENTS.md；小文件 + OS page cache，开销相对 LLM 调用可忽略。
func loadAgentsMD(ctx context.Context, paths []string) string {
	var b strings.Builder
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "WithAgentsMD: read failed", "path", p, "err", err)
			}
			continue
		}
		s := strings.TrimSpace(string(data))
		if s == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(s)
	}
	return b.String()
}
