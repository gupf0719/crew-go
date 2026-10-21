// Package sandbox 定义远程命令执行沙箱的可插拔 seam——业务把 skill 脚本 / 命令执行下沉到
// 远端沙箱（容器化运行时隔离），框架只经 Sandbox 接口跑一条命令、不 own 执行安全模型
// （权限 / 隔离 / 超时 / 认证全收敛在后端实现里）。
//
// 接口定义在使用方（NewBashTool 消费），后端（如远程 HTTP 沙箱服务、e2b / Docker / 本地 subprocess）
// 实现之——`sandbox_bash` 工具依赖接口不依赖具体 harness，接新 harness 零成本改工具。
package sandbox

import "context"

// ExecResult 是一次沙箱命令执行的结果。
type ExecResult struct {
	// Output 命令输出文本（供回灌 LLM）。后端不分离 stdout/stderr（如部分远程服务合并返回
	// stderr-优先-else-stdout 一段文本）时全部进此字段。
	Output string
	// Stdout / Stderr 分离的输出——后端能分离时填（Output 留空或作主展示）；不分离的后端留空。
	Stdout string
	Stderr string
	// ExitCode 命令退出码。后端拿不到时（如远程服务不返 exit code）为 0——**业务不应仅凭
	// ExitCode==0 断定成功**，须看 Output（命令失败信息可能在 stderr 文本里），对齐
	// "RPC 200 ≠ 命令成功"。
	ExitCode int
}

// Sandbox 是远端命令执行沙箱的可插拔后端——一个 Exec 撑起全部文件 / 脚本语义（read/write/ls
// 由 caller 用 shell 命令拼，无需后端各实现）。
type Sandbox interface {
	// Exec 在沙箱内执行一条命令（整段 bash，可含管道 / 重定向 / 多命令），返回输出。
	// 返回的 error 表示传输 / 后端故障（**非命令本身失败**——命令失败信息在 ExecResult）。
	Exec(ctx context.Context, command string) (*ExecResult, error)
}
