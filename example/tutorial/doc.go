// Package tutorial 是 crew-go 9 编排的教学集成测试——真 LLM（deepseek-v4-flash，OpenAI
// 兼容）跑通每个编排的最小示例。统一 //go:build integration，默认 `go test ./...` 不编译，
// 必须显式加 -tags=integration 才跑（单测/CI 默认路径不被 5 分钟级用例拖累）。API Key 从
// .env 自动读取。
//
// # 批量运行（必须带 -tags=integration）
//
//	go test -tags=integration ./example/tutorial/ -v -run TestAdv    # 进阶 9 场景 (01-08 + 10)
//	go test -tags=integration ./example/tutorial/ -v                 # 全部
//
// # 单独运行（复制粘贴即可）
//
//	go test -tags=integration -v -run TestAdv_01_BasicAgent            ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_02_SequentialAgent       ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_03_ParallelAgent         ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_04_LoopAgent             ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_05_RoutingAgent          ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_06_HandoffAgent          ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_07_PlanAgent             ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_08_ReflectAgent          ./example/tutorial/
//	go test -tags=integration -v -run TestAdv_10_Hooks                 ./example/tutorial/
//
// 注：TestAdv_09_GraphAgent / TestAdv_11_TypedGraph / TestAdv_12_Checkpoint /
// TestAdv_13_SuperstepAndSendFrom 在 v1→v2 channel-based 重构时随旧 example 一并删除——
// 等价场景见 `agent_v2/r30_graph_agent_hook_order_test.go` + `r31_graph_resume_test.go`
// (GraphAgent) / `compose_v2/checkpointer_*_test.go` (Checkpoint) /
// `compose_v2/graph_loop_test.go` + `fanout_test.go` (Superstep + SendFrom) /
// `compose_v2/channel_*_test.go` (typed Channel 等价 TypedGraph)；待补 example 见
// `doc/code/todo.md` "Example 覆盖补充" 表。
//
// 交互式 REPL demo（basic_chat / plan_chat / handoff_chat 等）见 example/orchestration/，
// 全部 example 目录导航见 example/README.md。
package tutorial
