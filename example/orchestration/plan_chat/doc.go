// Package main 是 PlanAgent 的多场景 replan-每步 demo。
//
// 通过 -scenario=<name> 切换不同任务结构，对照观察 replan 在什么情况下有价值、
// diff 以何种形态出现（= 不变 / ~ 修改 / - 砍掉 / + 新增）。
//
// # 运行方式
//
//	# 默认 qwen（需 QWEN_API_KEY）；或 -provider=deepseek（需 DEEPSEEK_API_KEY）
//	go run ./example/orchestration/plan_chat -scenario=<name>
//
// 启动后打印该 scenario 的 banner（描述、replan 看点、专家列表、建议对话脚本），
// 按提示输入；每执行完一步终端会打 "[plan←]" 显示本次 replan 的剩余计划与 diff。
// 输入 /exit 或 Ctrl-D 退出。
//
// # scenario 一览
//
// ## default — 基线对照（replan 价值低）
//
//   - 角色：researcher / writer / critic
//   - 任务结构：线性写作流水线，步骤事前可精确拆解
//   - replan 看点：通常只切片不改动，作为对照组帮助看出其他场景的差异
//   - 建议对话：
//     1. 整理 "Go goroutine 调度" 的博客素材，要有批评视角
//     2. 再加一段代码示例
//     3. 去掉批评部分，改成面向初学者的入门教程
//
// ## code_review — 代码 review/fix 循环（方向 A）
//
//   - 角色：coder / critic
//   - 任务结构：写代码 → 评审 → 修 → 再评审，循环次数事前不可定
//   - replan 看点：critic FAIL → 追加 coder+critic；critic PASS → 直接收尾
//   - 建议对话：
//     1. 实现一个 Go 函数 FizzBuzz(n int) []string
//     2. 再实现一个并发安全的 LRU 缓存
//   - critic 第一行强约束 "VERDICT: PASS/FAIL"，planner 据此判定是否再循环
//
// ## feasibility — 可行性调研早收尾（方向 B）
//
//   - 角色：researcher / writer / concluder
//   - 任务结构：调研事实可能让后续步骤变废，replan 砍掉
//   - replan 看点：FEASIBLE: NO → 砍 writer 直接 concluder；YES → 保留全链路；
//     PARTIAL → 追加一步绕路调研
//   - 建议对话：
//     1. 研究一下 Go 标准库 net/http 能否实现 HTTP/3 服务端
//     2. 再看看 sync.Map 是否适合做高频写入的本地缓存
//
// ## fault — 故障诊断（方向 C）
//
//   - 角色：log_inspector / db_inspector / code_inspector / fixer
//   - Mock 工具：read_log / query_db / read_code（内嵌假数据）
//   - 任务结构：每步发现决定下一步查哪个组件，诊断路径事前不可预测
//   - replan 看点：log → 提示 DB → 追加 db_inspector → 发现 schema 不匹配 →
//     追加 code_inspector + fixer
//   - 建议对话：
//     1. 接口 /api/order POST 返回 500，帮我定位并给修复方案
//
// ## budget — 预算约束回退（方向 D）
//
//   - 角色：flight / hotel / activity / budgeter
//   - Mock 工具：flight_search / hotel_search / activity_search（返回固定报价）
//   - 任务结构：约束触发时回退 / 替换已规划步骤
//   - replan 看点：超预算 → budgeter STATUS: OVER → replan 改用 budget 酒店
//     或砍 activity，diff 同时出现 - / ~ / +
//   - 建议对话：
//     1. 下周去上海出差 3 天，预算 2000 元，帮我规划机票+酒店+1 个活动
//     2. 如果预算降到 1500 元呢？
//
// ## drill — codebase 递归钻取（方向 E）
//
//   - 角色：overview / driller / summarizer
//   - Mock 工具：list_dir / read_file（内嵌假 repo 结构）
//   - 任务结构：根据当前理解决定下一个钻取点，深度事前不定
//   - replan 看点：overview 指 service/ → driller 进 service/ → 指 order/ →
//     driller 进 order/ → summarizer 收尾
//   - 建议对话：
//     1. 帮我看下这个 repo 主要在干嘛，重点说复杂度高的模块
//
// ## arbiter — 多源对比仲裁（方向 F）
//
//   - 角色：searcher_a / searcher_b / searcher_c / arbiter
//   - Mock 工具：search_a / search_b / search_c（三源对同题可能给不同答案）
//   - 任务结构：分歧出现时追加查证；一致时直接收尾
//   - replan 看点：源 A/B 分歧 → arbiter DISAGREE → replan 追加 source_c
//     → 重新 arbiter；一致则 AGREE → 直接收尾
//   - 建议对话：
//     1. Go goroutine 的初始栈大小到底是 2KB 还是 8KB？
//     2. 关闭一个已经关闭的 channel 会发生什么？
//
// ## writing_team — 扩展写作团队（ISSUE-72 R7）
//
//   - 角色：7 个写作类专家协作
//   - replan 看点 / 建议对话：见启动 banner
//
// # 架构
//
// scenario 定义已抽到 example/scenarios 中立公共包（各 scenario_*.go 通过 init() 注册
// builder，scenarios.Names() 列全部 8 个 / scenarios.Build(name,...) 构造）。本 demo
// （main.go）提供共享基础设施：
//   - newJSONPlanner：真 replan-每步 Planner，每次调用重新跑 plannerAgent，闭包缓存
//     prevRemaining 以打印 diff
//   - traceHook：OnAgentStart/OnModelStart/OnModelEnd/OnToolStart/OnToolEnd 打印
//   - makeExecInputBuilder：格式与 agent_v2 默认 defaultExecInputBuilder 对齐 + 打 trace
package main
