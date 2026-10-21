// 包 schedule 提供通用定时任务调度能力，是与 agent 无关的独立叶子包。
//
// 心智模型就是 crontab：给一条触发规则（cron 表达式 / 固定间隔 / 一次性）和一个
// handler，到点就调 handler。handler 干什么完全由业务决定——算数据、跑 agent、
// 调接口都行，本包一概不管，也不 import agent_v2。
//
// 两种运行形态共用同一个 JobFunc：
//   - Local：进程内调度器（对标 APScheduler），本包直接提供。
//   - XXL-JOB：（规划中，未实现）拟注册为 XXL-JOB 执行器，由调度中心按 cron 触发、
//     按路由策略在多集群节点间取重。当前无实现（见 doc/code/todo.md backlog）。
package schedule

import "context"

// JobFunc 是定时任务的执行体。
//
// ctx 在调度器 Stop 时被取消，handler 应尽量感知以支持优雅停；XXL-JOB 形态下 ctx
// 还会携带调度中心下发的超时。返回的 error 用于失败上报（本机记日志 / 回调，
// XXL-JOB 回报调度中心 FAIL）。只想要一个裸函数就用 Simple 适配。
type JobFunc func(ctx context.Context) error

// Simple 把一个无参无返回的函数适配成 JobFunc——最省事的写法。代价是它永远上报
// 成功（吞掉一切结果，panic 仍会被调度器兜住转成 error 上报）。需要取消 / 超时感知
// 或失败上报时，请直接写 JobFunc。
func Simple(fn func()) JobFunc {
	return func(context.Context) error {
		fn()
		return nil
	}
}
