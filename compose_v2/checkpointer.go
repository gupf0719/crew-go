package compose_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Checkpointer 是 ns-aware checkpoint 存储抽象——业务可注入自己的实现
// （Redis / Postgres / S3 等）。
//
// 由 compose 旧版 Checkpointer（17 方法）精简而来。当前 8 方法：
// SaveNS / LoadNS / ExistsNS / DeleteSubtree / ListPending / LoadAllInSession /
// SaveBatch / Cleanup——覆盖 (sid, ns) 物理分离寻址；不保留旧 Save/Load 无 ns
// 兼容层。跨 Run 的会话历史**不在这一层**（cp 单次 Run 临时态、成功即清）——
// 走 agent_v2 的会话历史层，见 ISSUE-98。
//
// 物理寻址：cp 按 (sessionID, ns) 二元组索引——单一 sid 下多 ns 表达嵌套
// 子图 cp 物理分离。
//
// **实现契约**：
//   - SaveNS：cp.ID/cp.NS 自动填充为入参 sessionID/ns（覆盖 cp 自带值）
//   - 所有方法**并发安全**
//   - DeleteSubtree：删 sessionID 下 nsPrefix 这次执行的子树（nsPrefix 自身 + 经 `/`
//     往下的后代，判据 [IsNSInSubtree]；同一个节点的 `#`/`@` 兄弟执行不在内）；
//     prefix="" 删整 session
//
// # 残留：清理部分失败时留下的半棵子树，会不会被后面的清理扫掉
//
// 非空前缀的删除允许不原子（见 `DeleteSubtree` 给实现者的契约），所以会有残留。
// **判据只有一条，跟「哪个函数调的」无关**：
//
//	DeleteSubtree(sid, P) 留下的残留，会被之后任何 DeleteSubtree(sid, P') 扫掉，
//	其中 IsNSInSubtree(P', P) 为真（P' 是 P 自身，或经 `/` 在 P 之上的某一层）。
//
// 所以要问的不是「哪个调用点」，是「**这次清理之后，还有没有更外层的清理者**」：
//
// 先问「这次调用会不会留下残留」，再问「有没有更外层的清理者」：
//
//   - **不会留残留**：空前缀在多数 backend 上是单条原子操作（`sql` 是单条 DELETE，
//     `redis` 整个 `DeleteSubtree` 都是原子 Lua、空非空都不留）。⚠️ 这是**实现性质
//     不是接口保证**——业务自研 backend 若统一走「枚举 + 分批」，空前缀同样会留残留。
//   - **会留残留 + 有更外层清理者**（子图之上的父 Run、嵌套 Run 之上的祖先 Run）⟹
//     会被扫掉，**但只在它成功收尾、或以 [ErrRunAborted] 终局失败时**。两个例外：
//     祖先带**普通业务错**退出时一次 DeleteSubtree 都不发（实测三档：成功→
//     DeleteSubtree、业务错→SaveNS、ErrRunAborted→DeleteSubtree）；开了
//     [WithStaleWriteGuard] 且本次执行被判陈旧时，成功与 abort 两条路都在 purge
//     **之前**返回，同样不扫。
//   - **会留残留 + 没有更外层清理者** ⟹ 留着。今天三个形态：嵌套 Abort（业务显式放弃
//     某个子树）；**业务自己择机调 `DeleteSubtree(sid, 非空ns)` 显式清**（本包别处明确
//     推荐过这个写法）；以及业务在顶层 push 了非空 ns 的 Run——最后这个**框架代码里零实例**
//     （但业务可达：`WithGraphNS(ctx, "tenant-a")` + 裸 `Run`，入口会回落到 ctx 里的 ns）
//     （框架里 `WithRunNS` 要么是 `if ns := GetGraphNS(ctx); ns != ""` 的转发，要么是
//     子图节点无条件传 childNS，而子图按构造就在父 Run 内）。
//
// 留着的残留**对执行是够不着的**（auto-load 走 `LoadNS(sid, 精确 ns)`，够不到后代），
// 但**对枚举是可见的**——`ListPending` 与 `LoadAllInSession` 都会返回它（实测）。业务按
// sid 建恢复面板 / 告警时会看到一条永远处理不掉的挂起项，最终由业务自己的 `Cleanup`
// 回收：**框架没有自动 reaper**。
//
// ⚠️ 这段刻意**不列调用点清单**。那种写法在 `sql` 的 godoc 里试过三版，每版都在修上一个
// 错的同时造出新错——它的输入维（调用点集合、各自运行期的 ns、祖先的退出路径）全在别的
// 包，散文追不上。上面这条判据不枚举调用点，加第五个调用点时它自动仍然正确；调用点集合
// 本身由 `delete_subtree_callsite_test.go` 机械对账。
type Checkpointer interface {
	// SaveNS 按 (sessionID, ns) 持久化 cp。
	// cp.ID/cp.NS 自动填入 sessionID/ns。返 error 时 cp 状态未定义。
	SaveNS(ctx context.Context, sessionID, ns string, cp *Checkpoint) error

	// LoadNS 按 (sessionID, ns) 精确寻址。不存在返 (nil, nil)（不返 error 让
	// caller 用 nil cp 判断"无快照"）。
	//
	// **非破坏性 + 幂等承诺**：LoadNS 不修改 cp store——重复调用同 (sessionID, ns)
	// 返同 cp 内容（语义层面幂等）。业务可在 LoopAgent 多 round / BasicAgent ReAct
	// 多 round / retry / Resume 路径自由复用 LoadNS 调用本身, 无副作用。
	//
	// **⚠️ 返回值是否深拷贝由实现决定**：`InMemoryCheckpointer.LoadNS` 返存储内**同**
	// 指针 (零拷贝快); 自定义 Redis/PG/S3 实现通常天然深拷贝 (序列化 round-trip)。
	// **业务勿写返回 cp 的字段** (Channels map / PendingInterrupts slice 等)——
	// InMemory 实现下会直接 mutate store 破坏 cp invariant. 业务想修改 cp 自行
	// 深拷贝 (或在自定义实现内加拷贝层).
	//
	// **物理分离持久化**：子 cp 按 (sid, ns) 独立存，LoadNS 是幂等读——同一 ns
	// cp 可重复 Load 拿到同一份，不是"取一次即消费、第二次拿不到"的一次性语义。
	// 协议简单可预测，业务无消费时序负担。
	LoadNS(ctx context.Context, sessionID, ns string) (*Checkpoint, error)

	// ExistsNS 快速判断 (sessionID, ns) cp 是否存在。
	ExistsNS(ctx context.Context, sessionID, ns string) (bool, error)

	// DeleteSubtree 删 sessionID 下 nsPrefix 这次执行的子树：nsPrefix 自身，以及经 `/`
	// 往下的后代（范围由 [IsNSInSubtree] 定义）。返删除数量。prefix="" 表示删整个 session。
	//
	// ⚠️ **同一个节点的其他执行不在范围里**：`sub#0`（fan-out 实例）、`sub@1`（重访实例）
	// 跟 `sub` 是兄弟，各自清自己的。按「`sub` 名下的一切」删的话，一次执行成功就把兄弟
	// 还挂着的存档连带删了。
	//
	// # 给实现者的契约（本方法自己要保证的，到此为止）
	//
	//   - 判据是 [IsNSInSubtree]，**段级匹配**——`sub` 的子树含 `sub/x`，**不含** `subx`
	//     （别用裸字符前缀，业务 PG backend 的经典坑），也**不含** `sub#0`、`sub@1`。
	//   - 非空前缀的实现**允许不原子**（`sql` 那个是「枚举 + 分批删」）：失败时返
	//     「**已删条数** + 那个错」，留下半棵子树是可接受的。**别为此加事务或重试**——
	//     引擎那侧对残留有自己的处置（见 [Checkpointer] 类型 godoc 的「残留」一节）。
	//   - 空前缀（删整 session）**应当**是单条操作、天然原子。
	DeleteSubtree(ctx context.Context, sessionID, nsPrefix string) (int32, error)

	// ListPending 返 sessionID 下所有 Status=Pending 的 cp。
	// 用于 Resume 入口枚举有 pending cp 的 ns 子树, 业务 caller / framework
	// graph_loop 拿来决定哪些 ns 需要续跑.
	//
	// ⚠️ godoc 修正 (audit follow-up #5): v1 时代曾配 `IsResumeTarget` helper
	// 做"祖先 prefix 自检", v2 已删此 helper, 走 map ID 命中协议.
	ListPending(ctx context.Context, sessionID string) ([]NSCheckpoint, error)

	// LoadAllInSession 返 sessionID 下所有 ns cp（业务跨 ns 全景查询用）。
	//
	// 性能注意：N 大时开销显著，业务自行决定调用频率。
	LoadAllInSession(ctx context.Context, sessionID string) ([]NSCheckpoint, error)

	// SaveBatch 一次原子写多个 cp（同 sessionID 下不同 ns）。
	//
	// 内存实现走顺序 SaveNS；事务后端可优化为真原子。best-effort 失败时返 error，
	// caller 需自行处理部分成功状态。
	SaveBatch(ctx context.Context, sessionID string, batch map[string]*Checkpoint) error

	// Cleanup 按 sid 维度保留最新 keepLastN 个 sid（按 sid 下所有 ns cp 中最大的
	// CreatedAt 排序），超出部分整 sid 删除。
	//
	// keepLastN <= 0 等价 0：删全部 sid。返删除的 cp 总数。
	Cleanup(ctx context.Context, keepLastN int32) (int32, error)
}

// NSCheckpoint 是 ListPending / LoadAllInSession 的返回元素，含 ns + cp。
type NSCheckpoint struct {
	NS         string
	Checkpoint *Checkpoint
}

// Checkpoint 是 cp 持久化结构——含 NS/ParentNS/Status/GraphID 元数据字段
// （对齐 ISSUE-70 + ISSUE-50）+ 业务 channel state。
//
// **嵌套子图 cp 物理分离**（Phase 5.1.a Step 4）：子图 cp 通过 SaveNS(sid,
// childNS, cp) 写到独立 ns 路径，**不再嵌入** Checkpoint 自身（旧 SubgraphSnapshots
// 字段已被否决并删除）。
type Checkpoint struct {
	// ── ISSUE-70 / ISSUE-50 元数据字段 ──
	ID        string           `json:"id,omitempty"`         // session ID（SaveNS 自动填）
	NS        string           `json:"ns,omitempty"`         // 本 cp 的 ns 路径
	ParentNS  string           `json:"parent_ns,omitempty"`  // 父 ns（O(1) 反查祖先）
	Status    CheckpointStatus `json:"status,omitempty"`     // Unset / Pending / Failed（只是报告位）
	SchemaVer int              `json:"schema_ver,omitempty"` // schema 版本（migration 用）
	GraphID   string           `json:"graph_id,omitempty"`   // ISSUE-50 P0：防串台

	// CreatedAt 是本次写入的时间戳（`time.Now().UnixNano()`），`Cleanup` 按它排序
	// 决定保留哪些会话。
	//
	// **框架自动盖**（`guardedSaveNS`，与 `Epoch` 同处）——但只在 caller 没给时盖，
	// 所以业务想自己控时间线可以显式填。
	//
	// ⚠️ **绕过 `Run` 直接写 backend 的路径不经过盖章**（自己调 `SaveNS` /
	// `SaveBatch`）：那样写进去的 cp `CreatedAt` 恒 0，会被 `Cleanup` 当成最旧的
	// 优先删掉。业务直接写时需自填。
	CreatedAt int64 `json:"created_at,omitempty"`

	// Epoch 是本次写入的**世代标记**——框架每次 SaveNS 前换一个新的随机 token。
	//
	// **用途**：`WithStaleWriteGuard()` 开启后，Run 在载入 cp 时记下它的 Epoch，
	// 写回前比对槽里的是否仍是同一个；不是则说明**另一个副本在这中间推进过**，
	// 返 ErrCheckpointStale 而不是静默覆盖它的成果。
	//
	// **为什么用随机 token 而非递增计数器**：我们只需判"变没变"、不需要比大小。
	// 递增计数器跨进程重启会从头开始，与 cp 里存的旧值撞号 → 误判"没变"
	//（实例身份那套踩过这个坑，靠 Restore 时 bump 兜底；这里用随机值从源头免疫）。
	//
	// **不开 guard 时它只是被写入、不被读取**——业务 backend 照常序列化即可，
	// 无需理解它的语义。
	Epoch string `json:"epoch,omitempty"`

	// TaskID 标识**这份存档属于哪一次任务**——「新开」时铸一枚，此后随存档一路带下去
	// **不换**（跟 `Epoch` 正相反，那个每次写都换）。
	//
	// # 它解决什么
	//
	// 中断寻址 ID 原先七维**全是位置**（graphNS / nodeID / instanceID / visit /
	// toolCallID / toolCallIdx / idx），而位置在新任务里全部重置 ⟹ 同一 sid 下两个任务
	// 走到同一个地方时 ID **确定性相等**（实测两个任务七维逐字段相同）。后果不是「状态错」，是
	// **给任务 A 的答案被任务 B 的问题消费掉**，`err == nil` 全程无告警——用户批的是
	// 转账，执行的是另一件事。校验拦不住：陈旧答案不是「对不上」，是**正好对上了**。
	//
	// 掺进这一维之后，两个任务的 ID 必然不同 ⟹ 陈旧答案落空、节点重新提问。
	//
	// # 为什么不能复用 `Epoch`
	//
	// `Epoch` 每次 `SaveNS` 都换（`Durability=Sync` 每超步写一次），是「**写代**」；
	// 而中断 ID 必须**跨 Resume 往返稳定**（人在思考期间调用方攥着它）。拿写代算 ID
	// 会让同一次 Run 内跨超步就漂。这一维要的是「**任务代**」：只在新开路径铸。
	//
	// # 业务自研 backend
	//
	// 照常序列化即可。**漏了不报错**，退化分两层（都静默）：跨任务 ID 退回必撞（= 引入
	// 这一维之前的行为）；且**首跑与续跑算出的 ID 不同**（首跑时代号在内存里、续跑读回
	// 空值），调用方手里的答案配不上、HITL 要多绕一轮才收敛。
	//
	// ⚠️ 引擎在续跑时**沿用存档里的值哪怕它是空的、绝不铸新**——铸新会让每次续跑都换
	// 一枚 ⟹ ID 每次都变 ⟹ **永不收敛**。退化必须是确定性的。`checkpointer_conformance`
	// 有断言。
	TaskID string `json:"task_id,omitempty"`

	// InputDigest 是这份存档**新开时**那次调用交来的摘要（调用方经 [WithInputDigest] /
	// [WithInputIdentity] 给出）。跟 `TaskID` 一样，新开时写一次、此后续跑一路沿用，不随后来
	// 的调用换。
	//
	// auto-load 捡起这份存档时跟本次调用的摘要比（显式 [WithResumeFrom] 那条路不比——那是调用方
	// 自己选的档），对不上时按摘要的角色处理：它是调用的身份（[WithInputIdentity]，存档槽由它
	// 派生）⟹ 两次输入不同的调用撞进了同一个槽，拒绝（不可重试、不清档——槽里那份属于另一次
	// 调用）；它只作核对（[WithInputDigest]，身份另有来源，如调用方给的 key）⟹ 同一次调用、输入
	// 变了，照样续跑、打告警。
	//
	// 它不决定「捡不捡」——续跑与否只看内容与任务（`HasResumableWork` / `BelongsToTask`）。输入
	// 变了不换一次新开，是因为「同一次调用、输入变了」的判断只有给身份的一方做得了：身份按内容
	// 定的调用，输入变了就已经是另一个槽；身份由 key 定的调用，调用方说了这是同一次。
	//
	// 业务自研 backend 照常序列化即可。漏了不报错：撞槽认不出来（后一次调用续上前一次调用的
	// 进度、`err == nil`），输入变了的告警也不再出现。
	InputDigest string `json:"input_digest,omitempty"`

	// ── 业务 channel state ──
	Channels          map[string]taggedValue `json:"channels,omitempty"`
	PendingInterrupts []*AskQuery            `json:"pending_interrupts,omitempty"`

	// CurrentFrontier 待执行成员。**身份内联在记录里**（`FrontierMember{NodeID,
	// InstanceID}`），不是「NodeID 数组 + 平行的身份数组」。
	//
	// 同一个 NodeID 可以有 N 个并发实例（唯一来源：`SendFrom` 同 Target 投递多次），
	// 只有 NodeID 时框架分不出谁是谁（ISSUE-96 已实证 6 个静默丢数据的侧面，详见
	// doc/issue/issue-96-branch-instance-identity.md）。
	//
	// ⚠️ **ISSUE-96 首版是并行数组，理由是「缺字段天然退化成静态边语义」——退化那半
	// 内联之后仍然成立**（缺 `instance_id` 键 → 空串 → 静态边语义），换掉的只是「两个
	// 数组按下标对齐」那半：那是类型系统够不着的外部约束，错位的后果是实例互相冒充。
	// 见 `FrontierMember` 类型 godoc。
	CurrentFrontier []FrontierMember `json:"current_frontier,omitempty"`

	// NodeVisits 跟踪每个节点的成功完成次数（audit Round 3 Bug #1 修复）。
	// 跨 Run/Resume 持久化让 InterruptID 编入 visit 维度——同节点 ReAct 多轮 /
	// LoopAgent 重访 / PlanAgent 循环时不同访问的 ask_user 算不同 ID 不串台。
	NodeVisits map[NodeID]int `json:"node_visits,omitempty"`

	// ── PA-H1（四编排 review）：部分失败超步的成功成员 ──

	// CompletedFrontier 中断超步里已成功完成的成员：其 writes 已 commit 进
	// Channels，Resume 重跑 CurrentFrontier（失败成员）成功后，这些成员**不重跑
	// NodeFunc**，只由 CompletedTargets 贡献出边、由 DeferredSends 重放投递。
	// 身份同样内联（见 CurrentFrontier / FrontierMember godoc）。
	CompletedFrontier []FrontierMember `json:"completed_frontier,omitempty"`

	// CompletedTargets 是上面那些成员**当时算好的出边目标**。
	//
	// **为什么要存、而不是 Resume 时重算**：出边是「那个成员成功」的一部分，在它成功
	// 的那一刻就随它的执行算完了（算不出来它当时根本不算成功）。Resume 时重算读到的是
	// 它跑完**之后**的状态——含兄弟成员与重跑成员的产出——跟它自己当时看到的不是一份
	// 东西；而且重算就意味着「已经成功、已经提交」的成员还能在出边上失败，那正是本字段
	// 引入时要消灭的那个半成功状态。
	//
	// **业务自研 backend 漏序列化不报错**，只是发散 fan-out 的成功分支后继静默丢失
	//（A→X、B→Y 部分失败，Resume 后 X 永远不跑、graph 假成功）——与 CompletedFrontier
	// 同款失效形态，`checkpointer_conformance` 有断言。
	CompletedTargets []NodeID `json:"completed_targets,omitempty"`

	// ConsumedAnswers 是**已经兑付过的答案位**（InterruptID 列表，见
	// compose_v2/answer_consumption.go）。
	//
	// 一个答案只能满足一个提问。调用方累积传历次答案是本框架预期的形态，所以中断寻址的
	// 身份一旦算错，后一个提问会被前一个的旧答案**静默满足**——用户根本没被问到而
	// `err == nil`。记下「这一位付过了」之后，任何一次身份判据出错都会当场终局失败
	// （`ErrRunAborted`，错误正文带节点 + ns + 那个身份），而不是静默生效。
	//
	// **业务自研 backend 漏序列化不报错**，只是这道兜底跨 Run 静默失效（同 Run 内仍在）。
	ConsumedAnswers []string `json:"consumed_answers,omitempty"`

	// Signals 是这张图**已提交的上行信号**（见 compose_v2/signal.go）——转交给谁、走哪条
	// 分支这类交给上方消费方的决定。做出它的那一步已经提交、续跑时不再执行，图跑完时就靠这里
	// 把它交上去。
	//
	// **业务自研 backend 漏序列化不报错**，只是续跑之后那些决定静默丢失：转交没发生、路由落到
	// 兜底，`err == nil`。`checkpointer_conformance` 有断言。
	Signals []SignalRecord `json:"signals,omitempty"`

	// CompletedSignals 是部分失败超步里**已成功成员**做出的上行信号——跟 CompletedFrontier 同一族，
	// 先记在一边，等这一超步的其余成员续跑成功时跟它们合并（同一超步是并发的，一个收件人只能有一个
	// 成员做决定）后才并入 Signals。
	//
	// **业务自研 backend 漏序列化不报错**：成功兄弟做出的决定续跑后静默丢失；续跑成员也做了决定时，
	// 本该报的设计错误也报不出来、它的值静默胜出。
	CompletedSignals []SignalRecord `json:"completed_signals,omitempty"`

	// RecipientKey 是这个任务铸收件人用的密钥（见 [SignalRecipient]）：一个任务一把，顶层新开时铸，
	// 嵌套图沿用，每张图的存档都带一份。续跑时靠它算出同一个收件人——上一个 Run 写下、存在写入者
	// 存档里的决定才认得出是发给谁的。
	//
	// **业务自研 backend 漏序列化不报错**，只是顶层续跑时密钥重铸、收件人变了，停下之前已提交的
	// 转交 / 路由决定续跑后静默丢失（`err == nil`）。它是密钥：存档本身就该按会话数据的敏感度保管。
	RecipientKey []byte `json:"recipient_key,omitempty"`

	// DeferredSends 中断超步里成功成员发的 SendFrom 动态投递（wire 形态）——
	// 与 CompletedFrontier 同时机重放（并入重跑超步的 next frontier + payload）。
	DeferredSends []SerializedSend `json:"deferred_sends,omitempty"`

	// NodeStates 节点私有中断态（见 node_state.go 的类型 godoc）——键是
	// `nodeStateKey(NodeID, InstanceID, Visit)`，值是 tagged JSON。
	//
	// 用途：让「一个节点内做了 N 件带副作用的事」的形状在中断重跑时跳过已完成的那几件
	// （典型是 tools 节点一轮 N 个 tool_call、其中一个抛中断）。
	//
	// **map keyed by ID 不是平行数组**——身份在键里，没有下标对齐可错（frontier 那轮的
	// 教训）。业务自研 backend 漏序列化本字段不报错，退化成「读不到进度 → 全部重跑」=
	// 引入本字段前的行为，是**安全侧**退化。
	NodeStates map[string]taggedValue `json:"node_states,omitempty"`

	// PendingSendWrites 是"这批写没能提交、待 Resume 补 apply"的 SendDirective
	// payload（wire 形态）。**Resume 在跑任何节点之前补 apply**，等价于"上次那批
	// ApplyAll 成功了"。
	//
	// **为什么需要**：SendDirective 的 target 与 payload 分两处走——target 进
	// CurrentFrontier，payload 走 ApplyAll 进 Channels。payload 那批 ApplyAll
	// 失败时一条都没提交，但 target 已进 frontier 且产 send 的节点已成功（不
	// 重跑，否则 visit 漂移 + 副作用双跑）→ 不存这批 writes 的话 payload 无从
	// 再生，Resume 时 target 静默读到零值。
	//
	// 与 DeferredSends 的区别：那个存"待调度的 target + payload"，这个只存
	// "待补的 writes"——target 已经在 CurrentFrontier 里了。
	PendingSendWrites []SerializedWrite `json:"pending_send_writes,omitempty"`
}

// SerializedSend 是 SendDirective 的 cp wire 形态（PA-H1）。
type SerializedSend struct {
	Target NodeID `json:"target"`
	// Payload 顺序保留（Append 等顺序敏感 reducer）。
	Payload []SerializedWrite `json:"payload,omitempty"`
	// Owner 发出本指令的节点 ID——Resume 时框架据此清掉"重跑目标的旧 send"防
	// Target 双发（issue-95#1）。**业务自实现 Checkpointer 必须序列化本字段**：
	// 漏了不报错，但重跑目标的旧 send 会残留 → 双发（跑
	// checkpointer_conformance 可抓）。
	//
	// **零值即旧行为**：Owner=="" 的旧 cp 判"不属于任何重跑目标"→ 保留（=本
	// 字段引入前的行为）。重跑集合恒不含空 NodeID（Compile 期
	// ValidateNodeIDForNS 拒空 nodeID），故不会误清。
	Owner NodeID `json:"owner,omitempty"`

	// OwnerInstanceID 发出者**自己**的实例身份（ISSUE-96）——与 Owner 配对，业务
	// 自研 backend **必须一并序列化**。
	//
	// **漏了不报错，只是静默退化**：purge"重跑成员的旧 send"是拿实例集合匹配的，
	// 少了本字段匹配键恒为 {Owner, ""}，跟带实例身份的重跑成员永远匹配不上 ⟹ 旧
	// send 一条都清不掉，Resume 时重放叠上重跑新产 = Target 双发。
	// 发出者是静态边节点时为空串（那类节点恒单实例，Owner 的 NodeID 已唯一）。
	OwnerInstanceID string `json:"owner_instance_id,omitempty"`

	// InstanceID 本指令派生出的**目标**实例的身份（ISSUE-96）——与 OwnerInstanceID
	// 是两回事：那个是"谁发的"，这个是"发给谁"。业务自研 backend **必须序列化**。
	//
	// **漏了不报错，只是静默退化**：deferred send 是给"上次部分失败超步的成功成员"
	// 留的重放记录，Resume 时按它重建 target 实例。身份掉了 ⟹ 重放出来的 N 个实例
	// 全拿空串 ⟹ 彼此无法区分 —— HITL 的 InterruptID 撞车（问题被 dedupe 吞掉、
	// 用户永远看不到）、子 cp 撞同一 namespace 互覆、观测 span 共用一个槽。
	InstanceID string `json:"instance_id,omitempty"`
}

// SerializedWrite 是单条 ChannelWrite 的 cp wire 形态：(channel, 类型 tag, JSON 值)。
// Type/Data 与 cp.Channels 的 taggedValue 同协议（RegisterValueType 注册名 + JSON 编码）。
type SerializedWrite struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// CheckpointStatus 标识 cp 生命周期状态（对齐 ISSUE-70）。
type CheckpointStatus int

const (
	// CPStatusUnset 是默认值（旧 cp 反序列化兼容）
	CPStatusUnset CheckpointStatus = iota
	// CPStatusPending 节点中断挂起，等 Resume
	CPStatusPending
	// CPStatusFailed 上次 Run **带着错误退出**，这份存档是失败现场不是等答案的挂起。
	//
	// # 为什么要跟 Pending 分开
	//
	// 「cp 存在 ⇒ 真的有人在等答案」是 auto-load 赖以成立的不变量：Run 入口捡起 Pending
	// cp 时假定它是个 HITL 挂起。但失败退出也会留下有 frontier 的存档，两者都标 Pending
	// 的话这个不变量就破了——下次同 sid 跑会静默从失败点恢复、再次撞同一个错，业务只看到
	// 「又失败了」，看不出上一次也是这么失败的。
	//
	// # 光看字段分不出来，必须由出口标
	//
	// 失败存档和 durability 中途存档**长得一样**（都是有 frontier、`PendingInterrupts`
	// 为空）。但后者是**崩溃恢复点**，必须能被自动捡起。区别只有 Run 出口知道——中途写
	// 的时候还不知道这次会不会失败。所以这一档是出口打的标，不是靠字段推出来的。
	//
	// # 标了之后会怎样
	//
	//   - **存档本身保留** ⟹ 现场（frontier / channel / 已完成的成员）一条不少
	//   - 业务能从 Status 看出「上次是失败不是挂起」
	//   - `ListPending` 不返它——那个 API 的语义是「谁在等人回答」，失败现场没人在等
	//
	// ⚠️ **它不再决定「捡不捡这份档」**。三处闸门（Run 入口 auto-load / 子图载入 /
	// `agent_v2.Resume`）统一读 [Checkpoint.HasResumableWork]，**按内容判**（frontier 或
	// 挂起清单非空），不看档位——理由见那个方法的 godoc。
	//
	// 所以「从失败现场续跑」在 `agent_v2` 的 9 个编排上是**通的**：同 sid 再跑一次就是
	// 从失败点重试（不带答案的 Resume 亦然），上游已成功的成员不重放。
	//
	// 这段曾经逐字写着相反的话（「auto-load 认不了它，判据是 `Status == CPStatusPending`」
	// 「对 Failed 档一律返 `ErrNoPendingCheckpoint`」）——那是撤掉档位 gate 之前的行为。
	//
	// **不需要区分确定性错和瞬时错**——那个判断框架做不了（它看到的只是一个 error，
	// 判不了下次会不会好），也不必做：标出「这是失败现场」就够了，要不要再试是业务的事。
	CPStatusFailed
)

// Resumable 报这个**档位**能不能用来恢复执行——[CPStatusPending] 和 [CPStatusFailed]
// 都能（两者都存着完整的 frontier 和 channel 状态）。
//
// ⚠️ **引擎的「捡不捡」判据已经不读档位了**（2026-08-13）：auto-load / 子图载入 /
// `agent_v2.Resume` 统一读 [Checkpoint.HasResumableWork]（按**内容**判——frontier 或
// 挂起清单非空）。本谓词保留给观测与报告场景；别再拿它当控制流判据，那会重造
// 「同一个问题两处各判各的」（`subgraph.go` 曾用本谓词、根 auto-load 用
// `== CPStatusPending`，同一份失败子档一条路捡一条路不捡，实测嵌套 Resume 时
// 副作用重放）。
func (s CheckpointStatus) Resumable() bool {
	return s == CPStatusPending || s == CPStatusFailed
}

// HasResumableWork 报「这份档里还有没跑完的东西」——frontier 非空（有节点等着跑）或
// 挂起清单非空（有人等着答）。**这是引擎「捡不捡这份档」的唯一判据**：Run 入口的
// auto-load、子图载入、`agent_v2.Resume` 三处闸门共用本谓词。
//
// # 为什么按内容判，不按 Status 档位判
//
// 三条，每条都实测过：
//
//  1. **对 backend 漏序列化鲁棒**。业务自研 backend 漏存 `Status` 时档读回 [CPStatusUnset]——
//     按档位判的话，一次真 HITL 中断退化成「fresh run 重放副作用 + 换新 InterruptID +
//     答案永远配不上」，全程只有一条 warn。按内容判照样续上、答案送达。
//  2. **拒得掉业务残档**。业务把一份「跑完了」的快照存进槽（frontier 空、无挂起），
//     按存在判会把它当未完成任务捡起来——实测 Run 静默返 nil、一个节点都没跑、存档
//     还被当成功清掉。按内容判它不是「可续的活」，照常 fresh run。
//  3. **与出口同源**。引擎出口的「没 frontier 也没挂起就不写档」早退、
//     `SnapshotForGraph` 的按内容盖章，用的都是这同一个谓词——判据只有一份，不漂。
//
// 档位（`Status`）从此只承担**报告**（观测事件、`ListPending` 过滤、debug dump），
// 不承担控制流。曾按档位挡「失败档别自动捡」——那道 gate 的立论（隐式 auto-load 会
// 静默捡起失败档再撞同错）在顶层已被 `agent_v2` 的新任务判定（恒 fresh）结构性覆盖，
// 而它在嵌套层挡掉的恰恰是**合法的续跑**：Resume 重跑失败分支时子图捡不起自己的
// 失败子档 ⟹ fresh run ⟹ 已完成超步的副作用重放（实测扣款 1→2；只把档位掰成
// Pending、数据一字节不改，重放消失）。
//
// 矛盾档（挂起非空而 frontier 空）会被本谓词放进来、随后被 `ErrCheckpointNotResumable`
// 自洽守卫**响亮拒绝**——这是有意的：那种档该被看见，不该被静默跳过。
func (cp *Checkpoint) HasResumableWork() bool {
	return cp != nil && (len(cp.CurrentFrontier) > 0 || len(cp.PendingInterrupts) > 0)
}

// BelongsToTask 判「这份存档属于代号为 taskID 的那次任务」。
//
// **两侧任一为空 ⟹ 恒 true（安全侧）**：taskID 空 = 调用方没有任务代号可给（直连
// `compose_v2.Run` 的裸调用、或引擎版本混用），cp.TaskID 空 = 旧存档 / 业务自研 backend
// 漏序列化——这几种情况下退化成引入这一维之前的行为（**照捡**），不引入调用方无法满足
// 的新要求（守卫合法性规约）。
//
// # 它给 auto-load 闸补的那一维
//
// 「有没有可续的内容」（[Checkpoint.HasResumableWork]）回答不了「**是谁的**内容」。
// 同 sid 跨任务复用时，上个被放弃任务留下的挂起档对内容判据来说与本任务的进度**无法
// 区分**——实测（重试第 2 次尝试清掉「新开」信号后）：B 的 model 永不产生 tool_call，
// 而 B 用自己的工具执行了 A 的待办、停在 A 的中断上，err 看着像正常中断。身份维把
// 「别捡上个任务的残留」和「这一次从头开始」拆开——重试要前者不要后者，而「新开」信号
// 只有一位、两者拆不开（那个只清信号的修法已实施并回退过一次）。
func (cp *Checkpoint) BelongsToTask(taskID string) bool {
	if cp == nil || taskID == "" || cp.TaskID == "" {
		return true
	}
	return cp.TaskID == taskID
}

// String 返人类可读名。
func (s CheckpointStatus) String() string {
	switch s {
	case CPStatusPending:
		return "pending"
	case CPStatusFailed:
		return "failed"
	default:
		return "unset"
	}
}

// ── 哨兵错误（对齐 compose 旧版） ──

// ErrNoPendingCheckpoint 是"显式 Resume 意图 + cp 不存在"的统一哨兵（GA-M2，
// 四编排 review 衍生协议修复）。agent_v2 九编排 Resume 入口（commonResumePrep）
// 在 (sid, ns) 无挂起 cp 时返本哨兵 wrap——sid 拼错 / cleanupOnSuccess 已清 /
// 从未跑过，三种情况都属调用方错误。
//
// 设计取舍：显式 resume 意图遇到无 cp 时 fail-fast，绝不静默跑业务逻辑
// （修复前 crew-go 静默 fresh-run：GraphAgent 空 channels 跑出垃圾 final 且
// err=nil）。业务用 errors.Is(err, ErrNoPendingCheckpoint) 区分"等用户答案"
// vs"会话已结束/不存在"。
var ErrNoPendingCheckpoint = errors.New("no pending checkpoint for resume")

// ErrGraphIDConflict 是 GraphID 防串台机制的统一哨兵（ISSUE-50 P0）。
// 检测到两个 GraphID 共享同 (sid, ns) 时返此 error——**写路径和读路径都查**：
// `SaveNS` 落盘前、Run 入口的 auto-load、显式 `WithResumeFrom` 载入、子图载入子档。
// （要全集就 grep 本哨兵的 `%w` 点，别指望这行清单不漂。）
//
// **它报的不是「这里有存量状态」，是「身份本身不自洽」**——同一个槽存着另一张图的
// channel 布局和 frontier，接着跑会错位崩盘，没有安全的默认行为可定义，所以只能拒。
//
// # 撞上之后怎么办：两条出路，各有代价
//
// **错误正文只陈述事实、不给建议**，因为两条出路的代价截然不同、而且**其中一条会毁数据**——
// 该选哪条只有调用方知道。
//
//  1. **换一个 sessionID**：旧存档原样留着（它可能正挂着另一个执行者等人回答的问题）。
//     代价是这个 sid 上的旧任务从此没人收——要么业务自己择机
//     `Checkpointer.DeleteSubtree(sid, ns)` 显式清，要么留给运维。
//  2. **[WithFreshRun]**：本次忽略旧存档从头跑。⚠️ **它顺带会毁掉那份旧存档**——本次
//     跑成功后出口按 sid 所有权清整棵子树（见 [WithSessionID]），根 Run 的 ns 是 `""`、
//     是所有 ns 的祖先，于是**整个 sid 底下的东西一起没**，包括别人的挂起存档。
//     ⚠️ 而且本次**中断**时也不安好：SaveNS 那道守卫不看 Status，槽里那个别人的 GraphID
//     会让本次的存档写不进去，于是这次自己的 HITL 反而不可续。
//     ⟹ 只在「这个 sid 确实归我、旧档可弃」时才用它。
//
// 想显式表达「我知道它在那儿、我要它没」，直接调 `Checkpointer.DeleteSubtree(sid, ns)`
// ——那是这个意图唯一诚实的写法，[WithFreshRun] 只是它的隐式版本、而且只在成功路径生效。
//
// # 嵌套子 agent 撞上时，业务两个杠杆都没有
//
// `agent_v2` 的 sub-agent 走的是「父 sid + 框架算出来的 ns」，业务既传不了 [WithFreshRun]、
// 也换不了 sid（两者都不是它给的）。这条路上撞上本错有两个含义：
//
//   - **节点里裸调了 `sub.Run(ctx, ...)`**，没走 `agent_v2.RunSubAgent`：子 agent 没有自己的 ns，
//     存档落进父图自己的槽（实测，常见）——出路是改走 `RunSubAgent`；
//   - 同一个父 sid 上先后跑了拓扑不同的两棵编排树——出路在编排侧（换一个 `RunSession`），
//     不在本次调用的参数里。
//
// # 不可重试，也不能被吞
//
// 它实现 [NonRetryable]：槽里那份存档还在，重跑必然撞同一个槽。曾经是 `errors.New`，
// [IsRetryable] 把它当可重试——配了重试的编排会把整个成员连同副作用再跑几遍、每次都撞上。
// 它也在 [MustPropagate] 的集合里：两个含义都是编排结构出了问题，转成给模型看的工具失败无济于事
// ——模型换不换参数都修不了它，冲突的那份存档也还留在槽里。
var ErrGraphIDConflict error = &graphIDConflictError{}

// graphIDConflictError 是 [ErrGraphIDConflict] 的类型。不导出：调用方只用 `errors.Is`
// 比那一个值，不该有第二个实例。
type graphIDConflictError struct{}

func (*graphIDConflictError) Error() string { return "graph_id conflict" }

// NonRetryable 实现 [NonRetryable]——身份冲突重跑必然一样。
func (*graphIDConflictError) NonRetryable() {}

// ErrCheckpointStale 表示**写回时发现 cp 已被别人推进**——`WithStaleWriteGuard()`
// 开启时才可能返回。
//
// 含义：本次 Run 载入 cp 后、写回前，另一个执行者（通常是并发 Resume 同一
// sessionID 的另一个进程副本）已经写过这个槽。本次的结果**没有被写入**，因为
// 覆盖上去会让对方的成果静默消失。
//
// **它不代表副作用没发生**：本次 Run 已经执行完了，有副作用的节点该跑的都跑了。
// 拿到这个错误意味着"你和别人重复干了同一份活，且你的账没记上"——业务应据此
// 补偿 / 告警 / 人工介入，而不是简单重试（重试只会再跑一遍副作用）。
//
// 根因是同一 sessionID 被并发 Run/Resume。正确的解法在**调用方**：会话亲和路由、
// 分布式锁、或串行队列。本守卫只负责让这件事**不再静默**。
var ErrCheckpointStale = errors.New("checkpoint stale: concurrently advanced by another writer")

// StaleWriteError 是 `ErrCheckpointStale` 的**结构化形态**——`errors.As` 取它拿到
// 出错的操作、会话、世代，不必从错误文案里 parse 字符串。
//
// `errors.Is(err, ErrCheckpointStale)` 对它同样成立（Unwrap 到那个哨兵），业务只关心
// "是不是陈旧"时照旧用 `errors.Is` 即可。
//
// **`Op` 是给观测用的低基数维度**（`save` / `purge-success` / `purge-abort`），
// 可直接当 metric label；`SessionID` / `NS` / `Baseline` / `Current` 是排查用的高基数
// 字段，**别拿去当 label**（会炸 cardinality），也别原样回传给前端 / 远端对端
// （`agui` 和 `a2a/server` 都做了脱敏，见各自的处置点）。
type StaleWriteError struct {
	// Op 出错的操作：save（写 cp）/ purge-success（成功后清理）/ purge-abort（终局失败清子树）
	Op string
	// SessionID / NS 定位是哪个会话的哪一层
	SessionID, NS string
	// Baseline 本 Run 载入时的世代；空串表示"载入时槽就是空的"
	Baseline string
	// Current 槽里当前的世代；空串表示"槽已被别人删空"
	Current string
	// Detail 人类可读的补充说明（哪一种冲突形态）
	Detail string
}

func (e *StaleWriteError) Error() string {
	return fmt.Sprintf("%s (session=%s ns=%s op=%s: %s)",
		ErrCheckpointStale.Error(), e.SessionID, e.NS, e.Op, e.Detail)
}

// Unwrap 让 errors.Is(err, ErrCheckpointStale) 成立。
func (e *StaleWriteError) Unwrap() error { return ErrCheckpointStale }

// ── InMemoryCheckpointer 实现 ──

// InMemoryCheckpointer 内存实现——测试 + 单进程场景。
type InMemoryCheckpointer struct {
	mu sync.RWMutex
	// data[sessionID][ns] = *Checkpoint
	data map[string]map[string]*Checkpoint
}

// NewInMemoryCheckpointer 创建空 InMemoryCheckpointer。
func NewInMemoryCheckpointer() *InMemoryCheckpointer {
	return &InMemoryCheckpointer{data: make(map[string]map[string]*Checkpoint)}
}

// SaveNS 按 (sid, ns) 存 cp。
//
// **GraphID 防串台**：若已存在 (sid, ns) 的 cp 且 GraphID != 入参 cp.GraphID，
// 返 ErrGraphIDConflict（fail-fast 阻挡静默错数据）。
func (c *InMemoryCheckpointer) SaveNS(_ context.Context, sessionID, ns string, cp *Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("checkpoint is nil")
	}
	if sessionID == "" {
		return fmt.Errorf("sessionID is empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.data[sessionID] == nil {
		c.data[sessionID] = make(map[string]*Checkpoint)
	}

	// GraphID 冲突守卫（ISSUE-50 P0 cross-audit Round 3）
	if existing, ok := c.data[sessionID][ns]; ok {
		if existing.GraphID != "" && cp.GraphID != "" && existing.GraphID != cp.GraphID {
			return fmt.Errorf("%w: sid=%q ns=%q existing=%q new=%q",
				ErrGraphIDConflict, sessionID, ns, existing.GraphID, cp.GraphID)
		}
	}

	// 自动填 ID/NS（覆盖 cp 自带值）
	cp.ID = sessionID
	cp.NS = ns
	if cp.ParentNS == "" && ns != "" {
		cp.ParentNS = ParentNS(ns)
	}

	c.data[sessionID][ns] = cp
	return nil
}

// LoadNS 按 (sid, ns) 取 cp。不存在返 (nil, nil)。
func (c *InMemoryCheckpointer) LoadNS(_ context.Context, sessionID, ns string) (*Checkpoint, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.data[sessionID] == nil {
		return nil, nil
	}
	cp, ok := c.data[sessionID][ns]
	if !ok {
		return nil, nil
	}
	return cp, nil
}

// ExistsNS 快速判 (sid, ns) cp 是否存在。
func (c *InMemoryCheckpointer) ExistsNS(_ context.Context, sessionID, ns string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.data[sessionID] == nil {
		return false, nil
	}
	_, ok := c.data[sessionID][ns]
	return ok, nil
}

// DeleteSubtree 删 sid 下 nsPrefix 这次执行的子树（自身 + 经 `/` 往下的后代，判据 [IsNSInSubtree]）。
func (c *InMemoryCheckpointer) DeleteSubtree(_ context.Context, sessionID, nsPrefix string) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data[sessionID] == nil {
		return 0, nil
	}
	var deleted int32
	for ns := range c.data[sessionID] {
		if IsNSInSubtree(nsPrefix, ns) {
			delete(c.data[sessionID], ns)
			deleted++
		}
	}
	if len(c.data[sessionID]) == 0 {
		delete(c.data, sessionID)
	}
	return deleted, nil
}

// ListPending 返 sid 下所有 Status=Pending 的 cp。
func (c *InMemoryCheckpointer) ListPending(_ context.Context, sessionID string) ([]NSCheckpoint, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.data[sessionID] == nil {
		return nil, nil
	}
	var out []NSCheckpoint
	for ns, cp := range c.data[sessionID] {
		// 只返真正等答案的。**失败现场（CPStatusFailed）不返**——这里是业务查「谁在等答案」
		// 的入口，失败现场不属于这一类。⚠️ 它**不等于**「失败档不会被续跑」：2026-08-13 起三处
		// 续跑闸门按内容判（`HasResumableWork`）、不读 Status，失败档照样被 auto-load /
		// `agent_v2.Resume` 捡起（不带答案 = 重试失败分支）。
		if cp.Status == CPStatusPending {
			out = append(out, NSCheckpoint{NS: ns, Checkpoint: cp})
		}
	}
	return out, nil
}

// LoadAllInSession 返 sid 下所有 ns cp。
func (c *InMemoryCheckpointer) LoadAllInSession(_ context.Context, sessionID string) ([]NSCheckpoint, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.data[sessionID] == nil {
		return nil, nil
	}
	out := make([]NSCheckpoint, 0, len(c.data[sessionID]))
	for ns, cp := range c.data[sessionID] {
		out = append(out, NSCheckpoint{NS: ns, Checkpoint: cp})
	}
	return out, nil
}

// SaveBatch 顺序写——内存实现 best-effort，SaveNS 失败立即返。
func (c *InMemoryCheckpointer) SaveBatch(ctx context.Context, sessionID string, batch map[string]*Checkpoint) error {
	for ns, cp := range batch {
		if err := c.SaveNS(ctx, sessionID, ns, cp); err != nil {
			return fmt.Errorf("SaveBatch sid=%q ns=%q: %w", sessionID, ns, err)
		}
	}
	return nil
}

// Cleanup 按 sid 维度保留最新 keepLastN 个 sid——按 sid 下所有 ns cp 的最大
// CreatedAt 排序，超出部分整 sid 删除。
func (c *InMemoryCheckpointer) Cleanup(_ context.Context, keepLastN int32) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if keepLastN <= 0 {
		// 删全部
		var deleted int32
		for sid := range c.data {
			deleted += int32(len(c.data[sid]))
			delete(c.data, sid)
		}
		return deleted, nil
	}
	if int32(len(c.data)) <= keepLastN {
		return 0, nil
	}
	type sidStamp struct {
		sid    string
		latest int64
	}
	stamps := make([]sidStamp, 0, len(c.data))
	for sid, byNS := range c.data {
		var latest int64
		for _, cp := range byNS {
			if cp.CreatedAt > latest {
				latest = cp.CreatedAt
			}
		}
		stamps = append(stamps, sidStamp{sid: sid, latest: latest})
	}
	// 按 latest 降序排（插入排序——cp 数量小）。latest 撞值（纳秒级 CreatedAt 相同）时按
	// sid 字典序二级 tie-break：保证 keepLastN 边界保哪个 / 删哪个确定，防 stamps 初始顺序
	// 来自 map 迭代（随机）导致非确定删除——keep 最新 + sid 最大那批。
	for i := 1; i < len(stamps); i++ {
		for j := i; j > 0 && (stamps[j-1].latest < stamps[j].latest ||
			(stamps[j-1].latest == stamps[j].latest && stamps[j-1].sid < stamps[j].sid)); j-- {
			stamps[j-1], stamps[j] = stamps[j], stamps[j-1]
		}
	}
	var deleted int32
	for _, s := range stamps[keepLastN:] {
		deleted += int32(len(c.data[s.sid]))
		delete(c.data, s.sid)
	}
	return deleted, nil
}

// 编译期接口实现检查
var _ Checkpointer = (*InMemoryCheckpointer)(nil)

// ── ctx 协议 ──

type checkpointerKey struct{}
type sessionIDKey struct{}
type threadIDKey struct{}

// withCheckpointer 注入 Checkpointer 到 ctx——Run 入口内部用，不导出。
func withCheckpointer(ctx context.Context, cp Checkpointer) context.Context {
	if cp == nil {
		return ctx
	}
	return context.WithValue(ctx, checkpointerKey{}, cp)
}

// WithCheckpointerCtx 注入 Checkpointer 到 ctx——caller-facing helper。
//
// 业务（如 agent_v2 高层 wrapper）想让下游 Run 调用从 ctx 取 Checkpointer 而不是
// 显式传 RunOption 时用此 helper。区别于 RunOption WithCheckpointer——RunOption
// 是显式给某次 Run 注入，本 helper 是把 cp 沿 ctx 链透传给所有下游调用。
//
// 典型用法：
//
//	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
//	events, err := agent.Run(ctx, sess, input)  // BasicAgent 内部 GetCheckpointer 拿到 cp
func WithCheckpointerCtx(ctx context.Context, cp Checkpointer) context.Context {
	// audit¹¹ HIGH-2 fix: isFrameworkNil 检 typed nil Checkpointer (与 WithCheckpointer 协议对称).
	if isFrameworkNil(cp) {
		return withCheckpointer(ctx, nil) // 等价 untyped nil 注 nil — 不破坏 "无 cp" idiom
	}
	return withCheckpointer(ctx, cp)
}

// GetCheckpointer 从 ctx 取 Checkpointer——节点函数 / 子图节点可调用。
// nil 表示业务未注入——子图嵌套场景下中断会丢失嵌套 cp（必须显式注入）。
func GetCheckpointer(ctx context.Context) Checkpointer {
	if cp, ok := ctx.Value(checkpointerKey{}).(Checkpointer); ok {
		return cp
	}
	return nil
}

// withSessionID 注入 sessionID 到 ctx——框架内部用（Run 入口、每次节点执行的 base）。跟公开的
// WithSessionIDCtx 只差一件：不在所在的钩子调用上记「推过身份」——框架自己推的不是钩子推的，而节点
// 执行这条路每次都走，记号要沿 ctx 链查一遍。
func withSessionID(ctx context.Context, sid string) context.Context {
	if sid == "" {
		return ctx
	}
	return context.WithValue(ctx, sessionIDKey{}, sid)
}

// WithSessionIDCtx 公开版本: 业务/observability 在非 Run 路径需 push sessionID 到
// ctx 时用 (e.g., 业务自己 spawn goroutine 调 framework helper; observability 测试
// 触发 sessionByID 不同 sid).
//
// Run 入口已自动 push, 业务通常不调本 helper. **多次同 sid push 是 idempotent**
// (ctx.Value 直接覆盖, GetSessionID 总返最新值).
//
// 在业务钩子里推（推在钩子拿到的 ctx 上、再把它返回）不生效——身份取框架的，采纳时会告警，见
// [CallHook]。
func WithSessionIDCtx(ctx context.Context, sid string) context.Context {
	if sid == "" {
		return ctx
	}
	markIdentityPush(ctx)
	return context.WithValue(ctx, sessionIDKey{}, sid)
}

// GetSessionID 从 ctx 取 sessionID——节点函数 / 子图节点可调用。
// 空字符串表示业务未注入。
func GetSessionID(ctx context.Context) string {
	if sid, ok := ctx.Value(sessionIDKey{}).(string); ok {
		return sid
	}
	return ""
}

// WithThreadIDCtx push 跨 run 会话 id (thread id) 到 ctx——observability 读出作
// gen_ai.conversation.id span attr，用来聚类同一个会话的多次 Run / 多轮 turn。
//
// thread id 是业务的会话 / 对话标识，**跨多次 agent Run 稳定**（如多轮聊天 app
// 里浏览器侧持久的会话 id）；而 GetSessionID（= RunID）是单次 run 标识。两者职责
// 不同——一个会话含多次 run。
//
// **threadID 必须跨 run 稳定**：若传"每次 run 都新建的 id"（= 等于 RunID），会让
// gen_ai.conversation.id ≡ crew.run.id，退化成假区分——这种场景别 push（保持不 emit）。
//
// **框架不自动注入**（会话 id 是业务概念，框架不知道哪些 run 属同一会话），业务在
// Run 入口按需 push。空则 observability 不 emit
// gen_ai.conversation.id（不造无意义的 = crew.run.id 重复）。
//
// 在业务钩子里推（推在钩子拿到的 ctx 上、再把它返回）不生效——会话 ID 是对话的身份、取框架的，采纳时会
// 告警，见 [CallHook]。
func WithThreadIDCtx(ctx context.Context, threadID string) context.Context {
	if threadID == "" {
		return ctx
	}
	markIdentityPush(ctx)
	return context.WithValue(ctx, threadIDKey{}, threadID)
}

// GetThreadID 从 ctx 取 thread id（跨 run 会话标识）。空字符串表示业务未注入。
func GetThreadID(ctx context.Context) string {
	if tid, ok := ctx.Value(threadIDKey{}).(string); ok {
		return tid
	}
	return ""
}
