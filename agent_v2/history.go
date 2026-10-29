package agent_v2

// 会话历史层（ISSUE-98）——把消息流按**会话**留下来，跨 Run 活着。
//
// # 它补的是哪个洞
//
// 框架里已有三层存储，但**都按单次 Run 切片**：cp 的主键是 per-Run 的 sessionID、
// store 的 key 前缀是 per-Run 的 RunID、cp 自己的 ThreadID 字段在 SaveNS 里被填成
// sessionID。会话身份（`compose_v2.WithThreadIDCtx`）声明过、被 trace 和 handoff
// 消费，唯独没有一层按它组织消息。
//
// 而完整的消息流**一直都在**：`basicMessagesCh` 是 append-only 的 Append reducer，
// 每轮 LLM 产出、每条工具结果都进去，4 层压缩只作用于 modelFn 内的局部变量、从不写回。
// 它随 cp 的「成功即清」一起蒸发——差距不是「没有历史」，是「守着完整历史没留下」。
//
// # 归属不需要记录
//
// channel 是 agent 私有的：父 agent 的 channel 里，子 agent 的产出是 `tool` 角色
// （父调了个工具、拿到结果），子 agent 的 channel 里才是它自己的对话。所以
//
//	channel 里每条消息的归属 = 拥有这个 channel 的那个 agent
//
// 写入时用 agent 名标上即可（见 HistoryAgentAnnotation），不需要在消息里额外携带。
//
// # 它不负责执行正确性
//
// 工具会不会重跑、Resume 从哪继续，全由 cp 的 frontier 决定，跟历史一条都不沾。
// 所以这一层是**旁路**：写失败吞掉 + warn，不打断主链路（见 historyWriter）。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// HistoryStore 是会话历史的持久化后端。接口定义在使用方（agent_v2），实现可以是
// 内存 / Redis / PG / 任何 append-only 存储。
//
// # append-only 是硬约定
//
// 「增量写不需要游标」这件事完全建立在它上面：框架每次只把**新产生的那几条**交给
// Append，从不回头改写、从不按位置定位。实现方若做了去重、改写或截断，历史就不再是
// 「上次那轮到底说了什么」的可靠回放——这一层的全部价值就没了。
//
// 这也是它**不复用 `store.Store`** 的原因：那层是「提炼后的知识」（`WithStoreMemory`
// 走 Search 检索片段），为了保持当前认知必须能改写和删除既有条目；历史是「逐字时序
// 回放」。读取形状不同（按相似度返片段集合 vs 按时序返有序流），写入语义直接冲突。
//
// # 实现方注意
//
//   - Append 的 msgs 是框架持有的 slice，实现方**不得原地改写**它或它的元素；要留存
//     请自己拷贝。**这个「元素」是深的**：框架交给 Append 的 `*schema.Message` 是一份
//     浅拷贝——顶层字段和 Annotations 是新的，但 ContentBlocks / ToolCalls 等切片仍与
//     还在图内流转的那条消息**共享底层数据**。后端要跨 goroutine 长期持有或做异步序列
//     化，应对整条消息做深拷贝，不能假设独占这些子结构。
//   - Load 返回的 slice 会被调用方读（喂给模型），实现方同样应返回快照而非内部引用。
//   - **Annotations 的值按 JSON 类型读回**：布尔还是布尔、数字还是数值（读回成 float64 也算）、数组还是数组
//     （[]any 也算）、对象还是对象。框架有标注按布尔判（skill 正文、return-direct 结果的标记），读回成文本 "true"
//     时判据认不出、静默退化（第 0 层截断不再保护 skill 正文）。把整条消息按 JSON 存取就满足；只存得下字符串的
//     结构（Redis hash 字段、字符串列）要把值按 JSON 编码后再存。
//   - 两个方法都可能被多 goroutine 并发调用（异步 worker + 主流程读），需自己保证
//     线程安全。
//   - **Load 要能只读尾部**（[HistoryQuery] 的 Before + Limit）：框架每次 Run 只读最近一段（见
//     history_read.go），后端要把「取 Before 之前最近 Limit 条」下推成一次有界的读——SQL 用
//     `WHERE seq < ? ORDER BY seq DESC LIMIT ?` 再反转、Redis 列表用 LRANGE 负下标。在内存里
//     读出全部再切片当然也满足契约，但那正是这个接口要省掉的成本。
//   - **Load 要能按条件读**（[HistoryQuery] 的 MessageID / Task / Agent，非空的都要满足）：框架的去重
//     （「这个任务里记过这条没有」「这条是不是已经在别的归属下记过」）与「只看自己」的视图都按条件查。
//     消息 ID、任务章（[HistoryTaskAnnotation]）、归属（[HistoryAgentAnnotation]）写入时抽成带索引的列，
//     按条件读就只碰命中的记录——SQL 用 `WHERE conv = ? AND task = ? AND seq < ? ORDER BY seq DESC LIMIT ?`。
//     在进程里过滤的用 [HistoryQuery.Matches] 判，跟框架的判据一致。More 按满足条件的算：最早一条满足条件
//     的记录之前只剩不满足的时 More 为 false。
//   - 业务自研 backend 跑 `agent_v2/history_conformance.RunHistoryStoreTests` 验证语义。
type HistoryStore interface {
	// Append 把 msgs 追加到 key 对应的会话末尾。**只追加**，见接口 godoc。
	Append(ctx context.Context, key HistoryKey, msgs []*schema.Message) error

	// Load 按写入顺序返回 q 圈定的那一段：满足 q 的过滤条件的记录里，q.Before 之前（空 = 会话末尾）
	// 最近的 q.Limit 条（<= 0 不限）。**零值 q 读整场会话。** 会话不存在、没有满足条件的记录时返空页、
	// 不报错——「没聊过」「没记过」不是错误。返回页的 Start 是第一条的位置，More 报更早还有没有满足
	// 条件的；见 [HistoryPage]。
	Load(ctx context.Context, key HistoryKey, q HistoryQuery) (HistoryPage, error)
}

// HistoryKey 定位一条会话。
//
// # 可移植的长度上限：两个维度各 255 个字符
//
// 内置 SQL 后端里最窄的是 MySQL（`owner VARBINARY(1020)` / `conv_id VARBINARY(1020)`），按一个字符最多
// 4 字节算就是 **255 个字符**；两列加 `id BIGINT` 组成索引 `(owner, conv_id, id)`，1020+1020+8 = 2048
// 字节，在 InnoDB 的 3072 字节索引上限内。PG 的 `TEXT` 与 SQLite 的 `TEXT` 没有长度上限，但**照它们定会
// 让可移植性假高**（CLAUDE.md：从最弱的实现归纳是对的，从最强的归纳才错）。
//
// ⚠️ **ConvID 常直接来自 wire**（`compose_v2.WithThreadIDCtx` 声明的那个，a2a 侧就是对端给的
// ContextID），业务控制不住它长什么样。超上限时各后端的表现**不一样**：MySQL 严格模式（默认）报
// `Data too long for column 'owner'` ⟹ 整批 Append 失败；PG 约 2704 字节撞 btree 索引行上限；SQLite
// 照样成功。而历史层的写是**旁路**（失败吞 + warn，不打断主链路），所以线上症状是「同一个长 ConvID 在
// SQLite 上历史正常、搬到 MySQL 后模型开始忘上一轮」，不是一个报错。要接外来的 ConvID，自己先截或哈希。
//
// 这条上限由 `agent_v2/history_conformance` 的 LongKeys 用例钉住（两个维度 × 两档字符各一遍）。
// ⚠️ SQLite 与 in-memory 后端**恒过**那条用例，别因为它绿就以为这个维度验过了——它是给业务自研后端与
// 真 PG / MySQL 的。
type HistoryKey struct {
	// Owner 是数据归属方（租户 / 用户）。**必填、无默认值。**
	//
	// 它不在这里是为了好看：ConvID 通常来自 wire（HTTP 请求带的 threadId、A2A 的
	// ContextID），是**续接标识符不是授权令牌**——猜中别人的就能拉到别人的完整对话。
	// 单租户场景填个固定值即可，但不给「忘了填」留口子。
	Owner string

	// ConvID 是会话身份，取自 `compose_v2.GetThreadID(ctx)`——业务
	// `WithThreadIDCtx` push，或由 a2a-server（ContextID）/ agui（threadId）自动注入。
	ConvID string
}

// String 是给日志和错误信息看的，**不要拿它当存储主键**。
//
// 它就是把两段拼起来，没有转义——`{Owner:"acme/eu", ConvID:"c-1"}` 和
// `{Owner:"acme", ConvID:"eu/c-1"}` 拼出来一模一样。这两个是不同租户，key 撞了就是
// 一方读到另一方的完整对话。Owner 常带层级、ConvID 直接来自 wire，业务控制不住值里
// 有没有分隔符。
//
// 后端要么用复合主键（两列 / 两级 key，根本不拼），要么拼之前对两段各自转义。
// `history_conformance` 的 KeyIsolation 会撞这个形状。
func (k HistoryKey) String() string { return k.Owner + "/" + k.ConvID }

// HistoryAgentAnnotation 是消息上标注**归属**的 annotation 键。
//
// 值是那个 agent 在编排树里的**位置**（`br1` / `step2`，嵌套时 `outer/inner`），不是它
// 自己的名字——名字框架不校验唯一，两个分支挂同名 agent 归属就撞了。位置来自编排注册
// 时给的名字（AddBranch / AddStep / AddAgent / AddSubAgent），框架对它强制唯一。
// 详见 history_slot.go。
//
// 顶层没有父编排时回落 agent 名（顶层只有一个 agent 在跑，不存在撞）。
//
// 出了 channel 就没有「谁的 channel」这个上下文了，所以在离开的那一刻把它固化下来。
// 要展示给人或模型看请用 HistoryAgentDisplayAnnotation，那个才是 agent 名。
const HistoryAgentAnnotation = "crew_history_agent"

// HistoryAgentDisplayAnnotation 是消息上标注**归属者显示名**的 annotation 键——
// 值为那个 agent 自己的 `Name()`。
//
// 为什么要跟 HistoryAgentAnnotation 分成两个：那个存的是**编排注册位置**（`br1` /
// `step2`），它保证唯一、适合做比较；但拿去给人和模型看就太晦涩了——转交视图会把别人的
// 消息改写成 `For context: [X] said: ...`，X 是 `br1` 时模型根本不知道那是谁。
//
// 所以：**位置用于比较，名字用于显示**。缺这个键时显示回落到位置（旧数据 / 业务自造的
// 消息都可能没有），不影响正确性。
const HistoryAgentDisplayAnnotation = "crew_history_agent_display"

// historyCfgCtxKey 是「这次运行的会话历史配置」的 ctx key（私有类型防碰撞）。
type historyCfgCtxKey struct{}

// WithHistoryCtx 在**运行入口**声明这次运行的会话历史配置。
//
//	ctx = agent_v2.WithHistoryCtx(ctx, hs, "tenant-a")
//	events, err := seq.Run(ctx, sess, input)
//
// # 为什么要有它（而不是让编排去子里找）
//
// `WithHistory` 是 BasicAgent 的 option，配在叶子上。编排层要建写入器就得先拿到配置，
// 而它手上只有 `Agent` 接口——于是曾经写过一版「递归遍历子树去找」。那条路是错的：
//
//   - 编排可以互相引用成环，而环停在一条运行期永远走不到的分支上时，静态遍历照样会
//     走进去 ⟹ **fatal stack overflow，进程直接死**（Go 的栈溢出不是 panic，recover
//     兜不住）
//   - 共享 sub-agent 实例的 DAG 上是**指数级**扫描，而且**没开历史时最贵**（找不到
//     配置才会把整棵树走完，而历史是 opt-in）
//   - 业务用不可比较的类型实现 Agent 时身份去不了重，上面两条都没法收口
//
// 这些全是「去找」这个动作自造的问题。运行入口声明之后一条都不存在——不需要遍历，
// 也就没有环、没有指数级、没有身份问题。
//
// # 跟 WithHistory 的关系
//
// 两者都在时**以 ctx 为准**：它更靠近这次运行，而且是唯一能覆盖「编排里一个 BasicAgent
// 都没有」那种形状的声明方式。只配 `WithHistory` 的既有写法照常工作（编排向**直接子**
// 借，不递归）。
func WithHistoryCtx(ctx context.Context, hs HistoryStore, owner string, opts ...HistoryOption) context.Context {
	if isFrameworkNil(hs) {
		panic("WithHistoryCtx: typed nil HistoryStore (含 Go interface 陷阱)")
	}
	hc := &historyConfig{store: hs, owner: owner}
	for _, o := range opts {
		if o != nil {
			o(hc)
		}
	}
	if hc.owner == "" && hc.ownerFunc == nil {
		panic("WithHistoryCtx: owner 必填且无默认值——" +
			"ConvID 通常来自 wire，单靠它谁猜中谁就能拉到别人的会话；" +
			"单租户填固定值，多租户用 WithHistoryOwnerFunc")
	}
	return context.WithValue(ctx, historyCfgCtxKey{}, hc)
}

// historyCfgFromCtx 取运行入口声明的历史配置；没声明返 nil。
func historyCfgFromCtx(ctx context.Context) *historyConfig {
	hc, _ := ctx.Value(historyCfgCtxKey{}).(*historyConfig)
	return hc
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

// historyConfig 是 WithHistory 装配出来的运行期配置（Config.History）。
type historyConfig struct {
	store     HistoryStore
	owner     string
	ownerFunc func(context.Context) string
}

// ownerSourceTracker 记「这个历史库上，归属来源见过哪几档」。
//
// ⚠️ **它必须挂在比 historyConfig 活得久的东西上**。config 有两个创建点，生命周期
// 差一个数量级：[WithHistory] 是构造期（per-Agent，跨 Run 活着），而 [WithHistoryCtx]
// **每次调用新建一个**（运行入口 per-request 调）。把「见过哪几档」记在 config 上，
// 后一条路上的 flag 每次清零 ⟹ 一次请求只走一档 ⟹ **两档都真的走过这个条件永远不成立**
// ⟹ warn 永远不喊。实测过。
//
// 挂在 `HistoryStore` 上：那是业务给的、跨请求复用的对象，正好是「这场部署」的粒度。
type ownerSourceTracker struct {
	sawCtx    atomic.Bool
	sawStatic atomic.Bool
	warnOnce  sync.Once
}

// ownerSourceTrackers 按 HistoryStore 实例记，key 是接口值本身。
//
// 用 sync.Map 而不是给 HistoryStore 接口加方法——那是业务实现的接口，不能为了框架
// 内部的一个 warn 去改它的契约。
var ownerSourceTrackers sync.Map // HistoryStore → *ownerSourceTracker

func trackerFor(hs HistoryStore) *ownerSourceTracker {
	if v, ok := ownerSourceTrackers.Load(hs); ok {
		return v.(*ownerSourceTracker)
	}
	v, _ := ownerSourceTrackers.LoadOrStore(hs, &ownerSourceTracker{})
	return v.(*ownerSourceTracker)
}

// resolveOwner 定 owner：显式 ownerFunc 优先 → ctx 里的调用方身份 → 静态 owner。
//
// # 中间那档为什么必须有
//
// ConvID 是从 [compose_v2.GetThreadID] **自动流过来**的（见 historyKeyFrom，就在
// 隔壁一行），而 owner 一度只看这里的两个显式配置、从不问 [compose_v2.GetOwner]。
// **同一个 ctx，一个维度自动继承、另一个不继承**——那个不对称本身就是 bug。
//
// 后果实测过：业务接 `agui.WithOwnerResolver` 做多租户（框架逼着配，不配 panic），
// 又照 godoc 写 `WithHistory(hs, "prod")`，于是两个租户的对话全落进 `Owner="prod"`
// 一个分区——**租户 A 的原话进了租户 B 的 prompt**。而 [compose_v2.WithOwner]、
// `agui.WithOwnerResolver`、`a2a/server.WithOwnerResolver` 三处 godoc 都说这两件事是
// 「同一条规矩」，读到的人的合理结论正是「历史层已经处理了」。
//
// # 为什么放在中间而不是最前
//
// 显式 `WithHistoryOwnerFunc` 仍然最优先——业务有意让历史按「组织」共享而鉴权主体
// 是「用户」时，那个粒度差异是合法的，得给它一个说得出口的表达方式。
//
// # 单租户行为逐字不变
//
// 从不调 [compose_v2.WithOwner] 的部署里 `GetOwner` 恒返空串，直接落到静态 owner
// ——跟这一档存在之前完全一样。
func (hc *historyConfig) resolveOwner(ctx context.Context) string {
	if hc.ownerFunc != nil {
		if o := hc.ownerFunc(ctx); o != "" {
			return o
		}
	}
	if o := compose_v2.GetOwner(ctx); o != "" {
		hc.noteOwnerSource(ctx, true)
		return o
	}
	hc.noteOwnerSource(ctx, false)
	return hc.owner
}

// noteOwnerSource 记下这次解析走的是哪一档，两档**都真的被走过**时喊一次。
//
// # 为什么不是「看到 ctx 身份就喊」
//
// 那样在**全认证部署**里也会喊，而那种部署什么都没丢：协议层强制配了
// `WithOwnerResolver`（不配构造期 panic）⟹ 每个请求都带身份 ⟹ 静态那一档**永远走不到**
// ⟹ 历史全在各租户分区里、一条都没散。第一个请求到达时框架**无法知道**后面会不会有
// 不带身份的请求，而「散不散」完全取决于后者存不存在——**在信息不全时下判断，就会对
// 正确的配置报警**。
//
// 改成两档都命中过才喊：
//
//   - 全认证部署：静态档永不命中 → 永不喊
//   - 混合部署：两档都走过 → 喊，而且此时「历史分散在两个分区」**是已经发生的事实**，
//     不是预测
//
// # 消息里为什么不带具体的 owner 值
//
// 只喊一次，带上的那个值就是**随机第一个**走到该档的租户——读日志的人容易以为只有它
// 有问题。要么不带，要么标明是样例；这里选不带。
func (hc *historyConfig) noteOwnerSource(ctx context.Context, fromCtx bool) {
	if hc.owner == "" {
		return // 没配静态档，不存在「分散」这回事
	}
	if isFrameworkNil(hc.store) {
		return
	}
	tr := trackerFor(hc.store)
	if fromCtx {
		tr.sawCtx.Store(true)
	} else {
		tr.sawStatic.Store(true)
	}
	if !tr.sawCtx.Load() || !tr.sawStatic.Load() {
		return
	}
	tr.warnOnce.Do(func() {
		slog.WarnContext(ctx, "agent_v2: 会话历史分散在两个 owner 分区",
			"static_owner", hc.owner,
			"detail", "部分请求带着调用方身份（按各自租户分区）、部分没有（落进配置的静态 owner）",
			"impact", "按静态 owner 直查 HistoryStore 只看得到后半截",
			"fix", "把 WithHistory 的静态 owner 留空并配 WithHistoryOwnerFunc，让归属只有一个来源")
	})
}

// HistoryOption 调整会话历史行为。
type HistoryOption func(*historyConfig)

// WithHistoryOwnerFunc 让 owner 按请求解析（多租户：从 ctx 里的鉴权信息取）。
//
// 返空串时回落到 WithHistory 的静态 owner——所以「多租户为主、少数内部调用走默认
// 租户」这种混合场景不用写两套配置。
func WithHistoryOwnerFunc(fn func(ctx context.Context) string) HistoryOption {
	return func(hc *historyConfig) { hc.ownerFunc = fn }
}

// WithHistory 开启会话历史（**opt-in**：不配就一行不写、零开销）。
//
//	agent := agent_v2.NewBasicAgent(
//	    agent_v2.WithName("assistant"),
//	    agent_v2.WithModel(m),
//	    agent_v2.WithHistory(hs, "tenant-a"),
//	)
//	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-42")   // 会话身份，必须声明
//	agent.Run(ctx, sess, schema.UserMessage("在吗"))
//
// owner 见 HistoryKey.Owner——必填，多租户用 WithHistoryOwnerFunc 按请求覆盖。
//
// # 开了它，模型看到的历史才是完整的
//
// 不开时，多轮对话喂给模型的「上一轮」读自 RunSession.Events，而 events 只记首尾：
// 上一轮调过什么工具、工具返回了什么，模型**完全看不到**——它只看到一个结论，没法
// 判断那个结论是刚查的还是编的，也就没法判断该不该重查。开了之后 prior 改从这一层
// 读，拿到的是逐字的完整流。
//
// # 会话身份没声明就不写
//
// 没有 `WithThreadIDCtx` 就没有「这是哪个会话」，写下去也无从取回。这种情况下框架
// 每个 Run warn 一次并跳过——opt-in 了历史却没给会话身份，是配置漏了，不该静默。
//
// # 构造期 fail-fast（三条，都 panic）
//
//   - hs 为 nil（含 Go typed nil interface 陷阱）
//   - owner 为空且没给 ownerFunc
//   - **没配 WithName**——名字是历史里的**显示名**（跨归属改写会把它塞进模型 prompt，
//     业务回看历史也读它），而自动生成的名字每次构造都不一样。不起名就会看到
//     `For context: [agent_v2_a1b2c3d4] said: ...` 这种没有意义的标识符。
//     （归属本身按编排位置算、不受名字影响，见 history_slot.go——但显示名没有替代品。）
//
// # 嵌套时子 agent 自己的 WithHistory 不生效
//
// 一场会话一条流：嵌套的子 agent 读写都沿用**父的**写入器，自己配的 store / owner /
// ConvID 全部被忽略（且无日志）。子的产出靠 HistoryAgentAnnotation 区分归属，不靠分流。
//
// # 写失败不会打断主链路
//
// 这一层是旁路：后端写失败、后端 panic、后端卡住不返回，一律吞掉 + warn，Run 照常出
// 答案。执行正确性挂在 cp 的 frontier 上，跟历史一条都不沾。代价是历史可能缺批次——
// 换句话说，**不要把它当审计凭证用**。
func WithHistory(hs HistoryStore, owner string, opts ...HistoryOption) Option {
	return func(c *Config) {
		if isFrameworkNil(hs) {
			panic("WithHistory: typed nil HistoryStore (含 Go interface 陷阱)")
		}
		hc := &historyConfig{store: hs, owner: owner}
		for _, o := range opts {
			if o != nil {
				o(hc)
			}
		}
		if hc.owner == "" && hc.ownerFunc == nil {
			panic("WithHistory: owner 必填且无默认值——" +
				"ConvID 通常来自 wire，单靠它谁猜中谁就能拉到别人的会话；" +
				"单租户填固定值，多租户用 WithHistoryOwnerFunc")
		}
		c.History = hc
	}
}

// ---------------------------------------------------------------------------
// 异步写入器
// ---------------------------------------------------------------------------

// historyWriterBacklogWarn 是「后端慢到该报警」的积压阈值——**不是容量上限**，队列无界。
//
// 一批 = 一个节点的产出（一轮 LLM 输出、或一组工具结果），而产出一批要经过一次 LLM
// 往返（秒级）。所以**一个 Run 内能产生的批数天然有上界**：≈ 2×MaxRounds+1（默认 21），
// fanout 子 agent 也只是常数倍。队列长度被 LLM 速率钳死，无界不等于无限——最坏情况就是
// 整个 Run 的批都堆着，跟 channel 里本来就有的 messages 同一量级。
const historyWriterBacklogWarn = 64

// historyWriter 是单 worker 后台历史写入器。
//
// **有序**：单 worker + FIFO 队列保证批次按产生顺序落盘——历史是时序回放，乱序即失真。
//
// **旁路**：写失败 / 后端 panic 一律吞掉 + warn，**不**让主链路失败。执行正确性挂在
// cp 的 frontier 上（工具重不重跑、Resume 从哪继续），历史丢了不影响跑对。这跟 cp
// 的 async 档正相反——那边写失败要 abort Run，因为 cp 就是 Resume 的唯一依据。
//
// # 为什么是 mutex 队列而不是 channel
//
// 这里的处境有两个特点，合起来让 channel 成为错误的载体：
//
//  1. **多发送方**：tools 节点 N>=2 时 fanout 的每个 goroutine 里，若挂着 `AgentAsTool`
//     子 agent，子 agent 的节点体都会 recordHistory 到**同一个父 writer**。
//  2. **关闭者既非发送方也非接收方**：`closeAndJoin` 由 Run 出口的 defer 调。
//
// Go 的惯例是「只有发送方关 channel」。对照 `compose_v2` 的 asyncCpWriter——它的并发注释
// 写着「submit 与 closeAndJoin 都只在 Run goroutine 调」，那是**单发送方自己关**，论证
// 成立；historyWriter 长得一样却少了这个前提，只能靠「所有 recordHistory 都同步在节点体
// 内、而 Run 返回时节点必已结束」这条时序。时序今天成立（executeFrontier 的 wg.Wait 无
// ctx 旁路），但它是**注释级约定**：框架里已经有能活过 Run 返回的 goroutine（流式桥、
// SSE consume），谁哪天在流式路径上加一行记历史，就是 send on closed channel = panic
// 整进程。而 `sync.Once` 只能防 double-close，防不住这个。
//
// 换成 mutex + 队列后，「收摊之后还有人写」变成一次 warn + 丢一批旁路日志，不再是 panic。
//
// # 为什么队列无界
//
// 原来是「有界 64，满了阻塞等」。那让一个**明确定位为旁路**的层拥有了卡住主链路的能力，
// 跟本结构自己声明的「不让主链路失败」自相矛盾——不让它 fail，却让它 hang。更糟的是有个
// 闭环死锁：worker 卡在慢后端 → 队列满 → submit 阻塞在节点里 → Run 出不来 →
// closeAndJoin 跑不到 → worker 永远等不到收摊信号。（同款形状 `compose_v2/durability.go`
// 的 submit 注释里分析过一次。）
//
// 无界则 submit 永不阻塞，代价是积压占内存——而上面说了批数有上界，这个代价是有限的。
// 积压超过 historyWriterBacklogWarn 时 warn 一次，让「后端慢」可观测。
//
// **并发**：submit 可能被并发调用（同超步多节点 / fanout 多子 agent），mutex 保护队列。
// worker 侧唯一的跨 goroutine 状态是 store，由实现方自负线程安全（HistoryStore godoc 已
// 声明）。
// **key 在入口定死**：owner 从 ctx 解析一次（多租户 ownerFunc）、ConvID 从 ThreadID
// 取一次，之后整个 Run 不再变。逐节点重算既是白费，也给「同一个 Run 里前后写到不同
// 租户」留了口子。
type historyWriter struct {
	store HistoryStore
	key   HistoryKey
	// task 是这条流写进历史的记录要盖的任务章（[HistoryTaskAnnotation]）：建写入器那一刻所在任务的
	// RunID。嵌套位置沿用父的写入器，被调方属于调用方的任务，章也就是同一个；DetachTask 开的独立任务
	// 剥掉了写入器、自己建新的，章是它自己的。
	task string
	// origin 是这个写入器所在的执行从哪些仍在执行的任务里脱离出来（[compose_v2.DetachedFrom] 的 sid，编码好的
	// 来源章，见 encodeHistoryOrigin）：经 DetachTask 开的独立任务写下的记录盖上它（historyOriginAnnotation），
	// 「这条产出是不是已经在别的归属下记过」据此认得出独立任务写的那份（见 finalAlreadyInOtherSlot）。不是脱离
	// 出来的写入器为空。
	origin string

	// clientPrior 报调用方提供了前文、这次运行的读却要以它为准、不读历史层（见 caller_history.go）：写入器是嵌套的
	// （建它时已经在一个正在执行的任务里，它只记自己那段子对话），或者建它时读历史层失败、前文没写进去。在写入器交
	// 出去之前定下，之后只读。
	clientPrior bool

	mu     sync.Mutex
	cond   *sync.Cond
	queue  [][]*schema.Message
	closed bool
	warned bool // 积压 warn 每个排空周期最多一次（worker 每次取批清零）——不会刷爆

	// drained / inFlight / dead 支撑 waitDrained——**「读自己刚写的」这条保证**。
	//
	// 写是异步的，读是同步的。同一个 Run 里嵌套调用时（父建了写入器、子沿用），子读历史
	// 时父那批可能还在队列里没落盘：读到空 → 「读成功只是没内容」→ 调用方不回落 events
	// → 子失忆。这不是罕见竞态——后端只要有 200µs 延迟（比任何真实 Redis/PG 都快）就必现。
	//
	// 跨 Run 不需要它（Run 出口 closeAndJoin 已排空），所以顶层 agent 的读恒零等待。
	drained  *sync.Cond
	inFlight bool // worker 正在写某一批（已出队、还没落盘）
	dead     bool // worker 已退出（正常收摊或猝死）——防 waitDrained 永久等一个不存在的消费者

	// recorded / recordedHere 记「哪些消息已经进过历史」，键是**调用方交进来的那个指针**
	// （盖章前的原件）。指针够用：产出物从执行体一路传到边界不经序列化，同一条就是同一个
	// 指针；边界记的也是原件指针（盖章走的是浅拷贝，不影响）。
	//
	// **两个集合，因为产出和输入问的不是同一个问题**：
	//
	//   - `recorded` 是**全局**的，服务产出，但**只在「这次调用的范围内确实产生过记录」时
	//     才问它**（见 settleHistorySlot）：叶子记在更深的位置，中间每一层的边界都该跳过，
	//     不然同一条产出多个归属，第二份读回时被判成「别人说的」走 Rewrite。加范围这道
	//     前提是因为**执行体可以每轮返回同一个消息对象**（`var okMsg = ...` 是很自然的
	//     写法）——只问全局就把「同一个对象」当成「同一条产出」，Loop 第 2 轮起静默丢。
	//   - `recordedHere` 按**流**（位置）分：「这条消息在这条流里进过历史了吗」。它服务
	//     输入，而输入不能用全局判——SelfOnly 扇出下每条分支流各自要完整（有问有答），
	//     三条分支共享同一条输入指针时**每条都该记一份**。用全局判就变成「谁先谁赢」，
	//     实测并发下份数随调度在 1~4 之间翻。按流分之后：Loop 第 N 轮的输入就是第 N-1
	//     轮的产出、同一条流 ⟹ 命中跳过；扇出的兄弟分支是不同的流 ⟹ 各记各的。
	//
	// **输入那侧的查与登记是一次原子操作**（`claimRecordHere`），不是「先查后写」——那中间
	// 的窗口正是上面那个随调度翻的来源。产出那侧只读（`alreadyRecorded`），因为它的判据
	// 前面还压着「这次调用范围内产生过记录」这道前提，两条并发边界要同时穿过去得先各自
	// 有过写入，那时全局表里已经有了。
	//
	// **只管同一个 Run 之内**：writer 每个 Run 新建，集合跟着重置。规模上界是这个 Run
	// 里进过历史的消息条数。
	recorded     map[*schema.Message]bool
	recordedHere map[recordSlotKey]bool
	// recordedIDs 是进过历史的消息：ID → 那些消息的内容键（inputContentKey），只服务输入侧的「这条输入本 Run 记过没有」
	// （见 inputRecorded）。输入经过每一层 agent 入口都被拷一份（ownRunInput），叶子记下的是它自己那份，上层手里那份
	// 指针不同、ID 与内容相同。只看 ID 不够：节点用结构体拷贝派生新输入（`d := *in` 再改内容）时派生的那条带着原话的
	// ID，按 ID 判就把原话当成记过、一条不剩（审计实测）。产出侧不用它：同一个对象跨轮复用是合法的多次产出（ID 与内容
	// 都相同），只能按指针加范围判。
	recordedIDs map[string][]string

	// claimedRootInputs 记「哪些原始输入已经有人认领着要写了」，键是 input 自带的
	// 身份（见 rootInputID）。
	//
	// 共享历史视图下同一条原始输入会被交给多个执行体（转交的每一跳、并发扇出的每个
	// 分支），只该留一份。原来的判据是**读一遍历史看在不在**——顺序多跳成立（前一跳
	// 写完了后一跳才读），并发扇出不成立：三个分支同时读、都没看到、都记一份。实测
	// 连跑八次，同一条输入分别记了 1/2/2/3/1/2/2/3 份，**每次都不一样**。
	//
	// 认领是在 writer 的锁里做的一次 CAS，而 writer 是单 worker——所以它是这条流上
	// 唯一天然串行的点。判据从「读侧看看有没有」换成「写侧抢一次」，并发下才有定论。
	//
	// **只管同一个 Run 之内**：writer 每个 Run 新建，认领集跟着重置。跨 Run 续接同
	// 一条输入（Resume 重跑到同一步）仍靠读历史判——那时前一个 Run 的记录早已落盘，
	// 读得到。两个判据各管一段，都要留。
	claimedRootInputs map[string]bool

	// pendingRoot 是顶层 GraphAgent 这次 Run 的原始输入，等着排在下一批入队的记录前面补记（见 deferRootInput）。只在
	// 持锁时读写：submit 入队一批之前取走它、决定补不补；图跑完时收回（endRootInputWindow）。vettedRoot 是入口按跨 Run
	// 门槛判过的那条原始输入，出口结算认得它、不再读一次历史。
	pendingRoot *pendingRootInput
	vettedRoot  *schema.Message

	unnamedWarned bool // 「有 agent 没起名却在写历史」每条流只提醒一次

	// ctx 是 Run 的 ctx。⚠️ **排空时它可能已经 Done**：Run 因调用方 ctx 超时/取消而结束
	// 是常规路径，那时 closeAndJoin 这个 defer 才跑，于是每一批都拿着死 ctx 去 Append——
	// 尊重 ctx 的后端（PG/Redis）会立刻返 context.canceled，**整批积压全丢**，只留一串
	// warn。这是有意的：ctx 取消意味着调用方已经不要这次结果了，为它硬撑着写完历史没有
	// 意义；但要知道「取消 = 这一段历史大概率丢了」。
	ctx context.Context
	wg  sync.WaitGroup
}

// pendingRootInput 是一条等着补记的原始输入：原件（登记表按它认）、盖好归属的那份、它所在的流。
type pendingRootInput struct {
	msg     *schema.Message
	stamped *schema.Message
	slot    string
}

// newHistoryWriter 启动单 worker。store 非 nil、key 完整由 caller 保证。
func newHistoryWriter(ctx context.Context, store HistoryStore, key HistoryKey) *historyWriter {
	w := &historyWriter{store: store, key: key, task: currentHistoryTask(ctx), ctx: ctx,
		origin: encodeHistoryOrigin(compose_v2.DetachedFrom(ctx))}
	w.cond = sync.NewCond(&w.mu)
	w.drained = sync.NewCond(&w.mu)
	w.wg.Add(1)
	go w.loop()
	return w
}

// loop 一次锁取走队列里攒下的全部批次，**逐批**写。
//
// **一次取走**是为了减少锁争用和 cond 唤醒——主流程快于后端时队列会攒，取一次比取一次
// 唤醒一次划算。
//
// **但不把它们拼成一次 Append**：写失败和后端 panic 的处理粒度就是「一批」，合并会让一条
// 坏消息拖垮同批的其他批次（实测：三批里中间那批让后端 panic，合并写下前后两批一起陪葬）。
// 失败隔离是正确性、减少往返只是性能，这里优先前者。另外「合并省往返」的动机本来是缓解
// 队列积压，而队列改成无界之后积压压力本身就不成立了。
func (w *historyWriter) loop() {
	defer func() {
		// **worker 猝死检测**：`writeOne` 的 recover 兜得住后端 panic，兜不住
		// `runtime.Goexit`——deferred 照跑、goroutine 消失，而 `closed` 还是 false。
		// 那之后 submit 全部照收（不满足 closed 分支、一条 warn 都没有）、队列只涨不落，
		// `closeAndJoin` 又因 wg 已归零立即返回，积压**静默蒸发**。
		//
		// 生产里业务后端主动 Goexit 很少见，但**测试替身在 Append 里调 `t.Fatal` 就是
		// 这个形状**——业务照 history_conformance 写自己 backend 的测试时很容易踩，
		// 症状是「历史莫名少了一截，没有任何日志」。这里留个明确的信号。
		if r := recover(); r != nil {
			panicerr.Log(w.ctx, slog.LevelError, "agent_v2: 会话历史 worker 因 panic 退出，后续写入将静默堆积",
				panicerr.New(r), "key", w.key.String())
		}
		w.mu.Lock()
		alive := !w.closed
		diedWorking := w.inFlight // 死在写批中途——本批 + 同一次取批里剩下的批都没落盘
		backlog := len(w.queue)
		w.mu.Unlock()
		// 猝死的判据是三个信号取或，缺一不可：
		//
		//   - alive：closed 还是 false = 根本没到收摊，肯定不是正常退出；
		//   - diedWorking：**收摊排空的窗口里**（closed 已置）死掉时 alive 是 false，
		//     而 worker 取批是把整个队列搬进局部 slice——没写完的批次在局部变量里，
		//     `len(w.queue)` 恒为 0，按队列长度判在这个窗口里永远不触发。死在写批
		//     中途的可靠信号是 inFlight 还没来得及清（正常退出必然已清，见 loop 尾）；
		//   - backlog：兜「死在两次取批之间、队列里还有没取走的」（理论路径，顺手覆盖）。
		//
		// 正常收摊三个全 false：return 的前提是队列空 + closed，且 inFlight 在处理完
		// 上一个 slice 时已清——不会误报。
		if alive || diedWorking || backlog > 0 {
			slog.ErrorContext(w.ctx, "agent_v2: 会话历史 worker 提前退出（非正常收摊）——"+
				"没落盘的批次已丢失；后续写入会堆在队列里无人消费、且不会有警告；"+
				"若你的 HistoryStore 实现里调了 t.Fatal / runtime.Goexit，那就是原因",
				"key", w.key.String(), "closed", !alive, "died_working", diedWorking, "backlog", backlog)
		}
		w.mu.Lock()
		w.dead = true
		w.inFlight = false
		w.mu.Unlock()
		w.drained.Broadcast() // 猝死也要放行等待者，否则读会永久等一个不存在的消费者
		w.wg.Done()
	}()
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed {
			w.cond.Wait()
		}
		if len(w.queue) == 0 { // 只可能是 closed
			w.mu.Unlock()
			return
		}
		batch := w.queue
		w.queue = nil
		w.warned = false
		w.inFlight = true
		w.mu.Unlock()

		for _, msgs := range batch {
			w.writeOne(msgs)
		}

		w.mu.Lock()
		w.inFlight = false
		w.mu.Unlock()
		w.drained.Broadcast()
	}
}

// waitDrained 等到「队列空且没有正在写的批次」——读之前调，兑现「读得到自己刚写的」。
//
// 只在**同一个 Run 内**有实际等待：跨 Run 时上一个 Run 的出口已 closeAndJoin 排空，队列
// 本来就是空的，这里一次加锁就返回。所以顶层 agent 的每轮读是零成本的，代价只落在真正
// 需要它的地方——嵌套子 agent 读父那条流。
//
// **worker 死了就不等**（正常收摊或 panic/Goexit 猝死）：那时队列里的东西永远不会落盘，
// 继续等就是把主链路挂死在一个旁路组件上。读到的可能不全，但那已经由猝死那条 ERROR 日志
// 说明了。
//
// **ctx 一取消也不等**。这里跑在图内节点（preamble）里，而框架对业务 NodeFunc 的契约是
// 「必须尊重 ctx，deadline 才是超步的外部兜底」——框架自己的节点体不能例外。后端尊重 ctx
// 时不需要这条（worker 的 Append 会 fail-fast、间接放行），但后端**无视 ctx 挂死**时它是
// 唯一的出口：没有它，取消/超时连图都收敛不了，本超步的 cp 存不上、hook 收不了尾；有了它，
// 图正常收敛、状态落盘，最坏只是挂在出口的 closeAndJoin——那是一个有记录的既有取舍
// （见其 godoc「为什么不给 join 加超时」），且发生在**一切该保存的都保存之后**。
//
// 放弃等待后读到的可能不含刚提交的批次——但 ctx 都取消了，这次读的结果本来就没人要了
// （后续 Load 多半也因 ctx 失败 → 报 false 回落 events）。
//
// **它只解「写侧挂死」**：出口放行之后紧接着还要调后端的 Load。若违约后端的 Load 与
// 挂死的 Append 共用一把锁（或同样无视 ctx），照样卡在 Load 里，图仍收敛不了。真要
// 对无视 ctx 的后端完全免疫，得业务自己在实现里加超时——框架这层兜不住第二段。
func (w *historyWriter) waitDrained(ctx context.Context) {
	// AfterFunc 回调里先 Lock/Unlock 再 Broadcast：ctx.Err() 的翻转在 mutex 之外，
	// 不这样做会有经典 missed-wakeup——等待方检查完条件、还没 park 进 Wait 的窗口里
	// Broadcast 发出去就丢了。回调先抢锁会被堵到 Wait 真正 park（Wait 释放锁）之后。
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		//nolint:staticcheck // 空临界区是有意的：只为跟等待方的 check→park 窗口建立顺序
		w.mu.Unlock()
		w.drained.Broadcast()
	})
	defer stop()

	w.mu.Lock()
	defer w.mu.Unlock()
	for !w.dead && ctx.Err() == nil && (len(w.queue) > 0 || w.inFlight) {
		w.drained.Wait()
	}
}

// writeOne 写一批，**panic-safe**：业务 backend panic 时 recover 转 warn。worker 跑在
// 独立 goroutine，Run 入口的 outer recover 够不着它——不自己兜则 panic 逃逸 crash 整
// 进程。而历史是旁路，让它崩掉整个服务显然不成比例。
func (w *historyWriter) writeOne(msgs []*schema.Message) {
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(w.ctx, slog.LevelWarn, "agent_v2: 会话历史后端 panic，本批丢弃",
				panicerr.New(r), "key", w.key.String(), "count", len(msgs))
		}
	}()
	if err := w.store.Append(w.ctx, w.key, msgs); err != nil {
		// warn 不是 debug：历史丢失不常见、丢了很难发现（读的时候只是「少了几条」，
		// 没有任何信号说这是丢的还是本来就没有）。静默 = 业务永远查不出来。
		slog.WarnContext(w.ctx, "agent_v2: 会话历史写入失败，本批丢弃",
			"key", w.key.String(), "count", len(msgs), "err", err)
	}
}

// submit 入队一批，**永不阻塞**（见结构体 godoc「为什么队列无界」）。
//
// 收摊之后到的批次丢弃 + warn，不 panic——那是「流式桥之类脱离 Run 生命周期的 goroutine
// 迟到了」的信号，丢一批旁路日志远好过崩掉整个进程。
func (w *historyWriter) submit(msgs []*schema.Message) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		slog.WarnContext(w.ctx, "agent_v2: 写入器已收摊，本批会话历史丢弃——"+
			"recordHistory 只能从节点体内同步调用，不能从活过 Run 的后台 goroutine 调",
			"key", w.key.String(), "count", len(msgs))
		return
	}
	if p := w.pendingRoot; p != nil {
		w.pendingRoot = nil
		// 这一批里有这条输入就不补：转发形状下子 agent 收到的就是它、记在自己名下，这一份就排在最前。否则先补它（归属是
		// 顶层），再写这一批。只看这一批、不看登记表：边界补记先认领输入、读一次历史判产出之后才入队，认领了还没入队的
		// 那份挡不住别的分支先入队的一批（审计实测：并发扇出里原话排到了委派分支的问答之后）。更早入队过它的批次已经把
		// 待补取走了。
		if !batchHasInput(msgs, p.msg) {
			w.ensureRecordMaps()
			w.recorded[p.msg] = true
			w.recordedHere[recordSlotKey{msg: p.msg, slot: p.slot}] = true
			w.noteRecordedID(p.msg)
			w.queue = append(w.queue, []*schema.Message{p.stamped})
		}
	}
	w.queue = append(w.queue, msgs)
	backlog := len(w.queue)
	shouldWarn := backlog >= historyWriterBacklogWarn && !w.warned
	if shouldWarn {
		w.warned = true
	}
	w.mu.Unlock()
	w.cond.Signal()

	if shouldWarn {
		slog.WarnContext(w.ctx, "agent_v2: 会话历史积压——后端过慢，队列在涨",
			"key", w.key.String(), "backlog", backlog)
	}
}

// closeAndJoin 通知 worker 收摊 + 等它把积压写完再退出。Run/Resume 出口 defer 调。
//
// **幂等**：重复调只有第一次有效果（嵌套子 agent 拿到的是 no-op closer，正常只有创建者
// 调一次；幂等是为了让「多调一次」不至于炸）。
//
// ⚠️ **这里的 join 没有超时，后端违约时它会一直等**：排空耗时 = 积压批数 × 单批延迟。
// 后端若完全无视 ctx 且永不返回（连接池耗尽 / 没配超时的 client / 对端半开），`Run` 就
// 回不来——即使调用方 ctx 早已取消。同款残余在 `compose_v2/durability.go` 的 asyncCpWriter
// 也写明过，属 backend 违约，框架靠「业务给 ctx deadline + 后端尊重 ctx」兜底。
//
// **为什么不给 join 加超时**：阻塞式排空正是跨 agent 顺序的保证——`Sequential(A, B)` 里
// A 的 writer 必须在 A.Run 返回前排空完毕，B 的写入才开始追加。加超时会让「历史是时序
// 回放」这个不变量在慢后端下悄悄失真（乱序比丢失更难查）。所以这里选择等，把「别让写入
// 器无限期挂住主链路」的责任交给后端的 ctx 尊重。
//
// 注意这跟 submit 侧的取舍不同：submit 改成永不阻塞（队列无界）是因为它在**图执行中途**，
// 卡住就是卡住整个 Run 且会连带死锁；close 在**出口**，等的是已经产生的那些批次落盘。
func (w *historyWriter) closeAndJoin() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		w.cond.Broadcast()
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// ---------------------------------------------------------------------------
// ctx 透传
// ---------------------------------------------------------------------------

type historyCtxKey struct{}

// historyCtxVal 是 ctx 里的历史状态。
//
// **两种「不写」不是一回事**，所以用一个显式状态而不是「writer 是不是 nil」：
//
//   - ctx 里根本没有这个值 = 还没人开历史。此时一个配了 WithHistory 的 agent 跑起来
//     **应该**开一条。
//   - suppressed = 这条分支被明确摘掉了（内部决策 LLM）。此时哪怕嵌套的 agent 自己
//     配了 WithHistory 也**不能**重新开——否则摘除形同虚设，业务给分类器随手配一个
//     历史就把它的内部推理灌进用户的会话。
type historyCtxVal struct {
	w          *historyWriter
	suppressed bool
}

// withHistoryWriter 把本次 Run 的写入器放进 ctx，供图内节点取用。
func withHistoryWriter(ctx context.Context, w *historyWriter) context.Context {
	return context.WithValue(ctx, historyCtxKey{}, historyCtxVal{w: w})
}

// historyWriterFrom 取本次 Run 的写入器；没开历史 / 被摘掉时返 nil。
func historyWriterFrom(ctx context.Context) *historyWriter {
	v, _ := ctx.Value(historyCtxKey{}).(historyCtxVal)
	return v.w
}

// historySuppressed 报这条分支是否被明确摘掉了。
func historySuppressed(ctx context.Context) bool {
	v, _ := ctx.Value(historyCtxKey{}).(historyCtxVal)
	return v.suppressed
}

// WithoutHistoryCtx 声明**这条分支下产生的对话不进历史**。
//
// 给「内部决策」用：它们跟用户不对话，产出是给框架看的判定。让这些落进会话历史，业务回看时会读到
// 一堆自己从没说过、也不该看见的内容，下一回合模型还会把评审请求当成用户说过的话。框架调这些决策方时
// 都已经摘掉了（连同用户的插话一起，见 decisionCtx）：编排的分类器、评估器与反馈教练，`NewPlanAgent` 的
// planner，`WithRouter` 的路由函数与 `WithRouteDecider` 的决策方，`WithReflector` / `WithEvalFunc`，
// `WithLoopExitCondition` 的退出判断，`WithTurnGate` 的闸门——它们里面再起的子 agent 继承这条禁止，业务
// 无需再调。
//
// **框架不认识的决策点要业务自己调一次**：GraphAgent 节点里自己写的判断、Parallel 的 Merger / Aggregator
// 里拿 agent 当裁判（这两个回调的产出是内容，框架不替它们摘）。
//
// **是「禁止」不是「当前没有」**：这条分支下哪怕有 agent 自己配了 WithHistory 也不会
// 重新开写入器——否则业务给分类器随手配一个历史就绕过了整个摘除。它是执行状态：从这里
// [DetachTask] 出去开的独立任务不在这次决策里，禁止随之剥掉（独立任务进不进、进哪个会话的历史，
// 由它自己的会话 ID 决定——DetachTask 连会话 ID 一起剥了，要进哪个自己推）。
//
// （反过来说，若某天出现「后一个决策要看前一个决策的推理过程」的编排，这条豁免就得
// 重新评估——那种场景下污染主历史换 grounding 可能是划算的。）
func WithoutHistoryCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, historyCtxKey{}, historyCtxVal{suppressed: true})
}

// ---------------------------------------------------------------------------
// 写入 / 读取入口（供 BasicAgent 调用）
// ---------------------------------------------------------------------------

// startHistory 在 Run / Resume 入口装配本次调用的历史写入器，返回 (新 ctx, 收尾函数)。
//
// 收尾函数必须 defer 调用（drain 完 pending 写 + 等 worker 退出）——"谁启动谁负责退出"。
// 没开历史 / 会话身份缺失时返原 ctx + no-op，调用方不必分情况处理。
//
// **身份缺失就整个不启**：没有 ConvID 或 owner 就没有「这是哪个会话的、谁的」，写下去
// 也无从取回。这里 warn 一次然后不建 writer——后续所有 recordHistory 天然变成 no-op，
// 不用在每个写入点重复判断，也不会每条消息吼一遍。
//
// **嵌套沿用父的**：ctx 里已有写入器时直接复用。一场会话只该有一条流——子 agent 的
// 产出照样进同一个 (Owner, ConvID)，靠 HistoryAgentAnnotation 区分是谁说的。另起一个
// 会让同一场对话的写入分散到两个 worker，顺序就不再有保证了。
func startHistory(ctx context.Context, hc *historyConfig, agentName string) (context.Context, func()) {
	noop := func() {}
	if historyWriterFrom(ctx) != nil || historySuppressed(ctx) {
		return ctx, noop
	}
	if hc == nil || hc.store == nil {
		return ctx, noop
	}
	key, ok := historyKeyFrom(ctx, hc)
	if !ok {
		slog.WarnContext(ctx, "agent_v2: 开了会话历史但没拿到会话身份，本次调用不写历史——"+
			"会话 ID 用 compose_v2.WithThreadIDCtx(ctx, convID) 声明"+
			"（a2a-server / agui 会自动注入），owner 见 WithHistory / WithHistoryOwnerFunc",
			"agent", agentName,
			"conv_id", compose_v2.GetThreadID(ctx),
			"owner", hc.resolveOwner(ctx))
		return ctx, noop
	}
	w := newHistoryWriter(ctx, hc.store, key)
	// 调用方提供了前文时定下这次运行以谁为准（见 caller_history.go）。拥有者 = 建写入器时还不在任何正在执行的任务里：
	// 各个 agent 入口都在盖自己的任务章之前建写入器，所以这里看到的任务只能是外面的（节点里、钩子里、嵌套调用里调起的）
	if prior, declared := callerPrior(ctx); declared {
		if _, nested := enclosingTask(ctx); nested {
			w.clientPrior = true
		} else {
			w.clientPrior = !seedCallerPrior(ctx, w, prior)
		}
	}
	return withHistoryWriter(ctx, w), w.closeAndJoin
}

// markFromAgent 返回一份带 from_agent 标注的浅拷贝（Raw 视图用，保留原 role）。
// 拷贝而非原地改：历史层返回的消息可能被后端复用，改它会污染存储。
func markFromAgent(m *schema.Message, fromAgent string) *schema.Message {
	cp := *m
	ann := make(map[string]any, len(m.Annotations)+1)
	for k, v := range m.Annotations {
		ann[k] = v
	}
	ann["from_agent"] = fromAgent
	cp.Annotations = ann
	return &cp
}

// recordSlotKey 是「某条消息在某条流里」的键。
type recordSlotKey struct {
	msg  *schema.Message
	slot string
}

// markRecorded 登记这批消息已经进过历史（slot 是它们所属的那条流）。
//
// **必须在入队时同步登记，不能等 worker 落盘**——边界紧跟着 sub-agent 返回就要读，
// 那时异步的那批多半还在队列里。
func (w *historyWriter) markRecorded(slot string, msgs []*schema.Message) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureRecordMaps()
	for _, m := range msgs {
		if m != nil {
			w.recorded[m] = true
			w.recordedHere[recordSlotKey{msg: m, slot: slot}] = true
			w.noteRecordedID(m)
		}
	}
}

// noteRecordedID 登记一条进了历史的消息的 ID 与内容键。调用方须持锁。
func (w *historyWriter) noteRecordedID(m *schema.Message) {
	if m.ID == "" {
		return
	}
	k := inputContentKey(m)
	for _, have := range w.recordedIDs[m.ID] {
		if have == k {
			return
		}
	}
	w.recordedIDs[m.ID] = append(w.recordedIDs[m.ID], k)
}

// inputContentKey 是「同一条输入」比对内容的那部分：角色、文本与内容块（不含标注——记历史时会盖归属）。
func inputContentKey(m *schema.Message) string {
	var b strings.Builder
	b.WriteString(string(m.Role))
	b.WriteByte(0)
	b.WriteString(m.Content)
	if len(m.ContentBlocks) > 0 {
		b.WriteByte(0)
		// 编不出来就只比文本：内容块里放着编不成 JSON 的东西时，宁可把两条当成同一条（少记）也不报错
		if raw, err := json.Marshal(m.ContentBlocks); err == nil {
			b.Write(raw)
		}
	}
	return b.String()
}

// ensureRecordMaps 惰性初始化两个登记表。调用方须持锁。
func (w *historyWriter) ensureRecordMaps() {
	if w.recorded == nil {
		w.recorded = make(map[*schema.Message]bool, 16)
	}
	if w.recordedHere == nil {
		w.recordedHere = make(map[recordSlotKey]bool, 16)
	}
	if w.recordedIDs == nil {
		w.recordedIDs = make(map[string][]string, 16)
	}
}

// inputRecorded 报这条输入是不是已经进过历史（全局，只读）：同一个指针，或同一个消息 ID 且内容相同（见 recordedIDs）。
func (w *historyWriter) inputRecorded(m *schema.Message) bool {
	if m == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.recorded[m] {
		return true
	}
	if m.ID == "" {
		return false
	}
	k := inputContentKey(m)
	for _, have := range w.recordedIDs[m.ID] {
		if have == k {
			return true
		}
	}
	return false
}

// alreadyRecorded 报这条消息是不是已经进过历史（全局，只读，按指针——服务产出，见 recorded）。
func (w *historyWriter) alreadyRecorded(m *schema.Message) bool {
	if m == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recorded[m]
}

// claimRecordHere 抢这条消息**在这条流里**的记录权。
//
// 跟 claimRecord 的区别是「同一条消息在别的流里记过」不算数——SelfOnly 扇出下每条分支
// 流各自要完整，共享同一条输入指针的兄弟分支该各记一份。
func (w *historyWriter) claimRecordHere(slot string, m *schema.Message) bool {
	if m == nil {
		return false
	}
	k := recordSlotKey{msg: m, slot: slot}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensureRecordMaps()
	if w.recordedHere[k] {
		return false
	}
	w.recordedHere[k] = true
	w.recorded[m] = true
	w.noteRecordedID(m)
	return true
}

// claimRootInput 抢这条原始输入的记录权：第一个抢到的返 true，之后同 nonce 一律 false。
//
// nonce 为空表示这条输入不参与共享（非转交 / 非扇出场景，每个位置收到的本来就是不同的
// 输入）——直接放行。见 claimedRootInputs 说明。
func (w *historyWriter) claimRootInput(nonce string) bool {
	if nonce == "" {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.claimedRootInputs[nonce] {
		return false
	}
	if w.claimedRootInputs == nil {
		w.claimedRootInputs = make(map[string]bool, 4)
	}
	w.claimedRootInputs[nonce] = true
	return true
}

// recordedInThisRun 报「**这个 Run 里**已经把这条输入写进历史了吗」。
//
// 跟「这条输入的身份在历史里出现过吗」是两个问题，而后者会把**跨 Run 复用同一个消息
// 对象**（包级 var 的固定指令、失败重试拿同一个 input 再跑、业务缓存消息复用）误判成
// 「已经有了」——那是新的一轮，不是重复。实测复用对象连跑三轮，第 3 轮的 prompt 以
// assistant 结尾、本轮问题整条被挤出去，全程无报错。
//
// 写入器的登记表是 per-Run 新建的，正好答这个问题。
func recordedInThisRun(ctx context.Context, input *schema.Message) bool {
	w := historyWriterFrom(ctx)
	if w == nil {
		return false
	}
	return w.inputRecorded(input)
}

// claimSharedRootInput 报「这一层该不该把这条共享的原始输入记进历史」。
//
// 两个判据各管一段，缺一不可：
//
//   - **读历史**管跨 Run：前一个 Run 记的早已落盘，Resume 重跑到同一步时读得到。
//     认领集管不了这段——writer 每个 Run 新建。
//   - **认领**管同一个 Run 内的并发：扇出的分支同时起跑，读历史时谁都还没写完，
//     三个都判「没记过」于是都记。认领在 writer 的锁里做，那是这条流上唯一天然
//     串行的点。
//
// 没有写入器（没开历史 / 被摘除）时返 false——那种情况下 recordHistory 本来就是 no-op，
// 提前返回省掉后面的拷贝盖章。
func claimSharedRootInput(ctx context.Context, prior []*schema.Message, input *schema.Message) bool {
	w := historyWriterFrom(ctx)
	if w == nil {
		return false
	}
	// 身份为空时回落到指针：产出侧有几处直接构造 `&schema.Message{}`（`ToolResultMessage`
	// / `AssistantToolCallMessage` / model adapter 解析响应），它们不铸身份，而兜底补铸
	// 盖在拷贝上、调用方手里那条恒空。两者在**一个 Run 之内**都稳定，够这个认领用。
	key := rootInputID(input)
	if key == "" {
		key = fmt.Sprintf("%p", input)
	}
	if priorHasRootInputID(prior, rootInputID(input)) {
		return false
	}
	// prior 是只读回来的最近一段；这条输入若是本任务更早记下的（转交了很多跳、每跳调了很多工具），
	// 不在这一段里——按消息 ID 在这个任务写下的记录里查一遍。
	if nonce := rootInputID(input); nonce != "" {
		if hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: nonce}, true, anyHistoryRecord); hit {
			return false
		}
	}
	return w.claimRootInput(key)
}

// recordHistory 把一批刚产生的消息交给历史层。
//
// 调用约定（**同生共死**）：只在产生它们的节点**成功**之后调。节点失败时 writes 会被
// 整批丢弃（这正是 Resume 要重跑全部工具的原因），若历史先写了、channel 回滚了，
// Resume 重跑后同一条消息会进历史两遍。所以不能在工具逐个返回时就写——那时还不知道
// 这批会不会整体回滚。
//
// 调用约定之二（**只能从节点体内同步调**）：写入器在 Run/Resume 出口 `closeAndJoin` 收摊，
// 而那个 defer 晚于 `compose_v2.Run` 返回；节点内同步调用因此恒在收摊之前——
// `executeFrontier` 的 `wg.Wait()` 没有 ctx 旁路，超步循环的 ctx 检查在它**之前**，所以
// Run 返回时所有 NodeFunc 必然已结束。
//
// **不得从节点里 spawn 的后台 goroutine 调**（流式桥、异步 hook 之类）：那类 goroutine
// 框架明确不为它同步生命周期（见 `compose_v2/channel_manager.go` 的 NodeFunc 契约），
// 能活过 Run 返回，正好落在收摊之后。今天框架内确有这样的 goroutine
// （`streamReaderToSchema` 的桥接、openai client 的 SSE consume），它们只是碰巧不写历史。
//
// 违约的后果是**那一批被丢弃 + 一条 warn**（`submit` 判已收摊），不会崩进程——写入器改成
// mutex 队列时就是为了把这件事从「约定」变成「结构」。但它仍然是违约：丢掉的是历史内容，
// 而且丢得很安静，只有那条 warn 能看出来。真要在流式路径上记历史，得先给那条 goroutine
// 一个明确的生命周期，而不是指望写入器多等一会。
//
// agentName 盖在每条消息的 HistoryAgentAnnotation 上（归属固化，见文件头）。
//
// 没开历史 / 被 WithoutHistoryCtx 摘掉时是 no-op。
func recordHistory(ctx context.Context, owner, display string, msgs []*schema.Message) {
	w := historyWriterFrom(ctx)
	if w == nil || len(msgs) == 0 {
		return
	}
	w.markRecorded(HistorySlot(ctx), msgs)
	bumpHistoryReceipt(ctx)
	// 拷贝一份再盖章：msgs 里的 *schema.Message 是 channel 里那批的同一批指针，
	// 直接改它们的 Annotations 会污染正在喂给模型的消息。
	stamped := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		// **派生输入一律不入库**，判据见 [DerivedInputAnnotation]：从历史里已有的东西
		// 完全可重算的拼装物，记了就是把同一份内容存两遍。
		//
		// ⚠️ 这道过滤长在 recordHistory 里而不是某个调用点上，是因为**能产生派生消息的
		// 入口不止一个**。原先 `isDerivedInput` 只有三个消费点（编排边界的 input/final、
		// preamble 的 user input），**tools 节点那条路一个都没有**——它整批
		// `recordHistory(ctx, ..., results)` 无过滤入库，而 `results` 里混着框架合成的
		// 消息（工具重复调用提醒就是一条）。后果不是多存一条：它作为 `role=user` 留在
		// append-only 历史里，**下一个回合原样重放进新回合的 prompt**，此后每一轮都喂
		// 给模型。多轮对话里它永久贴着。
		//
		// 上面的 `markRecorded` 仍然盖章（**有意**）：盖了之后编排边界那几处就不会再来
		// 记一遍——「从不入库」要在所有路径上都成立，而不是只在这一处跳过。
		if isDerivedInput(m) {
			continue
		}
		stamped = append(stamped, w.stamp(m, owner, display))
	}
	if len(stamped) == 0 {
		return
	}
	w.submit(stamped)
}

// stamp 给一条要写进历史的消息盖归属、任务章与来源（见 stampHistoryAgent），返回拷贝。
func (w *historyWriter) stamp(m *schema.Message, owner, display string) *schema.Message {
	cp := stampHistoryAgent(m, owner, display, w.task)
	if w.origin != "" {
		cp.Annotations[historyOriginAnnotation] = w.origin
	}
	return cp
}

// deferRoot 登记一条待补的原始输入（见 deferRootInput）。
func (w *historyWriter) deferRoot(m, stamped *schema.Message, slot string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pendingRoot = &pendingRootInput{msg: m, stamped: stamped, slot: slot}
	w.vettedRoot = m
}

// dropPendingRoot 收回还没补的原始输入（见 endRootInputWindow）。
func (w *historyWriter) dropPendingRoot() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pendingRoot = nil
}

// rootVetted 报 m 是不是入口按跨 Run 门槛判过的那条原始输入（见 deferRootInput）。
func (w *historyWriter) rootVetted(m *schema.Message) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return m != nil && w.vettedRoot == m
}

// batchHasInput 报一批要入队的记录里有没有 in 这条输入：盖章是浅拷贝，ID 与内容都保留，按 ID 加内容键认（同 recordedIDs）。
func batchHasInput(batch []*schema.Message, in *schema.Message) bool {
	if in.ID == "" {
		return false
	}
	k := inputContentKey(in)
	for _, m := range batch {
		if m.ID == in.ID && inputContentKey(m) == k {
			return true
		}
	}
	return false
}

// noteHistoryRecorded 把 msgs 登记成「这次调用里由我记过」，不写进历史：model 节点处理完之后在提交之前作废、续跑重放
// 定下的那一轮时用（见 modelCallRecord）——那一轮在上一次执行里已经记过，这次不再写。
//
// 登记跟 recordHistory 开头那两步一样（全局登记表 + 范围回执）：编排边界补记看这两样认「成员自己记过的产出」，不登记
// 的话它替成员再记一份（审计实测：Sequential 成员的最终答复在历史里两份，ID 与归属都相同）。边界那侧不能改成按消息
// ID 查历史去重——同一条流里同一个对象跨轮复用时每轮都得记（见 priorHasMessageIDInOtherSlot）。
func noteHistoryRecorded(ctx context.Context, msgs []*schema.Message) {
	w := historyWriterFrom(ctx)
	if w == nil || len(msgs) == 0 {
		return
	}
	w.markRecorded(HistorySlot(ctx), msgs)
	bumpHistoryReceipt(ctx)
}

// stampHistoryAgent 返回一份浅拷贝，Annotations 上标好归属 agent 与任务章（task 为空时不盖）。
//
// 浅拷贝够用：改的只有 Annotations 这一个 map，其余字段（ContentBlocks / ToolCalls）
// 共享底层数据、双方都只读。
func stampHistoryAgent(m *schema.Message, owner, display, task string) *schema.Message {
	cp := *m
	// 兜底铸造身份：产出侧有若干处直接构造 `&schema.Message{}`（model adapter 解析响应、
	// 流聚合出口），业务也可能手搓。盖在**拷贝**上跟盖归属同一个理由——原对象可能是
	// channel 里那批的同一批指针，正在喂给模型，也可能是业务持有的；原地改它会让业务
	// 手搓的消息凭空长出身份，把「跨 Run 复用同一对象」这个形状拖进按身份的判据里。
	if cp.ID == "" {
		cp.ID = newUUID()
	}
	anns := make(map[string]any, len(m.Annotations)+3)
	for k, v := range m.Annotations {
		// 前文标注只由写前文那一处盖：agent 的记录带着它（钩子把前文原样转进了产出之类）会被读成调用方的前文——不按归属
		// 过滤、被挪到最前
		if k == CallerHistoryAnnotation {
			continue
		}
		anns[k] = v
	}
	anns[HistoryAgentAnnotation] = owner
	if task != "" {
		anns[HistoryTaskAnnotation] = task
	} else {
		// 盖章前的原件可能带着别的任务的章（业务把上一个任务读回来的消息再交进来）——不清掉的话
		// 这条记录会冒充那个任务的。
		delete(anns, HistoryTaskAnnotation)
	}
	delete(anns, historyOriginAnnotation) // 同理：来源章只由写入器盖（recordHistory）
	// 位置跟名字不同才存显示名——顶层回落时两者相同，没必要多存一份。
	if display != "" && display != owner {
		anns[HistoryAgentDisplayAnnotation] = display
	}
	cp.Annotations = anns
	return &cp
}

// historyDisplayName 取一条历史消息的归属**显示名**：有显示名用显示名，没有回落到位置。
func historyDisplayName(m *schema.Message) string {
	if m == nil || m.Annotations == nil {
		return ""
	}
	if d, _ := m.Annotations[HistoryAgentDisplayAnnotation].(string); d != "" {
		return d
	}
	o, _ := m.Annotations[HistoryAgentAnnotation].(string)
	return o
}

// historyKeyFrom 组 key；会话身份缺失时返 ok=false。
func historyKeyFrom(ctx context.Context, hc *historyConfig) (HistoryKey, bool) {
	convID := compose_v2.GetThreadID(ctx)
	if convID == "" {
		return HistoryKey{}, false
	}
	owner := hc.resolveOwner(ctx)
	if owner == "" {
		// 构造期已 fail-fast 过「两个都没给」，走到这里只能是 ownerFunc 返了空串
		// 且没配静态 owner——业务动态解析失败，同样不该静默写到一个空租户下。
		return HistoryKey{}, false
	}
	return HistoryKey{Owner: owner, ConvID: convID}, true
}

// ---------------------------------------------------------------------------
// 内存参考实现
// ---------------------------------------------------------------------------

// InMemoryHistoryStore 是 HistoryStore 的进程内参考实现——单测、本地开发、
// 以及「只要单进程内多轮对话」的场景够用。**进程重启即失忆**，生产接 Redis / PG。
type InMemoryHistoryStore struct {
	mu    sync.RWMutex
	convs map[HistoryKey]*memHistoryConv
}

// memHistoryConv 是一场会话的记录，连同写入时按消息 ID、任务章、归属建的位置索引（位置升序）——按条件读时
// 只看命中的那些位置，不扫整场会话（见 [HistoryQuery]）。
type memHistoryConv struct {
	msgs    []*schema.Message
	byID    map[string][]int
	byTask  map[string][]int
	byAgent map[string][]int
}

func (c *memHistoryConv) index(m *schema.Message, pos int) {
	if m == nil {
		return
	}
	add := func(idx map[string][]int, k string) {
		if k != "" {
			idx[k] = append(idx[k], pos)
		}
	}
	add(c.byID, m.ID)
	if t, ok := m.Annotations[HistoryTaskAnnotation].(string); ok {
		add(c.byTask, t)
	}
	if a, ok := m.Annotations[HistoryAgentAnnotation].(string); ok {
		add(c.byAgent, a)
	}
}

// candidates 返回 q 的过滤条件下要逐条核对的位置（升序）：取几个条件里命中最少的那份索引；没有过滤条件时
// 返回 nil、all 为 true（整场会话都是候选）。
func (c *memHistoryConv) candidates(q HistoryQuery) (pos []int, all bool) {
	all = true
	pick := func(idx map[string][]int, k string) {
		if k == "" {
			return
		}
		p := idx[k]
		if all || len(p) < len(pos) {
			pos, all = p, false
		}
	}
	pick(c.byID, q.MessageID)
	pick(c.byTask, q.Task)
	pick(c.byAgent, q.Agent)
	return pos, all
}

// NewInMemoryHistoryStore 建一个空的内存历史存储。
func NewInMemoryHistoryStore() *InMemoryHistoryStore {
	return &InMemoryHistoryStore{convs: make(map[HistoryKey]*memHistoryConv)}
}

// Append 追加到会话末尾。存的是 msgs 元素的指针快照——caller 不改写元素是接口约定。
func (s *InMemoryHistoryStore) Append(_ context.Context, key HistoryKey, msgs []*schema.Message) error {
	if key.Owner == "" || key.ConvID == "" {
		return fmt.Errorf("history: key 不完整 (owner=%q conv=%q)", key.Owner, key.ConvID)
	}
	if len(msgs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.convs[key]
	if c == nil {
		c = &memHistoryConv{byID: map[string][]int{}, byTask: map[string][]int{}, byAgent: map[string][]int{}}
		s.convs[key] = c
	}
	for _, m := range msgs {
		c.index(m, len(c.msgs))
		c.msgs = append(c.msgs, m)
	}
	return nil
}

// Load 返回 q 圈定的那一段（slice 快照，元素仍是共享指针）。游标是消息在会话里的下标（十进制）。
// 会话不存在返空页。
func (s *InMemoryHistoryStore) Load(_ context.Context, key HistoryKey, q HistoryQuery) (HistoryPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.convs[key]
	var src []*schema.Message
	if c != nil {
		src = c.msgs
	}
	end := len(src)
	if q.Before != "" {
		b, err := strconv.Atoi(string(q.Before))
		if err != nil || b < 0 {
			return HistoryPage{}, fmt.Errorf("history: invalid cursor %q", q.Before)
		}
		if b < end {
			end = b
		}
	}
	cand, all := []int(nil), true
	if c != nil {
		cand, all = c.candidates(q)
	}
	if all {
		start := 0
		if q.Limit > 0 && end-q.Limit > 0 {
			start = end - q.Limit
		}
		if start >= end {
			return HistoryPage{}, nil
		}
		out := make([]*schema.Message, end-start)
		copy(out, src[start:end])
		return HistoryPage{Messages: out, Start: HistoryCursor(strconv.Itoa(start)), More: start > 0}, nil
	}
	// 候选位置里 end 之前的，从新往旧逐条核对其余条件；凑够 Limit 条之后再找一条，判 More
	var hits []int // 新 → 旧
	more := false
	for i := sort.SearchInts(cand, end) - 1; i >= 0; i-- {
		if !q.Matches(src[cand[i]]) {
			continue
		}
		if q.Limit > 0 && len(hits) == q.Limit {
			more = true
			break
		}
		hits = append(hits, cand[i])
	}
	if len(hits) == 0 {
		return HistoryPage{}, nil
	}
	out := make([]*schema.Message, len(hits))
	for i, p := range hits {
		out[len(hits)-1-i] = src[p]
	}
	return HistoryPage{Messages: out, Start: HistoryCursor(strconv.Itoa(hits[len(hits)-1])), More: more}, nil
}

// validateHistoryConfig 在构造期挡住「开了会话历史却没给 agent 起名」。
//
// 历史层里一场会话是一条流，多个 agent 的话混在一起，靠消息上的归属标记区分谁说的；
// 读回时按当前 agent 名过滤。所以 **agent 名在这里是持久身份，不是日志标签**。
//
// 而不给名字时框架自动生成的名字**每次构造都不一样**。于是：写进去的历史标着上个实例
// 的名字，这个实例按自己的名字一过滤，一条都对不上——
//
//	prior 空 → 但 historyPrior 报 ok=true（后端确实读成功了，只是过滤后没剩下）
//	     → 不回落 session events → 模型只看到当前这一句话
//
// **开了历史反而比不开更失忆**，且全程无报错。命中的正是历史层的主场景：agui 的
// Handler 每个连接构造一个 agent、a2a server 同理、进程重启必然换名——这些恰恰是
// 「跨 Run 留住对话」最需要生效的地方。
//
// 单 agent 场景随便起一个稳定的名字即可（"assistant"），多 agent 共享一场会话时名字
// 就是它们各自的身份。不给「忘了起名」留口子，跟 Owner 必填同理。
func validateHistoryConfig(cfg *Config) {
	if cfg == nil || cfg.History == nil || cfg.History.store == nil {
		return
	}
	if !cfg.nameExplicit {
		panic("agent_v2: WithHistory 要求显式 WithName——名字是历史里的显示名，" +
			"跨归属改写时会进模型 prompt、业务回看历史也读它；自动生成的名字每次构造都不同，" +
			"不起名模型和人看到的都是 agent_v2_xxxxxxxx 这种没有意义的标识符")
	}
}

// warnUnnamedAgentInHistory 提醒「这个 agent 参与了会话历史，却没有显式命名」。
//
// 构造期的检查（validateHistoryConfig）以「自己配了 WithHistory」为前提，够不着这条路：
// 子 agent 可以完全不配历史，只因为挂在一个开了历史的父下面就参与进来。而构造它的时候
// 还不知道自己会被嵌套到哪里，所以只能在运行时说。
//
// **归属改用编排位置之后，不起名有两档后果**（见 history_slot.go）：
//
//  1. **多数路径**（编排的分支 / 步骤 / 转交目标）：读回**不受影响**——位置来自编排注册名，
//     跟 agent 叫什么无关。但自动名会进**显示名**（HistoryAgentDisplayAnnotation），
//     于是跨归属改写会往模型 prompt 里塞 `For context: [agent_v2_a1b2c3d4] said: ...`
//     ——模型看到一串没有意义的标识符，无从判断那句话该有多少分量；业务回看也读到乱码。
//  2. **AgentAsTool 路径更严重**：它把 `t.agent.Name()` 当 nodeID 传给 RunSubAgent，
//     而那个 nodeID 就是位置。所以未命名子 agent 的**位置本身**每次构造都变
//     （`agent_v2_38e2ba3f` → `agent_v2_20b901b4`），跨 Run 真的读不回自己的历史。
//     这条路径上「起个名」不是为了好看，是正确性要求。
//
// 只提醒不拦截：不配历史的嵌套子 agent 是完全正常的用法，拦下来会打断一大片既有代码；
// 而且真正的修法（起个名）业务一看就知道。每条流最多一条，不刷屏。
func warnUnnamedAgentInHistory(ctx context.Context, cfg *Config) {
	if cfg == nil || cfg.nameExplicit || historySuppressed(ctx) {
		return
	}
	w := historyWriterFrom(ctx)
	if w == nil {
		return // 没参与任何历史流，名字随便
	}
	w.mu.Lock()
	already := w.unnamedWarned
	w.unnamedWarned = true
	w.mu.Unlock()
	if already {
		return
	}
	slog.WarnContext(ctx, "agent_v2: 有 agent 在写会话历史却没有显式 WithName——"+
		"自动名每次构造都变：它会进显示名被塞进模型 prompt（`For context: [自动名] said: ...`）、"+
		"业务回看历史读到乱码；**若这个 agent 是经 AgentAsTool 挂上去的，连归属位置都是这个名字**，"+
		"跨 Run 读不回自己的历史。参与历史的 agent 都该显式命名，嵌套的子 agent 也一样",
		"key", w.key.String(), "auto_generated_name", cfg.Name)
}

// DerivedInputAnnotation 标记**框架用模板拼出来的 sub-agent 输入**。
//
// # 什么算派生
//
// 流水线式的编排（Sequential / Reflect / Plan）step 之间不共享对话——数据通路是
// `LastValue` 传值，下一步要看到什么由编排**显式构造**：
//
//	"原始目标：{original}\n\n上一步（{prev}）的产出：\n{output}"
//
// 这条消息没有作者。不是用户说的（用户只说了原始目标那一句），也不是哪个 agent 说的
// （产出部分是引用的），是框架为驱动下一步而拼的。而它只能用 user 角色承载——`Agent.Run`
// 收单条消息，「这轮要做什么」天然是 user。
//
// # 判据：拼进去的东西是不是已经在历史里
//
// **不是「谁拼的」，是「拼的成分可不可重算」**——框架拼的也可能带着别处找不到的内容：
//
//	Sequential  原始目标 + 上一步产出           两样都在历史里 → 纯重复，标
//	Reflect     原始目标 + 上轮输出 + 评审反馈   critique 来自被摘除的 reflector → 不标
//	Plan        原始目标 + 你的任务 + 各步产出   任务描述来自被摘除的 planner   → 不标
//
// 后两者不记的话，历史里会只剩「actor 说了 A，然后说了 B」——看不出中间发生过什么。
// 内部决策 LLM 的产出（plan / critique）被 `WithoutHistoryCtx` 挡在历史外，它们唯一的
// 落脚点就是这条派生输入。
//
// 另一头：LLM 在 tool_call 里生成的 `AgentAsTool` 输入**不算派生**——模型确实要求子 agent
// 做这件事，是真实发生的。
//
// # 消费者
//
// 会话历史：带这个标记的不写。理由跟当初把它移出 Sequential 消息流时一样
// （`sequential_builder.go` 的 `seqAgentMessagesCh` 注释）——
//
//  1. 每条产出被复述两遍：一次作为本步产出，一次嵌在下一步的模板里；
//  2. 角色错位：「上一步 agent 说的话」以用户口吻在流里再现，回放这份记录的人会读到
//     用户从没说过的话。
//
// 而它随时能从「原始目标 + 各步产出」重算——那两样都在历史里，编排每轮也都重新拼。
//
// 业务自己写编排（GraphAgent 的 NodeFunc 里调 sub-agent）时，如果同样是拼模板造输入，
// 用 `MarkDerivedInput` 标一下。
const DerivedInputAnnotation = "derived_orchestration_input"

// MarkDerivedInput 返回一份标记为「框架拼装」的浅拷贝，见 DerivedInputAnnotation。
//
// 拷贝而非原地改：业务自定义的 ExecInputBuilder 可能返回它自己持有的对象，改它会波及
// 调用方看不见的地方。msg 为 nil 时原样返回。
func MarkDerivedInput(msg *schema.Message) *schema.Message {
	if msg == nil {
		return nil
	}
	cp := *msg
	ann := make(map[string]any, len(msg.Annotations)+1)
	for k, v := range msg.Annotations {
		ann[k] = v
	}
	ann[DerivedInputAnnotation] = true
	cp.Annotations = ann
	return &cp
}

// isDerivedInput 报这条消息是不是框架拼出来的。
func isDerivedInput(msg *schema.Message) bool {
	if msg == nil {
		return false
	}
	v, ok := msg.Annotations[DerivedInputAnnotation].(bool)
	return ok && v
}

// rootInputNonce 读一条消息的原始 input nonce（没有返空串）。
// rootInputID 取一条消息的稳定身份。
//
// 身份在**构造期**铸造（`schema.UserMessage` 等），所以它在消息开始流动之前就定死了
// ——这正是判据可靠的前提：中间层拷贝一份、hook 换掉整条消息，身份都跟着走或明确换新，
// 不像「事后打个标记」那样会在拷贝时静默落空。
//
// 兜底铸造点在 `recordHistory`（写进历史之前），覆盖业务手搓 `&schema.Message{}` 的路径。
func rootInputID(msg *schema.Message) string {
	if msg == nil {
		return ""
	}
	return msg.ID
}

// priorHasMessageIDInOtherSlot 报这条消息是不是已经**在别的归属下**记过了。
//
// # 为什么是「别的归属」而不是「记过没有」
//
// 要挡的害处是**同一份内容挂在两个归属下**——顶层图透传子 agent 的产出指针时，子在自己
// 的流里记过一次，顶层出口若再记一次，模型下一轮就读到两遍。这天生是跨归属现象。
//
// 而**同一条流内的重复是真的重复**，必须留：执行体每轮返回同一个消息对象
// （`var okMsg = schema.AssistantMessage(...)` 然后每次返回它，在 Go 里很自然）时，
// Loop 各轮的产出都得记下来。按「记过没有」判会把「同一个对象跨轮复用」当成「同一条
// 产出」，第 2 轮起静默丢——那正是 `TestHistory_ExecutorReusesMessageObject_AllRoundsRecorded`
// 钉的不变量（我第一版没限归属，当场把它打红）。
//
// 与输入侧 `priorHasRootInputIDInSlot` 恰好相反，两者都对：
//
//   - 输入侧**限本流**：兄弟分支刚写进去的那份不该挡住我，每条分支流各自要完整（有问有答）
//   - 产出侧**限别的流**：同流内是真重复，跨流才是归属重记
//
// 为什么产出侧必须查**持久化历史**而不是本次调用的登记表：那两道守卫
// （`producedHistoryHere` 范围回执 / `alreadyRecorded` 全局登记表）都是 per-call 的，
// 而 Resume **不重跑已完成节点** ⟹ 回执不会再被 bump、登记表也是新的 ⟹ Run 里记过的
// 东西在 Resume 那次调用里「查无此记」。跨调用的事实只有落盘的历史知道。
func priorHasMessageIDInOtherSlot(prior []*schema.Message, id, slot string) bool {
	if id == "" {
		return false
	}
	for _, m := range prior {
		if m.ID != id {
			continue
		}
		if o, _ := m.Annotations[HistoryAgentAnnotation].(string); o != slot {
			return true
		}
	}
	return false
}

func priorHasRootInputIDInSlot(prior []*schema.Message, nonce, slot string) bool {
	if nonce == "" {
		return false
	}
	for _, m := range prior {
		if rootInputID(m) != nonce {
			continue
		}
		if o, _ := m.Annotations[HistoryAgentAnnotation].(string); o == slot {
			return true
		}
	}
	return false
}

// priorHasRootInputID 报 prior 里有没有带指定 nonce 的消息——即这场对话的原始 input
// 是否已被前面某一跳记进历史。跨进程时 prior 从历史 backend 读（共享），所以第一次 Run
// 记的那份 Resume 后仍能匹配到。
// priorHasRootInputIDInSlot 是按**流**（归属位置）限定的版本。
//
// 全局版问的是「这条输入在整场会话里出现过没有」——那对**跨 Run** 是对的（Resume 重跑到
// 同一步时前一个 Run 的记录已落盘），对**并发扇出**是错的：兄弟分支刚写进去的那份会让
// 其余分支判「记过了」，于是它们的流里只有答没有问，而谁先写取决于调度。
func priorHasRootInputID(prior []*schema.Message, nonce string) bool {
	if nonce == "" {
		return false
	}
	for _, m := range prior {
		if rootInputID(m) == nonce {
			return true
		}
	}
	return false
}
