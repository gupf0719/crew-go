package agent_v2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// RepeatToolReminderAnnotation 标「这条是框架因为检测到工具重复调用而合成的提醒」。
//
// 它同时打 [DerivedInputAnnotation]——**提醒不进会话历史**。判据走既有那条规约
// （见 `history_slot.go` 的派生输入分档）：**拼进去的东西是不是已经在历史里**。
// 重复的那几次工具调用本身已经逐条在历史里了，提醒是从它们**完全可重算**的派生物，
// 跟 Sequential 那种「原始目标 + 上一步产出」同档，所以不记。
//
// 反例对照：Reflect 的评审反馈 / Plan 的任务描述来自被摘除的内部决策 LLM，派生输入是
// 它们**唯一**的落脚点，那两处才记。
const RepeatToolReminderAnnotation = "repeat_tool_reminder"

// repeatToolState 是「同一个调用连续重复了几次」的链，存在 BasicAgent 图的 basicRepeatChainCh 里（随存档走）。
//
// 链要标识的是**一个任务里的一条 ReAct 调用流**——「这个循环里，模型在原地打转」——而这正是一张 BasicAgent 图
// 自己的状态：每次 Run 一张图（任务换了就是新的图、从空开始），并行分支、嵌套的被调方各跑各的图。曾经它挂在
// 会话 KV 上、按键区分「哪条流」，键先后漏过两维：只按 agent 名分时，两个同名的并行分支撞进同一条链（计数跨
// 分支合并，链上的并发写裸奔在锁外，实测必现 DATA RACE）；补上编排位置之后又少了任务——上一个任务里的重复
// 次数累计到下一个任务，新任务第一次调就收到提醒（审计实测）。放进图状态之后不需要键，那几维由「这是哪张图」
// 天然给出；它也跟着这一批的提交走：一批中断时这一批的计数随这次执行作废，续跑重跑这一批时重新计——曾经计数
// 在交出中断之前就记进了会话 KV，续跑重算一遍，同一批被算两次；续跑不带原 session 时链又从头计（均审计实测）。
//
// 字段导出、带 JSON tag：它要过存档的序列化（init 里注册了值类型）。
type repeatToolState struct {
	// Key 是工具名 + 规范化参数的摘要，见 repeatToolKey。
	Key   string `json:"key"`
	Count int    `json:"count"`
	// Fired 记已经为哪些阈值喊过了——**按阈值去重而不是按次数**：链继续增长时只在跨过下一档时再喊一次，
	// 不是每轮都喊。
	Fired []int `json:"fired,omitempty"`
}

func (s *repeatToolState) clone() *repeatToolState {
	if s == nil {
		return &repeatToolState{}
	}
	return &repeatToolState{Key: s.Key, Count: s.Count, Fired: append([]int(nil), s.Fired...)}
}

func (s *repeatToolState) fired(th int) bool {
	return slices.Contains(s.Fired, th)
}

// repeatTurn 是 tools 节点这一次执行手上的那份链：这一批第一次观察调用时从 channel 里已提交的那份拷一份出来，
// 这一批的调用逐个喂给它，节点成功后写回 channel；节点失败或中断时随这次执行一起丢掉。拷一份而不是就地改，
// 是因为 channel 里那份是已提交的状态，这一批没成之前不能动它。用到才拷：没开检测时不建链。
type repeatTurn struct {
	mu        sync.Mutex
	committed *repeatToolState // channel 里已提交的那份，只读
	st        *repeatToolState // 这一批的链，第一次观察时才建
}

func newRepeatTurn(committed *repeatToolState) *repeatTurn {
	return &repeatTurn{committed: committed}
}

// result 返回这一批之后的链；这一批没观察过调用时返回 nil（不必写回）。
func (t *repeatTurn) result() *repeatToolState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.st
}

// repeatTurnKey 把 [repeatTurn] 交给这一批的 toolsFn。它是执行状态：钩子返回别处存下的 ctx 时，采纳取框架这次的。
type repeatTurnKey struct{}

func withRepeatTurn(ctx context.Context, t *repeatTurn) context.Context {
	return context.WithValue(ctx, repeatTurnKey{}, t)
}

func repeatTurnFrom(ctx context.Context) *repeatTurn {
	t, _ := ctx.Value(repeatTurnKey{}).(*repeatTurn)
	return t
}

func init() {
	// 跨 cp 边界持久化 ⟹ 必须注册。漏注册的后果是整份存档写不出来（Snapshot 逐个 marshal channel 值，一个失败整份失败）。
	compose_v2.RegisterValueType[*repeatToolState]("*agent_v2.repeatToolState")
}

// RepeatToolConfig 配置工具重复调用检测。
//
// # 它解决什么
//
// 模型死命捶同一个工具、同样参数，今天框架**毫无反应**——唯一的防线是 maxRounds，
// 而那是个悬崖：撞上直接抛 [MaxIterationsError]。中间这段（已经重复了 3 次、8 次）
// 框架一句话都不说，而那正是最该打断它的时候。
//
// # 三条非显然的判据
//
//   - **在工具执行之后计数，不是之前**。因为**被拒绝的调用也走执行之后**——
//     而「模型死命捶一个被审批拒掉的工具」恰恰是最该打断的循环。我们的
//     `WithApprovalMode(ApprovalAsk)` + Deny 路径直接吃到这条。
//   - **框架工具对链是透明的**：既不计数也不重置。否则模型只要在两次重复之间插一个
//     `activate_skill` 就能把链洗白，而它其实还在原地打转。
//   - **预览截断只约束模型可见文本，不约束检测键**。键按完整参数串算（存的是摘要）——两个只在第 600
//     个字符处不同的调用不是同一个调用。反过来预览必须截断：大 payload 会**无界地骑进
//     下一个请求**，而那恰恰是在循环场景下。
type RepeatToolConfig struct {
	// Enabled 关时零开销（不算键、不建链）。默认关。
	Enabled bool

	// Thresholds 是喊话的档位，**按连续重复次数**。<= 0 个元素时用默认 [3, 5, 8]。
	//
	// 分档是有意的：第一档只给一句泛化的提醒（模型多半自己就拐弯了），后面几档才
	// 点名工具、次数和参数——一上来就给详版会让正常的重试也吃一堆噪音。
	Thresholds []int

	// PreviewMaxChars 是提醒里参数预览的上限。<= 0 时用默认 500。
	PreviewMaxChars int
}

const (
	defaultRepeatToolPreviewMaxChars = 500
)

func defaultRepeatToolThresholds() []int { return []int{3, 5, 8} }

func (c *RepeatToolConfig) thresholds() []int {
	if c == nil || len(c.Thresholds) == 0 {
		return defaultRepeatToolThresholds()
	}
	out := append([]int(nil), c.Thresholds...)
	sort.Ints(out)
	return out
}

func (c *RepeatToolConfig) previewMaxChars() int {
	if c == nil || c.PreviewMaxChars <= 0 {
		return defaultRepeatToolPreviewMaxChars
	}
	return c.PreviewMaxChars
}

// repeatToolKey 把一次调用规范化成检测键：工具名 + **深度按 key 排序**后的参数 JSON，取 sha256 摘要。
//
// 排序是承重的：`{"a":1,"b":2}` 和 `{"b":2,"a":1}` 是同一次调用，模型（或 provider 的
// JSON 序列化）换个字段顺序就能骗过检测，那这个检测基本等于没有。
//
// 参数解析不出来时退回**原始串**——解析失败的那两次如果逐字相同，那仍然是同一次调用。
//
// 存摘要不存原文：键随计数链进存档（basicRepeatChainCh）。原文可能很大，每份存档会多一份同样大的副本（参数本身已经在
// 消息里存了一份）；解析失败时的原始串还可能含非法 UTF-8，经 JSON 往返被换成 U+FFFD，续跑后跟同一个调用对不上、从 1
// 重计（审计实测）。摘要按完整的规范化串算，只在第 600 个字符处不同的两个调用照样是两个键。
func repeatToolKey(name, argsJSON string) string {
	canon := argsJSON
	var v any
	if err := json.Unmarshal([]byte(argsJSON), &v); err == nil {
		var sb strings.Builder
		writeCanonical(&sb, v)
		canon = sb.String()
	}
	sum := sha256.Sum256([]byte(name + "\x00" + canon))
	return hex.EncodeToString(sum[:])
}

// writeCanonical 按 key 排序写出 JSON 值。只处理 encoding/json 会产出的那几种类型。
func writeCanonical(sb *strings.Builder, v any) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			b, _ := json.Marshal(k)
			sb.Write(b)
			sb.WriteByte(':')
			writeCanonical(sb, x[k])
		}
		sb.WriteByte('}')
	case []any:
		sb.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeCanonical(sb, e)
		}
		sb.WriteByte(']')
	default:
		b, _ := json.Marshal(x)
		sb.Write(b)
	}
}

// observeRepeatToolCall 把一次**已经执行完**的调用喂给这一批的链，返回该喊话时的提醒消息
// （不该喊时返 nil）。
//
// 调用方必须保证：框架工具**不进来**（透明语义由调用方过滤，见 RepeatToolConfig 的判据）。
// turn 为 nil（不在 BasicAgent 的 tools 节点里）时什么都不做。
func observeRepeatToolCall(turn *repeatTurn, cfg *RepeatToolConfig, name, argsJSON string) *schema.Message {
	if turn == nil || cfg == nil || !cfg.Enabled {
		return nil
	}
	k := repeatToolKey(name, argsJSON)

	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.st == nil {
		turn.st = turn.committed.clone()
	}
	chain := turn.st
	if chain.Key != k {
		*chain = repeatToolState{Key: k, Count: 1}
		return nil
	}
	chain.Count++

	// 跨过哪一档就喊哪一档；同一档只喊一次。
	var hit int
	for _, th := range cfg.thresholds() {
		if chain.Count >= th && !chain.fired(th) {
			hit = th
		}
	}
	if hit == 0 {
		return nil
	}
	chain.Fired = append(chain.Fired, hit)

	return buildRepeatToolReminder(name, argsJSON, chain.Count, hit, cfg)
}

// buildRepeatToolReminder 造提醒消息。
//
// 第一档（最低阈值）只给泛化提醒；更高档才点名工具 + 次数 + 参数预览。
func buildRepeatToolReminder(name, argsJSON string, count, hit int, cfg *RepeatToolConfig) *schema.Message {
	ths := cfg.thresholds()
	var body string
	if len(ths) > 0 && hit == ths[0] {
		body = "[system_reminder] 你刚刚连续用同样的参数调了同一个工具好几次，结果没有变化。" +
			"换个做法：改参数、换工具，或者直接说明你卡在哪儿。"
	} else {
		body = fmt.Sprintf("[system_reminder] 你已经连续 %d 次用同样的参数调用 %q，结果没有变化。\n"+
			"参数：%s\n"+
			"这条路走不通。改参数、换工具，或者明确说明你被什么挡住了——"+
			"如果是外部原因（缺凭证、权限被拒、服务不可用），直接说出来，不要继续重试。",
			count, name, truncateForPreview(argsJSON, cfg.previewMaxChars()))
	}
	m := schema.UserMessage(body)
	m.Annotations = map[string]any{
		RepeatToolReminderAnnotation: true,
		DerivedInputAnnotation:       true, // 不进会话历史，理由见 RepeatToolReminderAnnotation
	}
	return m
}

// truncateForPreview 按 UTF-8 边界截断，超了在尾部标明省略了多少。
func truncateForPreview(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return fmt.Sprintf("%s…（省略 %d 字节）", s[:cut], len(s)-cut)
}

// utf8Start 判断这个字节是不是一个 UTF-8 码点的起始字节（非 continuation）。
func utf8Start(b byte) bool { return b&0xC0 != 0x80 }
