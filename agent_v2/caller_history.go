package agent_v2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"

	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// 调用方提供的前文：这场对话在本次之前的轮次，由调用方交来（典型是 AG-UI 前端每次请求都带上的完整 messages），
// 不是本进程里哪个 agent 产出的。
//
// # 一次运行只认一个来源
//
// 前文与历史层描述的是同一段对话，两份放在一起没有可靠的对法：客户端会裁掉、改写、重新生成前面的轮次，历史层里又有
// 客户端看不到的工具往返与子对话。所以一次运行里每个 agent 看到的早先轮次只取其中一份：
//
//   - 以服务端为准：这次运行有拥有这场对话的写入器（见下），建写入器时读得出这场对话的记录，前文需要写的也写成了。
//     历史层里这场对话一条记录都没有时（服务端第一次见到它：对话从别处开始，或者历史层后来才配上），写入器建好时把
//     前文写进去，排在这次运行的一切记录之前；此后每一轮都由服务端自己记，前文不再看、也不再写。客户端改过的早先轮次
//     （编辑前面的消息、重新生成答复）不会改写服务端的记录，模型照旧看到服务端那份——要这类前端功能，让前端拥有历史
//     （不配历史层）。
//   - 以客户端为准：其余情形。前文排在 session events 之前交给模型，历史层这次不读。包括：没配历史层；配了历史层的
//     agent 是在一个正在执行的任务里被调起的（它的写入器只记它自己那段子对话，不代表这场对话）；建写入器时读历史层
//     失败（判不出要不要写前文），或者前文没写进去；某个 agent 读历史时读失败（只影响它这一次）。
//
// 拥有者是建写入器时 ctx 上没有正在执行的任务的那一个（[enclosingTask]），也就是这次运行最外层的 agent。编排从自己
// 的整棵静态子树借历史配置（[borrowHistoryConfig]），树里只要有成员配了历史层，写入器就由最外层的编排拥有；GraphAgent
// 只能从 [WithHistoryCtx] 拿配置，图里节点调起的 agent 配的历史层是嵌套写入器。
//
// 两个代价：
//
//   - 一场对话的各轮要用同一套配置。某几轮由没配历史层的 agent 接（同一个 thread 换了 agent，或者只有节点里调起的
//     agent 才配了），那几轮进不了服务端的记录，以服务端为准的轮看不到它们。
//   - 前文只在服务端第一次见到这场对话时写一次。那一次读历史层失败或者写失败，这次运行照样以客户端为准、看得到前文，
//     但历史层里随后有了这次运行的记录，之后的运行不会再写前文——那几轮从此不在服务端的记录里。
//
// # 只取 user / assistant 的文本
//
// system / developer、工具调用与结果、多模态块都不取。调用方能伪造历史：指令只认 agent 自己的前言
// （[WithSystemPrompt] / [WithPromptTemplate]），工具往返的配对与可信度由服务端自己的记录承载。伪造的 user /
// assistant 文本最多做到提示词注入，碰不到框架的控制决定——审批、转交、存档都在服务端。
//
// 内部决策 LLM（[WithoutHistoryCtx]）看不到前文；[DetachTask] 开的独立任务看不到（不是这场对话的这一轮）。

// CallerHistoryAnnotation 标「这条消息是调用方提供的前文」（值为 true）：它不是哪个 agent 说的，读会话历史时不按 agent
// 过滤，每个 agent 的每种视图都原样看到；写进历史层的那份也带着它，归属（[HistoryAgentAnnotation]）是保留值
// callerHistoryOwner——说话的是调用方，框架按这个归属查就能只取回前文，不必翻整场会话。它的消息 ID 是确定性派生的——几个写入器同时写过时
// 会有不止一份，业务直接读历史层时按 ID 去重，并把它们排在最前（它们是服务端第一次见到这场对话之前的轮次）。
const CallerHistoryAnnotation = "crew_caller_history"

type callerHistoryKey struct{}

// callerHistoryOwner 是写进历史层的前文的归属（见 CallerHistoryAnnotation）。「只看自己」的视图按归属读，前文另按它取。
const callerHistoryOwner = "crew_caller_history"

// callerHistoryVal 是 ctx 上的前文：过滤、派生 ID 之后的消息（只读）。
type callerHistoryVal struct {
	msgs []*schema.Message
}

// WithCallerHistoryCtx 声明这场对话在本次之前的轮次（调用方提供的前文，按时间顺序）。谁说了算见本文件开头：有拥有
// 这场对话的写入器时以服务端为准（服务端第一次见到这场对话时把它写进去），否则以它为准。
//
// 只取 role 为 user / assistant 的文本（Content），别的一概不取：system / developer、工具调用与结果、多模态块。
// 给的是这一轮**之前**的轮次——这一轮的输入照常作为 Run 的 input 传，别放进来，否则模型看到它两遍。空的也算声明了
// （这场对话的第一轮）。
//
// 续跑（Resume）时也要推，而且推的应当是 Run 时那一份：续跑里才开始跑的成员（Sequential 的后一步之类）同样要看到
// 前文，跟同一个任务里先跑的成员看到的一样。
func WithCallerHistoryCtx(ctx context.Context, msgs []*schema.Message) context.Context {
	kept := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || m.Content == "" {
			continue
		}
		var t *schema.Message
		switch m.Role {
		case schema.RoleUser:
			t = schema.UserMessage(m.Content)
		case schema.RoleAssistant:
			t = schema.AssistantMessage(m.Content)
		default:
			continue
		}
		// 不沿用调用方给的 ID（不可信，也不该跟服务端的消息共用一个身份空间），按位置与内容确定性派生：同一份前文
		// 不论被谁、写进去几次，都是同一组 ID，读的时候去得了重
		t.ID = callerTurnID(len(kept), t.Role, t.Content)
		kept = append(kept, t)
	}
	return context.WithValue(ctx, callerHistoryKey{}, callerHistoryVal{msgs: kept})
}

// callerTurnID 是前文第 i 条的确定性 ID。前缀不与框架铸的 UUID 撞。
func callerTurnID(i int, role schema.MessageRole, content string) string {
	sum := sha256.Sum256([]byte(strconv.Itoa(i) + "\x00" + string(role) + "\x00" + content))
	return "caller-" + hex.EncodeToString(sum[:16])
}

// callerPrior 返回 ctx 上声明的前文（共享的底层切片，只读）；第二个返回值报有没有声明（声明了一份空的也算）。
// 被 WithoutHistoryCtx 摘掉的分支当作没声明。
func callerPrior(ctx context.Context) ([]*schema.Message, bool) {
	if historySuppressed(ctx) {
		return nil, false
	}
	v, ok := ctx.Value(callerHistoryKey{}).(callerHistoryVal)
	return v.msgs, ok
}

// callerHistory 返回调用方的全部前文，每次一份新拷贝（带 CallerHistoryAnnotation）：并发分支各拿各的，钩子就地改了
// 也碰不到别人那份。没有声明、或者这条分支被 WithoutHistoryCtx 摘掉了时返回 nil。以客户端为准时用它。
func callerHistory(ctx context.Context) []*schema.Message {
	msgs, _ := callerPrior(ctx)
	if len(msgs) == 0 {
		return nil
	}
	out := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, callerCopy(m))
	}
	return out
}

// callerCopy 拷一条前文，标上 CallerHistoryAnnotation。消息是新拷贝、标注是新 map；ID 不变。
func callerCopy(m *schema.Message) *schema.Message {
	cp := *m
	cp.Annotations = map[string]any{CallerHistoryAnnotation: true}
	return &cp
}

// isCallerHistory 报 m 是不是调用方提供的前文。
func isCallerHistory(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, _ := m.Annotations[CallerHistoryAnnotation].(bool)
	return v
}

// seedCallerPrior 在拥有者的写入器建好、交出去之前调：历史层里这场对话一条记录都没有时把前文写进去，排在这次运行的
// 一切记录之前。返回这次运行能不能以服务端为准——读历史层失败（判不出要不要写），或者前文没写进去时返回 false。
//
// 同步写、不进写入器的队列：写没写成要在第一次读历史之前知道，写失败时这次运行改用客户端的前文，而不是读出一场空
// 对话。几个运行同时判出「这场对话是空的」时各写一份，ID 相同，读的时候去重（见 callerFirst）。
func seedCallerPrior(ctx context.Context, w *historyWriter, prior []*schema.Message) (ok bool) {
	existing, err := w.store.Load(ctx, w.key, HistoryQuery{Limit: 1})
	if err != nil {
		slog.WarnContext(ctx, "agent_v2: 读会话历史失败，这次运行以调用方提供的前文为准、前文不写进历史层",
			"key", w.key.String(), "err", err)
		return false
	}
	if len(existing.Messages) > 0 || len(prior) == 0 {
		return true
	}
	seed := make([]*schema.Message, 0, len(prior))
	for _, m := range prior {
		c := callerCopy(m)
		c.Annotations[HistoryAgentAnnotation] = callerHistoryOwner
		seed = append(seed, c)
	}
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(ctx, slog.LevelWarn, "agent_v2: 会话历史后端 panic，调用方提供的前文没写进去，这次运行以它为准",
				panicerr.New(r), "key", w.key.String(), "count", len(seed))
			ok = false
		}
	}()
	if err := w.store.Append(ctx, w.key, seed); err != nil {
		slog.WarnContext(ctx, "agent_v2: 调用方提供的前文没写进会话历史，这次运行以它为准",
			"key", w.key.String(), "count", len(seed), "err", err)
		return false
	}
	return true
}

// callerFirst 返回以服务端为准时这个 agent 看到的历史：写进历史层的前文按 ID 去重、排在最前（它们是服务端第一次见到
// 这场对话之前的轮次；几个写入器同时写过时有不止一份，晚写的那份会落在别的记录之后），其余记录按 project 投影成这个
// agent 的视图（project 返回 nil 的不要）。前文不投影：它不是哪个 agent 说的，每种视图都原样看到。
func callerFirst(recorded []*schema.Message, project func(*schema.Message) *schema.Message) []*schema.Message {
	out := make([]*schema.Message, 0, len(recorded))
	seen := make(map[string]bool)
	for _, m := range recorded {
		if !isCallerHistory(m) {
			continue
		}
		if m.ID != "" {
			if seen[m.ID] {
				continue
			}
			seen[m.ID] = true
		}
		out = append(out, m)
	}
	for _, m := range recorded {
		if m == nil || isCallerHistory(m) {
			continue
		}
		if p := project(m); p != nil {
			out = append(out, p)
		}
	}
	return out
}
