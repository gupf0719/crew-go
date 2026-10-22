package agent_v2

// 单次 Run 内**复用摘要结果**——AutoCompact 不再每轮把整段消息流重新摘要一遍。
//
// # 为什么要有它
//
// 压缩只作用于喂给模型的那一份、不写回消息流（channel 里留的是完整原文，会话历史、工具账都靠它）。
// 于是越过阈值之后，下一轮读出来的仍是原始全量、仍然超阈值，又摘要一次。实测（2026-10-20）：一次 Run
// 里 200 轮工具调用、窗口 64k，摘要被调 194 次，摘要输入累计 82MB 字符——随轮数平方增长。
//
// # 做法
//
// 压缩成功时把结果记成一份**备忘**放进 BasicAgent 图的一条 channel（随存档走，续跑接着用）：
// 「消息流的前 Upto 条，由 Head 这几条代表」。Head 就是那次压缩输出里尾部之前的部分——前导 system、
// 长期记忆注入、受保护的组、摘要。下一轮先把消息流的前 Upto 条换成 Head，再照常跑 MicroCompact /
// AutoCompact：没超阈值就直接用；又超了就在「Head + 之后新增的消息」上再摘要一次，上一份摘要成了这次
// 摘要的输入，产出新的备忘。消息流本身一条没改。
//
// # 什么时候不用
//
// 备忘只在消息流的前 Upto 条没变时成立。`OnModelStart` 钩子可以改写这一轮的消息（[消费] 档），插入、
// 删除、换掉其中任何一条，前缀的指纹（Anchor）就对不上，备忘作废、这一轮照旧在全量上压缩——钩子每轮
// 都确定性地做同一件改写时前缀不变，备忘照用。

import (
	"context"
	"encoding/json"
	"hash"
	"hash/fnv"
	"strconv"
	"strings"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func init() {
	compose_v2.RegisterValueType[*compactionMemo]("*agent_v2.compactionMemo")
}

// compactionMemo 是一次 AutoCompact 的结果：消息流的前 Upto 条由 Head 代表。
type compactionMemo struct {
	// Upto 是被 Head 代表的那段的长度（消息流的前 Upto 条）。
	Upto int `json:"upto"`
	// Anchor 是消息流前 Upto 条的指纹，见 [prefixAnchor]。
	Anchor string `json:"anchor"`
	// Head 是那段的压缩形态：前导 system + 长期记忆注入 + 受保护的组 + 摘要（+ 文件重注入）。
	Head []*schema.Message `json:"head"`
}

// prefixAnchor 是一段消息的指纹：逐条送给模型的全部内容——角色、身份、正文、推理文本、拒答、内容块（工具调用
// 的名字与参数、工具结果、图片等）。跨存档往返稳定（不看指针）；插入、删除、换掉、改了其中任何一处都对不上。
//
// 送给模型的内容逐字都要进指纹，不能只看长度或配对 ID：钩子每轮往前缀里插一条「第 N 轮」这类长度不变的消息、
// 改历史里某次工具调用的参数（脱敏）、改推理文本时，只比长度会把上一轮的结果套到这一轮上，钩子这一轮的改写
// 就丢了（实测）。标注不进：它不发给模型，框架自己也会在后续步骤给消息补标注。算一遍是 O(前缀字节数)，每轮
// 几毫秒，跟一次模型调用比可以忽略。
func prefixAnchor(msgs []*schema.Message) string {
	h := fnv.New64a()
	var b strings.Builder
	for _, m := range msgs {
		b.Reset()
		if m != nil {
			b.WriteString(string(m.Role))
			b.WriteByte('|')
			b.WriteString(m.ID)
			b.WriteByte('|')
			b.WriteString(strconv.Itoa(len(m.Content)))
			b.WriteByte('|')
			b.WriteString(strconv.Itoa(len(m.ReasoningContent)))
			for _, tc := range schema.GetToolCalls(m) {
				b.WriteByte('|')
				b.WriteString(tc.ID)
			}
			if id := schema.GetToolResultID(m); id != "" {
				b.WriteString("|r:")
				b.WriteString(id)
			}
		}
		b.WriteByte('\n')
		_, _ = h.Write([]byte(b.String())) // hash.Hash 的 Write 不返回错误
		if m != nil {
			writeAnchorField(h, m.Content)
			writeAnchorField(h, m.ReasoningContent)
			writeAnchorField(h, m.Refusal)
			if len(m.ContentBlocks) > 0 {
				blocks, err := json.Marshal(m.ContentBlocks)
				if err != nil {
					// 编不出来的内容块（业务塞了不可编码的 Data）按错误文本算：同一份内容每轮得到同一个指纹，备忘照常
					// 成立；换了内容大概率换了错误文本
					blocks = []byte("unencodable:" + err.Error())
				}
				writeAnchorField(h, string(blocks))
			}
		}
	}
	return strconv.Itoa(len(msgs)) + ":" + strconv.FormatUint(h.Sum64(), 16)
}

// writeAnchorField 把一段内容连同它的长度写进指纹：几段内容前后相接时，长度把边界分开（「ab」+「c」与「a」+「bc」
// 不同）。
func writeAnchorField(h hash.Hash64, v string) {
	_, _ = h.Write([]byte(strconv.Itoa(len(v)) + ":")) // hash.Hash 的 Write 不返回错误
	_, _ = h.Write([]byte(v))
}

// apply 把消息流的前 Upto 条换成 Head；备忘对这份消息流不成立时原样返回、headLen 为 0。
func (m *compactionMemo) apply(msgs []*schema.Message) (reduced []*schema.Message, upto, headLen int) {
	if m == nil || m.Upto <= 0 || m.Upto > len(msgs) || prefixAnchor(msgs[:m.Upto]) != m.Anchor {
		return msgs, 0, 0
	}
	reduced = make([]*schema.Message, 0, len(m.Head)+len(msgs)-m.Upto)
	reduced = append(reduced, m.Head...)
	reduced = append(reduced, msgs[m.Upto:]...)
	return reduced, m.Upto, len(m.Head)
}

// compactionMemoCtx 是模型节点交给 modelFn 的备忘：in 是这一轮开始时 channel 里的那份，out 由 modelFn
// 在这一轮压缩成功时填，模型节点成功后写回 channel。
type compactionMemoCtx struct {
	in  *compactionMemo
	out *compactionMemo
}

type compactionMemoKey struct{}

// withCompactionMemo 放这一轮的备忘。每轮都放（为空也放）：这一轮的模型回调里调起的子 agent 要拿到它
// 自己图里的那份，不是父的。
func withCompactionMemo(ctx context.Context, c *compactionMemoCtx) context.Context {
	return context.WithValue(ctx, compactionMemoKey{}, c)
}

func compactionMemoFrom(ctx context.Context) *compactionMemoCtx {
	c, _ := ctx.Value(compactionMemoKey{}).(*compactionMemoCtx)
	return c
}

// nextCompactionMemo 在这一轮压缩成功之后算新的备忘。
//
// orig 是套用备忘之前的消息流，upto / headLen 是这一轮套用的结果，compacted 是压缩输出、tailLen 是它
// 尾部（原样保留的那几条）的长度。尾部是输入的后缀，输入是「Head + 消息流第 upto 条之后」——尾部的
// 起点落在 Head 之后时，换算回消息流就是新的 Upto；落进 Head 里（极少见）时这一轮的结果没法表示成
// 「消息流的一个前缀」，不记。
func nextCompactionMemo(orig []*schema.Message, upto, headLen int, inputLen int, compacted []*schema.Message, tailLen int) *compactionMemo {
	tailStart := inputLen - tailLen
	if tailLen < 0 || tailStart < headLen || tailLen > len(compacted) {
		return nil
	}
	newUpto := upto + (tailStart - headLen)
	if newUpto <= 0 || newUpto > len(orig) {
		return nil
	}
	head := append([]*schema.Message(nil), compacted[:len(compacted)-tailLen]...)
	return &compactionMemo{Upto: newUpto, Anchor: prefixAnchor(orig[:newUpto]), Head: head}
}
