package a2a

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// remoteOutput 累积远端这一次应答的内容：按到达顺序分段，artifact 的段按 ID 认——append 为 false 的更新整段替换
// （A2A 里替换 artifact 的办法：服务端一次模型调用流中途失败、它的片段作废时这样发，见 a2a/server 的流桥）。曾经一律
// 追加，作废的半截跟重来的完整答复拼成一份，当成远端的答复交给宿主（实测 "partial-full"）。
//
// 它同时管往宿主转发的片段（chunk 事件）：转发时带一个 run_id，某段被替换时对这个 run_id 发 reset_stream、换一个新的、
// 把替换后的完整内容重发一遍——宿主的前端按 run_id 撤回已显示的那些、显示新的。
type remoteOutput struct {
	segs       []*outputSeg
	byArtifact map[a2atypes.ArtifactID]*outputSeg

	agentName string
	runID     string // 当前转发片段用的 run_id
	emitted   bool   // 当前 run_id 下转发过片段
}

type outputSeg struct {
	text   strings.Builder
	blocks []*schema.ContentBlock
}

func newRemoteOutput(agentName string) *remoteOutput {
	return &remoteOutput{byArtifact: map[a2atypes.ArtifactID]*outputSeg{}, agentName: agentName, runID: newChunkRunID()}
}

// add 追加一段（消息、任务快照里的 artifact）。forward 为真时同时转发给宿主。
func (o *remoteOutput) add(ctx context.Context, parts a2atypes.ContentParts, forward bool) {
	t, bs := PartsToContent(parts)
	seg := &outputSeg{blocks: bs}
	seg.text.WriteString(t)
	o.segs = append(o.segs, seg)
	if forward {
		o.forward(ctx, t)
	}
}

// taskArtifact 应用任务快照（Task 事件）里的一个 artifact：快照是这个 artifact 此刻的完整内容，按 ID 整段替换，之后同 ID
// 的增量接在后面、append=false 再替换。快照里新出现的 artifact 不转发给宿主（同改动前）；替换掉已有内容时照样撤回、重发。
// 曾经快照不按 ID 登记：重新挂上任务（SubscribeToTask 第一个事件就是带已有 artifacts 的快照）之后同 ID 的 append=false
// 被当成追加，又拼出「半截 + 完整」（审计实测）。
func (o *remoteOutput) taskArtifact(ctx context.Context, id a2atypes.ArtifactID, parts a2atypes.ContentParts) {
	if id == "" {
		o.add(ctx, parts, false)
		return
	}
	t, bs := PartsToContent(parts)
	seg, ok := o.byArtifact[id]
	if !ok {
		seg = &outputSeg{blocks: bs}
		seg.text.WriteString(t)
		o.byArtifact[id] = seg
		o.segs = append(o.segs, seg)
		return
	}
	o.replace(ctx, seg, t, bs)
}

// artifactUpdate 应用一次 artifact 更新：appendMode 为真时接在同一个 artifact 的段后面，否则整段替换。替换掉已有内容时
// 撤回已转发的片段、把替换后的完整内容重发一遍。
func (o *remoteOutput) artifactUpdate(ctx context.Context, id a2atypes.ArtifactID, parts a2atypes.ContentParts, appendMode bool) {
	t, bs := PartsToContent(parts)
	seg, ok := o.byArtifact[id]
	if !ok {
		seg = &outputSeg{}
		o.byArtifact[id] = seg
		o.segs = append(o.segs, seg)
	}
	if appendMode || !ok {
		seg.text.WriteString(t)
		seg.blocks = append(seg.blocks, bs...)
		o.forward(ctx, t)
		return
	}
	o.replace(ctx, seg, t, bs)
}

// replace 把一段整段换成 t。新文本以旧文本开头、且这段是最后一段时，它就是追加：只转发多出来的那部分，不撤回——有的服务端
// 每个增量都用 append=false 重发累积的全文，逐个当替换处理的话宿主前端每个增量都撤一次、重画一次（宿主是 agui 时每次还
// 发一份重述整场对话的快照）。不是最后一段时只转发多出的那部分会接到后面几段的后面，照常撤回重发。
func (o *remoteOutput) replace(ctx context.Context, seg *outputSeg, t string, bs []*schema.ContentBlock) {
	old := seg.text.String()
	seg.blocks = bs
	if old == t {
		return
	}
	if len(o.segs) > 0 && o.segs[len(o.segs)-1] == seg && strings.HasPrefix(t, old) {
		seg.text.WriteString(t[len(old):])
		o.forward(ctx, t[len(old):])
		return
	}
	seg.text.Reset()
	seg.text.WriteString(t)
	o.retract(ctx)
	o.forward(ctx, o.String())
}

// forward 把一段文本作为 chunk 转发给宿主。
func (o *remoteOutput) forward(ctx context.Context, text string) {
	if text == "" {
		return
	}
	if sink := compose_v2.GetEventSink(ctx); sink != nil {
		sink.Send(compose_v2.Event{
			Type:     "chunk",
			Content:  text,
			Metadata: map[string]any{"agent": o.agentName, "source": "a2a_remote", "run_id": o.runID},
		})
		o.emitted = true
	}
}

// retract 撤回当前 run_id 下转发过的片段，之后的片段用新的 run_id。
func (o *remoteOutput) retract(ctx context.Context) {
	if o.emitted {
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			sink.Send(compose_v2.Event{
				Type:     "reset_stream",
				Content:  "remote agent replaced its streamed output",
				Metadata: map[string]any{"reason": "remote_artifact_replaced", "run_id": o.runID, "agent": o.agentName},
			})
		}
	}
	o.runID, o.emitted = newChunkRunID(), false
}

// String 是累积的文本，按段的到达顺序拼接。
func (o *remoteOutput) String() string {
	var b strings.Builder
	for _, s := range o.segs {
		b.WriteString(s.text.String())
	}
	return b.String()
}

// Blocks 是累积的非文本内容块。
func (o *remoteOutput) Blocks() []*schema.ContentBlock {
	var out []*schema.ContentBlock
	for _, s := range o.segs {
		out = append(out, s.blocks...)
	}
	return out
}

// newChunkRunID 铸一个转发片段用的 run_id（随机 UUID v4 形状）。
func newChunkRunID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read 在受支持的平台上不返回错误
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
