package a2a

import (
	"context"
	"strings"
	"testing"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"

	"crew-go/compose_v2"
)

func textParts(s string) a2atypes.ContentParts { return a2atypes.ContentParts{a2atypes.NewTextPart(s)} }

// 重新挂上任务时第一个事件是带已有 artifacts 的任务快照，之后同 ID 的 append=false 是替换：结果只剩替换后的那份。
func TestRemoteOutput_TaskSnapshotThenReplace(t *testing.T) {
	o := newRemoteOutput("r")
	ctx := context.Background()
	o.taskArtifact(ctx, "X", textParts("partial-"))
	o.artifactUpdate(ctx, "X", textParts("full"), false)
	if got := o.String(); got != "full" {
		t.Fatalf("快照之后的 append=false 应整段替换，实际 %q", got)
	}
}

// 快照之后同 ID 的追加接在快照内容后面；另一个 artifact 各自成段、顺序不变。
func TestRemoteOutput_TaskSnapshotThenAppend(t *testing.T) {
	o := newRemoteOutput("r")
	ctx := context.Background()
	o.taskArtifact(ctx, "X", textParts("ab"))
	o.artifactUpdate(ctx, "Y", textParts("1"), false)
	o.artifactUpdate(ctx, "X", textParts("c"), true)
	o.taskArtifact(ctx, "X", textParts("abc")) // 内容没变的快照不动
	if got := o.String(); got != "abc1" {
		t.Fatalf("应是 X 段 abc 加 Y 段 1，实际 %q", got)
	}
}

// recordingEvents 收 forward / retract 发给宿主的事件。
type recordingEvents struct{ evs []compose_v2.Event }

func (r *recordingEvents) Send(ev compose_v2.Event) { r.evs = append(r.evs, ev) }

// 服务端每个增量都用 append=false 重发累积的全文：按追加处理，宿主只收到多出来的那部分、不撤回（曾经每个增量都撤一次、
// 重发一遍全文）。新文本不以旧文本开头时仍是替换。
func TestRemoteOutput_CumulativeReplaceIsAppend(t *testing.T) {
	rec := &recordingEvents{}
	ctx := compose_v2.WithEventSink(context.Background(), rec)
	o := newRemoteOutput("r")
	o.artifactUpdate(ctx, "X", textParts("a"), false)
	o.artifactUpdate(ctx, "X", textParts("ab"), false)
	o.artifactUpdate(ctx, "X", textParts("abc"), false)
	var chunks strings.Builder
	for _, ev := range rec.evs {
		if ev.Type == "reset_stream" {
			t.Fatalf("累积全文的替换不该撤回：%+v", rec.evs)
		}
		chunks.WriteString(ev.Content)
	}
	if o.String() != "abc" || chunks.String() != "abc" {
		t.Fatalf("宿主应收到 abc（不重复），实际累积 %q、转发 %q", o.String(), chunks.String())
	}
	o.artifactUpdate(ctx, "X", textParts("xyz"), false)
	if last := rec.evs[len(rec.evs)-2]; last.Type != "reset_stream" || o.String() != "xyz" {
		t.Fatalf("新文本不以旧文本开头时应撤回重发，实际 %+v", rec.evs)
	}
}

// 被替换的不是最后一段时，即便是前缀扩展也撤回重发：只转发多出的部分会接到后面那段的后面。
func TestRemoteOutput_PrefixReplaceOfEarlierSegment(t *testing.T) {
	rec := &recordingEvents{}
	ctx := compose_v2.WithEventSink(context.Background(), rec)
	o := newRemoteOutput("r")
	o.artifactUpdate(ctx, "X", textParts("a"), false)
	o.artifactUpdate(ctx, "Y", textParts("1"), false)
	o.artifactUpdate(ctx, "X", textParts("ab"), false)
	resets := 0
	for _, ev := range rec.evs {
		if ev.Type == "reset_stream" {
			resets++
		}
	}
	if resets != 1 || o.String() != "ab1" || rec.evs[len(rec.evs)-1].Content != "ab1" {
		t.Fatalf("应撤回一次并重发 ab1，实际 %q %+v", o.String(), rec.evs)
	}
}
