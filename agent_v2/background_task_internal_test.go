package agent_v2

import (
	"fmt"
	"testing"
	"time"

	"crew-go/schema"
)

func terminalTask(id, conv, agent string, endedAt time.Time) *bgTask {
	done := make(chan struct{})
	close(done)
	return &bgTask{id: id, toolName: "build", conv: conv, agent: agent, admitted: true, returned: true,
		status: BackgroundTaskCompleted, text: "R-" + id, startedAt: endedAt, endedAt: endedAt, done: done}
}

func insertTasks(m *BackgroundTaskManager, ts ...*bgTask) {
	for _, t := range ts {
		m.tasks[t.id] = t
		m.byConv[t.conv] = append(m.byConv[t.conv], t)
	}
}

// TestBackgroundNotes_DeliveredOnlyWhenSeenInCommittedStream：通知在已提交的消息流里被看到才算送达——那一轮没成
// （消息流里没有它）就一直再发；看到之后不再发，跟哪个任务、哪个 session 对象无关；别的 agent 收不到。
func TestBackgroundNotes_DeliveredOnlyWhenSeenInCommittedStream(t *testing.T) {
	m := NewBackgroundTaskManager(nil)
	sc := &backgroundScope{cfg: &BackgroundTasksConfig{Manager: m}, conv: "o/c", agent: "a", task: "T"}
	insertTasks(m, terminalTask("bg_1", sc.conv, "a", time.Now()))

	notes := m.notes(sc, nil)
	if len(notes) != 1 {
		t.Fatalf("没送达过的应发 1 条，got %d", len(notes))
	}
	if again := m.notes(sc, nil); len(again) != 1 {
		t.Fatalf("送进去的那一轮没提交（流里没有它）时应再发，got %d", len(again))
	}
	other := *sc
	other.agent = "b"
	if got := m.notes(&other, nil); len(got) != 0 {
		t.Fatalf("别的 agent 不该收到，got %d", len(got))
	}
	m.seen(sc, []*schema.Message{schema.UserMessage("无关"), notes[0]})
	if again := m.notes(sc, nil); len(again) != 0 {
		t.Fatalf("在已提交的流里看到之后不该再发，got %d", len(again))
	}
	newTask := *sc
	newTask.task = "T2"
	if again := m.notes(&newTask, nil); len(again) != 0 {
		t.Fatalf("送达之后换了任务（同一 RunID 重跑、新 session 对象）也不该再发，got %d", len(again))
	}
}

// TestBackgroundTask_ExpiredAndExcessTasksSwept：过了保留期的、超出每个会话保留个数的结束了的任务被清掉，在跑的
// 不动；不再有动静的会话里的过期任务由别的会话的动静顺手清掉。
func TestBackgroundTask_ExpiredAndExcessTasksSwept(t *testing.T) {
	m := NewBackgroundTaskManager(&BackgroundTaskManagerConfig{Retention: time.Minute})
	now := time.Now()
	old := terminalTask("bg_old", "o/c", "a", now.Add(-2*time.Minute))
	fresh := terminalTask("bg_fresh", "o/c", "a", now)
	running := &bgTask{id: "bg_run", conv: "o/c", agent: "a", admitted: true, status: BackgroundTaskRunning, startedAt: now.Add(-time.Hour), done: make(chan struct{})}
	insertTasks(m, old, fresh, running)
	m.lookup("o/c", "bg_fresh")
	if _, ok := m.tasks["bg_old"]; ok {
		t.Fatal("过了保留期的任务没被清掉")
	}
	if _, ok := m.tasks["bg_fresh"]; !ok {
		t.Fatal("没过期的任务被清掉了")
	}
	if _, ok := m.tasks["bg_run"]; !ok {
		t.Fatal("在跑的任务被清掉了")
	}

	idle := terminalTask("bg_idle", "o/idle", "a", now.Add(-2*time.Minute))
	insertTasks(m, idle)
	m.lastSweep = time.Time{}
	pending := &bgTask{id: "bg_new", conv: "o/other", done: make(chan struct{})}
	if ok, _ := m.admit(pending); !ok {
		t.Fatal("fixture 没搭对：应能转入后台")
	}
	if _, ok := m.tasks["bg_idle"]; ok {
		t.Fatal("不再有动静的会话里过期的任务没被顺手清掉")
	}

	m2 := NewBackgroundTaskManager(nil)
	for i := 0; i < backgroundMaxRetainedPerConv+5; i++ {
		insertTasks(m2, terminalTask(fmt.Sprintf("bg_%03d", i), "o/full", "a", now.Add(time.Duration(i)*time.Millisecond)))
	}
	m2.lookup("o/full", "bg_000")
	if n := len(m2.byConv["o/full"]); n != backgroundMaxRetainedPerConv {
		t.Fatalf("一个会话最多留 %d 个结束了的任务，留了 %d 个", backgroundMaxRetainedPerConv, n)
	}
	if _, ok := m2.tasks["bg_000"]; ok {
		t.Fatal("超出保留个数时应先丢最早的")
	}
}
