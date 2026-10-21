package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a/server"
	"crew-go/agent_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// naiveStore 是「不认识 owner 的朴素后端」——业务换持久后端（PG / Redis）时最常见的
// 形态：一张按 taskID 索引的表，Get 按 ID 查、List 返回全部。
//
// 它正是 [server.WithTaskStore] 的目标形态：SDK 自带的 InMemory 有 Authenticator、
// 会按 user 分区，而换掉它的整个用途就是不用它。
type naiveStore struct {
	mu    sync.Mutex
	tasks map[a2atypes.TaskID]*taskstore.StoredTask
}

func newNaiveStore() *naiveStore {
	return &naiveStore{tasks: map[a2atypes.TaskID]*taskstore.StoredTask{}}
}

func (s *naiveStore) Create(_ context.Context, task *a2atypes.Task) (taskstore.TaskVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[task.ID]; ok {
		return taskstore.TaskVersionMissing, taskstore.ErrTaskAlreadyExists
	}
	s.tasks[task.ID] = &taskstore.StoredTask{Task: task, Version: 1}
	return 1, nil
}

func (s *naiveStore) Update(_ context.Context, req *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok := s.tasks[req.Task.ID]
	if !ok {
		return taskstore.TaskVersionMissing, a2atypes.ErrTaskNotFound
	}
	v := prev.Version + 1
	s.tasks[req.Task.ID] = &taskstore.StoredTask{Task: req.Task, Version: v}
	return v, nil
}

func (s *naiveStore) Get(_ context.Context, id a2atypes.TaskID) (*taskstore.StoredTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.tasks[id]
	if !ok {
		return nil, a2atypes.ErrTaskNotFound
	}
	return st, nil
}

// List 按 taskID 升序全量返回（分页照 PageSize 切，token 就是「下一个 ID」）。
func (s *naiveStore) List(_ context.Context, req *a2atypes.ListTasksRequest) (*a2atypes.ListTasksResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	if req.PageToken != "" {
		for len(ids) > 0 && ids[0] <= req.PageToken {
			ids = ids[1:]
		}
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 50
	}
	total := len(s.tasks)
	next := ""
	if len(ids) > pageSize {
		next = ids[pageSize-1]
		ids = ids[:pageSize]
	}
	out := make([]*a2atypes.Task, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.tasks[a2atypes.TaskID(id)].Task)
	}
	return &a2atypes.ListTasksResponse{Tasks: out, TotalSize: total, PageSize: pageSize, NextPageToken: next}, nil
}

var _ taskstore.Store = (*naiveStore)(nil)

// serveOn 在给定 store 上起一个 server 实例（可多次调用共用同一个 store 模拟横向扩容）。
func serveOn(t *testing.T, ag agent_v2.Agent, st taskstore.Store) func(string) *a2aclient.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	base := "http://" + ln.Addr().String()
	h := server.Handler(ag, base,
		server.WithOwnerResolver(func(ctx context.Context) string {
			o, _ := ctx.Value(probeWhoKey{}).(string)
			return o
		}),
		server.WithTaskStore(st))
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		who := r.Header.Get("X-Tenant")
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), probeWhoKey{}, who)))
	})
	go func() { _ = http.Serve(ln, wrapped) }()
	t.Cleanup(func() { _ = ln.Close() })
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(base + "/.well-known/agent-card.json"); err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return func(tenant string) *a2aclient.Client {
		c, err := a2aclient.NewFromCard(context.Background(), server.Card(ag, base),
			a2aclient.WithJSONRPCTransport(&http.Client{Transport: &tenantRT{tenant: tenant}}))
		if err != nil {
			t.Fatalf("client(%s): %v", tenant, err)
		}
		return c
	}
}

// sendAndGetTaskID 让某租户发一条消息（asker agent 会停在 input-required），返回 taskID。
func sendAndGetTaskID(t *testing.T, c *a2aclient.Client, text string) a2atypes.TaskID {
	t.Helper()
	var tid a2atypes.TaskID
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart(text))
	for ev, err := range c.SendStreamingMessage(context.Background(), &a2atypes.SendMessageRequest{Message: msg}) {
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			tid = v.ID
		case *a2atypes.TaskStatusUpdateEvent:
			tid = v.TaskID
		}
	}
	if tid == "" {
		t.Fatal("没拿到 taskID")
	}
	return tid
}

// taskText 把一个 Task 里所有可读文本（history + status message）拼起来。
func taskText(tk *a2atypes.Task) string {
	var sb strings.Builder
	for _, h := range tk.History {
		txt, _ := partsText(h.Parts)
		sb.WriteString(txt)
		sb.WriteString(" | ")
	}
	if tk.Status.Message != nil {
		txt, _ := partsText(tk.Status.Message.Parts)
		sb.WriteString(txt)
	}
	return sb.String()
}

// ---- 缺口 1：List 裸透传 ----

func TestPersistentStore_ListDoesNotLeak(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	mk := serveOn(t, ag, newNaiveStore())
	ca, cb := mk("tenant-A"), mk("tenant-B")

	sendAndGetTaskID(t, ca, "A 的机密请求")

	resp, err := cb.ListTasks(context.Background(), &a2atypes.ListTasksRequest{})
	if err != nil {
		t.Logf("[list] B 被拒: %v", err)
		return
	}
	var all strings.Builder
	for _, tk := range resp.Tasks {
		all.WriteString(taskText(tk))
	}
	t.Logf("[list] B 拿到 %d 个 task, totalSize=%d, 内容=%q", len(resp.Tasks), resp.TotalSize, all.String())
	if strings.Contains(all.String(), "A 的机密请求") || strings.Contains(all.String(), "你的密码是什么") {
		t.Errorf("!!! 泄漏：B 经 tasks/list 读到 A 的内容")
	}
	if len(resp.Tasks) != 0 {
		t.Errorf("!!! B 名下没有 task，却列出了 %d 个", len(resp.Tasks))
	}
}

// 正向：A 自己列得到自己的（否则「整个坏掉」也能让上面那条绿）。
func TestPersistentStore_ListOwnStillWorks(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	mk := serveOn(t, ag, newNaiveStore())
	ca := mk("tenant-A")

	sendAndGetTaskID(t, ca, "A 的机密请求")

	resp, err := ca.ListTasks(context.Background(), &a2atypes.ListTasksRequest{})
	if err != nil {
		t.Fatalf("A 列自己的 task: %v", err)
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("A 应看到自己那 1 个 task，实际 %d 个", len(resp.Tasks))
	}
	if !strings.Contains(taskText(resp.Tasks[0]), "A 的机密请求") {
		t.Errorf("A 列出来的 task 没有自己的内容: %q", taskText(resp.Tasks[0]))
	}
}

// ---- 缺口 2：多实例归属表各记各的 ----

func TestPersistentStore_MultiInstanceOwnership(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	shared := newNaiveStore()
	mk1 := serveOn(t, ag, shared) // 实例 1
	mk2 := serveOn(t, ag, shared) // 实例 2（同一份持久后端）

	tid := sendAndGetTaskID(t, mk1("tenant-A"), "A 的机密请求")

	// 对照组：同一个实例上 B 读 → 应被拒
	if got, err := mk1("tenant-B").GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: tid}); err == nil {
		t.Errorf("!!! 实例 1 上 B 就读到了 A 的 task: %q", taskText(got))
	} else {
		t.Logf("[实例1] B 被拒: %v", err)
	}

	// 实测组：换一个实例
	got, err := mk2("tenant-B").GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: tid})
	if err == nil {
		t.Errorf("!!! 实例 2 上 B 读到了 A 的 task: %q", taskText(got))
	} else {
		t.Logf("[实例2] B 被拒: %v", err)
	}

	// tasks/list 同样得跨实例守住——它跟 tasks/get 走的是不同的下游。
	resp, err := mk2("tenant-B").ListTasks(context.Background(), &a2atypes.ListTasksRequest{})
	if err != nil {
		t.Logf("[实例2/list] B 被拒: %v", err)
		return
	}
	var all strings.Builder
	for _, tk := range resp.Tasks {
		all.WriteString(taskText(tk))
	}
	if len(resp.Tasks) != 0 || strings.Contains(all.String(), "A 的机密请求") {
		t.Errorf("!!! 实例 2 上 B 经 tasks/list 拿到 %d 个 task: %q", len(resp.Tasks), all.String())
	}
}

// TestPersistentStore_OwnerStampIsVisibleOnTheWire 钉住归属戳**对 task 的主人可见**。
//
// 这是有意的，不是疏忽：戳要留在返回的 Task 上，后续 `Update` 才能靠它认归属而不必再
// 读一次后端。想在读出口把它抹掉的话，抹的是**内层返回的那个对象**——SDK 自带的
// InMemory 每次 Get 都深拷贝无所谓，业务后端很可能直接返内部持有的结构，抹一次就把
// 后端里那条记录**永久去了戳**（然后 fail-closed 让它谁都读不到）。
//
// 代价是主人在自己的 task metadata 里会看到一个 `crew.` 前缀的键——那是他自己的
// 身份，不是别人的。要改这个决定，先解决上面那个「抹谁的对象」的问题。
func TestPersistentStore_OwnerStampIsVisibleOnTheWire(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	mk := serveOn(t, ag, newNaiveStore())
	ca := mk("tenant-A")

	tid := sendAndGetTaskID(t, ca, "A 的机密请求")
	got, err := ca.GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: tid})
	if err != nil {
		t.Fatalf("A 读自己的 task: %v", err)
	}
	v, ok := got.Metadata["crew.task.ownerDigest"]
	if !ok {
		t.Fatalf("返回的 Task 上没有归属戳，metadata=%v", got.Metadata)
	}
	// 戳是**摘要**不是明文——它会跟着 task 回到客户端、进业务持久后端、进推送通知的
	// body，而这一层需要的全部能力只是「比相等」，没有理由把身份本身发出去。
	if v == "tenant-A" {
		t.Error("归属戳是明文 owner——它随 task 上 wire，暴露面没必要扩大")
	}
	sum := sha256.Sum256([]byte("tenant-A"))
	if v != hex.EncodeToString(sum[:]) {
		t.Errorf("归属戳 %v 不是 tenant-A 的摘要", v)
	}
}

// 正向：A 自己在另一个实例上读得到自己的 task（横向扩容的正当用途）。
func TestPersistentStore_MultiInstanceOwnerStillReads(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	shared := newNaiveStore()
	mk1 := serveOn(t, ag, shared)
	mk2 := serveOn(t, ag, shared)

	tid := sendAndGetTaskID(t, mk1("tenant-A"), "A 的机密请求")

	got, err := mk2("tenant-A").GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: tid})
	if err != nil {
		t.Fatalf("A 在实例 2 上读自己的 task 失败: %v", err)
	}
	if !strings.Contains(taskText(got), "A 的机密请求") {
		t.Errorf("A 在实例 2 上读到的 task 没有自己的内容: %q", taskText(got))
	}
}

// ---- 缺口 3：拒绝 vs 不存在，wire 上可区分 ----

func TestPersistentStore_RejectIndistinguishableFromMissing(t *testing.T) {
	ag := newAsker(t, "你的密码是什么")
	mk := serveOn(t, ag, newNaiveStore())
	ca, cb := mk("tenant-A"), mk("tenant-B")

	tid := sendAndGetTaskID(t, ca, "A 的机密请求")

	_, errReject := cb.GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: tid})
	_, errMissing := cb.GetTask(context.Background(), &a2atypes.GetTaskRequest{ID: "task-that-never-existed"})
	if errReject == nil {
		t.Fatal("B 居然读到了 A 的 task")
	}
	if errMissing == nil {
		t.Fatal("不存在的 task 居然读到了")
	}
	t.Logf("拒绝  : %q", errReject.Error())
	t.Logf("不存在: %q", errMissing.Error())
	if errReject.Error() != errMissing.Error() {
		t.Errorf("!!! 存在性预言机：两条路径的错误正文不同\n拒绝  : %q\n不存在: %q", errReject.Error(), errMissing.Error())
	}
}
