package agent_v2

// 白盒：streamReaderToSchema 未导出，且要构造「生产者 goroutine 被掐断」这种
// 接口实现者的内部行为，走 package agent_v2（规约见 testing-conventions.md）。

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	crew_modules_common "crew-go/modules/common"
)

// goexitStreamReader 第一次 Recv 就 runtime.Goexit——模拟上游 hook reader 内部
// 一句 logger.Fatal（zap.WithFatalHook 配置下）把桥接 goroutine 掐断。
type goexitStreamReader struct{}

func (r *goexitStreamReader) Recv(ctx context.Context) (*crew_modules_common.Message, error) {
	runtime.Goexit()
	return nil, io.EOF // unreachable
}

func (r *goexitStreamReader) Close() error { return nil }

// TestStreamBridge_GoexitClosesStream 守「生产者被掐断时消费方不永久阻塞」。
//
// # 它防的是什么
//
// streamReaderToSchema 起桥接 goroutine 往 out 里 pump。它的 recover-转-SendError
// 只兜 panic；`runtime.Goexit` 下 recover 返 nil、一个字都不发——修复前 out **永不
// 闭合**，消费方 `Recv` 永久阻塞（实测无缓冲 channel 上消费者卡死）。
//
// 修法是 eino schema.StreamReader 同款：**无条件 `defer CloseSend(out)`**，注册在
// 最前 ⟹ 跑在最后（错误先进流、流再闭合）。`CloseSend` 幂等（sendOnce），EOF 分支
// 的显式关闭不冲突。
//
// sanity-revert：把那行 `defer schema.CloseSend(out)` 去掉 → 本用例红在
// 「1 秒内没等到流终点」。
func TestStreamBridge_GoexitClosesStream(t *testing.T) {
	out := streamReaderToSchema(context.Background(), &goexitStreamReader{})

	type recvResult struct {
		err error
	}
	done := make(chan recvResult, 1)
	go func() {
		_, err := out.Recv(context.Background())
		done <- recvResult{err: err}
	}()

	select {
	case r := <-done:
		// **承重**：流有终点。EOF（干净闭合）或错误都行——不许无限阻塞。
		if r.err == nil {
			// 收到一条消息？生产者第一步就 Goexit，不该有消息——再收一次要终点
			select {
			case r2 := <-func() chan recvResult {
				c := make(chan recvResult, 1)
				go func() { _, e := out.Recv(context.Background()); c <- recvResult{err: e} }()
				return c
			}():
				if r2.err == nil {
					t.Fatal("生产者已被掐断，流里不该还有第二条消息")
				}
			case <-time.After(time.Second):
				t.Fatal("生产者被 Goexit 掐断后流没有终点：第二次 Recv 阻塞超 1 秒")
			}
			return
		}
		if !errors.Is(r.err, io.EOF) {
			t.Logf("流以错误闭合（可接受）：%v", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("生产者被 Goexit 掐断后流没有终点：消费方 Recv 阻塞超 1 秒\n" +
			"  （修复点：streamReaderToSchema 的无条件 defer CloseSend(out)）")
	}
}

// truncatingStreamReader 发 n 条 chunk 后 Goexit——模拟上游 hook reader 内部
// 一句 logger.Fatal（zap.WithFatalHook 配置）把桥接 goroutine 掐断。
type truncatingStreamReader struct {
	sent, n int
}

func (r *truncatingStreamReader) Recv(ctx context.Context) (*crew_modules_common.Message, error) {
	if r.sent >= r.n {
		runtime.Goexit()
	}
	r.sent++
	return &crew_modules_common.Message{Role: "assistant", Content: "chunk"}, nil
}

func (r *truncatingStreamReader) Close() error { return nil }

// TestStreamBridge_TruncationIsDistinguishableFromCleanEOF 守「截断不许冒充干净收尾」。
//
// # 它防的是什么
//
// 无条件闭流解决了「消费方永久阻塞」，但闭流本身**没有信息量**：`io.EOF` 在「上游正常
// 发完」和「生产者被 Goexit 掐断」下**一字不差**。于是半截 chunk 被聚合成 assistant
// 消息、当最终答复写进历史返给用户——一条静默的错答案。
//
// 修法与同函数 panic 路径一致（SendError 再闭流），判据是 `concluded` 旗。
//
// # 断言选在哪
//
// **收到 chunk 之后那一次 Recv 必须给出非 EOF 的错误**——这是消费方唯一能用来分辨
// 两种情况的信号。断言「不阻塞」不够（那是上一条哨兵守的维度，截断照样能通过）。
//
// sanity-revert：把 defer 里 `if !concluded { SendError }` 那支去掉 → 本用例红在
// 「截断与干净收尾不可区分」。
func TestStreamBridge_TruncationIsDistinguishableFromCleanEOF(t *testing.T) {
	ctx := context.Background()
	out := streamReaderToSchema(ctx, &truncatingStreamReader{n: 2})

	var got int
	var endErr error
	for i := 0; i < 8; i++ {
		msg, err := out.Recv(ctx)
		if err != nil {
			endErr = err
			break
		}
		if msg != nil {
			got++
		}
	}
	t.Logf("收到 %d 条 chunk，终止 err=%v", got, endErr)

	// **承重 1**：确实收到了部分内容（fixture 自检——否则下面断的不是截断场景）
	if got != 2 {
		t.Fatalf("该收到 2 条 chunk（fixture 没搭对），实际 %d", got)
	}
	// **承重 2**：流有终点（上一条哨兵的维度，这里顺带守住不回归）
	if endErr == nil {
		t.Fatal("流没有终点——消费方会永久阻塞")
	}
	// **承重 3（本条的要害）**：终止信号不能是干净 EOF，否则半截内容会被当完整输出
	if errors.Is(endErr, io.EOF) {
		t.Error("生产者被掐断，消费方收到的却是干净 io.EOF——与「上游正常发完」不可区分\n" +
			"  后果：半截 chunk 被聚合成 assistant 消息，当最终答复写进历史返给用户")
	}
}

// slowCloseReader：发 n 条 chunk 后 Goexit，且 Close() 里慢 50ms——把「生产者收尾」
// 拉长到足以让阻塞中的消费方分辨出 defer 的执行次序。
type slowCloseReader struct {
	sent, n int
}

func (r *slowCloseReader) Recv(ctx context.Context) (*crew_modules_common.Message, error) {
	if r.sent >= r.n {
		time.Sleep(20 * time.Millisecond) // 让消费方先阻塞在 Recv 上
		runtime.Goexit()
	}
	r.sent++
	return &crew_modules_common.Message{Role: "assistant", Content: "c"}, nil
}

func (r *slowCloseReader) Close() error {
	time.Sleep(50 * time.Millisecond) // 关连接慢——放大三道 defer 的次序差
	return nil
}

// TestStreamBridge_ErrorReachesBlockedConsumer 守三道 defer 的**注册次序**。
//
// # 为什么单独一条
//
// 另两条哨兵的消费方是「生产者跑完之后才去读」——那时 `errCh` 与 `ch` 都已就绪，
// SendError 和 CloseSend 谁先跑都读得到错。**生产里不是这样**：消费方正阻塞在
// `Recv` 上等下一个 chunk，这时哪个 defer 先跑**直接决定**它收到 EOF 还是收到错。
//
// 承重的是：`defer schema.CloseSend(out)` **注册在最前 ⟹ 跑在最后**（LIFO），
// 所以「错误先进流、流再闭合」。谁把这三道 defer 的注册位置挪一下（比如为了让
// 兜底最先注册），**修复就在主要场景下静默失效，而另两条哨兵照样绿**——独立审计
// 变异实测过这一点。这跟 async cp worker 那条 `defer wg.Done()` 是同一形状：
// **承诺了某个次序，却没有任何断言守它**。
//
// sanity-revert：把 `defer schema.CloseSend(out)` 改成最后注册（⟹ 最先跑）→
// 本用例红在「阻塞中的消费方收到 EOF」。
func TestStreamBridge_ErrorReachesBlockedConsumer(t *testing.T) {
	ctx := context.Background()
	out := streamReaderToSchema(ctx, &slowCloseReader{n: 2})

	// 先把 chunk 收完，让下一次 Recv 真正阻塞在「等生产者」上
	for i := 0; i < 2; i++ {
		if _, err := out.Recv(ctx); err != nil {
			t.Fatalf("第 %d 条 chunk 该正常收到，实际 err=%v", i+1, err)
		}
	}

	_, err := out.Recv(ctx) // 此刻生产者还在 Close() 里睡着——真·阻塞等待
	t.Logf("阻塞中的消费方拿到：%v", err)

	if err == nil {
		t.Fatal("该拿到一个结论")
	}
	// **承重**：阻塞中的消费方必须拿到错误而不是 EOF。次序反了 ⟹ 流先闭合 ⟹
	// 消费方立刻从 Recv 返回 EOF，之后投进 errCh 的错再没人看 ⟹ 半截当完整。
	if errors.Is(err, io.EOF) {
		t.Error("阻塞中的消费方收到 io.EOF：三道 defer 的次序被破坏了\n" +
			"  （CloseSend 必须注册在最前 ⟹ 跑在最后：错误先进流、流再闭合）")
	}
}
