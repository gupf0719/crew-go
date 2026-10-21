package openai

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// decodeAll 把 body 解码成事件列表；oneByte 为 true 时每次 Read 只给一个字节，模拟网络把
// 数据切得很碎的情形。
func decodeAll(t *testing.T, body string, oneByte bool) []sseEvent {
	t.Helper()
	var r io.Reader = strings.NewReader(body)
	if oneByte {
		r = iotest.OneByteReader(r)
	}
	dec := newSSEDecoder(r)
	var out []sseEvent
	for {
		ev, err := dec.next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("解码出错：%v", err)
		}
		out = append(out, ev)
	}
}

// CRLF 被拆在两次读之间时，不能把 CR 当成一个行尾、LF 当成另一个——那样会凭空多出一个
// 空行，把同一个事件的两行 data 拆成两个事件。
func TestSSEDecoder_CRLFSplitAcrossReads(t *testing.T) {
	body := "data: a\r\ndata: b\r\n\r\nevent: error\r\ndata: c\r\n\r\n"
	for _, oneByte := range []bool{false, true} {
		got := decodeAll(t, body, oneByte)
		if len(got) != 2 || got[0].data != "a\nb" || got[0].event != "" || got[1].event != "error" || got[1].data != "c" {
			t.Fatalf("oneByte=%v：事件 = %+v，want [{data:a\\nb} {event:error data:c}]", oneByte, got)
		}
	}
}

// 规范：没有 data 的事件不派发；注释行与未知字段忽略；event 只作用于它所在的那个事件。
func TestSSEDecoder_EventBoundaries(t *testing.T) {
	body := ": comment\n\nevent: ping\n\nevent: error\nid: 7\nfoo: bar\ndata: x\n\ndata: y\n\n"
	got := decodeAll(t, body, false)
	if len(got) != 2 || got[0].event != "error" || got[0].data != "x" || got[1].event != "" || got[1].data != "y" {
		t.Fatalf("事件 = %+v", got)
	}
}

// 读出错时攒着的事件每一行都完整：先交出事件，下一次再交错误。
func TestSSEDecoder_ReadErrorKeepsCompleteLines(t *testing.T) {
	boom := errors.New("boom")
	r := io.MultiReader(strings.NewReader("data: done\n\ndata: full\n"), iotest.ErrReader(boom))
	dec := newSSEDecoder(r)
	if ev, err := dec.next(); err != nil || ev.data != "done" {
		t.Fatalf("第一个事件 = %+v err %v", ev, err)
	}
	if ev, err := dec.next(); err != nil || ev.data != "full" {
		t.Fatalf("每一行都完整收到的事件应照常交出，实际 %+v err %v", ev, err)
	}
	if _, err := dec.next(); !errors.Is(err, boom) {
		t.Fatalf("之后应交出读错误，实际 %v", err)
	}
}

// 读出错时攒着的事件里有半截的行：丢弃，错误原样交出。
func TestSSEDecoder_ReadErrorDropsPendingEvent(t *testing.T) {
	boom := errors.New("boom")
	r := io.MultiReader(strings.NewReader("data: done\n\ndata: half"), iotest.ErrReader(boom))
	dec := newSSEDecoder(r)
	ev, err := dec.next()
	if err != nil || ev.data != "done" {
		t.Fatalf("第一个事件 = %+v err %v", ev, err)
	}
	if _, err := dec.next(); !errors.Is(err, boom) {
		t.Fatalf("读出错应原样交出，实际 %v", err)
	}
}

// 行超过上限时那一行没交出来，而它属于攒着的那个事件：这个事件缺了一行，丢弃，交出 ErrTooLong。
// 这时最后交出的那一行是完整的，按「最后一行有没有行尾」判会把缺行的事件当完整的交出去。
func TestSSEDecoder_TooLongLineDropsPendingEvent(t *testing.T) {
	body := "data: done\n\ndata: a\ndata: b\ndata: " + strings.Repeat("x", 100) + "\n\n"
	dec := newSSEDecoderWithMaxLine(strings.NewReader(body), 32)
	if ev, err := dec.next(); err != nil || ev.data != "done" {
		t.Fatalf("第一个事件 = %+v err %v", ev, err)
	}
	ev, err := dec.next()
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("缺了一行的事件不该交出，应交出 ErrTooLong，实际事件 %+v err %v", ev, err)
	}
}
