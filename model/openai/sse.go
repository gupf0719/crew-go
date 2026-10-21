package openai

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

// maxSSELineBytes 是单行 SSE 的上限。provider 一般逐 token 发小块，但也有把一大段工具参数
// 塞进一个 chunk 的；bufio.Scanner 默认的 64KB 不够用。超过上限按读错误处理。
const maxSSELineBytes = 16 << 20

// sseEvent 是解码出来的一个 SSE 事件。
type sseEvent struct {
	// event 是 event 字段的值；没给时为空（规范里的默认类型 message）。
	event string
	// data 是这个事件各行 data 字段的值，按 \n 连接。
	data string
}

// sseDecoder 按 WHATWG HTML 规范里 Server-Sent Events 的解析规则，把响应体切成事件：
//   - 行以 CRLF、LF 或单独的 CR 结束；流开头的 UTF-8 BOM 去掉
//   - 冒号开头的行是注释（`: keep-alive`）
//   - 字段名与值以第一个冒号分开，值开头的一个空格去掉（`data:x` 与 `data: x` 等价）
//   - 同一个事件的多行 data 用 \n 连接；空行派发事件，没有 data 的事件不派发
//   - id、retry 等字段本客户端用不到，按规范忽略
//
// 跟规范有一处不同：流结束时最后一个事件后面没有空行，规范要求丢弃它，这里照常派发。
// provider 常把 finish_reason 放在最后一帧，丢掉它会把一次完整的输出误判成截断；而被截成
// 半截的那一帧解析 JSON 时照样报错，不会被当成完整内容放过去。
type sseDecoder struct {
	sc        *bufio.Scanner
	firstLine bool
	event     string
	data      strings.Builder
	hasData   bool
	// lastUnterminated 是最后切出的那一行有没有行尾：读出错时 Scanner 会把缓冲里剩下的半行
	// 也交出来，靠它分辨攒着的事件里有没有半截的行。
	lastUnterminated bool
}

func newSSEDecoder(r io.Reader) *sseDecoder {
	return newSSEDecoderWithMaxLine(r, maxSSELineBytes)
}

// newSSEDecoderWithMaxLine 同 newSSEDecoder，单行上限另给（测试用小上限造超长行）。
func newSSEDecoderWithMaxLine(r io.Reader, maxLine int) *sseDecoder {
	d := &sseDecoder{firstLine: true}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, min(4096, maxLine)), maxLine)
	sc.Split(d.scanLine)
	d.sc = sc
	return d
}

// scanLine 是 scanSSELine 外面一层，记下切出的这一行有没有行尾。
func (d *sseDecoder) scanLine(data []byte, atEOF bool) (int, []byte, error) {
	advance, token, err := scanSSELine(data, atEOF)
	if token != nil {
		d.lastUnterminated = advance == len(token)
	}
	return advance, token, err
}

// next 返回下一个事件。响应体读完时返回 io.EOF；读出错时返回那个错误。读出错时攒着的事件
// 如果每一行都完整收到了（只差派发它的空行），先照常交出、下一次再交错误——跟流正常结束时
// 照常派发最后一帧一致；里面有半截的行就丢弃，直接交错误。
//
// 行超过上限（bufio.ErrTooLong）时那一行一个字节都没交出来，而它属于攒着的那个事件（中间没有
// 空行）：这个事件缺了一行，同样丢弃。这时 lastUnterminated 还停在上一行上，不能拿它判。
func (d *sseDecoder) next() (sseEvent, error) {
	for d.sc.Scan() {
		line := d.sc.Text()
		if d.firstLine {
			d.firstLine = false
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		if line == "" {
			if ev, ok := d.dispatch(); ok {
				return ev, nil
			}
			continue
		}
		d.field(line)
	}
	if err := d.sc.Err(); err != nil {
		if !d.lastUnterminated && !errors.Is(err, bufio.ErrTooLong) {
			if ev, ok := d.dispatch(); ok {
				return ev, nil
			}
		}
		d.dispatch() // 丢弃带半截行的事件
		return sseEvent{}, err
	}
	if ev, ok := d.dispatch(); ok {
		return ev, nil
	}
	return sseEvent{}, io.EOF
}

// field 处理一行非空的字段行。
func (d *sseDecoder) field(line string) {
	if line[0] == ':' {
		return
	}
	name, value := line, ""
	if i := strings.IndexByte(line, ':'); i >= 0 {
		name, value = line[:i], strings.TrimPrefix(line[i+1:], " ")
	}
	switch name {
	case "event":
		d.event = value
	case "data":
		if d.hasData {
			d.data.WriteByte('\n')
		}
		d.data.WriteString(value)
		d.hasData = true
	}
}

// dispatch 取出当前攒着的事件并清空缓冲；没有 data 时返回 false（规范：不派发）。
func (d *sseDecoder) dispatch() (sseEvent, bool) {
	ev := sseEvent{event: d.event, data: d.data.String()}
	ok := d.hasData
	d.event = ""
	d.data.Reset()
	d.hasData = false
	return ev, ok
}

// scanSSELine 是 bufio.SplitFunc：按 CRLF、LF 或单独的 CR 切行。CR 落在已读数据的末尾时
// 先多读一点，看它后面是不是 LF，免得把一个 CRLF 当成两个行尾、凭空多出一个空行。
func scanSSELine(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	i := bytes.IndexAny(data, "\r\n")
	if i < 0 {
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	}
	if data[i] == '\n' {
		return i + 1, data[:i], nil
	}
	if i+1 < len(data) {
		if data[i+1] == '\n' {
			return i + 2, data[:i], nil
		}
		return i + 1, data[:i], nil
	}
	if atEOF {
		return i + 1, data[:i], nil
	}
	return 0, nil, nil
}
