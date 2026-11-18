package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
)

func readJSONReply(r io.Reader, id string) (rpcResponse, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxResponseBytes+1))
	if err != nil {
		return rpcResponse{}, fmt.Errorf("read response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return rpcResponse{}, oversizedHTTPReply()
	}
	reply, found, err := findRPCReply(body, id)
	if err != nil {
		return rpcResponse{}, &badResponseError{fmt.Errorf("decode envelope: %w", err)}
	}
	if !found {
		return rpcResponse{}, &badResponseError{fmt.Errorf("no response for request %s", id)}
	}
	return reply, nil
}

type sseCursor struct {
	id    string
	retry time.Duration
}

type sseRPCMessage struct {
	rpcResponse
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// sseReadError 表示传输中断。解码失败或回调终止不会触发重连。
type sseReadError struct{ error }

func (e *sseReadError) Unwrap() error { return e.error }

func (s *sseCursor) read(ctx context.Context, r io.Reader, dispatch func(sseRPCMessage) (bool, error)) (bool, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), maxResponseBytes+1)
	scan.Split(sseLines())
	var data bytes.Buffer
	id, kind := s.id, ""
	first := true
	for scan.Scan() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		line := scan.Bytes()
		if first {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			first = false
		}
		if len(line) == 0 {
			if kind == "" || kind == "message" {
				done, err := dispatchSSEMessages(ctx, data.Bytes(), dispatch)
				if err != nil {
					return false, err
				}
				if done {
					return true, nil
				}
			}
			// 空 data 的引导事件也能提交位置。断在空行之前的帧由服务端重放。
			s.id, kind = id, ""
			data.Reset()
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "data":
			if data.Len()+len(value)+1 > maxResponseBytes {
				return false, oversizedHTTPReply()
			}
			data.Write(value)
			data.WriteByte('\n')
		case "id":
			if bytes.IndexByte(value, 0) < 0 {
				id = string(value)
			}
		case "event":
			kind = string(value)
		case "retry":
			if millis, err := strconv.ParseUint(string(value), 10, 64); err == nil {
				s.retry = time.Duration(min(millis, uint64(math.MaxInt64/int64(time.Millisecond)))) * time.Millisecond
				s.retry = max(s.retry, 25*time.Millisecond)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := scan.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return false, oversizedHTTPReply()
		}
		return false, &sseReadError{err}
	}
	return false, &sseReadError{io.ErrUnexpectedEOF}
}

// sseLines 把 CR 立即视为换行，随后跳过可能跨读的 LF。
func sseLines() bufio.SplitFunc {
	var afterCR bool
	return func(data []byte, _ bool) (int, []byte, error) {
		offset := 0
		if afterCR && len(data) > 0 && data[0] == '\n' {
			offset = 1
		}
		afterCR = false
		data = data[offset:]
		i := bytes.IndexAny(data, "\r\n")
		if i < 0 {
			return offset, nil, nil // EOF 前没有换行的尾部是未完成的字段。
		}
		afterCR = data[i] == '\r'
		return offset + i + 1, data[:i], nil
	}
}

func dispatchSSEMessages(ctx context.Context, raw []byte, dispatch func(sseRPCMessage) (bool, error)) (bool, error) {
	messages, err := rpcMessages(raw)
	if err != nil {
		return false, &badResponseError{fmt.Errorf("decode SSE data: %w", err)}
	}
	for _, message := range messages {
		var msg sseRPCMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return false, &badResponseError{fmt.Errorf("decode SSE message: %w", err)}
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if done, err := dispatch(msg); err != nil || done {
			return done, err
		}
	}
	return false, nil
}

func rpcMessages(raw []byte) ([]json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, nil
	}
	messages := []json.RawMessage{raw}
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &messages); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func findRPCReply(raw []byte, id string) (rpcResponse, bool, error) {
	messages, err := rpcMessages(raw)
	if err != nil {
		return rpcResponse{}, false, err
	}
	for _, message := range messages {
		var reply rpcResponse
		if err := json.Unmarshal(message, &reply); err != nil {
			return rpcResponse{}, false, err
		}
		if responseIDKey(reply.ID) == id && (reply.Result != nil || reply.Error != nil) {
			return reply, true, nil
		}
	}
	return rpcResponse{}, false, nil
}

func oversizedHTTPReply() error {
	return &badResponseError{fmt.Errorf("response exceeds %d bytes", maxResponseBytes)}
}
