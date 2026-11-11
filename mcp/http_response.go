package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func readHTTPResponse(r io.Reader, contentType, id string) (rpcResponse, error) {
	if contentType == "text/event-stream" {
		return readSSEReply(r, id)
	}
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

func readSSEReply(r io.Reader, id string) (rpcResponse, error) {
	limited := &io.LimitedReader{R: r, N: maxResponseBytes + 1}
	scan := bufio.NewScanner(limited)
	scan.Buffer(make([]byte, 4096), maxResponseBytes+1)
	var data bytes.Buffer
	for scan.Scan() {
		line := scan.Text()
		if line == "" {
			if data.Len() == 0 {
				continue
			}
			reply, found, err := findRPCReply(data.Bytes(), id)
			if err != nil {
				return rpcResponse{}, &badResponseError{fmt.Errorf("decode SSE data: %w", err)}
			}
			if found {
				return reply, nil
			}
			data.Reset()
			continue
		}
		if field, value, _ := strings.Cut(line, ":"); field == "data" {
			data.WriteString(strings.TrimPrefix(value, " "))
			data.WriteByte('\n')
		}
	}
	if limited.N == 0 {
		return rpcResponse{}, oversizedHTTPReply()
	}
	if err := scan.Err(); err != nil {
		return rpcResponse{}, fmt.Errorf("read SSE response: %w", err)
	}
	return rpcResponse{}, fmt.Errorf("SSE closed before request %s received a response: %w", id, io.ErrUnexpectedEOF)
}

func findRPCReply(raw []byte, id string) (rpcResponse, bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return rpcResponse{}, false, nil
	}
	messages := []json.RawMessage{raw}
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &messages); err != nil {
			return rpcResponse{}, false, err
		}
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
