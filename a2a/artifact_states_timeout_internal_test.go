package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// 问人那一刻取 artifact 状态时远端卡住不回：等到 artifactStatesTimeout 就放弃（退化成取回时不扣除），不一直挂到调用方的
// 期限——问人这一步不该被一个卡住的 GetTask 拖住。
func TestArtifactStates_HangingGetTaskGivesUpAtTimeout(t *testing.T) {
	old := artifactStatesTimeout
	artifactStatesTimeout = 100 * time.Millisecond
	t.Cleanup(func() { artifactStatesTimeout = old })

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	card := &a2atypes.AgentCard{
		Name: "Hanging Agent", Description: "GetTask 卡住",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(srv.URL+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
	}
	ra, err := NewRemoteAgentFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	seen := ra.artifactStates(ctx, "task-1")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("GetTask 卡住时应在 %v 左右放弃，实际等了 %v（挂到了调用方期限）", artifactStatesTimeout, elapsed)
	}
	if seen != nil {
		t.Fatalf("取不到状态时应返回 nil，实际 %+v", seen)
	}
}

// 取 artifact 状态的时限不短于等远端状态的另一处上限：取不到时授权后重新挂上（正常流程）就不扣除，答复里静默带上授权前的内容，
// 慢但正常的远端不该落进这一档（曾经是 5 秒，审计实测慢 6 秒的远端就中）。
func TestArtifactStatesTimeout_NotShorterThanAuthGrace(t *testing.T) {
	if artifactStatesTimeout < authReattachGrace {
		t.Fatalf("artifactStatesTimeout=%v 短于 authReattachGrace=%v", artifactStatesTimeout, authReattachGrace)
	}
}

// methodNotFoundRemote 起一个对任何 JSON-RPC 调用都回 -32601 的远端，记下收到的请求体。
func methodNotFoundRemote(t *testing.T, bodies *[]string) *RemoteAgent {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读请求体：%v", err)
		}
		*bodies = append(*bodies, string(raw))
		var rpc struct {
			ID any `json:"id"`
		}
		if err := json.Unmarshal(raw, &rpc); err != nil {
			t.Errorf("解析请求：%v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
			"error": map[string]any{"code": -32601, "message": "method not found"}}); err != nil {
			t.Errorf("写响应：%v", err)
		}
	}))
	t.Cleanup(srv.Close)
	card := &a2atypes.AgentCard{
		Name: "No GetTask", Description: "不支持 GetTask",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(srv.URL+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
	}
	ra, err := NewRemoteAgentFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	return ra
}

// 问人那一刻只要 artifact：请求不带历史（historyLength 0，规范定为「不返回历史」），少拉一半载荷。
func TestArtifactStates_RequestsNoHistory(t *testing.T) {
	var bodies []string
	ra := methodNotFoundRemote(t, &bodies)
	ra.artifactStates(context.Background(), "task-1")
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"historyLength":0`) {
		t.Fatalf("问人时的 GetTask 应带 historyLength 0，实际请求 %q", bodies)
	}
}

// 远端不支持 GetTask 时没有可扣的依据，不是故障：不告警（曾经每次问人都多一条 WARN）。
func TestArtifactStates_NoWarnWhenGetTaskUnsupported(t *testing.T) {
	var sb bytes.Buffer
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	var bodies []string
	ra := methodNotFoundRemote(t, &bodies)
	if seen := ra.artifactStates(context.Background(), "task-1"); seen != nil || len(bodies) != 1 {
		t.Fatalf("fixture 没搭对：seen=%+v 请求 %d 次", seen, len(bodies))
	}
	if strings.Contains(sb.String(), "artifact") {
		t.Fatalf("远端不支持 GetTask 时不该告警，实际日志：%s", sb.String())
	}
}
