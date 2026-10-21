package server

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// TestInterceptor_NoFalseUnknownPayloadAlarm 钉住「不认识这个负载」的告警**不误报**。
//
// 这条抓到过一个真 bug：`requestTaskID` 原先返 `(TaskID, bool)`，`false` 同时表示「不认识」
// 和「认识但不指向 task」。于是**每次新建会话**（空 taskID 的 SendMessage，最常见的请求）
// 都打一条 ERROR。ERROR 级别的假警报比没有告警更糟——真警报被淹在噪声里。
//
// 修法是把 bool 换成三态（[taskIDLookup]）。按本仓「借来的量」判别式：`bool` 这个名字里
// 没有「认不认识」的意思，它只装得下两个答案而这里有三个。
func TestInterceptor_NoFalseUnknownPayloadAlarm(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	ci := conformanceInterceptor{ownerResolver: func(context.Context) string { return "o" }}
	cases := map[string]any{
		"新建会话(空taskID)": &a2atypes.SendMessageRequest{Message: &a2atypes.Message{}},
		"新建会话(nil消息)":   &a2atypes.SendMessageRequest{},
		"取card":         &a2atypes.GetExtendedAgentCardRequest{},
		"列任务":           &a2atypes.ListTasksRequest{},
	}
	for name, payload := range cases {
		buf.Reset()
		_, _, err := ci.Before(context.Background(), &a2asrv.CallContext{}, &a2asrv.Request{Payload: payload})
		if err != nil {
			t.Fatalf("%s: Before 报错 %v", name, err)
		}
		if bytes.Contains(buf.Bytes(), []byte("未知的入站负载类型")) {
			t.Errorf("【%s】打了 ERROR 告警——这是合法的常规请求", name)
		}
	}
}
