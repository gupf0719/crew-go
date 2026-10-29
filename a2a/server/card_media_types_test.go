package server_test

// Card 的输入模式是媒体类型（A2A 规范：inputModes 是 MIME 类型）。自拼 a2asrv 栈、挂上 SDK 的 NewAgentCardEnforcer(card)
// 时，它按 card 的输入模式校验每条消息的每个部分：文本推断为 text/plain、数据部分推断为 application/json。曾经写的是 "text"，
// 不是任何部分会被推断出的媒体类型，挂上校验之后每条消息都被拒（ErrUnsupportedContentType）；按 interruptId 作答用的是数据部分，
// 所以 application/json 也得在里面。

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

func TestCard_InputModesPassSDKCardEnforcer(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("echo"), agent_v2.WithModel(&streamModel{}))
	srv := httptest.NewUnstartedServer(nil)
	base := "http://" + srv.Listener.Addr().String()
	card := server.Card(ag, base)
	enforcer, err := a2asrv.NewAgentCardEnforcer(card)
	if err != nil {
		t.Fatalf("card 的输入模式应是合法媒体类型：%v", err)
	}
	exec := server.NewExecutor(ag, server.WithOwnerResolver(func(context.Context) string { return "" }))
	srv.Config.Handler = a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec, a2asrv.WithCallInterceptors(enforcer)))
	srv.Start()
	t.Cleanup(srv.Close)

	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		part *a2atypes.Part
	}{
		{"文本", a2atypes.NewTextPart("你好")},
		{"按 interruptId 作答的数据部分", a2a.AnswerPart("iid-1", nil, "好")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.SendMessage(context.Background(), &a2atypes.SendMessageRequest{
				Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, tc.part)})
			if errors.Is(err, a2atypes.ErrUnsupportedContentType) {
				t.Fatalf("card 声明的输入模式不收这类部分：%v", err)
			}
			if err != nil {
				t.Fatalf("SendMessage：%v", err)
			}
		})
	}
}
