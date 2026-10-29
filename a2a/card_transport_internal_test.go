package a2a

import (
	"testing"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// TestCardTransport：名片只声明一种协议时记它（小写），声明了多种时不记——客户端最终选哪种 a2a-go 不对外交出。
func TestCardTransport(t *testing.T) {
	ifc := func(b a2atypes.TransportProtocol) *a2atypes.AgentInterface {
		return a2atypes.NewAgentInterface("http://x", b)
	}
	cases := map[string]struct {
		card *a2atypes.AgentCard
		want string
	}{
		"nil":     {nil, ""},
		"jsonrpc": {&a2atypes.AgentCard{SupportedInterfaces: []*a2atypes.AgentInterface{ifc(a2atypes.TransportProtocolJSONRPC)}}, "jsonrpc"},
		"同一种两个":   {&a2atypes.AgentCard{SupportedInterfaces: []*a2atypes.AgentInterface{ifc(a2atypes.TransportProtocolHTTPJSON), ifc(a2atypes.TransportProtocolHTTPJSON)}}, "http+json"},
		"多种":      {&a2atypes.AgentCard{SupportedInterfaces: []*a2atypes.AgentInterface{ifc(a2atypes.TransportProtocolJSONRPC), ifc(a2atypes.TransportProtocolHTTPJSON)}}, ""},
	}
	for name, c := range cases {
		if got := cardTransport(c.card); got != c.want {
			t.Errorf("%s: cardTransport=%q want %q", name, got, c.want)
		}
	}
}
