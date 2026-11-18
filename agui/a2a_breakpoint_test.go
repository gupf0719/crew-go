package agui_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func TestHandler_A2ABreakpointWaitsForExplicitContinuation(t *testing.T) {
	var writes atomic.Int32
	remoteBuilder := compose_v2.NewBuilder().WithName("remote_debug").WithInterruptBefore("write")
	remoteBuilder.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		writes.Add(1)
		return nil, nil
	})
	remoteBuilder.AddEdge(compose_v2.StartNode, "write")
	remoteBuilder.AddEdge("write", compose_v2.EndNode)
	remoteGraph, err := remoteBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	remote := agent_v2.NewGraphAgent(remoteGraph, agent_v2.WithGraphAgentName("remote_debug"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
			return schema.AssistantMessage("written")
		}))
	svc := httptest.NewUnstartedServer(nil)
	svc.Config.Handler = server.Handler(remote, "http://"+svc.Listener.Addr().String(), server.WithOwnerResolver(func(context.Context) string { return "test" }))
	svc.Start()
	defer svc.Close()
	delegate, err := a2a.NewRemoteAgent(t.Context(), svc.URL+"/.well-known/agent-card.json")
	if err != nil {
		t.Fatal(err)
	}

	var choice, result atomic.Value
	host := compose_v2.NewBuilder().WithName("debug_host")
	host.AddNode("delegate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		events, err := agent_v2.RunSubAgent(ctx, "remote", delegate, schema.UserMessage("do the write"))
		if err == nil {
			result.Store(agent_v2.FinalMessage(events).GetContent())
		}
		return nil, err
	})
	host.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Which path?", Reason: "ask_user"})
		if err == nil {
			choice.Store(answer.Content)
		}
		return nil, err
	})
	for _, node := range []compose_v2.NodeID{"delegate", "ask"} {
		host.AddEdge(compose_v2.StartNode, node)
		host.AddEdge(node, compose_v2.EndNode)
	}
	graph, err := host.Compile()
	if err != nil {
		t.Fatal(err)
	}
	h := ownedHandler(func(hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("debug_host"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
				return schema.AssistantMessage("done")
			}))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))

	start := parseSSE(postAGUI(h, `{"threadId":"debug","runId":"start","messages":[{"role":"user","content":"go"}]}`).Body.String())
	var pauseID, questionID string
	for _, item := range interruptsIn(start) {
		switch item["reason"] {
		case "crew-go:breakpoint":
			metadata, _ := item["metadata"].(map[string]any)
			if want := (map[string]any{"before": []any{"write"}, "delegated": true}); !reflect.DeepEqual(metadata["breakpoint"], want) {
				t.Fatalf("宿主应收到远端断点的原位置和转述标记，实际 %+v", item)
			}
			if message, _ := item["message"].(string); !strings.Contains(message, "write") || strings.Contains(message, "interruptId") {
				t.Fatalf("提示应保留节点描述，继续地址由宿主事件的 id 提供，实际 %q", message)
			}
			pauseID, _ = item["id"].(string)
		case "input_required":
			if item["message"] == "Which path?" {
				questionID, _ = item["id"].(string)
			}
		}
	}
	if pauseID == "" || questionID == "" || writes.Load() != 0 {
		t.Fatalf("宿主应同时挂起远端断点和本地提问，事件 %+v，写入 %d 次", start, writes.Load())
	}

	mixed := resumeEntries(t, h, "debug", "mixed", map[string]any{"interruptId": pauseID, "status": "cancelled"}, resolved(questionID, "left"))
	if findErrorCode(mixed) != "invalid_resume" || writes.Load() != 0 || choice.Load() != nil {
		t.Fatalf("取消远端断点时整批应保持挂起，实际 %+v，写入 %d，答复 %v", mixed, writes.Load(), choice.Load())
	}
	done := resumeEntries(t, h, "debug", "done", map[string]any{"interruptId": pauseID, "status": "resolved"}, resolved(questionID, "left"))
	if outcome := aguiBreakpointOutcome(t, done, "done"); outcome["type"] != "success" || writes.Load() != 1 || choice.Load() != "left" || result.Load() != "written" {
		t.Fatalf("准确继续时宿主收到远端结果和本地答复，实际 %+v，写入 %d，结果 %v，答复 %v",
			outcome, writes.Load(), result.Load(), choice.Load())
	}
}
