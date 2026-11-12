// sop_demo 用 GraphAgent 演示方案生成、校验回退、结构化人审和执行。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type approval struct {
	Approved bool   `json:"approved"`
	Reviewer string `json:"reviewer"`
}

const reviewSchema = `{
  "type": "object",
  "required": ["approved", "reviewer"],
  "additionalProperties": false,
  "properties": {
    "approved": {"type": "boolean"},
    "reviewer": {"type": "string", "minLength": 1}
  }
}`

func newHandler(execute func(string)) (http.Handler, error) {
	input := compose_v2.LastValue[*schema.Message]("input")
	draft := compose_v2.LastValue[string]("draft")
	attempts := compose_v2.LastValue[int]("attempts")
	valid := compose_v2.LastValue[bool]("valid")
	approved := compose_v2.LastValue[bool]("approved")
	out := compose_v2.LastValue[*schema.Message]("output")

	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, input)
	compose_v2.WithChannel(b, draft)
	compose_v2.WithChannel(b, attempts)
	compose_v2.WithChannel(b, valid)
	compose_v2.WithChannel(b, approved)
	compose_v2.WithChannel(b, out)
	b.AddNode("draft", func(_ context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		n, _ := compose_v2.Get(channels, attempts) // 首轮为零。
		msg, _ := compose_v2.Get(channels, input)  // 由入口写入。
		plan := "整理发布说明：" + msg.Content
		if n > 0 {
			plan += "\n验收：核对变更、版本号与回滚步骤"
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(draft, plan), compose_v2.Write(attempts, n+1)}, nil
	})
	b.AddNode("verify", func(_ context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		plan, _ := compose_v2.Get(channels, draft) // draft 已生成。
		return []compose_v2.ChannelWrite{compose_v2.Write(valid, strings.Contains(plan, "\n验收："))}, nil
	})
	b.AddNode("review", func(ctx context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		plan, _ := compose_v2.Get(channels, draft) // verify 放行了当前方案。
		n, _ := compose_v2.Get(channels, attempts)
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Question:       fmt.Sprintf("第 %d 版方案通过校验，请审核。\n%s", n, plan),
			ResponseSchema: json.RawMessage(reviewSchema),
		})
		if err != nil {
			return nil, err
		}
		var decision approval
		if err := json.Unmarshal([]byte(answer.Content), &decision); err != nil {
			return nil, fmt.Errorf("decode review: %w", err)
		}
		if !decision.Approved {
			return []compose_v2.ChannelWrite{
				compose_v2.Write(approved, false),
				compose_v2.Write(out, schema.AssistantMessage("审核拒绝，任务结束")),
			}, nil
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(approved, true)}, nil
	})
	b.AddNode("execute", func(_ context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		plan, _ := compose_v2.Get(channels, draft) // 审核通过的方案。
		execute(plan)
		return []compose_v2.ChannelWrite{compose_v2.Write(out, schema.AssistantMessage("已执行发布说明流程"))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "draft")
	b.AddEdge("draft", "verify")
	b.AddConditionalEdge("verify", func(_ context.Context, channels compose_v2.Channels) (compose_v2.NodeID, error) {
		ok, _ := compose_v2.Get(channels, valid) // verify 已写入。
		if ok {
			return "review", nil
		}
		return "draft", nil
	})
	b.AddConditionalEdge("review", func(_ context.Context, channels compose_v2.Channels) (compose_v2.NodeID, error) {
		ok, _ := compose_v2.Get(channels, approved) // review 已写入。
		if ok {
			return "execute", nil
		}
		return compose_v2.EndNode, nil
	})
	b.AddEdge("execute", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		return nil, err
	}
	return agui.Handler(func(h hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("release_sop"),
			agent_v2.WithGraphAgentHooks(h),
			agent_v2.WithGraphInputWriter(func(msg *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(input, msg)}
			}),
			agent_v2.WithGraphOutputReader(func(channels compose_v2.Channels) *schema.Message {
				result, _ := compose_v2.Get(channels, out) // 终态节点写入。
				return result
			}))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		agui.WithOwnerResolver(func(context.Context) string { return "sop-demo" })), nil
}

func main() {
	addr := flag.String("listen", "127.0.0.1:8094", "HTTP 监听地址")
	flag.Parse()
	h, err := newHandler(func(plan string) { log.Printf("执行方案 %q", plan) })
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/agui", h)
	// 流式响应需要 Flusher，由请求 ctx 提供期限。
	server := &http.Server{Addr: *addr, ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})}
	log.Printf("SOP demo %s/agui", *addr)
	log.Fatal(server.ListenAndServe())
}
