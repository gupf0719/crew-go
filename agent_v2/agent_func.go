package agent_v2

import (
	"context"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// NewAgentFunc 把接收和返回文本的同步函数包装成可加入编排的 Agent。
// 外部函数的副作用由调用方保证幂等：启用存档后，重试或续跑可重新调用尚未成功的函数。
// 函数应响应 ctx 取消。name 为空或 fn 为 nil 时 panic。
func NewAgentFunc(name, description string, fn func(context.Context, string) (string, error)) *GraphAgent {
	if name == "" {
		panic("NewAgentFunc: name is required")
	}
	if fn == nil {
		panic("NewAgentFunc: nil fn")
	}

	in := compose_v2.LastValue[string]("input")
	out := compose_v2.LastValue[string]("output")
	builder := compose_v2.NewBuilder().WithIdentityPrefix(name)
	compose_v2.WithChannel(builder, in)
	compose_v2.WithChannel(builder, out)
	builder.AddNode("call", func(ctx context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		text, _ := compose_v2.Get(channels, in) // 空输入也交给外部函数处理。
		result, err := fn(ctx, text)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, result)}, nil
	})
	builder.AddEdge(compose_v2.StartNode, "call").AddEdge("call", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		panic(err) // 上面的内部图必须可编译。
	}
	return NewGraphAgent(graph,
		WithGraphAgentName(name),
		WithGraphAgentDescription(description),
		WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			var text string
			if input != nil {
				text = input.Content
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(in, text)}
		}),
		WithGraphOutputReader(func(channels compose_v2.Channels) *schema.Message {
			text, _ := compose_v2.Get(channels, out) // 图成功时 call 已写出结果，结果可以为空串。
			return schema.AssistantMessage(text)
		}),
	)
}
