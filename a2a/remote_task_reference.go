package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"crew-go/agent_v2"
	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// taskRequestOrigin 绑定原始委派的身份。续答和订阅沿用该值。
type taskRequestOrigin struct {
	Scope   string `json:"scope"`
	Payload string `json:"payload"`
}

// failedTaskReference 只在重发原请求时提供上下文，不表示仍可续答的任务。
type failedTaskReference struct {
	TaskID    string            `json:"task_id"`
	ContextID string            `json:"context_id"`
	Origin    taskRequestOrigin `json:"origin"`
}

func init() {
	compose_v2.RegisterValueType[failedTaskReference]("a2a.failedTaskReference")
}

// selectedTaskRoute 在构造期间用 SDK 的名片更新检查识别所选接口。匹配后立即恢复完整名片。
func selectedTaskRoute(client *a2aclient.Client, card *a2atypes.AgentCard) (string, error) {
	single := *card
	for _, intf := range card.SupportedInterfaces {
		if intf == nil {
			continue
		}
		single.SupportedInterfaces = []*a2atypes.AgentInterface{intf}
		if client.UpdateCard(&single) != nil {
			continue
		}
		if err := client.UpdateCard(card); err != nil {
			return "", fmt.Errorf("restore agent card: %w", err)
		}
		return textHash(fmt.Sprintf("%q %q %q %q", intf.URL, intf.Tenant, intf.ProtocolBinding, intf.ProtocolVersion)), nil
	}
	return "", fmt.Errorf("could not identify the selected agent interface")
}

func taskReferenceScope(ctx context.Context, route, explicitTenant string) string {
	tenant, _ := a2atypes.TenantFrom(ctx)
	return textHash(fmt.Sprintf("%q %q %q %q %q", route, compose_v2.GetOwner(ctx), tenant, explicitTenant, compose_v2.GetThreadID(ctx)))
}

func originForParts(scope string, parts a2atypes.ContentParts) taskRequestOrigin {
	origin := taskRequestOrigin{Scope: scope}
	if data, err := json.Marshal(parts); err == nil {
		origin.Payload = textHash(string(data))
	}
	return origin
}

type taskReferenceExchangeKey struct{}

// taskReferenceExchange 只活在一次远端调用中。SDK 串行的发送、查询和订阅保留实际出站请求的归属。
type taskReferenceExchange struct {
	route        string
	previous     failedTaskReference
	retryContext string
	newRequest   bool
	scope        string
	sent         taskRequestOrigin
	sentContext  string
}

// taskReferenceInterceptor 在业务拦截器前清理本次操作的归属。业务处理完成后，以最终请求记录归属。
type taskReferenceInterceptor struct{ reset bool }

func (i taskReferenceInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	exchange, _ := ctx.Value(taskReferenceExchangeKey{}).(*taskReferenceExchange)
	if exchange == nil {
		return ctx, nil, nil
	}
	if i.reset {
		exchange.scope = ""
		return ctx, nil, nil
	}
	switch payload := req.Payload.(type) {
	case *a2atypes.SendMessageRequest:
		exchange.scope = taskReferenceScope(ctx, exchange.route, payload.Tenant)
		if msg := payload.Message; exchange.newRequest && msg != nil && msg.TaskID == "" {
			exchange.sent = originForParts(exchange.scope, msg.Parts)
			if old := exchange.previous; old.TaskID != "" && old.Origin.Scope == exchange.scope {
				// 业务拦截器可能提供可复用模板。写引用时只改这个请求自己的副本。
				message := *msg
				switch {
				case exchange.sent.Payload != "" && old.Origin == exchange.sent &&
					(message.ContextID == "" || message.ContextID == old.ContextID):
					message.ContextID = old.ContextID
					if id := a2atypes.TaskID(old.TaskID); !slices.Contains(message.ReferenceTasks, id) {
						message.ReferenceTasks = append(slices.Clone(message.ReferenceTasks), id)
					}
				case message.ContextID == "":
					message.ContextID = exchange.retryContext
				}
				request := *payload
				request.Message = &message
				req.Payload = &request
				msg = &message
			}
			exchange.sentContext = msg.ContextID
		}
	case *a2atypes.GetTaskRequest:
		exchange.scope = taskReferenceScope(ctx, exchange.route, payload.Tenant)
	case *a2atypes.SubscribeToTaskRequest:
		exchange.scope = taskReferenceScope(ctx, exchange.route, payload.Tenant)
	}
	return ctx, nil, nil
}

func (taskReferenceInterceptor) After(context.Context, *a2aclient.Response) error { return nil }

func (a *RemoteAgent) failedTaskKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":failed_task:" + s.RunID
}

func (a *RemoteAgent) loadFailedTaskReference(ctx context.Context, s *agent_v2.RunSession, pos string) failedTaskReference {
	if pos != "" {
		ref, _ := compose_v2.GetStepResult[failedTaskReference](ctx, "a2a.failed-task\x00"+pos)
		return ref
	}
	v, _ := s.GetValue(a.failedTaskKey(s))
	ref, _ := v.(failedTaskReference)
	return ref
}

func (a *RemoteAgent) saveFailedTaskReference(ctx context.Context, s *agent_v2.RunSession, pos string, ref failedTaskReference) error {
	if pos != "" {
		// 引用只指向已结束的远端任务。读取它时不接管任何流。
		return compose_v2.SaveStepResult(ctx, "a2a.failed-task\x00"+pos, ref, compose_v2.WithStreams(nil))
	}
	s.SetValue(a.failedTaskKey(s), ref)
	return nil
}
