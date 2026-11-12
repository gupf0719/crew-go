package a2a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// WithJSONRPCHTTPClient 为 JSON-RPC 指定 HTTP client，并保留 GetTask 的鉴权错误分类。
// client 为 nil 时使用与 SDK 默认值相同的三分钟超时。原 client 的设置不被修改。
func WithJSONRPCHTTPClient(client *http.Client) RemoteOption {
	return WithClientOptions(jsonRPCHTTPClient(client))
}

func jsonRPCHTTPClient(client *http.Client) a2aclient.FactoryOption {
	httpClient := http.Client{Timeout: 3 * time.Minute}
	if client != nil {
		httpClient = *client
	}
	httpClient.Transport = getTaskAuthRoundTripper{next: httpClient.Transport}
	return a2aclient.WithTransport(a2atypes.TransportProtocolJSONRPC, a2aclient.TransportFactoryFn(
		func(_ context.Context, _ *a2atypes.AgentCard, iface *a2atypes.AgentInterface) (a2aclient.Transport, error) {
			return getTaskAuthTransport{Transport: a2aclient.NewJSONRPCTransport(iface.URL, &httpClient)}, nil
		},
	))
}

type getTaskHTTPKey struct{}

type getTaskAuthTransport struct{ a2aclient.Transport }

func (t getTaskAuthTransport) GetTask(ctx context.Context, params a2aclient.ServiceParams, req *a2atypes.GetTaskRequest) (*a2atypes.Task, error) {
	var status atomic.Int32
	task, err := t.Transport.GetTask(context.WithValue(ctx, getTaskHTTPKey{}, &status), params, req)
	var httpErr *url.Error
	if err == nil || ctx.Err() != nil || errors.As(err, &httpErr) {
		return task, err
	}
	// SDK 丢弃了非 200 的数值状态码。HTTP client 已经处理响应（包括 cookie），
	// 在这里补上鉴权错误分类并保留 SDK 的原始错误链。
	switch status.Load() {
	case http.StatusUnauthorized:
		return task, fmt.Errorf("%w: %w", a2atypes.ErrUnauthenticated, err)
	case http.StatusForbidden:
		return task, fmt.Errorf("%w: %w", a2atypes.ErrUnauthorized, err)
	default:
		return task, err
	}
}

type getTaskAuthRoundTripper struct{ next http.RoundTripper }

func (t getTaskAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	resp, err := next.RoundTrip(req)
	status, _ := req.Context().Value(getTaskHTTPKey{}).(*atomic.Int32)
	if status != nil && resp != nil && err == nil {
		status.Store(int32(resp.StatusCode))
	}
	return resp, err
}
