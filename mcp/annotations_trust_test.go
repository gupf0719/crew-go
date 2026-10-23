package mcp_test

import (
	"context"
	"testing"

	"crew-go/mcp"
	"crew-go/tool"
)

// TestTrustedAnnotations_FactoryAndDirect：WithClientFactory 配 WithTrustedAnnotations 时信任随 server 走；不信任的、
// NewRemoteTool 直接建的都不认只读声明。
func TestTrustedAnnotations_FactoryAndDirect(t *testing.T) {
	ro := specs("lookup")
	ro[0].Annotations = &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(true)}
	f := &seqFactory{build: func(int) (mcp.Client, error) { return &memClient{name: "srv", tools: ro}, nil }}
	trusted := mcp.NewHub(mcp.WithClientFactory("srv", f.New, mcp.WithTrustedAnnotations()))
	if err := trusted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer trusted.Close()
	if !tool.IsReadOnly(trusted.Tools()[0]) {
		t.Error("信任的 server 声明只读的工具应算只读")
	}
	untrusted := mcp.NewHub(mcp.WithClientFactory("srv", f.New))
	if err := untrusted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if tool.IsReadOnly(untrusted.Tools()[0]) {
		t.Error("不信任的 server 的只读声明不该被认")
	}
	if tool.IsReadOnly(mcp.NewRemoteTool(&memClient{name: "d", tools: ro}, ro[0])) {
		t.Error("NewRemoteTool 直接建的工具不该被认只读")
	}
}

// TestTrustedAnnotations_ReconnectRevokesReadOnly：按快照（声明只读）注册，重连上的 server 把它改成有破坏性——已注册的
// 工具不再算只读（审批的只读放行档不再直接执行），清单漂移里也看得到这个工具变了。
func TestTrustedAnnotations_ReconnectRevokesReadOnly(t *testing.T) {
	ro := specs("lookup")
	ro[0].Annotations = &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(true)}
	destructive := specs("lookup")
	destructive[0].Annotations = &mcp.ToolAnnotations{ReadOnlyHint: boolPtr(false), DestructiveHint: boolPtr(true)}
	f := &seqFactory{build: func(n int) (mcp.Client, error) {
		if n == 1 {
			return nil, errDown
		}
		return &memClient{name: "srv", tools: destructive}, nil
	}}
	h := mcp.NewHub(mcp.WithPartialStart(), fastBackoff(),
		mcp.WithClientFactory("srv", f.New, mcp.WithToolSnapshot(ro), mcp.WithTrustedAnnotations()))
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	rt := h.Tools()[0]
	if !tool.IsReadOnly(rt) {
		t.Fatal("fixture: 按快照注册时（声明只读、信任）应算只读")
	}
	waitFor(t, "online", func() bool { return statusOf(t, h, "srv").State == mcp.ServerOnline })
	if tool.IsReadOnly(rt) {
		t.Error("重连上的 server 把工具改成有破坏性之后，已注册的工具不该再算只读")
	}
}
