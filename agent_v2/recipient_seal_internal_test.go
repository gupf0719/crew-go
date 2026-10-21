package agent_v2

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestRecipient_NotPrintedWithCtx：收件人放进 ctx 之后，各种打印 ctx 的写法都不露出它——ctx 是环境，
// 标准库 context 的 String() 会把链上每一层的值原样打印，裸 string 的话业务代码 fmt.Sprint(ctx) 就
// 读到了收件人、能伪造决定（审计实测：终点候选、被当工具用的孙辈、分类器、候选自己的钩子全部转走了
// 对话）。最外层、被别的层盖在下面、被摘掉之后（摘只在子层盖一个 nil，父层那一层还在链上）都要验。
func TestRecipient_NotPrintedWithCtx(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	type otherKey struct{}
	cases := map[string]context.Context{
		"转交·最外层":   withHandoffDest(context.Background(), "handoff:"+secret),
		"转交·被盖在下面": context.WithValue(withHandoffDest(context.Background(), "handoff:"+secret), otherKey{}, "x"),
		"转交·被摘掉之后": withoutHandoffDest(withHandoffDest(context.Background(), "handoff:"+secret)),
		"路由·最外层":   withRouteDest(context.Background(), "route:"+secret),
		"路由·被摘掉之后": withoutRouteDest(withRouteDest(context.Background(), "route:"+secret)),
	}
	for name, ctx := range cases {
		t.Run(name, func(t *testing.T) {
			var logBuf bytes.Buffer
			slog.New(slog.NewTextHandler(&logBuf, nil)).Info("ctx", "ctx", ctx)
			outs := map[string]string{
				"%v":    fmt.Sprintf("%v", ctx),
				"%+v":   fmt.Sprintf("%+v", ctx),
				"%s":    fmt.Sprintf("%s", ctx),
				"%#v":   fmt.Sprintf("%#v", ctx),
				"slog":  logBuf.String(),
				"Print": fmt.Sprint(ctx),
			}
			for verb, out := range outs {
				if strings.Contains(out, secret) {
					t.Errorf("%s 打印 ctx 露出了收件人：%s", verb, out)
				}
			}
		})
	}
	// 正向：框架自己读得到、摘得掉——遮罩只管打印
	if d, ok := getHandoffDest(cases["转交·被盖在下面"]); !ok || d != "handoff:"+secret {
		t.Errorf("框架自己读收件人应读得到，实际 %q ok=%v", d, ok)
	}
	if _, ok := getHandoffDest(cases["转交·被摘掉之后"]); ok {
		t.Error("摘掉之后不该再读得到收件人")
	}
	if d, ok := getRouteDest(cases["路由·最外层"]); !ok || d != "route:"+secret {
		t.Errorf("框架自己读路由收件人应读得到，实际 %q ok=%v", d, ok)
	}
}
