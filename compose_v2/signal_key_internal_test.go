package compose_v2

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestTaskRecipientKey_NotPrintedWithCtx：任务的收件人密钥放进 ctx 之后，各种打印 ctx 的写法都不露出它。
// 标准库 context 的 String() 会把链上每一层的值原样打印，裸 []byte 放进去的话，业务代码
// `fmt.Sprint(ctx)` 就读到了密钥、能自己算出任何一个收件人。
func TestTaskRecipientKey_NotPrintedWithCtx(t *testing.T) {
	key := []byte("SECRETKEY-0123456789abcdef")
	type otherKey struct{}
	ctx := context.WithValue(withTaskRecipientKey(context.Background(), key), otherKey{}, "x")

	var logBuf bytes.Buffer
	slog.New(slog.NewTextHandler(&logBuf, nil)).Info("ctx", "ctx", ctx)
	outs := map[string]string{
		"%v":   fmt.Sprintf("%v", ctx),
		"%+v":  fmt.Sprintf("%+v", ctx),
		"%s":   fmt.Sprintf("%s", ctx),
		"%#v":  fmt.Sprintf("%#v", ctx),
		"slog": logBuf.String(),
	}
	for verb, out := range outs {
		if strings.Contains(out, "SECRETKEY") || strings.Contains(out, fmt.Sprint(key)) {
			t.Errorf("%s 打印 ctx 露出了收件人密钥：%s", verb, out)
		}
	}
	// 正向：框架自己读得到
	if got := taskRecipientKeyFrom(ctx); !bytes.Equal(got, key) {
		t.Errorf("框架自己应读得到密钥，实际 %q", got)
	}
}
