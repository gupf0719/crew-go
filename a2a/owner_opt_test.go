package a2a_test

import (
	"context"

	"crew-go/a2a/server"
)

// testOwnerOpt 是测试用的固定 owner resolver——server.WithOwnerResolver 必配（构造期
// panic），单租户测试统一用它。
func testOwnerOpt() server.ExecutorOption {
	return server.WithOwnerResolver(func(context.Context) string { return "test" })
}
