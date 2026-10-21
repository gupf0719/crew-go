//go:build integration

package zk_test

// ZK binding 的协议合规测试（integration tag：依赖外部服务/Docker，默认 go test 不编译，
// 跑法 `go test -tags=integration ./...`，testing-conventions §1）。两种供给真 ZooKeeper 的方式（任一即可）：
//
//  1. **ZK_ADDR 直连**（无 Docker 的机器）：`ZK_ADDR=host:2181 go test -tags=integration ./...` 连任意现成 ZK
//     （本地 tarball 起的 / 远程 dev 集群 / CI service container）。
//  2. **testcontainers 自动起**：本地有 Docker daemon 时自动拉 zookeeper:3.9 容器。
//
// 两者都没有 → 自动 skip（不 fail 默认 go test）。
// 每个 conformance subtest 用独立 root 子树（WithRootPath）隔离——同一 ZK 多 subtest 不互撞；
// ZK_ADDR 指向共享集群时测试节点都建在 /a2a-conf-* 临时子树下，不碰 /crew/a2a 生产路径。

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a/registry"
	"crew-go/a2a/registry/conformance"
	"crew-go/a2a/zk"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestZK_Conformance(t *testing.T) {
	ctx := context.Background()

	addr := os.Getenv("ZK_ADDR")
	if addr == "" {
		// 无 ZK_ADDR → testcontainers 自动起容器
		req := testcontainers.ContainerRequest{
			Image:        "zookeeper:3.9",
			ExposedPorts: []string{"2181/tcp"},
			WaitingFor:   wait.ForListeningPort("2181/tcp").WithStartupTimeout(90 * time.Second),
		}
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Skipf("跳过 ZK conformance：无 ZK_ADDR 且无法启动 ZooKeeper 容器（需 Docker daemon 或 ZK_ADDR=host:2181）: %v", err)
		}
		t.Cleanup(func() { _ = container.Terminate(ctx) })

		host, err := container.Host(ctx)
		if err != nil {
			t.Fatalf("container host: %v", err)
		}
		port, err := container.MappedPort(ctx, "2181/tcp")
		if err != nil {
			t.Fatalf("mapped port: %v", err)
		}
		addr = fmt.Sprintf("%s:%s", host, port.Port())
	}

	// 每次 factory 用独立 root → 多 subtest 共享同一 ZK 容器但互不干扰。
	var rootCtr atomic.Int64
	conformance.RunDirectoryTests(t, func() registry.Directory {
		root := fmt.Sprintf("/a2a-conf-%d", rootCtr.Add(1))
		d, err := zk.New([]string{addr}, 10*time.Second, zk.WithRootPath(root))
		if err != nil {
			t.Fatalf("zk.New(%s): %v", addr, err)
		}
		t.Cleanup(func() { _ = d.Close() })
		return d
	})
}
