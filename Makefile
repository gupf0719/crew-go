# crew-go Makefile
#
# 测试分两层：
#   - test-unit         运行各模块的默认测试，无需外部服务，部分用例监听本机端口。
#   - test-integration  开启 integration tag，运行 example 下匹配 TestAdv 的用例（含 mock）。
#                       真模型用例按各自要求读取 API key，缺失时跳过。
#
# 详细规约见 doc/description/testing-conventions.md。

.PHONY: test test-unit test-integration test-all build vet lint cover tag

# 默认 test = 单测，与 CI 必过路径一致。
test: test-unit

# 根目录的 ./... 不含三个子模块，各自进目录跑，与 CI 的默认测试范围一致。
test-unit:
	go test ./...
	cd redis && go test ./...
	cd sql && go test ./...
	cd a2a/zk && go test ./...

# 教学集成用例同时运行匹配 TestAdv 的 mock 测试。
# 总超时 600s 容纳 PlanAgent / ReflectAgent 等多轮场景累积耗时。
test-integration:
	go test -tags=integration -v -run TestAdv -timeout 600s ./example/...

# 组合运行默认测试和教学集成测试。
test-all: test-unit test-integration

build:
	go build ./...

vet:
	go vet ./...
	cd redis && go vet ./...
	cd sql && go vet ./...
	cd a2a/zk && go vet ./...

# golangci-lint 静态检查（规则见 .golangci.yml；CI lint job 同此命令）。
# 首次需装：go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
lint:
	golangci-lint run

# 核心包覆盖率门禁（10 个核心包 >= 75%；CI coverage job 同此脚本）。
cover:
	bash scripts/check-coverage.sh

# 一键给所有 go module 打版本 tag（多 module 用路径前缀，见
# doc/description/three-way-comparison/sdk-versioning.md §五）。用法: make tag VERSION=v0.1.0
tag:
	@test -n "$(VERSION)" || (echo "用法: make tag VERSION=v0.1.0"; exit 1)
	git tag $(VERSION)
	git tag redis/$(VERSION)
	git tag sql/$(VERSION)
	git tag a2a/zk/$(VERSION)
	@echo "已打 4 module tag: $(VERSION) / redis/$(VERSION) / sql/$(VERSION) / a2a/zk/$(VERSION)"
	@echo "push: git push origin $(VERSION) redis/$(VERSION) sql/$(VERSION) a2a/zk/$(VERSION)"
