# crew-go Makefile
#
# 测试分两层：
#   - test-unit         默认 go test ./...，不带任何 build tag，纯单测，无网络依赖。
#   - test-integration  显式开启 integration tag，编译并运行 example/adv_*_test.go。
#                       依赖 QWEN_API_KEY（默认 model/qwen，dashscope 兼容 OpenAI）；DEEPSEEK_API_KEY 仍可备选；缺失时各用例 t.Skip。
#
# 详细规约见 doc/description/testing-conventions.md。

.PHONY: test test-unit test-integration test-all build vet lint cover tag

# 默认 test = 单测，与 CI 必过路径一致。
test: test-unit

# 单元测试：纯本地、毫秒级、不连外网。go.work 下根目录的 ./... 不含三个子模块，各自进目录跑（同 CI）。
test-unit:
	go test ./...
	cd redis && go test ./...
	cd sql && go test ./...
	cd a2a/zk && go test ./...

# 集成测试：走真实 LLM，单条用例 60~300s 不等。
# 总超时 600s 容纳 PlanAgent / ReflectAgent 等多轮场景累积耗时。
test-integration:
	go test -tags=integration -v -run TestAdv -timeout 600s ./example/...

# 全跑（单测 + 集成）。CI nightly 用。
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
