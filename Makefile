# FluxFeed 工程化命令入口
#
# 常用流程：
#   make dev    本地起 API + Worker + Web（依赖本机 go / npm 与中间件）
#   make up     用 Docker Compose 起全栈（含 MySQL / Redis / RabbitMQ / 监控）
#   make ci     格式检查 + vet + 测试 + 构建
#
# 运行 `make` 或 `make help` 查看全部命令。

SHELL := /bin/bash
.DEFAULT_GOAL := help

GO_VERSION := 1.25.4

GO  ?= go
NPM ?= npm
S   ?=

ROOT_DIR := $(CURDIR)
API_DIR  := $(ROOT_DIR)/apps/api
WEB_DIR  := $(ROOT_DIR)/apps/web
BIN_DIR  := $(ROOT_DIR)/build

COMPOSE := docker compose -f $(ROOT_DIR)/apps/docker-compose.yml

GO_BUILD_ENV   := CGO_ENABLED=0
GO_BUILD_FLAGS := -trimpath -ldflags='-s -w'

.PHONY: help
help: ## 显示所有可用命令
	@echo "FluxFeed 命令："
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- 环境与依赖

.PHONY: verify-go
verify-go: ## 校验本机 Go 版本是否为 1.25.4
	@v="$$($(GO) env GOVERSION | sed 's/^go//')"; \
	if [ "$$v" != "$(GO_VERSION)" ]; then \
		echo "Go 版本不匹配：当前 $$v，期望 $(GO_VERSION)"; \
		exit 1; \
	fi; \
	echo "Go $$v OK"

.PHONY: deps
deps: ## 下载 Go 依赖
	cd $(API_DIR) && $(GO) mod download

.PHONY: tidy
tidy: ## 整理 go.mod / go.sum
	cd $(API_DIR) && $(GO) mod tidy

.PHONY: deps-web
deps-web: ## 安装前端依赖（npm ci）
	cd $(WEB_DIR) && $(NPM) ci

# ---------------------------------------------------------------------- 构建

.PHONY: build
build: build-api build-worker ## 构建 API 与 Worker 二进制到 build/

.PHONY: build-api
build-api: ## 构建 API 二进制
	@mkdir -p $(BIN_DIR)
	cd $(API_DIR) && $(GO_BUILD_ENV) $(GO) build $(GO_BUILD_FLAGS) -o $(BIN_DIR)/fluxfeed-api ./cmd/feed

.PHONY: build-worker
build-worker: ## 构建 Worker 二进制
	@mkdir -p $(BIN_DIR)
	cd $(API_DIR) && $(GO_BUILD_ENV) $(GO) build $(GO_BUILD_FLAGS) -o $(BIN_DIR)/fluxfeed-worker ./cmd/worker

.PHONY: build-web
build-web: ## 构建前端静态资源到 apps/web/dist
	cd $(WEB_DIR) && $(NPM) run build

# ---------------------------------------------------------------------- 运行

.PHONY: run-api
run-api: ## 本地运行 API（读取 apps/api/configs/config.yaml）
	cd $(API_DIR) && $(GO) run ./cmd/feed

.PHONY: run-worker
run-worker: ## 本地运行异步 Worker
	cd $(API_DIR) && $(GO) run ./cmd/worker

.PHONY: web-dev
web-dev: ## 本地运行前端开发服务器
	cd $(WEB_DIR) && $(NPM) run dev

.PHONY: dev
dev: ## 一键本地启动 API + Worker + Web
	bash $(ROOT_DIR)/scripts/start.sh

# ------------------------------------------------------------------ 测试检查

.PHONY: test
test: ## 运行全部 Go 测试
	cd $(API_DIR) && $(GO) test ./...

.PHONY: test-race
test-race: ## 带竞态检测运行 Go 测试
	cd $(API_DIR) && $(GO) test -race ./...

.PHONY: cover
cover: ## 生成测试覆盖率报告 build/coverage.out
	@mkdir -p $(BIN_DIR)
	cd $(API_DIR) && $(GO) test -coverprofile=$(BIN_DIR)/coverage.out ./...
	cd $(API_DIR) && $(GO) tool cover -func=$(BIN_DIR)/coverage.out | tail -1

.PHONY: fmt
fmt: ## 格式化 Go 代码
	cd $(API_DIR) && $(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## 检查 Go 代码格式
	@files="$$(cd $(API_DIR) && gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		echo "以下文件未格式化，请执行 make fmt："; \
		echo "$$files"; \
		exit 1; \
	fi; \
	echo "gofmt OK"

.PHONY: vet
vet: ## go vet 静态检查
	cd $(API_DIR) && $(GO) vet ./...

.PHONY: lint
lint: ## golangci-lint 检查（未安装则跳过）
	@if command -v golangci-lint >/dev/null 2>&1; then \
		cd $(API_DIR) && golangci-lint run ./...; \
	else \
		echo "golangci-lint 未安装，跳过"; \
	fi

# ------------------------------------------------------------------- Docker

.PHONY: up
up: ## 构建并启动全栈（Docker Compose）
	$(COMPOSE) up -d --build

.PHONY: down
down: ## 停止并移除容器
	$(COMPOSE) down

.PHONY: down-v
down-v: ## 停止容器并删除数据卷
	$(COMPOSE) down -v

.PHONY: ps
ps: ## 查看容器状态
	$(COMPOSE) ps

.PHONY: logs
logs: ## 跟踪容器日志（make logs S=api）
	$(COMPOSE) logs -f $(S)

.PHONY: images
images: ## 重新构建镜像
	$(COMPOSE) build

.PHONY: restart
restart: down up ## 重启全栈

# ---------------------------------------------------------------------- 其他

.PHONY: clean
clean: ## 清理构建产物与测试缓存
	rm -rf $(BIN_DIR)
	cd $(API_DIR) && $(GO) clean -testcache

.PHONY: ci
ci: fmt-check vet test build ## 本地 CI：格式 + vet + 测试 + 构建
