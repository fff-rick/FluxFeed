# FluxFeed

FluxFeed 是一个基于 Go 构建的高并发个性化视频 Feed 分发系统。项目在完整短视频业务链路基础上，重点强化 Feed Engine、候选召回、排序过滤、Redis 缓存、异步事件处理与可观测性，目标是逐步演进为可扩展的生产级 Feed 基础设施。

## 项目定位

FluxFeed 不只是视频 CRUD 项目。当前核心链路围绕“内容供给 → Feed 分发 → 用户消费 → 行为反馈”展开：

```text
视频发布
   ↓
Feed Engine
   ↓
FeedRouter
   ↓
Timeline / Hot / Following / Recommend
   ↓
CandidateSource
   ↓
Filter → Ranker
   ↓
FeedAssembler
   ↓
用户曝光 / 播放 / 点赞 / 收藏 / 评论
   ↓
异步事件与数据沉淀
```

## 当前实现

### Stage 1：基础视频 Feed 平台

- Go + Gin REST API，Domain / Application / Infrastructure / Interfaces 分层。
- React + Vite Web 客户端。
- MySQL + GORM 持久化与 JWT 登录态。
- 视频发布、上传、播放及 Feed 消费链路。
- Timeline、Hot、Recommend 等 Feed 场景。
- 点赞、收藏、评论、关注、消息通知等互动能力。
- Redis Feed 缓存、热榜和互动计数。
- RabbitMQ 异步互动落库、视频发布事件和向量任务。
- Prometheus 指标、Grafana 看板与 k6 Feed 压测方案。

### Stage 2：Feed Core Engine 重构

当前二次开发已经将 Feed 从 Service 内部的场景分支升级为可扩展 Feed Engine：

- **FeedRouter**：统一完成 `scene -> Strategy` 路由，支持 Strategy 注册与扩展。
- **FeedStrategy**：拆分 Timeline、Hot、Following、Recommend 场景策略。
- **CandidateSource**：候选获取与 Feed Strategy 解耦，为后续多路召回提供统一扩展点。
- **CandidateFilter**：建立过滤管线，当前实现候选去重 `DeduplicateFilter`。
- **CandidateRanker**：建立统一排序扩展点，当前通过 `PreserveOrderRanker` 保持原场景排序语义。
- **FeedAssembler**：统一批量水合 Card、Stat、Viewer Action，降低 Strategy 职责。
- 保持原有 HTTP API、Redis Cache、Repository 与 Recommendation Service 兼容。
- 新增 Feed Router 与候选去重单元测试。

核心结构：

```text
HTTP Handler
     ↓
Feed Service
     ↓
FeedRouter
     ↓
FeedStrategy
     ↓
CandidateSource
     ↓
CandidateFilter
     ↓
CandidateRanker
     ↓
FeedAssembler
     ↓
FeedResult
```

详细设计见 [docs/stage2-feed-core-upgrade.md](docs/stage2-feed-core-upgrade.md)。

## 后续演进

后续重点围绕 Feed 系统本身继续深化：多路 Recall + Merge、Push/Pull Hybrid Feed、Feed Inbox、Redis 多级缓存、Kafka Event Bus、Outbox + 幂等消费、用户画像与推荐反馈闭环、限流/降级/重试，以及 OpenTelemetry 全链路观测。

## 技术栈

| 领域 | 技术 |
| --- | --- |
| Backend | Go / Gin |
| Frontend | React / Vite |
| Database | MySQL / GORM |
| Cache | Redis |
| MQ | RabbitMQ |
| Observability | Prometheus / Grafana |
| Load Test | k6 |
| Deployment | Docker Compose |

## 快速启动

前置依赖：Docker、Docker Compose。

```bash
cd apps
docker compose up -d --build
```

主要服务：

| 服务 | 地址 |
| --- | --- |
| Web | `http://127.0.0.1:5173` |
| API | `http://127.0.0.1:8080` |
| Health | `http://127.0.0.1:8080/health` |
| Metrics | `http://127.0.0.1:8080/metrics` |
| RabbitMQ | `http://127.0.0.1:15672` |
| Prometheus | `http://127.0.0.1:9090` |
| Grafana | `http://127.0.0.1:3000` |

停止服务：

```bash
cd apps
docker compose down
```

## 常用命令

项目根目录提供 `Makefile`，`make` 或 `make help` 可查看全部命令。

| 命令 | 说明 |
| --- | --- |
| `make verify-go` | 校验本机 Go 版本为 1.25.4 |
| `make dev` | 本地启动 API + Worker + Web |
| `make build` | 构建 `fluxfeed-api`、`fluxfeed-worker` 到 `build/` |
| `make test` | 运行全部 Go 测试（`make test-race` 带竞态检测，`make cover` 输出覆盖率） |
| `make fmt-check` / `make vet` / `make lint` | gofmt 检查 / go vet / golangci-lint |
| `make up` / `make down` / `make logs S=api` | Docker Compose 全栈启停与日志 |
| `make ci` | 本地 CI：格式检查 + vet + 测试 + 构建 |

等价的手工命令：

```bash
cd apps/api
go test ./...
```

```bash
npm --prefix apps/web run build
```

Feed 压测方案见 [docs/performance-testing.md](docs/performance-testing.md)。

> 当前项目 `go.mod` 指定 Go 1.25.4，请使用对应 Go 环境执行完整后端测试。

## 文档

| 文档 | 内容 |
| --- | --- |
| [Stage 2 Feed Core](docs/stage2-feed-core-upgrade.md) | 本轮 Feed Engine 重构说明 |
| [Architecture](docs/architecture.md) | 系统架构与核心链路 |
| [Product](docs/product.md) | 产品范围和模块地图 |
| [Optimization](docs/optimization.md) | Feed 性能与稳定性 |
| [Performance Testing](docs/performance-testing.md) | k6 压测与指标分析 |
| [Engineering](docs/engineering.md) | 工程规范与分层约定 |
| [Modules](docs/modules/README.md) | 业务模块设计 |

## License 与来源说明

FluxFeed 基于 GCFeed 项目进行二次开发。原项目已有的版权、License 与作者归属应继续保留；FluxFeed 的后续改造重点为 Feed Core 架构、高并发分发、缓存、事件驱动、推荐链路与系统治理等工程化升级。
