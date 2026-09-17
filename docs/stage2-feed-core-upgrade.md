# Stage 2.5：Feed Engine 完善

本阶段把 Feed 从“Service 内部按 scene 分支处理”升级为可扩展的 Feed Engine，同时保持现有 HTTP API、Redis 缓存、Repository 与推荐服务接口兼容。

## 新结构

```text
HTTP Handler
    ↓
Feed Service
    ↓
FeedRouter ── scene → Strategy
    ↓
Strategy (Timeline / Hot / Following / Recommend)
    ↓
Multi-Source CandidateSource
    ↓
CandidateMerger → CandidateFilter → CandidateRanker → CandidateMixer → Top-N
    ↓
FeedAssembler
    ↓
FeedResult
```

## 核心变化

- `FeedRouter`：独立管理 `scene -> Strategy`，支持并发安全注册和读取。
- `CandidateSource`：把候选获取从 Strategy 中解耦，提供 Timeline、Hot、Following、Recommend 四类来源；多路召回并行执行。
- `CandidateMerger`：按召回源注册顺序稳定合并候选。
- `CandidateFilter`：新增候选过滤扩展点，默认启用 `DeduplicateFilter`。
- `CandidateRanker`：提供 `PreserveOrderRanker`、`LatestRanker`、`HotRanker` 可插拔实现。
- `CandidateMixer`：提供按作者轮询的 `AuthorDiversityMixer`，减少同作者内容连续出现。
- `FeedAssembler`：集中完成 Card、Stat、Viewer Action 的批量水合，Strategy 不再直接关心组装细节。
- `RecommendStrategy`：首屏合并个性化、热门、最新、关注召回，统一执行合并、去重、排序、打散和 Top-N。
- 原有 `GET /api/feed-items`、`POST /api/feed-queries` 等接口保持兼容。

## 后续扩展方式

新增 Feed 场景时优先实现 `CandidateSource + Strategy` 并注册到 `FeedRouter`；多路召回可以在 CandidateSource 层合并候选，再通过 Filter/Ranker 管线处理，不需要修改 Service 核心流程。

## 验证说明

新增 `router_test.go` 覆盖路由注册和候选去重。代码已执行 `gofmt`。首次提交时打包环境自带 Go 1.23.2，而当时项目 `go.mod` 要求更高的 Go 版本；环境又禁止联网下载 toolchain/dependencies，因此无法在该环境完成 `go test ./...`。后续已将项目统一到 Go 1.25.4，并在此环境执行 `go test ./...`，全部通过。
