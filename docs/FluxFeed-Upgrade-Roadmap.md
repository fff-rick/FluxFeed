# FluxFeed 后续升级方案

> 项目定位：基于 Go 构建的高并发个性化视频 Feed 分发系统。\
> 当前进度：已完成 Stage 2.5 Feed Engine，引入多路召回、Merger、去重过滤、
> 可插拔 Ranker、Mixer 和统一 Assembler 管线。

## 总体升级路线

``` text
Feed Core
   ↓
多路召回
   ↓
Push / Pull Hybrid Feed
   ↓
Redis 多级缓存
   ↓
Kafka 事件驱动
   ↓
可靠消息与最终一致性
   ↓
推荐系统升级
   ↓
曝光反馈闭环
   ↓
系统治理
   ↓
可观测性
   ↓
压测与性能优化
   ↓
按需微服务拆分
```

核心原则：**先做深 Feed，再做分布式；先建立清晰边界，再拆微服务。**

------------------------------------------------------------------------

## Stage 2.5：完善 Feed Engine

**状态：已完成。**

### 目标

把当前单 CandidateSource 调用升级为真正的 Feed Pipeline。

``` text
Request
  ↓
FeedRouter
  ↓
FeedStrategy
  ↓
Multi-Source Recall
  ↓
Merge
  ↓
Filter
  ↓
Rank
  ↓
Re-Rank / Mix
  ↓
Assembler
  ↓
FeedResult
```

### 任务

-   支持一个 Strategy 调用多个 CandidateSource。
-   增加 Candidate Merger。
-   增加候选去重、过滤。
-   Ranker 从接口抽象升级为可插拔实现。
-   增加 Mixer / ReRanker。
-   为 Timeline、Hot、Following、Recommend 建立独立策略。
-   补充 Feed Core 单元测试。

### 完成标准

推荐 Feed 可以完成：

``` text
热门召回 + 最新召回 + 关注召回
            ↓
          Merge
            ↓
          Dedup
            ↓
          Rank
            ↓
          Top-N
```

实现说明：Timeline、Hot、Following、Recommend 使用独立 Strategy 并统一经过
`Merge -> Filter -> Rank -> Mix -> Top-N -> Assemble`。Recommend 首屏并行组合
个性化、热门、最新和关注召回；辅助召回仅参与首屏，后续页沿用个性化推荐游标。

------------------------------------------------------------------------

## Stage 3：Push / Pull Hybrid Feed

**状态：已完成。** 复用现有 RabbitMQ 发布事件和 Fanout Worker；普通作者
按粉丝批次写 Redis inbox，大 V 写作者 outbox，阈值为 10,000，容量分别为
1,000／500 条。关注流维持纯关注语义，推荐混合仍由 `recommend` 场景负责。

索引已升级为 `feed:following:*:v2`：同分 ZSET 使用完整发布时间纳秒与视频 ID
组成的定长字典序成员，HTTP 游标保持不变。首次读取从 MySQL 回填最近
1,000 条并记录覆盖边界；索引缺失、容量边界和 Redis 故障时回源 MySQL。
关注关系变化使用户快照失效，读取时再次校验作者仍被关注；旧 v1 键自然过期。

### 目标

实现真正的 Feed 分发模型，而不是只在请求时查询数据库。

### Pull：读扩散

``` text
用户请求
  ↓
查询关注作者
  ↓
拉取作者内容
  ↓
Merge + Sort
```

适合大 V 等高粉丝用户。

### Push：写扩散

``` text
作者发布视频
  ↓
Publish Event
  ↓
Fanout Worker
  ↓
查询 Followers
  ↓
写入用户 Feed Inbox
```

Redis：

``` text
feed:inbox:{userID}
```

### Hybrid

``` text
普通作者 → Push
大V作者   → Pull

读取 Following Feed：
Inbox + 大V内容
        ↓
   Merge + Dedup
        ↓
    Timeline Rank
```

### 重点

-   Fan-out Worker。
-   Feed Inbox。
-   Celebrity Threshold。
-   Push/Pull 动态选择。
-   Cursor Pagination。
-   Feed Inbox 容量控制。

------------------------------------------------------------------------

## Stage 4：Redis Feed Cache 工程化

**状态：已完成。** 第一批已完成 Feed 页、视频卡片和实时计数的
`L1 Local Cache -> L2 Redis -> singleflight -> MySQL -> Cache Fill` 主链路；
L1 采用 10,000 项有界短 TTL 缓存，L2 批量读写使用 MGET/Pipeline，视频卡片
空结果写入短期负缓存，所有逐项缓存 TTL 加稳定随机抖动。Redis 局部故障时
保留 L1 命中并仅回源缺失项。

第二批已完成 `user:following:*` 关注作者快照及关注变更失效、每用户固定
32 KiB 的 `feed:seen:*` Bloom Bitmap、推荐流已曝光过滤，以及 Timeline 页
stale-while-revalidate 异步刷新。缓存专项 k6 场景、本地 benchmark、Prometheus
命中率查询和 P95 验收基线已补齐，后续按实际压测结果调整容量与 TTL。

### 目标

建立面向 Feed 场景设计的缓存体系。

建议 Key：

``` text
feed:inbox:{uid}        ZSET
feed:hot:{category}     ZSET
feed:seen:{uid}         SET / Bloom
video:meta:{videoID}    HASH
user:following:{uid}    SET
```

### 多级缓存

``` text
Request
  ↓
L1 Local Cache
  ↓ miss
L2 Redis
  ↓ miss
Singleflight
  ↓
MySQL
  ↓
Cache Fill
```

### 需要解决

-   缓存穿透。
-   缓存击穿。
-   缓存雪崩。
-   Hot Key。
-   Big Key。
-   Cache Stampede。

### 技术

-   TTL Random Jitter。
-   singleflight。
-   Bloom Filter。
-   热点缓存。
-   异步刷新。
-   Redis Pipeline。

------------------------------------------------------------------------

## Stage 5：Kafka Event Bus

**状态：已完成。** 已建立统一 `Event` 信封与通用 EventBus 接口，引入
Kafka 实现并迁移 `VideoPublished`、点赞/收藏变更、评论、关注/取关、曝光/观看事件。视频发布
使用独立 Fanout／Embedding 消费组，互动事件由 Counter Consumer 落库；API
与 Worker 优先连接 Kafka，不可用时回退现有 RabbitMQ。已增加事件总线
Prometheus 指标和单节点 KRaft Compose 环境。Feature Consumer 已基于观看流水物化
用户兴趣向量，Recommendation Consumer 通过独立消费组补偿 Seen Bloom 标记；
Outbox、幂等、退避与 DLQ 属于 Stage 6。

Kafka 协议回环测试默认跳过；启动 Compose 后可执行：

``` bash
cd apps/api && KAFKA_BROKERS=localhost:9092 go test ./internal/infra/mq -run TestKafkaVideoEventFanout -count=1 -v
```

### 目标

将 Feed 的核心异步业务统一为事件驱动架构。

统一事件：

``` go
type Event struct {
    ID        string
    Type      string
    UserID    int64
    VideoID   int64
    Timestamp int64
    Payload   []byte
}
```

核心事件：

``` text
VideoPublished
VideoExposed
VideoViewed
VideoLiked
VideoUnliked
VideoCollected
VideoCommented
UserFollowed
UserUnfollowed
```

消费者：

``` text
Kafka
 ├── Fanout Consumer
 ├── Counter Consumer
 ├── Feature Consumer
 └── Recommendation Consumer
```

> RabbitMQ 不需要一开始直接删除。先抽象 EventBus，再迁移关键 Feed Event
> 到 Kafka，降低重构风险。

------------------------------------------------------------------------

## Stage 6：可靠消息与最终一致性

### 目标

解决 Feed 异步链路中的数据一致性问题。

重点场景：

``` text
DB 成功 / Kafka 失败
Kafka 成功 / DB 失败
Consumer 重复消费
Consumer 执行失败
Consumer Crash
```

### Transactional Outbox

``` text
BEGIN
  INSERT video
  INSERT outbox_event
COMMIT

        ↓

Outbox Worker
        ↓
      Kafka
        ↓
mark published
```

### Consumer

``` text
Event
 ↓
Idempotency Check
 ↓
Business Logic
 ↓
Record Event ID
 ↓
ACK
```

增加：

-   Idempotency。
-   Retry。
-   Exponential Backoff。
-   DLQ。
-   消费状态监控。

------------------------------------------------------------------------

## Stage 7：推荐系统升级

### 目标

形成完整的推荐工程 Pipeline。

``` text
Recall
  ↓
Pre-Rank
  ↓
Rank
  ↓
Re-Rank
  ↓
Filter
```

### 多路召回

-   Following Recall。
-   Hot Recall。
-   Latest Recall。
-   Interest Recall。
-   Similar Video Recall。
-   Collaborative Recall。

示例：

``` text
6 × Top100
   ↓
600 Candidates
   ↓
Dedup
   ↓
Pre-Rank 200
   ↓
Rank 50
   ↓
Re-Rank
   ↓
Feed 20
```

### 用户画像

行为信号：

``` text
曝光
点击
观看时长
完播
点赞
收藏
评论
关注
```

建立 User Profile / Content Feature，为排序提供特征。

------------------------------------------------------------------------

## Stage 8：曝光与反馈闭环

### 目标

让用户行为真正影响下一次 Feed。

区分事件：

``` text
Exposure
Click
Play
Valid Play
Finish
Like
Collect
Comment
Follow
```

闭环：

``` text
Recommendation
      ↓
     Feed
      ↓
User Behavior
      ↓
    Kafka
      ↓
Feature Update
      ↓
Recommendation
```

增加：

-   Seen Filter。
-   Exposure Dedup。
-   行为权重。
-   实时兴趣更新。
-   推荐负反馈。

------------------------------------------------------------------------

## Stage 9：系统治理

### 目标

保证依赖异常时 Feed 仍然可用。

实现：

-   Rate Limit。
-   Timeout。
-   Circuit Breaker。
-   Retry。
-   Bulkhead。
-   Graceful Degradation。

推荐降级：

``` text
Recommend
   ↓ timeout/error
Hot Feed
   ↓
Latest Feed
```

其他降级：

``` text
Redis 异常     → Local Cache / DB
画像服务异常   → Default Profile
排序异常       → Time / Hot Score
推荐服务异常   → Hot Feed
```

------------------------------------------------------------------------

## Stage 10：可观测性

### 目标

建立完整的 Feed Observability。

技术：

``` text
OpenTelemetry
Prometheus
Grafana
Structured Logging
Tracing
```

核心指标：

``` text
feed_request_total
feed_latency_seconds
feed_cache_hit_ratio
feed_empty_ratio
feed_duplicate_ratio

feed_candidate_count
feed_recall_latency
feed_rank_latency

feed_fanout_latency
feed_fanout_failure_total

kafka_consumer_lag
```

Grafana 分板：

``` text
01 Feed Overview
02 Feed Latency
03 Cache
04 Recommendation
05 Kafka
06 Fanout
07 MySQL
08 Errors
```

------------------------------------------------------------------------

## Stage 11：压测与性能优化

### 目标

使用真实数据证明优化效果。

使用 k6 测试：

-   Timeline Feed。
-   Hot Feed。
-   Recommend Feed。
-   Publish。
-   Like。
-   Follow。

记录：

``` text
QPS
P50
P95
P99
Error Rate
CPU
Memory
GC
Redis Hit Rate
MySQL QPS
Kafka Lag
```

采用：

``` text
Baseline
   ↓
定位瓶颈
   ↓
优化
   ↓
再次压测
   ↓
Before / After 对比
```

README 中只记录真实压测结果，不虚构并发量。

------------------------------------------------------------------------

## Stage 12：按需微服务拆分

最后再进行服务化，避免为了微服务而微服务。

建议：

``` text
FluxFeed
├── gateway
├── user-service
├── video-service
├── feed-service
├── recommendation-service
├── interaction-service
└── workers
    ├── fanout-worker
    ├── counter-worker
    └── feature-worker
```

优先独立：

1.  Feed Service。
2.  Recommendation Service。
3.  Fanout Worker。

用户、视频等普通 CRUD 模块不急于拆分。

------------------------------------------------------------------------

# 最终技术亮点

FluxFeed 最终重点形成以下能力：

1.  **可插拔 Feed Engine**：Router + Strategy + Recall + Filter + Rank +
    ReRank + Assemble。
2.  **Push/Pull Hybrid Feed**：针对普通作者与大 V 动态选择分发模式。
3.  **Redis 多级缓存**：解决热点、击穿、穿透和缓存风暴。
4.  **Kafka Event Bus**：实现 Feed 核心链路事件驱动。
5.  **Outbox + Idempotency + DLQ**：保证异步链路最终一致性。
6.  **多路召回推荐系统**：形成 Recall → Rank → ReRank Pipeline。
7.  **曝光反馈闭环**：用户行为实时反哺推荐。
8.  **系统治理**：限流、熔断、降级、超时与重试。
9.  **完整可观测性**：Metrics + Logs + Traces。
10. **k6 性能验证**：通过真实压测数据验证架构优化。

# 推荐开发顺序

``` text
Stage 2.5  Feed Engine 完善
     ↓
Stage 3    Hybrid Feed
     ↓
Stage 4    Redis Cache
     ↓
Stage 5    Kafka
     ↓
Stage 6    最终一致性
     ↓
Stage 7    推荐系统
     ↓
Stage 8    反馈闭环
     ↓
Stage 9    系统治理
     ↓
Stage 10   可观测性
     ↓
Stage 11   压测优化
     ↓
Stage 12   微服务拆分
```

其中 **Stage 2.5 ～ Stage 8 是 FluxFeed
的核心竞争力**。微服务拆分属于最后的架构演进，而不是项目的主要卖点。
