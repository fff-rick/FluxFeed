# FluxFeed 简历项目深挖面试：132 个问题与详细回答模板

> 核查日期：2026-10-02。代码基线：`48bc481077b160fd1d09d3cfb95e3eca9403f8fe`，结合当前工作区读取。简历起点为本仓库 [resume-highlights.md](resume-highlights.md) 的「可直接用于简历的项目经历（含 Stage 11 优化）」四条内容，同时覆盖前文各项技术亮点。未收到另一份个人简历，因此不推定实际投递版本、团队规模、个人独立贡献或线上用户量。
>
> 使用方法：按章节连续回答，先讲代码现状，再讲原因和故障边界，最后讲验证或改进。模板中的“我会”表示建议方案，不能改成“我已经做了”。历史压测与本次核查分开表述。本文不修改业务源码。

## 一、简历内容与问答覆盖

| 简历原始主张 | 连续追问范围 | 必须守住的表达边界 |
| --- | --- | --- |
| Feed 读取：游标、ID-Detail、Redis、批量、singleflight | Q009–Q030 | 通用页缓存主要接在 Timeline；热榜和推荐动态排序不能承诺无限期稳定翻页 |
| 分发与一致性：混合推拉、Inbox、Author Outbox、Kafka、Transactional Outbox、幂等、DLQ | Q031–Q068 | Author Outbox 是作者内容索引；Transactional Outbox 是可靠事件表，两者不同；点赞收藏不在数据库 Outbox 覆盖内 |
| 推荐反馈：多路召回、过滤、融合排序、作者打散、兴趣向量 | Q069–Q102 | hash n-gram 是本地文本特征基线；统一过滤只覆盖推荐 Service 的池，外层辅助源另有边界 |
| 性能优化：10 万视频、5 秒快照、P95、CPU、20 RPS | Q103–Q114 | 单机限定负载的历史实验，不是生产容量上限；未证明推荐效果提升 |

| 其他亮点条目 | 对应问答 |
| --- | --- |
| 稳定分页、ID-Detail、批量恢复顺序 | Q009–Q018 |
| 页/卡片/计数缓存、降级、singleflight、TTL 抖动 | Q019–Q030 |
| 分批 Fanout、关注回填、发布驱动、预热 | Q031–Q042 |
| RabbitMQ 持久化、ACK/NACK、事件拆分 | Q043–Q056 |
| 写幂等、重复消费、事务计数、最终一致、分片 | Q047–Q052、Q057–Q068 |
| 加权热度、分钟桶、60 分钟窗口 | Q013–Q014、Q065–Q068 |
| 候选召回、曝光过滤、精排、作者打散、Hot 快照 | Q069–Q087、Q093–Q094 |
| 视频向量、用户向量、观看事件、曝光聚合 | Q088–Q102 |
| 播放配置、预加载、QoS、Range、上传校验、faststart | Q115–Q119 |
| 可选登录、JWT、内部鉴权、消息、消息幂等 | Q120–Q124 |
| 分层、依赖注入、迁移索引、Compose、Kubernetes、测试 | Q001–Q008、Q011、Q125–Q132 |

## 二、项目全解：实现状态和证据

“已实现”表示存在实际代码调用链，不代表已经完成生产验证。“部分实现”表示主链路存在，但宣称的保障还有缺口。测试中的 Fake/Stub 是测试替身，不能据此认定生产功能为空壳。

| 功能 | 状态 | 核心位置与结论 |
| --- | --- | --- |
| 注册、登录、资料、JWT | 已实现 | E01/E20；密码 bcrypt，访问令牌 HS256；登出不撤销已签令牌 |
| 内部接口鉴权 | 部分实现，存在遗漏 | E01/E20；治理、内部消息、QoS、Outbox 有令牌校验，但 recommendation-candidates、exposure-decisions、exposures 三条内部路由未挂鉴权 |
| 发布、软删除、上传 | 已实现 | E07/E21；媒体存本地，上传处理同步执行 |
| Timeline/Hot/Following/Recommend | 已实现 | E02/E03；实际有两层推荐候选组合 |
| 稳定无限滚动 | 部分实现 | 时间线复合游标明确；热榜快照会过期；推荐重算分数和候选，缺会话级固定快照 |
| 分层缓存、批量组装、防击穿 | 已实现 | E02/E05；L1 有界 Map，L2 Redis，数据库批量补缺 |
| 缓存故障全场景降级 | 部分实现 | 卡片、计数、Timeline 可回源；配置了 Redis 的 Hot 读取报错直接失败 |
| 混合推拉与预热 | 已实现 | E06/E05；1 万粉丝阈值，500 人一批，Redis Inbox/Author Outbox |
| Fanout 持久化进度/作者模式切换 | 未实现完整协议 | E06；重试从粉丝游标 0 重做，无持久任务检查点和迁移状态机 |
| Kafka、Outbox、DLQ | 已实现但保障有边界 | E08–E11；至少一次思路，无业务事务与消费标记原子提交 |
| 全部写入都走 Outbox | 未实现 | E01/E12；点赞收藏 Redis→MQ；站内通知也不是独立可靠消费链 |
| 点赞、收藏、评论及计数 | 已实现 | E12/E13；数据库事实和计数事务更新，缓存分片聚合 |
| 缓存计数自动严格收敛 | 部分实现 | E05；JSON 15 秒过期仍会读 base/shards，不能证明 15 秒必回数据库 |
| 六路召回、负反馈过滤、规则排序 | 已实现 | E14/E15；有界池、统一过滤、部分召回降级 |
| 内容向量与兴趣画像 | 已实现的基线 | E16/E15；128 维文本 hash n-gram，非语义模型、非 ANN、非视频画面理解 |
| 曝光流水、聚合、Bloom 去重 | 已实现 | E17/E05；SQL 7 天与 Bloom 续期 30 天不是相同窗口 |
| 播放优化 | 已实现的基础能力 | E18/E21；时间线预加载、QoS、Range、faststart；无完整自适应码率系统 |
| 消息中心 | 已实现 | E19；分页、未读、已读、事件幂等；调用失败可能被忽略 |
| 限流、超时、隔舱、熔断、降级 | 已实现 | E22；进程内作用域，需要区分单实例与全局保障 |
| Metrics、日志、Trace | 已实现但链路不完整 | E23；HTTP/Kafka 埋点，Outbox 未保存原始 Trace 上下文 |
| Compose、Kubernetes | 有配置，生产验证不足 | E24；K8s 清单以 MySQL/Redis/RabbitMQ 等单副本为主，未完整覆盖 Kafka/Tempo 栈 |
| 搜索 | 界面空壳 | `apps/web/src/App.jsx` 的 TopNav 搜索输入无查询处理，后端无搜索路由 |
| 完整审核/管理平台、训练与 A/B 系统 | 未见完整实现 | 路由不含完整审核后台、训练任务和实验分流；文档存在不等于功能完成 |

README 的“后续演进”已经落后于部分源码：Kafka、Outbox、L1、多路召回、治理和 OTel 已有代码。旧面试清单把 RabbitMQ 作为唯一消息中间件也不再准确。

### 代码证据索引与推荐阅读顺序

正文使用 E 编号定位证据；链接指向真实文件，关键函数在问答中给出。顺序为入口装配 → Feed → 缓存/分发 → 事件 → 推荐反馈 → 配套模块 → 验证。

| 编号 | 文件与阅读重点 |
| --- | --- |
| E01 | [HTTP 装配](../apps/api/internal/interfaces/http/router/router.go)：`Register`、`newApplicationEventPublisher`；[API 入口](../apps/api/cmd/feed/main.go)、[Worker 入口](../apps/api/cmd/worker/main.go) |
| E02 | [Feed Service](../apps/api/internal/application/feed/service.go)：`GetFeed`、各 Strategy、`loadFeedPage`、`assembleFeedItems` |
| E03 | [候选来源](../apps/api/internal/application/feed/candidate_source.go)：`Load`、`loadCandidatePages`、`governedCandidateSource` |
| E04 | [候选管线](../apps/api/internal/application/feed/pipeline.go)、[Router](../apps/api/internal/application/feed/router.go) |
| E05 | [Redis 缓存](../apps/api/internal/infra/cache/feed_cache.go)、[本地缓存](../apps/api/internal/infra/cache/local_cache.go) |
| E06 | [Fanout Worker](../apps/api/internal/application/video/fanout_worker.go)、[Feed SQL](../apps/api/internal/infra/persistence/feed/gorm.go) |
| E07 | [发布 Service](../apps/api/internal/application/video/service.go)、[视频持久化](../apps/api/internal/infra/persistence/video/gorm.go) |
| E08 | [事件信封](../apps/api/internal/application/eventbus/event.go)、[事件适配器](../apps/api/internal/infra/mq/feed_event_bus.go) |
| E09 | [Kafka](../apps/api/internal/infra/mq/kafka.go)、[RabbitMQ](../apps/api/internal/infra/mq/rabbitmq.go) |
| E10 | [Outbox Worker](../apps/api/internal/application/eventbus/outbox.go)、[Outbox 仓储](../apps/api/internal/infra/persistence/outbox/gorm.go) |
| E11 | [消费去重仓储](../apps/api/internal/infra/persistence/eventbus/gorm.go)、[消费去重模型](../apps/api/internal/infra/persistence/eventbus/model.go) |
| E12 | [互动 Service](../apps/api/internal/application/interaction/service.go)、[互动 Worker](../apps/api/internal/application/interaction/worker.go) |
| E13 | [互动事务](../apps/api/internal/infra/persistence/interaction/gorm.go)、[互动模型](../apps/api/internal/infra/persistence/interaction/model.go) |
| E14 | [推荐 Service](../apps/api/internal/application/recommendation/service.go) |
| E15 | [推荐 SQL 和画像](../apps/api/internal/infra/persistence/recommendation/gorm.go)、[推荐领域](../apps/api/internal/domain/recommendation/entity.go) |
| E16 | [向量算法](../apps/api/internal/domain/embedding/hash_ngram.go)、[向量 Service](../apps/api/internal/application/embedding/service.go)、[画像 Worker](../apps/api/internal/application/embedding/feature_worker.go) |
| E17 | [曝光 Service](../apps/api/internal/application/exposure/service.go)、[曝光事务](../apps/api/internal/infra/persistence/exposure/gorm.go)、[曝光模型](../apps/api/internal/infra/persistence/exposure/model.go)、[行为规则](../apps/api/internal/domain/exposure/entity.go) |
| E18 | [播放 Service](../apps/api/internal/application/playback/service.go)、[播放 SQL](../apps/api/internal/infra/persistence/playback/gorm.go) |
| E19 | [消息 Service](../apps/api/internal/application/message/service.go)、[消息 SQL](../apps/api/internal/infra/persistence/message/gorm.go) |
| E20 | [账号 Service](../apps/api/internal/application/account/service.go)、[JWT](../apps/api/internal/infra/jwt/jwt.go)、[鉴权中间件](../apps/api/internal/interfaces/http/middleware/jwt_auth.go) |
| E21 | [上传 Handler](../apps/api/internal/interfaces/http/upload/handler.go)、[Range 测试](../apps/api/test/upload_static_range_test.go) |
| E22 | [HTTP 治理](../apps/api/internal/interfaces/http/middleware/governance.go)、[推荐熔断](../apps/api/internal/application/feed/governance.go)、[降级开关](../apps/api/internal/application/governance/switches.go) |
| E23 | [Metrics](../apps/api/internal/infra/metrics/metrics.go)、[Trace](../apps/api/internal/infra/observability/tracing.go)、[日志](../apps/api/internal/infra/observability/logging.go)、[HTTP 启停](../apps/api/internal/infra/httpgin/gin.go) |
| E24 | [Compose](../apps/docker-compose.yml)、[Kubernetes](../apps/deploy.yaml)、[迁移](../apps/api/internal/infra/persistence/migration/migration.go) |
| E25 | [压测脚本](../scripts/performance-load.js)、[优化记录](performance-diagnostic-hot-cache-2026-09-29.md)、[验收报告](performance-acceptance-2026-09-29.md)、[前期诊断](performance-diagnostic-medium-v1-2026-09-28.md) |
| E26 | [关系 Service](../apps/api/internal/application/relation/service.go)、[关系事务](../apps/api/internal/infra/persistence/relation/gorm.go) |
| E27 | [API 测试目录](../apps/api/test)、[推荐测试](../apps/api/internal/application/recommendation/service_test.go)、[缓存测试](../apps/api/internal/infra/cache/feed_cache_test.go) |

### 关键函数行号速查

行号以本次核查版本为准，后续源码调整后优先按函数名搜索。

| 功能 | 文件与起始行 |
| --- | --- |
| Feed 路由入口 | [internal/application/feed/service.go:264](../apps/api/internal/application/feed/service.go#L264) |
| 详情批量组装 | [internal/application/feed/service.go:519](../apps/api/internal/application/feed/service.go#L519) |
| 时间线 SQL | [internal/infra/persistence/feed/gorm.go:48](../apps/api/internal/infra/persistence/feed/gorm.go#L48) |
| 分批 Fanout | [internal/application/video/fanout_worker.go:114](../apps/api/internal/application/video/fanout_worker.go#L114) |
| 可靠事件调度 | [internal/application/eventbus/outbox.go:44](../apps/api/internal/application/eventbus/outbox.go#L44) |
| Kafka 消费去重窗口 | [internal/infra/mq/kafka.go:223](../apps/api/internal/infra/mq/kafka.go#L223) |
| 异步点赞 | [internal/application/interaction/service.go:315](../apps/api/internal/application/interaction/service.go#L315) |
| Hot 快照 | [internal/application/recommendation/service.go:238](../apps/api/internal/application/recommendation/service.go#L238) |
| 统一候选过滤 | [internal/application/recommendation/service.go:273](../apps/api/internal/application/recommendation/service.go#L273) |
| 曝光事务 | [internal/infra/persistence/exposure/gorm.go:35](../apps/api/internal/infra/persistence/exposure/gorm.go#L35) |
| 文本特征向量 | [internal/domain/embedding/hash_ngram.go:42](../apps/api/internal/domain/embedding/hash_ngram.go#L42) |
| 画像重算 | [internal/infra/persistence/recommendation/gorm.go:242](../apps/api/internal/infra/persistence/recommendation/gorm.go#L242) |

### 真实调用链和带中文注释的关键逻辑

以下片段是便于讲解的源码节选或等价简化，不作为完整可编译补丁；保留的函数名可在上述文件搜索。

**链路 A：时间线读取。** `cmd/feed/main.go → router.Register → feed.Handler → Service.GetFeed → TimelineStrategy.List → loadFeedPage → TimelineCandidateSource.Load → Repository.ListTimelinePage → CandidatePipeline.Run → FeedAssembler.Assemble → assembleFeedItems`。输入是 scene/cursor/limit/viewer，先得到排序 ID，再返回带作者、统计和用户状态的视频卡片。

```go
// E02：页缓存命中后，仍需单独组装卡片、计数和当前用户行为。
page, err := loadFeedPage(ctx, s.cache, s.scene, req.Cursor, limit,
    s.firstPageTTL, s.pageTTL, &s.group,
    func(loadCtx context.Context) (*FeedPage, error) {
        // E03 内部多取一条；游标来自排序边界，不是数据库 offset。
        return NewTimelineCandidateSource(s.scene, s.repo).Load(loadCtx, req, limit)
    })
// 后续执行去重、时间排序，再批量组装；err 的处理在完整源码中。
```

**链路 B：发布与分发。** `POST /api/videos → CreatePublished → Repository.Save`；Kafka 模式下一个事务写 `video + video_stat + outbox_event`。`cmd/worker → OutboxWorker → Kafka → fanout group → FanoutWorker`，另一个 embedding group 生成向量。输入是发布资料，最终生成权威视频记录、派生关注索引和向量。

```go
// E06：先预热，再根据当前粉丝规模决定发布扩散方式。
if followerCount >= domainfeed.BigCreatorFollowerThreshold {
    // 这是作者内容索引，不是保存待发消息的 outbox_event 表。
    return w.index.AddAuthorOutboxItem(ctx, event.AuthorID, item, w.outboxMaxLen)
}
// 小作者在循环中按 follower user_id 向后扫描，每批默认 500 人。
// AddInboxItems 用确定的 ZSET member，重做同一条不会添加重复成员。
```

**链路 C：互动。** `PUT like/favorite → Service.setAction → setActionAsync → GetVideoStat → FeedCache.SetActionState → PublishActionChanged → ActionWorker → Repository.SetAction`。Redis 更新和消息发送是两步，数据库事实和计数是同一个事务。输入是目标状态，接口返回快速计数；持久化随后完成。

```go
// E12：只有 Redis 判断状态发生变化，才发送事件。
if state.Delta != 0 {
    event := NewActionChangedEvent(userID, videoID, actionType, active, idempotencyKey)
    if err := s.actionPublisher.PublishActionChanged(ctx, event); err != nil {
        // 捕获到发送错误时同步落库；进程在发送前崩溃不经过此分支。
        return s.setActionSync(ctx, userID, videoID, actionType, active, idempotencyKey)
    }
}
```

**链路 D：推荐。** `RecommendStrategy.List → governedCandidateSource → recommendation.Service.Recommend → recallCandidates → 六路 ListCandidatesBySource → filterRecallPools → Interest/Similar 个性化 → mergeRecallPools → preRankCandidates → rankCandidates → cursor → interleaveByAuthor`。外层还并行取首屏 Hot/Latest/Following，再合并去重、Bloom 过滤、打散和组装。

```go
// E14：快照只保存不含用户过滤结果的 Hot 原始候选。
s.hotCandidateMu.Lock()
defer s.hotCandidateMu.Unlock()
if time.Now().Before(s.hotCandidatesExpireAt) && s.hotCandidateLimit >= limit {
    // 拷贝每个 Candidate，防止后面的打分污染其他请求看到的对象。
    return cloneCandidates(s.hotCandidates, limit), nil
}
// 未命中时持锁查库、复制保存，过期时间设为成功取回后的 5 秒。
// 六路取完后，仍对当前用户执行曝光、负反馈和屏蔽作者过滤。
```

**链路 E：行为反馈。** `POST /api/video-view-events → RecordViewEvent → SaveViewEvent → 事务内写流水/曝光聚合/Outbox → Kafka feature group → UserFeatureWorker → RefreshUserInterestVector`；recommendation group 仅对 exposed 事件维护 Seen，API 对 exposed 和负反馈同步尝试 MarkSeen。输入是观看行为，输出为去重状态及可用于后续排序的画像。

```go
// E17：同请求、同用户、同视频、同事件类型的重复写不再增加聚合计数。
result := tx.Clauses(clause.OnConflict{
    Columns: []clause.Column{{Name: "user_id"}, {Name: "video_id"},
        {Name: "request_id"}, {Name: "event_type"}},
    DoNothing: true,
}).Create(&eventModel)
// RowsAffected == 0 时加载已有记录并返回；首次插入才继续写聚合和 Outbox。
```

其他完整入口：账号 `Handler → account.Service → domain.User/Repository/JWT`；关系 `Handler → setFollow → 关系事务 → 索引失效/近期回填/通知`；消息 `Handler → message.Service → Repository`；播放 `Handler → playback.Service → Repository`；上传 `Handler.Create → 文件校验 → 保存 → ffprobe → ffmpeg → URL`。这些能力的业务边界分别在 Q041–Q042、Q115–Q124 展开。

## 三、架构与项目价值：Q001–Q008

### Q001：请用两分钟介绍 FluxFeed，突出你简历里的四条内容。

**回答模板：** FluxFeed 是基于已有 GCFeed 继续演进的 Go 短视频 Feed 项目，覆盖发布、内容分发、消费和反馈。我会先讲最高频的读链路：按场景选策略，取得轻量候选，再批量补齐卡片和计数；接着讲关注流按粉丝规模混合推拉，以及 Kafka 模式下部分写入的事务 Outbox。推荐部分使用六路候选、统一用户过滤和规则排序，观看行为会反向影响画像。

最有证据的优化是把推荐 Hot 原始候选缓存为 5 秒进程内快照。在固定 10 万视频数据集和单 API 的 5 RPS 历史对照中，P95 从文档记录的 267.58 ms 降到 62.42 ms。我会明确这是工程原型和限定实验；个人完成了哪些改造应按自己的提交和实际工作补充，不能把原项目全部算作独立开发。

**依据：** E01–E25；README 来源说明、简历四条主张。

### Q002：用户打开推荐页，一次请求到底经过哪些层？

**回答模板：** HTTP Handler 解析 scene、游标和登录身份，把它们组成 FeedRequest。Feed Service 经 Router 选 RecommendStrategy，策略调用受超时和隔舱保护的推荐服务，同时在首屏启动辅助源。推荐服务自己执行六路召回和规则打分，外层再做合并、去重、Seen 过滤及作者打散，最后由 Assembler 批量查卡片、统计和用户行为。

这意味着“请求推荐一次”不是“执行一条推荐 SQL”。性能分析必须拆开候选查询、用户过滤、向量读取和卡片组装，否则看见最终 P95 高却不知道该优化哪里。源码中主服务返回的推荐分没有完整带到外层 FeedPageItem，外层以保序方式处理主结果，也不能称为把所有来源统一重新精排。

**依据：** E02/E03/E04/E14。

### Q003：为什么要分 Domain、Application、Infrastructure、Interfaces？真的完全解耦了吗？

**回答模板：** 以点赞为例，Domain 定义行为类型、状态和仓储契约；Application 决定走同步还是异步、是否发通知；Infrastructure 实现 Redis 状态和 GORM 事务；Interfaces 处理鉴权、参数和 HTTP 响应。这样可以在测试中替换仓储，专门验证“重复点赞不加计数”等规则，而不要求每个测试都启动数据库。

但我不会说依赖方向完全纯净。Application 直接调用 infra/metrics，部分持久化仓储也依赖应用事件结构来构造 Outbox。这是当前实现的耦合点。若它们实际妨碍维护，再提取小型事件或观测接口；仅为了画出漂亮分层图而增加大量包装层，收益并不明确。

**依据：** E01/E02/E07/E12/E13。

### Q004：为什么选 Go、Gin 和 GORM，它们各自解决了什么？

**回答模板：** 这里主要是数据库、Redis、消息队列等 I/O 编排，Go 的 goroutine 和 context 适合并发召回及取消传播。Gin 提供 HTTP 路由和中间件，GORM 负责模型映射、事务和常见查询；复杂的协同过滤、索引修复则仍写明确 SQL。我会以代码中的 errgroup、WithContext 和 Transaction 说明具体用法。

选框架没有免除理解底层的责任。六路并发查询仍争用同一个数据库连接池，GORM 拼出的 SQL 仍可能扫描和排序大量行；索引有效性要看执行计划。项目性能改善也不是因为换了语言或 ORM，而是减少了重复昂贵工作。我不会声称项目比较过其他框架吞吐，仓库没有这样的对照实验。

**依据：** E03/E14/E15/E25、`apps/api/go.mod`。

### Q005：为什么 API 和 Worker 分进程，而不是每次请求开 goroutine？

**回答模板：** 发布接口的目标是可靠保存内容，不应该等几千粉丝索引和视频向量全部完成。独立 Worker 从消息系统消费任务，可以单独扩容或重启，也让 API 延迟与下游工作量部分解耦。若只在 Handler 里开 goroutine，请求返回后进程退出就可能把未完成任务直接丢掉，而且无法清楚观察积压。

但“进程分开”只是执行隔离，可靠性来自持久事件、重试、幂等和监控组合。当前发布在 Kafka 模式使用 Outbox，点赞仍存在 Redis 成功后尚未发消息的窗口，所以不能把独立 Worker 等同于所有任务必达。扩容前也要检查消费者组、分区和 Outbox 多实例竞争。

**依据：** E01/E06/E09/E10/E12。

### Q006：FeedRouter 和 CandidatePipeline 会不会过度设计？

**回答模板：** 当前已经存在 Timeline、Hot、Following、Recommend 四种真实场景，来源、排序和身份要求不同。Router 将场景选择从大分支中拿出来，Pipeline 把合并、去重、排序、打散和截断串成显式步骤，Assembler 则共享详情组装。这些接口目前有实际使用者，并不是只为未来想象的扩展预留。

我也会指出边界：PreserveOrderRanker 是有意保留来源顺序的实现，并不是一个复杂算法；注册新策略时，缓存和 Assembler 的注入仍有具体类型 switch，不能宣称完全无需修改装配。下一步应根据新增场景暴露的问题调整，而不是继续叠更多空接口。

**依据：** E02/E04。

### Q007：MySQL、Redis、MQ 各自保存什么，谁才是事实来源？

**回答模板：** MySQL 保存视频、关系、互动、观看事件和统计等持久事实，Redis 保存可重建的 Feed 索引、卡片、计数视图、分钟热榜及 Seen Bitmap，MQ 传递事件让 Worker 计算派生结果。关注缓存丢失后可以从当前关注关系和公开视频重建，这说明 Redis 索引不是唯一事实来源。

不过异步点赞收到请求时先改 Redis、后落 MySQL，因此在中间窗口，最新用户意图暂时只存在 Redis 和待发送事件里。这个例外直接影响恢复策略：不能简单说“Redis 丢了全部从库恢复就零损失”。要先判断数据库是否已经收到该意图，再决定能保证的是持久事实恢复还是完整写入无损。

**依据：** E05/E06/E12/E13/E17。

### Q008：这个项目究竟解决什么业务问题？只是堆中间件吗？

**回答模板：** 我会用刷短视频的三个体验解释：内容要快出现，连续翻页要可控，用户看过或明确不喜欢的内容应减少重复。分页和缓存对应读取体验，混合分发对应发布可见性，曝光和负反馈对应内容选择；消息、上传和播放提供完整可演示的业务闭环。

Kafka 和多路召回不是项目价值本身。若小规模场景用 MySQL 同步事务和简单时间线就满足要求，完全可以保持简单。我的项目价值在于能用具体代码和实验解释何时瓶颈出现、改了哪段工作以及付出怎样的一致性代价。目前没有留存、完播提升或商业收益数据，这些只能作为后续产品验证目标。

**依据：** E02/E06/E14/E17/E25。

## 四、分页与批量读取：Q009–Q018

### Q009：为什么时间线用游标，不直接用 page 和 offset？

**回答模板：** 时间线在用户翻页时不断插入新视频。若第一页取完又新增两条，第二页按固定 offset 取，边界就可能移动，用户看到重复项；深 offset 还可能让数据库处理许多最终不返回的行。项目把最后一条的发布时间和视频 ID 放进游标，下一页只查这个边界之后的内容。

例如第一页结束于 `(10:00, 100)`，下一页条件是时间早于 10:00，或者时间相同且 ID 小于 100。新发布的更晚内容不会推移这个条件。代价是不能直接跳到第 100 页，而且这只是对排序字段相对稳定的内容有效，不是跨所有请求持有数据库快照。

**依据：** E02 `parseTimelineCursor`；E06 `ListTimelinePage`。

### Q010：只用发布时间当游标不行吗？为什么加视频 ID？

**回答模板：** 时间戳不是唯一的，多条视频可能在相同数据库精度下拥有同一个发布时间。若下一页只用 `< published_at`，同一时间尚未返回的条目会被跳过；如果用 `<=`，又会把已返回条目带回来。加唯一 ID 后，排序和过滤都用同一对字段，形成确定的先后关系。

比如时间相同的 105、104、103 三条，第一页到 104 就结束，下一页还能取到 103。项目按发布时间倒序、ID 倒序，并把时间编码为 RFC3339Nano。验证时要专门构造同时间数据，不能只用每条时间都不同的样例，否则这个设计的价值根本没有被测到。

**依据：** E02/E06；E27 Feed 分页测试。

### Q011：这个 SQL 应该建什么索引，为什么字段顺序这样安排？

**回答模板：** 项目显式创建 `video(status, published_at DESC, id DESC)`。查询先限定已发布状态，再沿发布时间和 ID 的倒序边界读取，索引前缀与主要过滤、排序相匹配。如果把经常不限定的字段放在前面，就可能破坏这种访问方式。复合索引的前缀规则可对照 [MySQL 官方说明](https://dev.mysql.com/doc/refman/8.4/en/multiple-column-indexes.html)。

我不会说这个索引一定覆盖全部查询，因为轻量页还取 author_id，而索引定义没有显式包含它。实际是否回表、扫描多少行、是否 filesort，要在真实数据上检查执行计划。增加 author_id 也会扩大索引和写入成本，应先测收益；推荐 Hot 的计算热度排序更不能指望这个时间线索引直接解决。

**依据：** E06 `EnsureTimelineIndex`、`basePageQuery`。

### Q012：为什么多取一条？空页和缺失卡片会影响 has_more 吗？

**回答模板：** Source 用 `limit+1` 查询，超过 limit 就说明至少还有候选，裁掉多出的条目后用最后一条生成游标。这样不需要对每次刷流执行总数统计。数据库候选为空时返回空数组，避免客户端区分 null 和数组。

但是当前 has_more 来自候选页，后面去重、Seen 过滤或卡片缺失可能让实际返回不足 limit，甚至空页但仍有下一页。Assembler 不会自动补拉，所以不能把“返回少于一页”直接解释为结束。改进可以设有界补拉和扫描预算；测试应分别验证候选耗尽与展示过滤造成的空页。

**依据：** E03 `timelinePage`；E04 `assemblePipelineResult`；E02 `assembleFeedItems`。

### Q013：热榜游标和时间线游标一样稳定吗？

**回答模板：** 不一样。无 Redis 时热榜使用热度、发布时间和 ID 的复合游标，但热度会随互动变化；启用 Redis 时游标携带分钟级 window_end 和 offset，读取该窗口的聚合 ZSET。固定窗口键让短时间连续翻页更可控，却不是永久固定的榜单版本。

聚合结果只缓存两分钟，过期后相同窗口可能重新合并，而分钟桶也会过期。因此用户很久后继续翻页，或者榜单重建时，可能出现结果变化。严格稳定需要独立快照版本、明确有效期和过期响应。我会把现状表述为“窗口内分页”，不承诺动态热榜绝不重复遗漏。

**依据：** E02 `hotCursorPayload`；E03 `HotCandidateSource.Load`；E05 热榜方法。

### Q014：推荐有 rank_score 游标，就能避免所有重复和遗漏吗？

**回答模板：** 不能。推荐游标保存分数、发布时间和 ID，但每次请求都会重新召回、重新读取画像和互动数据，并计算新鲜度。即使 now 按分钟截断，跨分钟或发生行为反馈后，原来在边界上方的内容也可能掉到下方，排序集合本身已经变化。

项目还在精排后先截断到默认最多 50 条或 `limit+1`，再应用游标，实际是有界候选的连续读取。严格的会话翻页需要保存一段候选 ID/分数快照并绑定用户、排序版本和有效期；曝光去重可减少重复，但不能替代快照。面试时我会主动解释这两种保障的差别。

**依据：** E14 `Recommend`、`filterByCursor`。

### Q015：Base64 编码的游标安全吗？为什么不是加密？

**回答模板：** Base64 只是把 JSON 变成适合 URL 传递的字符串，任何客户端都能解码和构造。当前代码验证编码、时间格式和正数 ID，主要防止不合法输入；权限仍由登录身份、公开视频条件和关注关系控制，不能相信游标自带的任何权限含义。

如果以后游标绑定昂贵快照或包含用户专属状态，我会加入版本、用户范围、有效期及 HMAC，防止篡改后跨用户复用，并限制游标长度。当前用 SHA-1 摘要生成页缓存 key 只是缩短 key，并不构成签名或鉴权。这是编码、摘要和安全校验三个不同用途。

**依据：** E02 `parseTimelineCursor`、`feedPageCacheKey`；E14 `parseCursor`。

### Q016：ID-Detail 分离究竟少做了哪些工作？

**回答模板：** 页查询只取用于排序和定位的 video_id、author_id、published_at 等轻量信息，不把标题、作者头像和每种计数都拼进页缓存。拿到 ID 后，再对缺失卡片批量联查视频和作者，对缺失统计批量查询 video_stat；不同页面还可以共享相同视频的详情缓存。

好处是列表排序和字段变化不必绑在一个大对象里：一个热门视频出现在多个用户列表，卡片可以复用，而用户的 liked 状态仍单独读取。代价是增加了组装步骤和多层一致性边界。对于很小、几乎不复用的数据，一条联表 SQL 可能更简单，所以我会拿实际查询量和延迟来解释这次拆分。

**依据：** E02 `assembleFeedItems`；E06 三个 BatchGet 方法。

### Q017：批量 IN 查询不保证顺序，最后怎么保证 Feed 顺序？

**回答模板：** 仓储把卡片和统计返回为以 video_id 为键的 Map，Assembler 最后遍历原来的 pageItems，逐项查 Map，按原候选顺序构造结果。这样数据库按任何顺序返回，都不会破坏时间线或推荐结果，重复 ID 也会在前面的管线中处理。

遇到卡片查不到，代码跳过该条；统计缺失则构造零计数。这两个降级选择不同，因为没有媒体和标题的卡片没法展示，但缺统计还可以正常消费。测试时可以让假仓储故意打乱返回顺序、缺少一张卡片、缺少一份统计，验证结果序列及缺省行为，而不是只比条目数量。

**依据：** E02 `feedPageVideoIDs`、`assembleFeedItems`；E06。

### Q018：用户刚点赞完，刷新 Feed 为什么可能还是未点赞？

**回答模板：** 异步点赞的接口先改 Redis 并返回成功，Worker 稍后才写数据库，而 FeedAssembler 的 `BatchGetViewerActionStates` 读取 MySQL。两条路径看到的时间点不同，所以总数可能已经变化，当前用户的 liked 却短暂还是 false。这是读己之写体验的缺口，不是批量查询顺序问题。

当前前端可暂存乐观状态来减轻闪烁，但服务端也可以对当前用户优先读短期状态缓存，再批量回数据库。改造要保留取消点赞、多个设备操作和事件失败的语义，不能无限覆盖数据库结果。验证应在暂停 Worker 时执行点赞和刷流，再恢复消费观察收敛。

**依据：** E12 `setActionAsync`；E06 `BatchGetViewerActionStates`。

## 五、缓存深挖：Q019–Q030

### Q019：为什么拆成页、卡片和计数三个缓存？TTL 是多少？

**回答模板：** 它们变化频率和复用范围不同。页保存排序候选，Timeline 首页基础 TTL 为 5 秒、后续页 45 秒，并追加 10%–20% 的 key 确定性抖动；卡片基础 TTL 为 15 分钟，计数为 15 秒，缓存写入还有自己的抖动处理。当前用户行为不放进共享卡片。

比如作者改头像只影响展示字段，点赞只影响统计，不应使所有含该视频的页都重新排序。代价是同一响应的数据版本可能不完全一致，所以这些是展示缓存，不是强一致视图。回答 TTL 时还应区分 Redis 与 L1；尤其本地页从主动 SetPage 和 Redis 回填进入时，保留策略不同。

**依据：** E02 常量、`feedPageCacheTTL`；E05 `SetPage/SetCards/SetStats`。

### Q020：项目里的 L1 真的是 LRU 吗？怎么避免共享对象被修改？

**回答模板：** 不是 LRU。它是带互斥锁的有界 Map，上限 10000 个条目；容量满时先清理过期项，再任意淘汰一项。保存序列化字节，读写时复制字节切片，业务层反序列化成对象，避免请求直接改到共享对象。这比维护访问链表更简单。

限制也很明确：条目数上限不等于字节总量上限，任意淘汰可能降低热点命中率，满容量时清理会扫描 Map。若未来出现 L1 锁等待或缓存内存膨胀，我会用 profile 和命中指标判断是否需要按字节限制或成熟缓存实现，不把当前 Map 包装成高级淘汰算法。

**依据：** E05 `localCache`、`setStale`。

### Q021：singleflight 怎么合并回源，为什么闭包里面还要再查缓存？

**回答模板：** Timeline 缓存未命中时，以 scene、limit 和 cursor 生成 key，进入同一 Strategy 的 singleflight.Group。相同 key 的并发请求共享一次 load 的结果，闭包内再次读缓存，是为了处理“外面未命中，但排到我执行前其他请求已填好”的情况，避免多做一次数据库查询。

它只合并同时在途的请求，不持久保存结果，也不提供跨 API 实例互斥。比如三个副本同一时刻失效，最多仍可能各回源一次。这里先靠 L1/L2 和 TTL 控制成本；只有多个副本确实造成昂贵重复时，才考虑共享重建或其他策略，不能把 singleflight 说成分布式锁。

**依据：** E02 `loadFeedPage`。

### Q022：详情批量回源用了 singleflight，是不是任意重叠 ID 都会合并？

**回答模板：** 不会。`idListKey` 把 ID 按当前顺序拼成字符串，只有完全相同的 ID 序列才使用同一 key。请求 A 缺 `[1,2]`，B 缺 `[2,3]`，两次回源不会自动合成 `[1,2,3]`；即便集合相同但顺序不同，也可能不能合并。

这是简单实现的范围限制。如果实际很多请求缺同一批 ID 但顺序不一，可以对用于生成 key 的副本排序，同时保留展示顺序；真正跨请求合批则需要等待窗口、取消处理和逐请求错误分发，复杂度更高。当前先通过 Redis 批量读取和共享卡片降低这种重叠发生率。

**依据：** E02 `loadCards`、`loadStats`、`idListKey`。

### Q023：首个回源请求取消，会不会影响 singleflight 的其他等待者？

**回答模板：** 当前同步回源闭包使用进入该次执行的请求 ctx，如果它提前取消，数据库查询可能结束并把错误共享给其他等待者。反过来，使用 `Do` 等待共享结果的请求，也不是天然能按自己的 deadline 立刻退出。这是代码选择带来的并发边界。

如果业务要求更清晰的取消语义，我会区分共享加载的有限生命周期与各调用者等待期限，例如用有上限的独立加载 context 配合可选择等待的接口，而不是使用永不超时的 Background。验证时构造一个早取消的领头请求和多个正常请求，观察回源次数、结果以及 goroutine 是否及时退出。

**依据：** E02 `loadFeedPage`、`loadCards`、`loadStats`。

### Q024：软过期如何减少用户等待？是否可能出现后台刷新风暴？

**回答模板：** 主动 SetPage 时，L1 保存 freshTTL 和两倍的 hardTTL；超过新鲜期但未到硬过期时仍可返回旧页，同时启动后台刷新。刷新使用最长两秒的独立 context，并经过相同 key 的 singleflight，所以用户不用都等待数据库，而在途重建能够合并。

但每次 stale 命中仍可能创建一个 goroutine；singleflight 合并的是重叠执行，不保证整个旧值窗口只启动一次刷新。若上一轮快速失败，后续请求仍会反复重建。这种情况应该观察刷新失败率、后台任务数量和数据库压力，必要时增加刷新中标志和短退避。旧数据容忍也不适用于下线权限等强约束。

**依据：** E02 `getCachedPage/refreshFeedPage`；E05 `GetPageState/SetPage`。

### Q025：TTL 抖动是随机的吗？能解决哪些问题，不能解决什么？

**回答模板：** 当前由 key 的 FNV 哈希计算固定百分比，不是每次写入重新随机。相同 key 有稳定 TTL，不同 key 会散落在一段时间范围内，减少一批缓存同时写入后一起过期。Timeline 页的范围是基础 TTL 加 10%–20%。

它解决的是不同 key 的集中失效，不会把同一热门 key 的一万个请求拆开；后者依赖 singleflight、旧值服务或预热。更不能解决 Redis 整体不可达，因为那时所有 key 都可能回源。验证抖动要看批量 key 的过期分布，验证击穿则要压同一 key，这两类实验不能混为一谈。

**依据：** E02 `feedPageCacheTTL`；E05 `jitterTTL`。

### Q026：不存在的视频怎么避免反复穿透到数据库？

**回答模板：** Assembler 批量查卡片后，对确实没查到的 ID 补 nil；缓存层把 nil 编成特殊标记 `-`，使用约 30 秒的短缓存。再次读取时 Map 里有这个 ID 且值为 nil，组装会跳过，不再把它当作普通缓存缺失回源。

负缓存期限较短，是因为暂时不存在和永久不存在不完全一样，例如内容发布与派生索引到达时间不同。数据库错误不能写成“视频不存在”，否则短暂故障会变成持续漏内容。测试要分开“查询成功但空结果”和“查询失败”，并检查空标记过期后能恢复新出现的视频。

**依据：** E02 `missingCardIDs`、`assembleFeedItems`；E05 `missingCardValue/SetCards`。

### Q027：Redis 挂掉以后，Feed 一定能回数据库吗？

**回答模板：** 要按链路回答。Timeline 页读取异常会尝试数据库，卡片和统计也有批量补缺；Following 索引不可用时可落回 SQL。但 Hot Source 在 cache 不为 nil 时只走 Redis 窗口，读取失败返回错误，并不会自动换成累计热度 SQL。推荐主链路失败后的兜底还能继续尝试 Timeline。

因此简历中的“缓存降级”可以描述已接入回源的路径，不能扩大成所有场景无影响。即使有回源，缓存故障也会放大数据库负载，需要限流和隔舱配合。若要补 Hot 降级，还必须定义一分钟热榜变成累计热榜后的产品语义及游标兼容，不能悄悄切换。

**依据：** E02/E03 `HotCandidateSource.Load`、`FollowingCandidateSource.Load`。

### Q028：缓存 key 为什么不带用户 ID？会串用户的点赞状态吗？

**回答模板：** 通用页缓存当前主要用于 Timeline，其候选列表与用户无关，所以 key 使用 scene、limit 和 cursor。卡片、统计也都是公共字段。用户 liked/favorited 在组装最后根据 ViewerID 单独批量读取，因此不会把 A 的点赞状态存成 B 的公共卡片。

这个前提不能推广到推荐或关注页。如果以后复用 `loadFeedPage` 缓存用户专属结果，却仍沿用不带 ViewerID 的 key，就会出现串结果。代码审查时我会检查缓存对象是否含个性化字段，而不是只看 key 名字；双用户测试应构造不同关注和互动，验证共享公共字段但私有状态不同。

**依据：** E02 `feedPageCacheKey`、`assembleFeedItems`。

### Q029：作者改资料或删除视频后，旧卡片会立即消失吗？

**回答模板：** 当前账号更新和视频软删除没有接入完整的卡片主动失效链，卡片缓存可能继续保留旧昵称、头像或媒体信息。数据库补查会过滤非发布视频，但命中缓存时不一定重新检查状态，因此不能用“数据库做了软删除”推导“所有缓存立刻不可见”。

对于普通资料可以接受有限延迟，而删除或审核下线往往需要更严格保障。我会让状态变化触发卡片失效，并在最终输出增加轻量可见性过滤或版本校验；多副本 L1 也要考虑广播或短 TTL。验证要先把页和卡片预热，再删除并访问多个实例，才能覆盖真正风险窗口。

**依据：** E07 `Delete`；E20 `UpdateProfile`；E02/E05。

### Q030：你怎么证明缓存优化有效，而不是仅仅增加代码？

**回答模板：** 我会测三种状态：冷缓存看回源和构建成本，热缓存看稳定命中延迟，批量失效或缓存故障看数据库峰值。指标至少包括分层命中率、请求 P95/P99、数据库查询耗时及连接等待、同 key 的实际加载次数；不能只看 Redis 的总命中率。

项目已有缓存、Feed 和推荐指标，也有缓存单测及 benchmark，但微基准不能代替完整 HTTP 链路。最有说服力的是 Q103 起的固定数据集对照：减少 Hot 排序后 CPU 降幅远大于查询次数降幅，说明节省的是昂贵计算。其他缓存不能借用这组数据宣称同样幅度的收益。

**依据：** E23/E25/E27、`internal/infra/cache/cache_benchmark_test.go`。

## 六、关注流与分发：Q031–Q042

### Q031：为什么关注流要混合推拉，纯推或纯拉的问题在哪里？

**回答模板：** 纯推是在发布时给每个粉丝写索引，读便宜但发布成本随粉丝数增长；纯拉是在读时查询关注作者的内容，发布便宜但关注很多作者时读取更重。项目以一万粉丝为阈值，小作者写各粉丝 Inbox，大作者只写自己的 Author Outbox，用户读时把它们合并。

例如普通作者 300 个粉丝，推送成本可控；百万粉丝作者每次发布都同步扩散就很昂贵。阈值一万是当前实现参数，不是经实验得出的普适最优值。真正调参还要考虑发帖频率、活跃粉丝比例、每用户关注数及读写成本，而不仅看粉丝总量。

**依据：** E06 `HandleVideoPublished`；`domain/feed/entity.go`。

### Q032：Inbox 和 Author Outbox 保存在哪里，结构是什么？

**回答模板：** 当前 Fanout Worker 写 Redis ZSET。用户 Inbox 默认保留 1000 条，作者 Outbox 保留 500 条，写入后裁掉更老成员并刷新约 30 天 TTL。成员包含发布时间、视频 ID 和作者 ID，不包含完整卡片，所以它们是候选索引，详情仍由 Assembler 统一加载。

数据库虽然迁移了 InboxModel，但不能据此说 Worker 正在向数据库 Inbox 表分发。实际调用指向 FeedCache.AddInboxItems/AddAuthorOutboxItem；Following SQL 兜底从公开视频和当前关注关系查询。这种索引可重建方案减少了数据库 fanout 写放大，但 Redis 丢失后会带来冷启动回源。

**依据：** E01 `startWorkers`；E05/E06/E24。

### Q033：ZSET 的 score 为什么全部是零？时间不是应该放 score 吗？

**回答模板：** 这里为了保持纳秒时间和大 ID 的精确顺序，把时间、视频 ID、作者 ID 都格式化成固定宽度十进制字符串，所有 score 为零，再使用反向字典序范围读取。这样先比时间，再比视频 ID，同一时间的边界也能精确表达。

如果把纳秒时间直接塞进浮点 score，或把时间和 ID 拼进一个超大的数字，会有整数精度问题。代价是 member 更长、格式变更需要版本化，而且字典序方案要求字段宽度和正值假设成立。项目有同时间排序测试，我会用相同纳秒、相邻大 ID 的例子说明，而不是只背 ZSET 的常规用法。

**依据：** E05 `followingIndexMember`、`followingCursorPrefix`；E27 缓存测试。

### Q034：分批 Fanout 怎么避免漏粉丝？为什么按 ID 游标？

**回答模板：** Worker 默认每批查 500 个当前有效粉丝，按粉丝用户 ID 升序扫描，下批条件是大于上一批最后一个 ID。每批用 Pipeline 给这些用户写同一个视频索引，成功后推进内存中的 cursor，直到不足一批或没有结果。这避免一次加载整个粉丝集合。

它避免了 offset 受删除挪动的部分问题，但没有持有全体粉丝的一致性快照。Fanout 期间刚关注的用户可能落在已扫描范围，项目通过关注后的近期回填补充可见性；取关则在读时按当前作者集合过滤。答案应承认这些是不同链路共同支撑，而不是一个 ID 游标解决所有关系变更。

**依据：** E06 `ListFollowerIDs`、`HandleVideoPublished`；E26。

### Q035：处理到第三批崩溃，再消费会发生什么？

**回答模板：** 当前没有把 fanout 进度保存成持久任务，事件重投后从 cursor=0 重新扫描。之前成功写过的用户也会再写，但 ZADD 使用相同确定性 member，因此不会多出重复视频。代价是重做成本，尤其故障发生在大批次任务末尾时。

如果实际 fanout 重试代价过大，可以把任务拆为可持久恢复的批次，记录 event_id、作者、游标和状态；不能先记录游标成功再执行写入，否则崩溃会漏掉整批。更简单的起点是保留可重试幂等写、监控耗时和失败位置，再根据规模决定是否需要任务进度表。

**依据：** E06 `HandleVideoPublished`；E05 `AddInboxItems`。

### Q036：Redis 索引被裁剪后，用户继续翻旧内容怎么办？

**回答模板：** Following Source 不把有界缓存当成全量历史。冷启动时从 SQL 取 1001 条，最多放 1000 条，保存 complete 标志和最老边界；当游标进入不完整快照覆盖不到的范围，就回数据库。读索引时还检查集合是否达到容量以及页尾是否碰到裁剪边界。

例如用户有 3000 条历史，不能看到 Inbox 只有 1000 条就声称已经没有下一页。代码中的这些标记是在区分“确实结束”和“缓存不知道后面还有什么”。测试应超过 1000 条并跨过缓存边界，也覆盖多个大作者 Outbox，其中某个已裁剪但另一个仍有新内容的合并情况。

**依据：** E03 `FollowingCandidateSource.Load`；E05 `ListFollowingIndexPage`。

### Q037：Redis 清空后，关注流如何冷启动？会不会压垮数据库？

**回答模板：** Source 先检查 FollowingSnapshot 是否 ready，不存在时查 SQL 取得有界快照，写入索引并记录边界；如果缓存读取或写入失败，仍可用 SQL 返回结果。因此单个用户不会因为索引丢失就永远没有内容。

但当前这段冷启动没有像 Timeline 那样明确用 singleflight 合并同用户重建。Redis 大面积清空加上用户并发刷新，可能产生重复 SQL 和重建开销。我会优先加按用户的有限并发合并或分批预热，并保留数据库限流；压测要专门测全冷启动，不能拿稳定热缓存的几毫秒延迟代表灾后恢复能力。

**依据：** E03 `FollowingCandidateSource.Load`；E05 `BootstrapFollowingIndex`。

### Q038：读时如何合并多个作者 Outbox？时间复杂度怎样？

**回答模板：** 代码对 Inbox 和各拉模式作者 Outbox 发起批量范围查询，把成员解析为轻量候选，按当前关注作者过滤，用 video_id 去重，然后在应用层按时间和 ID 排序后截断。不是实现了专门的堆式多路归并；当前直接收集排序更容易检查正确性。

如果关注了 K 个大作者，每个取 L 条，合并前候选上界大致随 K×L 增长，排序约为 O(M log M)。K 大时首先要控制 Redis 查询和候选量，再评估用堆按需归并；只替换 sort 而仍一次取回大量成员，可能并不能解决网络和解析成本。

**依据：** E05 `ListFollowingIndexPage`、`sortFeedPageItemsByTimeline`。

### Q039：作者粉丝跨过一万阈值时，历史内容会丢吗？

**回答模板：** 发布时按当时粉丝数决定推或拉，读取时又查询当前大作者集合，所以跨阈值会遇到新旧索引布局不同的问题。原来推过的历史可能在粉丝 Inbox，之后的新内容在作者 Outbox；反向变化也可能留下旧 Outbox。当前没有完整模式迁移状态机。

因此我只能说已有推拉选择和数据库兜底，不能保证所有升降级边界都无缝。需要专门构造 9999→10000→9999 的关注变化，在已有缓存情况下发布和翻页。后续可固定作者模式版本、迁移窗口双读并补齐索引，再逐步清理旧模式；是否采用取决于这一转换的真实频率。

**依据：** E06 `CountFollowers/ListFollowingPullAuthorIDs`；E03/E05。

### Q040：Feed 预热是不是把所有粉丝的首页都提前生成？

**回答模板：** 不是。当前 Preheater 只按视频 ID 批量读取并缓存该视频的卡片和统计，卡片按 15 分钟基础 TTL，统计按 15 秒；随后 Fanout 再写候选索引。它降低的是新视频第一次组装详情的回源成本，没有为每个粉丝生成完整首页响应。

先预热再分发有利于读到新索引时详情已经准备好，但也把预热故障和 fanout 绑在一起：预热返回错误，Worker 会停止本次分发。是否允许预热失败后继续 fanout，是可用性与回源压力的取舍；我会用预热失败率和首读数据库负载决定，而不把缓存优化自动提升为发布硬依赖。

**依据：** E06 `PreheatFeedVideo`、`HandleVideoPublished`。

### Q041：新关注和取关分别怎么处理旧索引？

**回答模板：** 关系先在数据库事务中改状态和双方统计，随后 Service 使当前用户关注索引失效；新关注小作者还会读取其近期视频并回填 Inbox，大作者由拉模式读取。取关后即使旧 Inbox 暂存该作者的视频，读索引也按当前关注作者集合过滤。

关系作者列表自身也有缓存，所以失效链路非常关键。若数据库提交成功但 Redis 失效或回填失败，接口会返回回填错误，事实状态却已经改变。客户端重试必须幂等，服务端也应把“事实成功、派生视图待修复”当成可观测状态，不能假装是整体数据库回滚。

**依据：** E26 `setFollow/backfillFollowFeed`；E05 `InvalidateFollowingIndex`。

### Q042：A 关注 B、B 同时关注 A，会有什么并发风险？

**回答模板：** 关系仓储先锁操作者账号，再锁目标账号。A→B 的事务可能先拿 A，B→A 的事务先拿 B，随后都等待对方，形成死锁条件。事务能保证一次提交的原子性，但不会自动保证所有加锁顺序无死锁。

我会统一按较小用户 ID 到较大用户 ID 的顺序获取相关锁，并对可重试死锁设置有限重试与抖动；重试必须覆盖整个事务。验证需要真实 MySQL 并发执行相向关注，检查最终两边关系和计数，而不是只在内存假仓储中串行调两次。这里是代码审查可见风险，本文没有伪造线上死锁事故。

**依据：** E26 `Repository.SetFollow`、`lockNormalUser`。

## 七、可靠事件与一致性：Q043–Q056

### Q043：为什么已有 RabbitMQ 还要接入 Kafka？现在实际用哪个？

**回答模板：** 初期 RabbitMQ 已支持动作、发布和观看事件；后续统一事件信封和多消费组需求通过 Kafka 适配。API 和 Worker 在有 brokers 配置时尝试 Kafka，成功就用 FeedEventBus；初始化失败才回退 RabbitMQ。因此不能只根据 README 说系统只有 RabbitMQ，也不能说它们对同一事件始终双写。

Kafka 的实际用途是让 Fanout、Embedding、Feature 等消费路径各自订阅处理。选型理由可以讲事件多订阅与回放需求，但当前实现是否真的提供一键业务回放，还要看运维工具。没有基准实验就不声称 Kafka 在本项目一定比 RabbitMQ 快；更关键的是消息模式与故障边界是否清楚。

**依据：** E01 `newApplicationEventPublisher/newWorkerEventBus`；E08/E09。

### Q044：同一个视频发布事件，怎么让分发和向量 Worker 都收到？

**回答模板：** Kafka 适配器对同一个 VideoTopic 使用不同消费组：fanout group 处理关注分发，embedding group 处理向量。组内多个实例用于分摊分区工作，组间各自维护消费进度，因此两种业务都能看到事件。若错误地配置为同一组，事件可能被其中一个处理，另一个就拿不到完整流。

消费去重键也包含 group_id，避免 Fanout 标记完成后，Embedding 因为相同 event_id 被跳过。验证不是只看生产者发送成功，而是让同一事件在两组都产出各自派生结果，并暂停其中一组，确认另一组继续推进、暂停组恢复后补齐。

**依据：** E08 `consumeVideoPublished`；E11；Kafka 集成测试。

### Q045：为什么要统一 Event 信封，而不是直接发业务 JSON？

**回答模板：** 统一信封有 ID、Type、UserID、VideoID、Timestamp 和 Payload。消息系统可在不理解每个业务字段的情况下做分区、去重、指标和日志，业务 Worker 再把 Payload 解码为发布或行为事件。这样切换传输实现时，不必让每个应用服务直接依赖 Kafka Record。

但当前信封没有显式 schema_version，事件字段演进还需约定兼容性，例如新增可选字段、保留旧消费者可解码的语义。event_id 表示事件身份，request_id 表示请求关联，两者不能随意替代；同一请求可以产生多个事件，而重投同一事件应保留相同 ID。

**依据：** E08 `Event/New/Decode`；E09 `eventPartitionKey`。

### Q046：Transactional Outbox 解决的是哪一个失败窗口？

**回答模板：** 若先提交视频再发 MQ，进程恰好在中间崩溃，视频存在但分发事件永久缺失。Kafka 模式下，Repository.Save 在同一个 MySQL 事务里写视频、初始统计和 outbox_event，事务提交代表事件也有了持久记录。Worker 再轮询待发事件，发送成功后标记 published。

它把“业务是否提交”和“事件是否有记录”绑定起来，没有让 MySQL 和 Kafka 成为一个原子事务。发送成功但标记失败仍会导致重复投递，所以必须配合消费者幂等。这里的 Outbox 是可靠事件表，和关注流作者 Outbox 的候选 ZSET 完全不同，面试时要先消除名称歧义。

**依据：** E07 `Repository.Save`；E10。

### Q047：所有写接口都已经用 Outbox 了吗？

**回答模板：** 没有。装配代码明确启用的是视频发布、评论创建、观看事件和关注变化的数据库 Outbox；点赞收藏仍是 Redis 状态成功后直接投递事件，站内通知也主要是 Service 直接调用消息写入。这是判断可靠性时最容易被“项目有 Outbox”掩盖的事实。

比如 Redis 点赞成功后进程崩溃，还没发送 Kafka，就没有对应数据库 Outbox 待补发；短 TTL 不会凭空补出丢失意图。要补强可以让持久化命令和事件先落库，或引入能够与状态操作原子记录待发意图的方案。取舍是延迟、复杂度与数据保障，必须单独设计和验证。

**依据：** E01 `Register` 的 Enable 方法；E12 `setActionAsync`。

### Q048：Outbox 发送失败怎么重试，什么时候停止？

**回答模板：** Worker 默认每 500 毫秒调度一轮，每次取最多 100 条到期 pending 记录，逐条发送。失败次数加一，按指数退避设置下次尝试时间，延迟从一秒增长到上限段；达到十次尝试后标记 failed。成功后记录 published 时间。

这种设计避免一条短暂故障事件被永久遗忘，但不是无限保证最终成功。failed 需要告警和明确的修复、重放流程；目前接口主要提供 pending/failed 列表，没有完整重放工作流。还应观察 oldest pending age，因为 pending 数量不大也可能有一条关键发布事件长期卡住。

**依据：** E10 `DispatchOnce/FetchPending/MarkFailed`；E01 `monitorOutbox`。

### Q049：Outbox 发送成功、标记失败会怎样？可以叫 exactly-once 吗？

**回答模板：** 下轮仍把这条 pending 发出去，消费者收到相同 event_id，所以整体按至少一次投递考虑。Kafka 消费前查 `(group_id,event_id)`，业务处理成功后再写完成标记，并最后提交消费位点。这个顺序减少已完成事件被重复执行，但不构成端到端 exactly-once。

关键窗口是业务已提交、MarkProcessed 还没成功时崩溃，再次消费仍会执行业务。对数据库业务可以把幂等记录和事实更新放同事务，Redis 派生索引则依靠确定性写入或版本比较。正确说法是“重复投递可出现，通过幂等让部分业务效果收敛”，而不是“消息只会来一次”。

**依据：** E09 `handleEvent`；E10/E11/E13。

### Q050：消费去重先查再写，遇到并发还可靠吗？

**回答模板：** 两个执行者可能都在 IsProcessed 时看到未处理，随后分别执行业务；最终唯一键只能防止重复插入完成标记，不能撤销已经发生的两次副作用。Kafka 正常分区分配减少同记录并发，但重平衡、外部重放或重复业务事件仍要求处理器本身可重复执行。

项目的 Fanout ZADD、向量 Upsert 和从权威数据重算画像比较适合重复处理。计数直接无条件加一、发外部通知就不合适，必须把事件去重和增量事务化。验证应该故意在处理完成但标记前中断，再投同一事件，检查业务结果，而不只检查 consumer_event 表只有一行。

**依据：** E09/E11；E06/E13/E16。

### Q051：多个 Outbox Worker 同时运行会重复领取吗？

**回答模板：** 会有这种可能。FetchPending 只是查询 status 和 next_attempt_at 后按 created_at 排序，没有领取租约、行锁或 SKIP LOCKED 协议。两个 Worker 可以取到同一批，然后都发布。同 event_id 能帮助下游幂等，但额外流量和 attempts/status 竞争仍然存在。

当前单 Worker 场景可以先接受重复投递的设计边界；扩容前应补明确领取协议，例如事务内标记带过期时间的 processing 状态，崩溃后可重新领取。不能在持有数据库锁期间无限等 MQ，否则会把网络故障转为长事务。方案要同时解释独占、超时恢复和重复处理，而不是只加一把锁。

**依据：** E10 `FetchPending/DispatchOnce`。

### Q052：毒消息如何处理？DLQ 可用就能说自动恢复了吗？

**回答模板：** Kafka 对解码失败或缺少 ID/Type 的消息转发到原 topic 加 `.dlq`；业务失败则先有限重试，再转 DLQ，附带原 topic、消费组、错误和尝试次数。DLQ 发送失败时继续等重试，避免把原消息位点推进后连失败记录也丢了。

DLQ 只是隔离和留证，不是修复业务。重放前要定位格式、数据或依赖问题，保留原事件身份，并明确面向哪个消费组恢复，避免其他组重复副作用。当前没有完整可审计的重放管理台。DLQ 不可用时消费会受阻，所以既要监控新增数，也要监控转发失败和消费者停滞。

**依据：** E09 `consumeRecord/publishDLQUntilSuccess`；E23。

### Q053：Kafka 分区 key 为什么优先 video_id，会有什么热点？

**回答模板：** 当前有 VideoID 时用它做 key，否则用 UserID，再不行用 EventID。这样与同一视频相关的事件会按同 key 的分区规则进入对应分区，消费处理较容易保持该日志分区内的顺序。但热门视频的互动也可能集中到同一个分区，形成热点。

更重要的是分区顺序不是用户操作的绝对因果顺序：两个并发 API 可能先后发送颠倒，数据库同步兜底还会绕过正常消费顺序。若要解决同用户同视频的状态覆盖，需要事件版本或条件更新；仅增加分区数不能解决单 key 热点，还可能改变未来 key 到分区的映射。

**依据：** E09 `eventPartitionKey`；E12/E13。

### Q054：Kafka 不可用就回退 RabbitMQ，有没有隐藏的一致性问题？

**回答模板：** 回退发生在初始化，不是运行期间自动透明切换，也不是两边双写。API 与 Worker 分别做初始化决策：若 API 连 Kafka 成功而 Worker 当时失败转 RabbitMQ，双方就可能暂时使用不同通道。Kafka 模式下的 Outbox 调度还要求 Worker 的总线实现统一 Bus，RabbitMQ 路径不等价。

所以“支持回退”不能表述为任意故障无缝迁移。实际部署我会统一明确选择的后端，暴露所选模式并在不一致时拒绝就绪；迁移时安排双读、停写或排空等具体步骤。要测试的是双方启动顺序和局部网络故障，而不是仅把某个连接地址填错后看到进程能启动。

**依据：** E01 两个事件总线工厂和 `startWorkers`。

### Q055：RabbitMQ 持久消息加手工 ACK，就一定不会丢消息吗？

**回答模板：** 代码声明持久队列/交换机，发布消息用 Persistent，消费成功 ACK，处理错误 NACK 重新入队，解码失败则不重新入队。这些覆盖了部分重启与消费失败场景，但发布者确认和消费者 ACK 是两个不同方向；当前实现没有完整 publisher confirm 等待链。[RabbitMQ 官方确认机制](https://www.rabbitmq.com/docs/confirms)明确区分两者。

因此 Publish 调用返回成功不能直接当作业务端到端完成，API 在数据库提交后直接发送的旧路径也有进程崩溃窗口。反复 NACK requeue 还可能让毒消息热循环。我的表述是“使用持久化配置和显式确认”，而不是“绝对不丢”；更强保障要补可靠待发记录、发布确认和有界重试。

**依据：** E09 RabbitMQ 发布、声明、消费函数。

### Q056：如果让我现场验证事件可靠性，你会设计哪些实验？

**回答模板：** 我会沿窗口逐个注入故障：业务事务提交前终止应无事实和事件；提交后尚未发送时终止应保留 pending；发送后尚未标记时终止应出现重投但业务不重复；处理后位点提交前终止应再次收到并收敛。然后用永久失败事件验证重试上限、DLQ 和人工恢复资料是否完整。

项目已有 Outbox 调度、去重和 Kafka 相关测试，但单元测试的假总线不能证明真实 broker、重平衡和磁盘故障行为。本次只执行普通 Go 测试，未连接真实 Kafka。尤其点赞要单列 Redis 成功后崩溃的实验，它不会因为发布链路 Outbox 测过就自动获得同样保障。

**依据：** E10/E11；`internal/infra/mq/kafka_reliability_test.go`、`kafka_integration_test.go`。

## 八、互动、幂等与热度：Q057–Q068

### Q057：点赞为什么先写 Redis，再异步落数据库？接口真的完全不查库吗？

**回答模板：** 点赞需要快速返回状态和计数，把最终事实写入交给 Worker 可以缩短主请求等待事务的部分时间。Service 先读取数据库视频统计并确认视频可互动，再用 Redis 更新用户动作和分片增量，状态变化时发送消息，最后返回快速状态。因此“异步落库”不等于“接口零数据库访问”。

另外点赞成功后通知作者也可能继续访问数据库，所以要看整条链路的 trace，而不是只盯 Redis 操作。方案代价是数据库事实滞后，以及 Redis 与消息之间的失败窗口。若当前规模不需要这个复杂度，同步事务是更容易证明正确的基线，异步收益应靠同条件接口延迟和吞吐验证。

**依据：** E12 `setActionAsync/notifyLike`；E13 `GetVideoStat`。

### Q058：如何保证重复点赞不增加两次？PUT 与 toggle 有什么区别？

**回答模板：** 项目接口表达目标状态：PUT 表示 active，DELETE 表示 canceled，而不是每次请求把状态反转。数据库以用户、视频、动作类型定位唯一记录，比较原状态和目标状态；只有 inactive→active 才加一，active→inactive 才减一，状态相同 delta 为零。

这样网络超时后重发相同 PUT，不会像 toggle 那样把点赞取消。Redis 快路径也先读动作 Hash，再计算 delta，并记录最近幂等键。但这种目标状态幂等不是完整命令历史去重；旧命令延迟到达仍可能覆盖新状态，因此还要区分“重复同一请求”和“多个不同状态请求乱序”。

**依据：** E12 `Like/Unlike`；E13 `SetAction`；E05 `SetActionState`。

### Q059：Redis WATCH 与 Pipeline 分别保证什么？冲突会自动重试吗？

**回答模板：** SetActionState WATCH 用户的视频动作 key，读取旧状态后通过 TxPipelined 写动作、base 和分片增量。若被监视 key 在提交前被其他客户端修改，事务会中止。普通 Pipeline 主要批量传输命令，不能直接当作事务；项目 Fanout 的 Pipeline 也没有获得整体跨用户原子性。[Redis 官方事务说明](https://redis.io/docs/latest/develop/using-commands/transactions/)可用来核对 WATCH 和执行错误边界。

当前函数没有显式循环重试 WATCH 冲突，而是把错误传给 Service，因此并发操作同一动作可能返回失败。可改成有限重试，或用原子脚本简化读改写，但两者都解决不了 Redis 与 Kafka 的跨系统原子性，不能把 Lua 当作万能分布式事务。

**依据：** E05 `SetActionState/AddInboxItems`。

### Q060：点赞后立刻取消，消息顺序颠倒会怎样？

**回答模板：** Worker 按事件目标状态调用 SetAction，没有使用 OccurredAt 作为版本条件。若先处理取消、后处理较旧点赞，最终数据库会停在 active，违背用户最新意图；动作行保存最近幂等键也不能阻止不同 key 的旧事件覆盖。

正常同分区消费有序能减少问题，但 API 并发发送、超时后的同步兜底和重放仍可能打乱业务顺序。我会给每个用户-视频-动作建立单调版本，数据库只接受更新版本，Redis 返回也携带版本。测试应构造 like(v1)、unlike(v2) 的正序、逆序、重复序列，最终必须一致，而不是只验证重复 like。

**依据：** E12 `ActionChangedEvent/ActionWorker`；E13 `SetAction`。

### Q061：Redis 改成功、MQ 发布失败时，同步兜底就完全安全了吗？

**回答模板：** 捕获到 Publish 错误后，Service 调用同步 SetAction，因此正常错误返回路径能保存当前目标状态。但网络错误可能意味着“服务端收到了、客户端没收到确认”，后面仍可能消费到事件；必须依靠幂等，并继续考虑它和随后取消事件的顺序。

更糟的是 Redis 已变更后进程直接退出，根本不会执行同步兜底。下一次重试看到 Redis delta=0，就可能不再发事件。这说明兜底覆盖的是已观察到的错误，不覆盖所有崩溃窗口。要证明完整恢复，需要持久记录待发意图或对账机制，当前代码还不能做这种承诺。

**依据：** E12 `setActionAsync`；E05 `SetActionState`。

### Q062：事实记录和计数为什么要放同一事务？热点行有什么代价？

**回答模板：** 如果先写 interaction_action 再单独改 video_stat，中途失败会出现用户已点赞而计数没变。项目在一个事务里锁定公开视频、动作记录和统计行，计算 delta 后更新事实与计数，最后一起提交；画像失效也放在相关事务中，避免事实变了却永久沿用旧画像。

代价是同一热门视频的许多用户写入都可能竞争视频和统计行锁，所以 Redis 侧分片并没有消除数据库热点。优化前应看锁等待和 Worker 延迟；后续可按业务接受程度聚合持久化计数或缩小锁范围，但要保留事实校验、重放和计数对账，不能只把锁删掉。

**依据：** E13 `SetAction/lockPublishedVideo/updateVideoStatCounter`。

### Q063：16 个计数分片怎么选、怎么读？单 Redis 上真能扩展吞吐吗？

**回答模板：** 当前按用户 ID 映射到 16 个分片，把点赞、收藏变化写入对应 Hash；读取时将 base 与各分片增量相加，多个 HGETALL 可通过 Pipeline 批量发送，再生成短期 JSON 统计缓存。这样不同用户不必都改同一个增量 Hash，但每次聚合多了一组读取。

它不等于同一 Redis 实例突然有 16 倍写能力：这些命令仍由同实例处理，base 初始化/续期及汇总也仍有成本，数据库还有热点行。项目使用单节点 Redis 客户端，不能直接声称已经支持 Cluster 多节点事务；迁移前必须重新评估 key 布局和跨 key 操作约束。

**依据：** E05 `actionStatCounterShardCount`、`interactionStatCounterShardIndex`、`loadActionStatShardValues`。

### Q064：为什么“计数缓存 15 秒后自动与数据库一致”不够准确？

**回答模板：** 15 秒是统计 JSON 的 TTL。JSON 未命中时，代码优先尝试 base Hash 和 16 个分片，它们的 TTL 为 24 小时，并会在动作请求中续期；只有缓存计数也缺失才走数据库。因此旧 base 或增量偏差可能继续存在，不能以 JSON 过期推导固定 15 秒收敛。

例如某个用户的动作 key 丢失但数据库已有点赞，Redis 可能把重复 PUT 误判为新增，而 Worker 的数据库状态比较不会加计数，两个视图就出现差异。base 和不同分片过期节奏不一致也需要验证。改进应有明确权威快照、增量版本和对账切换协议，而不只是把 TTL 调小。

**依据：** E05 `getStats/actionStatWithPresence/queueActionStatBaseInit`；E13。

### Q065：热度分为什么是点赞 3、收藏 4、评论 5？它是推荐模型吗？

**回答模板：** 这是项目中的手工业务权重，累计热度 SQL 和互动增量都使用点赞 3、收藏 4、评论 5。理由可以解释为评论和收藏通常比轻量点赞反映更强参与，但没有训练或 A/B 证据证明这个比例最优，所以我会把它叫规则基线。

例如一条视频 10 个赞、2 个收藏、1 条评论，累计规则分为 43。推荐精排不会直接把这个大数与余弦相似度相加，而是先对热度做 log1p 缩放。若恶意刷评论，规则会被操纵，因此权重设计还需要可信行为、去重和反作弊约束，不能把“有公式”说成“推荐效果已验证”。

**依据：** E12 热度常量；E06/E15 `hotScoreExpression`；E14 `rankScore`。

### Q066：分钟桶如何组成一小时滑动热榜？

**回答模板：** 每次互动 delta 按当前分钟对该分钟 ZSET 执行 ZINCRBY，分钟桶保留两小时。读取热榜时，以分钟截断的 window_end 找最近 60 个桶，通过 ZUNIONSTORE 按 SUM 合并，再删除非正分数，窗口结果缓存两分钟并按排名分页。

它把“每次查询扫描全部历史互动”转成“读取有限桶的聚合结果”，但 ZUNIONSTORE 自身仍有成本，空窗口或同时未命中还可能重复重建。并且时间粒度是分钟，不是逐毫秒精确滑动。压测应观察窗口切换时延迟和 Redis CPU，不能只在同一个已构建窗口反复读取。

**依据：** E05 `AddHotScore/rebuildHotWindow/hotWindowMinuteKeys`。

### Q067：昨天点赞，今天取消，今天的热榜该怎么算？

**回答模板：** 当前实现按发生变更的时间写 delta：昨天 +3 在昨天桶，今天取消的 -3 在今天桶。读最近一小时可能只看到负增量，最后非正分数被移除。因此这个热榜更接近窗口内的净互动变化，而不是“最近一小时新增有效点赞的精确存量”。

这不一定是错误，但必须先定义产品想要的口径。如果希望撤销原事件对原窗口的贡献，需要保存原动作时间并修正相应桶，同时处理原桶已过期；如果希望衡量当前热度趋势，可以接受净变化。不能简单把所有负数都 clamp 掉后宣称统计严谨，测试要覆盖跨窗口撤销和重复撤销。

**依据：** E12 `recordActionHotScore`；E05 `AddHotScore/rebuildHotWindow`。

### Q068：评论既要幂等，又要改统计和发消息，如何处理重试？

**回答模板：** 创建评论先按用户和幂等键找已有记录，仓储在事务里写评论、增加 comment_count、使画像失效；Kafka 模式再写评论 Outbox。并发首次请求都查不到时，由唯一键冲突兜底加载已有评论，返回 delta=0，避免重复增加热度和通知。

通知仍是事务外直接调用，不能用评论 Outbox 证明通知必达。删除评论会校验评论作者、视频作者或管理员权限，已删除再次 DELETE 不再减计数。另一个边界是幂等键复用：当前主要返回旧结果，没有完整比较请求内容摘要，客户端不应拿同一 key 提交不同评论并期待冲突检测。

**依据：** E12 `CreateComment/DeleteComment`；E13。

## 九、多路召回与过滤：Q069–Q080

### Q069：推荐为什么要多路召回，六路分别提供什么？

**回答模板：** Hot 提供普遍受欢迎内容，Latest 提供新内容，Following 利用明确关注关系，Interest 用用户画像与候选向量相似性，Similar 用最近正反馈视频当种子，Collaborative 根据共同观看用户发现其他视频。它们覆盖不同信号，避免新用户或稀疏用户因单一特征缺失完全没有结果。

当前每路先取最多 200 条，过滤后通常保留目标 100 条，再去重合并，并不是每路无限召回。Interest/Similar 的原始池仍来自较新的、有向量的有限视频，不能声称在十万视频中做全局近邻搜索。评估时要看每路独占候选、重合度与有效结果，而非只看“有六个名字”。

**依据：** E14 `recallCandidates`；E15 `ListCandidatesBySource`。

### Q070：六路并发会不会一条失败导致全部取消？

**回答模板：** 推荐 Service 内部虽然使用 errgroup，但每个 goroutine 把自己的结果和错误写到固定位置，并返回 nil；等所有调用结束后统一判断。只要还有成功来源且父 context 未取消，就保留健康来源，并记录 partial_recall。全部来源失败才返回错误。

外层 Feed 的 loadCandidatePages 不完全相同，未被 optional 包装的来源错误会让 group 失败；辅助来源则可转换为空结果。因此不能只看到 errgroup 就推断所有层都 fail-fast。部分成功提升可用性，但一次主请求成功不代表推荐完整；压测验收专门把 partial_recall 也列为必须为零的事件。

**依据：** E14 `recallCandidates`；E03 `loadCandidatePages/optionalCandidateSource`。

### Q071：六个 goroutine 往结果切片写，会产生数据竞争吗？

**回答模板：** 当前预先按来源数量分配切片，每个 goroutine 只写自己固定索引的元素，等 Wait 完成后才遍历读取。没有并发 append 或多个来源争抢同一个槽位，因此这个使用方式与并发修改切片长度不同，设计上可保持清晰的数据所有权。

不过切片里的 Candidate 指针仍可能指向共享对象，尤其 Hot 缓存。如果只复制指针切片，后续过滤和打分就可能污染别的请求，因此还需要对象级复制。测试应同时覆盖 race 检查和数据隔离断言；无数据竞争不等于业务正确，例如按返回速度排列来源虽然不一定 race，却会导致排序不稳定。

**依据：** E14 `recallCandidates/cloneCandidates`；E03。

### Q072：为什么每路先取 200，再过滤到 100？候选不足怎么办？

**回答模板：** 如果只取 100 个，用户看过其中 80 个，过滤后这一来源只剩 20 个。两倍超取给过滤留出余量，同时保证数据库和应用处理有界。当前候选不足时接受较短结果，没有按各来源游标不断补拉，这是代码里的明确简化。

代价是活跃用户可能把最新或热门池看得差不多，虽然库里还有大量内容，系统却找不到足够新候选。改进前先统计过滤前后数量、空页比例和每用户候选耗尽速度，再决定有界补拉、扩大冷门池或降低重复限制；不能无限扩大候选，把延迟预算耗尽。

**依据：** E14 `recallPerSource/candidateOverfetchFactor/filterRecallPools`。

### Q073：为什么把曝光和负反馈过滤从每路 SQL 提到统一阶段？

**回答模板：** 旧思路可能在六路 SQL 中反复关联同一用户的曝光和负反馈，尤其排序前的相关子查询会扩大扫描成本。当前先取各路有界原始候选，汇总去重的 video_id 和 author_id，再查询一次曝光集合和负反馈排除集合，在内存里过滤各池。

这里“负反馈查一次”是一次仓储调用，内部仍可能分别查视频负反馈和屏蔽作者，不应说整套过滤只有一条 SQL。好处是用户过滤成本绑定候选数量，还让不含用户条件的 Hot 原始池可复用。风险是过滤后不足，需要用同一用户、同一输入对照验证过滤语义没有丢失。

**依据：** E14 `filterRecallPools`；E15 `ListNegativeCandidateExclusions`；E25 前期诊断。

### Q074：如何确认 Hot 原始候选真的能跨用户共享？

**回答模板：** 我会检查仓储 Hot 分支的完整查询条件：只看公开视频、热度表达式、发布时间和 ID，不加入当前用户的曝光、负反馈或关注条件。虽然方法签名带 userID，Hot 分支不使用它；用户过滤在取出候选后执行，所以缓存的是公共原始池，而不是某个用户的最终列表。

一旦未来 Hot 查询加入地域、语言或权限，这个假设就变了，必须把相应维度加入缓存 key 或移到后续过滤。验证时用两名曝光和屏蔽记录不同的用户请求，确认底层 Hot 查询可复用但最终结果不同，并修改一次返回对象，确认另一用户的候选没有被污染。

**依据：** E15 Hot 分支；E14 `listCandidatesBySource/filterRecallPools`；E27。

### Q075：新快照为什么用互斥锁，查询数据库期间持锁合理吗？

**回答模板：** 当前进程只维护一个 Hot 候选快照，锁内先判断期限和容量，未命中则由持锁者查库并保存。其他请求等待同一次重建，避免每个并发请求都做十万视频排序。实现简单，失败不更新快照，五秒后再按需重建。

代价是数据库慢时所有等待者被同一锁阻塞，而且 sync.Mutex 的等待不能直接响应 context；代码在拿锁后检查 ctx。当前单实例五秒复用带来已测收益，但长尾仍可能集中在重建时刻。进一步优化可以考虑旧值可用期间异步重建或可取消等待，前提是明确新鲜度容忍和测到的锁等待问题。

**依据：** E14 `hotCandidateMu/listCandidatesBySource`；E25。

### Q076：为什么命中 Hot 快照要复制 Candidate，而不是只复制 slice？

**回答模板：** Candidate 后续会被设置 Similarity、FreshnessScore、RankScore 和 Reason，过滤阶段也可能原地复用 slice。如果只复制 `[]*Candidate` 的外层切片，里面指针仍指向缓存对象，用户 A 的个性化分数可能被用户 B 看到。当前 cloneCandidates 对每个非 nil Candidate 做值拷贝。

写入快照时也先复制，首次回源返回的对象可以继续处理而不影响缓存。这里 Candidate 当前字段适合值拷贝；若未来新增 map、slice 等引用字段，还需要重新判断深拷贝范围。测试不是只看两次结果数量相同，而是修改第一次的字段，再断言缓存命中得到的原始字段未变化。

**依据：** E14 `cloneCandidates/listCandidatesBySource`；E27 `TestHotRecallCandidatesAreCachedAndCloned`。

### Q077：协同过滤具体是怎么算的，是矩阵分解吗？

**回答模板：** 当前是基于观看共现的在线 SQL 基线。先找我近期正向观看过的视频，再找也看过这些视频的其他用户，然后找这些用户看过的其他视频，按不同 peer 用户数聚合 affinity 排序，并排除我已有观看记录的视频。时间窗口主要为 30 天。

它没有训练用户和物品隐向量，也不是矩阵分解。优点是路径直观，少量行为就能给出关联候选；缺点是高活跃用户和热门视频会放大 join，热门偏置也明显。随着规模增长，可先限制种子和邻居，再考虑离线共现表及归一化权重，改造前应检查实际扫描行数和中间结果膨胀。

**依据：** E15 Collaborative 分支、`collaborativeEventTypes`。

### Q078：Interest 与 Similar 都算余弦相似度，区别在哪里？

**回答模板：** Interest 的参考向量是用户历史正反馈聚合出的画像，表达较长期偏好；Similar 先取最近一个正反馈视频作为 seed，用它的内容向量找相似候选，更偏近期意图，并排除种子自身。两者都在有限候选池里计算，不是向量数据库全库检索。

例如用户长期喜欢篮球，但刚完整看了做饭视频，Interest 和 Similar 可能提供不同方向的内容，这正是多路的意义。缺少画像时 Interest 返回基础候选，Similar 没有种子则为空。错误处理也不同，所以冷启动和依赖异常时不能要求每路都有相同数量，应该观察各来源贡献和最终多样性。

**依据：** E14 `personalizeInterestRecall/personalizeSimilarRecall/selectBySimilarity`。

### Q079：多个来源召回同一视频，如何合并推荐原因和分数？

**回答模板：** mergeRecallPools 用 video_id 建 Map，第一次保留一份对象，重复出现时保留较大的 Similarity，并按代码规则处理 Reason：Following 有较高优先级，Collaborative、Interest 也有特定覆盖逻辑。它不是简单让返回最快的来源赢，也没有保存完整多来源贡献向量。

这使去重结果确定且便于展示，但会损失“这个视频同时命中三个来源”的信息。后续若需要分析召回贡献或学习排序，可保存来源集合、各源原始分数，再统一归一化。不能直接把不同来源的分数相加，例如共同观看人数和余弦相似度量纲完全不同。

**依据：** E14 `mergeRecallPools/preRankCandidates`。

### Q080：你说统一过滤，但外层补充候选会不会绕过屏蔽作者？

**回答模板：** 有这个边界。推荐 Service 的六路池经过 SQL 曝光和负反馈统一过滤；但 Feed RecommendStrategy 首屏另加 Hot、Latest、Following，这些来源不经过那个 filterRecallPools。外层管线主要做去重和 Redis SeenFilter，没有等价的屏蔽作者、not_interested 过滤。

因此不能把内部服务的保障扩大为最终推荐输出的保障。一个用户屏蔽作者后，该作者仍可能从外层辅助源进入结果。最直接的改进是把必须遵守的用户排除规则放在所有来源合并后的统一出口，并保留调用开销预算；测试应让被屏蔽视频仅出现在辅助源，避免主服务单测通过造成误判。

**依据：** E02 `RecommendStrategy.List`；E04 `SeenFilter`；E14 `filterRecallPools`。

## 十、排序、向量与兴趣画像：Q081–Q092

### Q081：召回之后为什么还要预排序，再精排序？

**回答模板：** 六路候选合并后可能有数百条，直接对全部候选加载向量并计算个性化分数，会放大数据库和 CPU 成本。当前先用热度、新鲜度、已有相似度和来源加分粗排，最多保留 200 条，再批量读取向量和用户画像做最终规则打分。

预排序是成本和召回质量的取舍：早期分数不准时，真正适合用户的长尾内容可能在精排前已被丢掉。Following 和 Collaborative 的加分主要影响预排存活，不代表最终分数还保留同样奖励。验证除了预排耗时，还要比较各来源进入精排的比例和离线相关性，不能只看候选数量变少。

**依据：** E14 `preRankCandidates/rankCandidates`。

### Q082：精排公式是什么，为什么要对热度取对数？

**回答模板：** 有用户画像时，分数为 `0.70×similarity + 0.20×hot + 0.10×freshness`，其中 hot 是 `log1p(max(hotScore,0))/10`；没有画像时改为 `0.65×hot + 0.35×freshness`。新鲜度为 `1/(1+hours/72)`，未来时间先把年龄按零处理。

对数缩放是为了减少极热门视频原始计数对其他特征的压制，但除以十并不保证热度永远落在 0 到 1；权重也只是人工基线。比如某内容很新但没有互动，冷启动仍能获得新鲜度贡献。要调权重应使用固定候选和行为数据评价，不能把这组常量说成模型训练结果。

**依据：** E14 `rankScore/freshnessScore`。

### Q083：作者打散是如何做的？会改变推荐质量和分页吗？

**回答模板：** 内部推荐先按分数截取页面并生成游标，再把连续同作者的视频延后；外层 Feed 还有按作者分桶、轮转取一条的 Mixer。目的是避免一屏被同一作者占满，但这会改变原始分数的展示顺序，且两层打散可能重复干预。

游标要表达截断前排序边界，不能直接拿打散后的最后一个展示项当下一页分数边界，否则有跳过或重复风险。当前作者打散也无法在候选只有一个作者时创造多样性。我会同时看相邻同作者比例、原排序损失和跨页作者集中度，而不是把“每页打散”当作完整多样性策略。

**依据：** E14 `Recommend/interleaveByAuthor`；E04 `AuthorDiversityMixer`。

### Q084：向量加载失败后为什么还能返回推荐？这算正常成功吗？

**回答模板：** 若父 context 仍有效，rankCandidates 遇到视频向量或用户画像读取错误，会记录相应治理指标并降为热度、新鲜度等基础排序；Interest 缺画像可以保留基础候选，Similar 缺种子或向量则可能返回空。这让非核心特征故障不一定导致整页失败。

但 HTTP 200 只说明返回了内容，不代表个性化完整执行。现有压测重点监控 fallback、circuit_open、bulkhead_full、partial_recall 四项，还应注意 default_profile、rank_vector_fallback 等更细粒度决策。验证优化时不能用“更少算特征所以更快”冒充正常路径提速，要确认降级指标和结果语义。

**依据：** E14 错误分支；E23；E25 压测脚本。

### Q085：为什么外层辅助召回只在第一页执行？

**回答模板：** 主推荐游标是分数、时间和 ID，Timeline 和 Following 使用时间游标，Redis Hot 又使用窗口与 offset。这些游标不能互相解码复用，所以外层 firstPageOnlySource 只在没有游标时加入辅助结果，后续页面由主来源推进。

这是明确限制复杂度的选择，代价是首屏与后续页的来源分布不同，辅助结果也没有自己可继续推进的位置。若业务要求每页保持多源配额，就需要复合游标或服务端会话快照，分别记录各路消费位置和去重状态，不能简单删掉 firstPageOnly 检查就声称完成多路分页。

**依据：** E03 `firstPageOnlySource`；E02 `RecommendStrategy.List`。

### Q086：推荐完全故障时返回热门，为什么把下一页游标清空？

**回答模板：** governedCandidateSource 在主召回超时、隔舱满或熔断时，按顺序尝试 Hot 和 Timeline，兜底时清除输入游标，成功后也把 NextCursor 清空、HasMore 设为 false。原因是不能把主推荐分数游标交给另一种排序，也不能返回 Hot 游标让下一次被当成推荐游标解析。

这换来一个可返回的降级首屏，但不提供原推荐会话连续翻页保证，可能重复用户刚看过的候选，后续 SeenFilter 只能部分缓解。若产品要求连续性，应把降级模式和游标类型显式编码并让客户端识别，且在恢复时约定会话如何切换，而不是隐藏模式变化。

**依据：** E03 `governedCandidateSource.loadFallback`。

### Q087：如果面试官要求证明推荐效果提升，你拿什么数据？

**回答模板：** 当前证据支持工程性能和功能正确性，不支持点击率、完播率或留存提升。P95 下降只能说明返回更快，作者打散和曝光过滤也只是可验证的规则效果。没有真实实验就应明确回答尚未做效果验收，不能拿合成数据上的规则命中当作用户满意度。

我会先建立固定评估集，按时间划分历史与未来行为避免泄漏，比较热门基线、多路召回和不同排序；统计召回覆盖、重复率、作者集中度及正反馈相关指标。上线后再做明确分流的 A/B，并同时观察负反馈和播放质量。这个步骤是后续方案，不是当前仓库已经拥有的训练评估平台。

**依据：** E14/E25；当前测试与压测范围。

### Q088：你所谓的视频 embedding 是怎么生成的？

**回答模板：** 当前只用视频标题和描述构造文本，做字符与 token 特征处理，通过 FNV 哈希把 token、二元和三元字符片段投到 128 维向量，并用符号位区分正负贡献，最后 L2 归一化。模型名是 `hash-ngram-v1`，发布事件触发 Worker 生成并保存 JSON 向量。

这是一种可重复、无外部模型服务依赖的特征基线，适合跑通“发布→向量→推荐”的工程链。但它不理解视频画面、声音或真正语义，也会有哈希冲突。面试时我会主动说这是本地伪 embedding，而不是把“向量”两个字包装成使用了深度学习模型。

**依据：** E16 `HashNgramVectorizer/GenerateForPublishedVideo`。

### Q089：为什么用余弦相似度？零向量、维度错误怎么处理？

**回答模板：** 余弦看向量方向的一致性，相比直接点积更少受长度影响；当前文本向量生成后归一化，画像是加权平均，余弦实现仍会重新计算两边范数。比如两条标题都包含篮球相关字符片段，其哈希特征可能更接近，但这只是特征重合近似，不保证语义相同。

函数遇到维度不同返回错误，任一范数为零返回零相似度。召回阶段可能跳过不合法向量，精排阶段则保留其他得分分量。验证要包括空标题、不同维数和哈希冲突，而不是只用相同文本测相似度为一；新模型上线也不能把旧维度向量直接混用。

**依据：** E16 `CosineSimilarity`；E14 `selectBySimilarity/rankCandidates`。

### Q090：用户兴趣向量有哪些输入，各行为权重为什么不同？

**回答模板：** 仓储在最近 30 天窗口里读取有限正向观看信号，以及有效点赞、收藏、评论、关注作者近期内容，按权重累加向量再除以总权重。观看部分 finish/complete 权重为 3，valid_play 随时长增加但有上限；点赞 3、收藏 4、评论 5，关注内容贡献较弱。

这些权重表达的是启发式强弱关系，例如收藏通常比一次轻触播放更强，但没有效果实验验证最优性。系统还会过滤被明确负反馈排除的视频或作者，避免正向历史继续强化它们。每类行为有数量上限，因此画像不是用户所有历史的完整统计；重复行为和窗口衰减语义都需要单独评估。

**依据：** E15 `calculateUserInterestVector/eventWeight/vectorAccumulator`。

### Q091：画像为何要物化？事件重放会不会重复加权？

**回答模板：** 每次推荐都联查观看、互动、评论和关注来算画像，成本太高，因此用户向量落到物化表。读取优先取当前模型记录，不存在再按权威事实重算；观看事件驱动 FeatureWorker 也会重算，而点赞、评论、关系变更会在相应事务中失效画像，让后续请求重建。

重算不直接按消息再加一次向量，所以同一事件重复消费不会简单造成双倍权重，这是可靠性优势。但并发重算仍可能出现较早快照较晚写回，当前缺少明确画像版本比较；缓存中的空画像也要靠后续行为刷新。优化方向是按用户合并刷新、版本校验和控制重算频率，而不是每事件无限重算。

**依据：** E15 `LoadUserInterestVector/RefreshUserInterestVector/InvalidateUserInterest`；E16。

### Q092：如果换成真实 embedding 或 ANN，要改哪些地方？

**回答模板：** 生成侧已有 Vectorizer 接口，可以接模型服务，但仓储多处过滤固定的 HashNgramModel，因此不是只换一个实现就完成迁移。还需要明确 model/version/dimension，异步补算历史视频，隔离新旧用户画像，并验证缺向量和模型服务失败的回退策略。

ANN 解决的是更大候选空间的近邻检索，当前在近期 200 条里算余弦存在召回上限；接入后仍要处理公开视频权限、曝光和负反馈过滤，不能让向量库直接绕过业务规则返回。先以精确有限检索做效果对照，再评估新索引的召回损失、更新延迟和运维成本，这是扩展方案而非已实现能力。

**依据：** E16 Vectorizer；E15 Interest/Similar 与向量查询。

## 十一、曝光、负反馈与数据质量：Q093–Q102

### Q093：为什么同时保存观看流水和曝光聚合表？

**回答模板：** 流水保存“谁在什么请求下，对什么视频做了哪种行为”，用于画像、排查和离线分析；exposures 聚合保存用户-视频的首次曝光、最近曝光、次数和最后场景，服务在线去重。推荐只想知道最近是否看过，不必每次扫描大量原始观看事件。

SaveViewEvent 在同一事务里写事件，并且只有 exposed 才更新曝光聚合，首次插入才增加次数。播放、完播不会自动各算一次曝光。这个区分使曝光次数有明确口径，但也意味着客户端漏报 exposed 而只报 play 时，SQL 曝光聚合不会凭空补齐，需要端侧协议和数据质量检查。

**依据：** E17 `SaveViewEvent`、`CountsAsExposure`。

### Q094：曝光如何幂等？request_id 为空会发生什么？

**回答模板：** 数据库唯一键组合是 user_id、video_id、request_id、event_type。同一请求重复上报同事件类型时不再插入，直接加载已有结果，避免重复增加聚合。request_id 表示一次消费请求或曝光会话，因此它既要稳定用于重试，又不能被所有后续独立观看永远复用。

空 request_id 在仓储转为 NULL，不能依赖这个可空唯一组合达到相同幂等效果。另一个边界是 finish 与兼容旧客户端的 complete 在业务上类似，但字符串不同，去重键也不同，双报可能产生两个信号。客户端协议应规范事件身份；测试必须覆盖空 ID、重试和兼容事件组合。

**依据：** E17 曝光模型、`stringPtr`；行为领域事件常量。

### Q095：Redis Seen 是精确 Set 还是 Bloom？为什么选它？

**回答模板：** 当前每用户固定 `2^18` 位，也就是 32 KiB Bitmap，用三个稳定哈希位置标记 video_id。查询时三个位置全为一就判断已见，批量 GETBIT 通过 Pipeline 读取。相比保存大量完整 ID，它的空间上限简单，但代价是会误判“没看过”为“看过”。

这可以作为减少重复的近似过滤层，但不能拿它做权限拒绝或不可撤销的业务事实。SQL 曝光聚合仍保留精确记录。随着单用户累计插入数增加，误判风险会升高；当前注释也建议达到更高曝光规模后考虑分桶或扩容。要看实际过滤比例和新内容覆盖，不能只看 Bitmap 内存固定就认为永远合适。

**依据：** E05 `MarkSeen/SeenVideoIDs/seenOffsets`；E17。

### Q096：SQL 是最近七天，Bloom TTL 三十天，两者一致吗？

**回答模板：** 不一致。SQL 过滤明确比较 last_exposed_at 是否在最近七天，负反馈窗口为三十天；Bloom 对整个用户 key 设置三十天 TTL，且每次 MarkSeen 都续期，不会按某个视频自己的曝光时间清除位。因此持续活跃用户的旧位可能保留远超三十天。

这意味着 Bloom 不是精确“三十天滑动去重”，更不是七天 SQL 的无损缓存；还存在误判。若需要相同时间语义，可以按日或周分桶并只查询有效桶，或者对 Bloom 命中做精确验证。选择要结合允许重复和误过滤的业务成本，文档不能把 TTL 数值直接当作逐视频有效期。

**依据：** E05 `MarkSeen`；E15 领域窗口常量。

### Q097：为什么 API 同步 MarkSeen，Worker 又异步 MarkSeen？

**回答模板：** API 在行为事实保存成功后立即尝试更新 Seen，减少用户刚曝光就刷新又刷到的窗口；Kafka recommendation 消费组对 exposed 事件再更新一次，提供异步补充。Bitmap 同位重复 SETBIT 不会像计数累加那样重复增长，适合这两条路径。

但两条路径并不完全对等：API 还为负反馈 MarkSeen，EventWorker 当前只处理 exposed，因此负反馈同步写失败不能指望这个 Worker 自动补齐 Bitmap。SQL 负反馈事实仍然存在，是否拦住内容还取决于最终候选过滤。验证可以分别让曝光和负反馈遇到 Redis 故障，恢复后检查各自状态，而不是用曝光成功补写证明所有事件都有相同保障。

**依据：** E17 `RecordViewEvent`；`application/recommendation/event_worker.go`；E01。

### Q098：skip、not_interested、hide_author 三种负反馈如何区别？

**回答模板：** skip 和 not_interested 排除对应视频，hide_author 通过该视频找到作者后排除该作者候选；它们也会触发画像刷新，并把该视频记为 Seen。SQL 负反馈窗口是三十天，不是永久拉黑。当前把 skip 也当排除信号，属于比较强的启发式选择。

从产品上，快速划走可能只是当前没时间，不一定表示长期讨厌。我会建议区分短期降权和明确“不感兴趣”，并让“少看作者”有可撤销入口，但这是未来改进。代码核查还需回到 Q080：外层辅助源未统一执行同样排除，所以测试最终 Feed 输出比只测内部推荐服务更重要。

**依据：** E17 `IsNegativeFeedback/AffectsInterestProfile`；E15 `ListNegativeCandidateExclusions`。

### Q099：上报观看时长就足够可信了吗？怎么防刷画像？

**回答模板：** 当前服务端校验用户、视频、事件类型、字符串长度和非负 watch_ms，finish/complete 会规范 completed；它没有完整校验“观看时长不能超过视频长度”、连续会话顺序或可信端证明。画像权重做上限可缓解极大时长的影响，但不能防止不断制造新 request_id 重复提交。

因此画像建立在客户端信号基本可信的假设上。可以逐步加入会话事件去重、合理速率、时长上限和异常行为检测，先处理明显伪造而非一开始搭复杂风控。验证要构造超长时长、短时间大量完播、同设备多账户等情况，并确认不会放大画像和推荐热度，不能只测参数合法。

**依据：** E17 `NewViewEvent`；E15 `eventWeight`；E22。

### Q100：为什么反馈查询增加复合索引还可能很慢？

**回答模板：** 索引需要匹配真实查询的用户、事件类型、时间及视频条件，当前模型有用户-事件-时间-视频和视频-事件-时间-用户两种方向，部分 SQL 还用 FORCE INDEX。它们帮助缩小候选扫描，但多路重复负反馈子查询、协同过滤中间结果和热度排序仍可能消耗大量资源。

我会按来源对 SQL 执行计划看估计/实际行数、循环次数和排序成本，再决定索引或查询形态。FORCE INDEX 和 STRAIGHT_JOIN 是当前诊断后的明确约束，但数据分布变化后可能不再最优，需要回归。不能把“加了索引”当作已经消除瓶颈，更不能把慢查询计数为零等同于 SQL 没有性能问题。

**依据：** E15/E17；E25 前期诊断和 Hot 优化记录。

### Q101：为什么不把所有曝光和负反馈永远放在 Redis？

**回答模板：** 它们不仅用于即时过滤，还需要审计、画像重建和异常排查，保留 MySQL 流水与聚合有利于恢复和重算。Redis Seen 更适合加速“很可能看过”的判断，不能替代完整事件事实，尤其 Bitmap 不保留发生时间、次数和原因。

当前事件量增长后 MySQL 也会面临存储和查询压力。可以给原始流水制定归档/保留策略，让在线聚合和近期画像数据保持有界，历史进入分析系统；但项目还没有完整分区归档链路。优化时先界定哪些字段参与在线查询，不能仅为了减表把未来无法恢复的行为事实全部丢掉。

**依据：** E17 两类数据模型；E15 画像与过滤查询。

### Q102：个性化、曝光去重、作者打散会不会形成信息茧房？

**回答模板：** 会有这种可能。画像反复强化既有偏好，Seen 限制重复，作者打散只能增加来源多样性，不能保证主题多样性；六路里 Latest 和 Hot 提供一些非画像内容，但目前没有明确探索比例和长尾覆盖目标。不能因为有六路召回就认为已经解决偏置。

我会提出一个可验证的小改进：在不违反屏蔽规则的前提下，为新内容或低曝光内容留有限配额，记录曝光、完播和负反馈，比较它对长期覆盖与短期体验的影响。这里应先设对照和停止条件，不把随机插入当作天然改善。当前仓库没有这项效果实验，面试时作为延伸设计回答。

**依据：** E14 多路与排序规则；E04 作者 Mixer；扩展方案。

## 十二、性能证据与排障：Q103–Q114

### Q103：简历中 P95 从 267.58 降到 62.42 ms，实验条件是什么？

**回答模板：** 这是仓库保存的 2026-09-29 历史实验，不是这次文档生成时重新压测。数据快照为 medium-v1，约 1.2 万用户、10 万视频及向量、百万级关注/互动/观看记录，单 API 实例，用 100 个只读账号，推荐超时保持 500 ms。前后恢复同一快照，清 Redis、重启 API/Worker，5 RPS 预热五分钟，再正式 5 RPS 跑五分钟。

原始 k6 JSON 的 P95 是 267.585334 和 62.42484875 ms，原报告写作 267.58 与 62.42，前值按通常四舍五入应为 267.59。降幅约 76.7%，这是延迟下降百分比，不能说 QPS 提升 76.7%。CPU 数值来自容器采样，也不是每个请求独立的 CPU 成本。

**依据：** E25；`build/performance/medium-v1/formal-5rps-5m-20260929.json` 与 `hot-cache-formal-5rps-5m-20260929.json`。

### Q104：怎么证明瓶颈是 Hot 排序，而不是 Redis 或网络？

**回答模板：** 诊断先按召回来源拆延迟，结合 SQL 和 MySQL 资源观察。统一用户过滤后推荐已不再超时，但 5 RPS 下 MySQL CPU 仍连续逼近或超过一核，慢查询残留集中在 Hot：每次都对大量公开视频计算互动加权分并排序。Hot 原始查询又与用户无关，因此重复工作非常明确。

修改只在推荐 Service 加五秒原始候选快照，保留用户过滤和排序语义后，MySQL CPU 与请求延迟显著下降；这组对照支持根因判断。Trace 用于把请求与依赖关联，但源码没有给每条 SQL 都加细粒度 span，不能宣称完整调用树独立证明了一切，应同时拿出慢查询、执行计划和来源指标。

**依据：** E14/E15/E23/E25。

### Q105：为什么五秒本地快照够了，没有直接新增 Redis 缓存或物化表？

**回答模板：** 目标是消除单 API 对同一公共候选池的重复排序。池只有有限条 Candidate，五秒允许一定热度延迟，进程内保存和互斥重建就能复用，且不增加网络请求、Redis key 和序列化协议。缓存错误时不更新快照，个性化条件仍逐请求处理。

这不是说本地永远最好。副本数量增加后，每个实例仍每五秒各重建一次；若这再次成为数据库主要负载，可以用 Redis 共享快照或物化热度。当前先选择满足已有规模、可测收益明确的最小变更，代价是跨实例重复重建和五秒陈旧，需要在扩容时重新评估。

**依据：** E14 `listCandidatesBySource`；E25 优化设计。

### Q106：查询次数只下降约 5.8%，CPU 为什么能下降这么多？

**回答模板：** 查询次数没有反映每条 SQL 的成本。一条对十万视频做加权计算和排序的查询，比一次主键批量查十张卡片重得多。快照省掉的是这类昂贵查询的重复执行，而曝光、负反馈、向量和用户状态读取仍在，所以 Queries/s 只从约 241 到 227。

报告中 MySQL CPU 采样峰值从 106.91% 到 32.65%，支持“减少昂贵工作”这个解释，但峰值不是稳定均值，也受采样周期影响。我会同时展示 CPU 中位数、P95/P99、查询计划和降级指标，避免只挑最好看的单个峰值。CPU 超过 100% 在容器统计中也不自动等于机器已经用尽全部核心。

**依据：** E25 Hot 优化记录；查询成本分析是基于代码与实验的推断。

### Q107：P95 改善很大，P99 改善较小，为什么？

**回答模板：** 大多数请求命中五秒快照，常见路径显著变快；少数过期时刻仍需数据库排序，其他请求可能等待重建锁，尾部成本没有完全消失。原对照 P99 从约 300.16 到 206.21 ms，降幅约 31.3%，小于 P95 的降幅。这与仍存在重建慢路径相符，但不是单凭分位数就能确认所有尾延迟来源。

我会按快照命中/重建、来源查询、连接等待和 GC 等进一步分类采样。如果尾部主要来自重建，可评估旧值服务或提前刷新；若来自协同过滤，继续改快照未必有效。不能只汇报平均值或 P95，就把 P99 仍较高的问题省略。

**依据：** E14 锁内重建；E25 Before/After 表。

### Q108：20 RPS 三轮通过说明什么？能写成高并发生产能力吗？

**回答模板：** 它说明在该单实例、固定数据和账号分布下，Recommend 20 RPS、每轮五分钟的三次历史验收都通过既定条件。三轮 P95 分别约 64.99、63.95、63.96 ms，取中位数 63.96；P99 的轮间中位数约 202.62 ms。它不是把三轮全部请求混合后重新计算的全局分位数。

这个负载对十万视频、多路 SQL 有参考意义，但不能外推为真实大规模平台，更不能乘副本数就得到容量上限。数据分布、候选耗尽、账号热点、写比例和依赖资源都影响结果。简历保留“单机、数据规模、RPS、持续时间和无降级条件”，比笼统写高并发更能经受追问。

**依据：** E25 验收报告；`build/performance/medium-v1/acceptance-20260929/` 下的 `recommend-20rps-r1.json`、`recommend-20rps-r2.json`、`recommend-20rps-r3.json`。

### Q109：综合 50 RPS 的 104.11 ms，能与纯推荐 63.96 ms 直接比较吗？

**回答模板：** 不能当成同负载的退化或回归。综合测试中 Timeline 45%、Hot 15%、Recommend 20%、曝光 10%、Like 5%、Follow 3%、Publish 2%，总流量 50 RPS，运行十五分钟；写入会改变缓存、画像和候选，竞争资源也不同。Recommend P95 104.11 ms 是该混合场景的结果。

本轮 45001 个业务请求，Timeline 和 Hot P95 分别约 5.39、5.20 ms，HTTP 与脚本业务成功率为 100%，四类推荐治理增量为零。报告同时明确尚未完成综合 100 RPS 和 50 RPS 三十分钟稳定性测试，所以不能据此说最终容量已验收或确认无内存泄漏。

**依据：** E25 验收报告及 `mixed-50rps-15m.json`。

### Q110：k6 的 constant-arrival-rate 比固定并发用户更适合什么问题？

**回答模板：** 项目的正式档位使用按到达速率调度，目标是让被测系统承受指定请求进入速率；固定用户循环在接口变慢时会自然减少发送速度，可能掩盖系统在固定外部需求下的排队压力。arrival 模式仍依赖足够的虚拟用户和压测机能力，所以要看 dropped_iterations。

若丢弃迭代不为零，名义 20 RPS 不一定实际全部压到服务，不能拿低延迟宣称通过。还要区分 setup 中登录准备和正式业务请求数量，保证前后同样预热、数据和账号池。当前脚本也包含 smoke 的固定用户模式，它适合功能连通性检查，不能与容量档混为同一个实验。

**依据：** E25 `scripts/performance-load.js` 的 scenario 和阈值。

### Q111：业务成功率 100% 是否意味着推荐结果一定正确？

**回答模板：** 现有脚本除检查状态码，还检查 scene、items 是数组、是否有 next_cursor，并单独监控四类推荐治理事件，比只看 HTTP 200 更可靠。但 feedBodyIsValid 没要求 items 非空，也没有检查屏蔽作者、跨页重复或结果相关性，因此“业务成功率”只代表这些断言全部满足。

我会保留指标原口径，并补有代表性的语义校验：固定用户不得返回指定屏蔽内容，重复率和空页率在范围内，点赞最终状态与期望一致。不能把压力测试变成每请求全库校验，但可以抽样并用独立校验阶段。性能和推荐质量是两个验收维度，不应该共用一个“成功”掩盖差异。

**依据：** E25 `record/feedBodyIsValid`；E14/E17。

### Q112：请求都成功了，为什么还要检查 Outbox、Lag、DLQ？

**回答模板：** 异步接口可以很快返回，但任务可能全部堆在后面。如果只看前台吞吐，就可能用不断增长的积压换取表面成功。测试应在运行期间和结束后查看 pending 数、最老事件年龄、消费进度、重试与 DLQ，并确认数据库事实和派生索引最终收敛。

报告末尾这些指标回到基线是正向证据，但当前 Kafka Lag 埋点依据本次 poll 的 high watermark 与取到记录 offset 估算，不等同于独立比较 broker 末端与组已提交位点。空闲分区或进程失联时仅看这个 gauge 有盲区，生产验收应结合 broker/group 侧观测及具体业务对账。

**依据：** E09 `consume`；E23 `ObserveKafkaConsumerLag/SetOutboxStatus`；E25。

### Q113：一次推荐突然超时，你会按什么顺序排查？

**回答模板：** 先确认影响范围：仅 recommend 还是 timeline/hot 也慢，再看 HTTP 延迟、错误、限流和推荐治理决策，区别数据库慢、隔舱满、熔断及主动降级。用 request_id/trace_id 找具体请求，结合每路召回耗时、缓存命中、SQL 执行计划和连接池等待定位最慢环节。

若仅 Hot 重建时慢，检查快照是否频繁失效和是否多实例同时重建；若协同路慢，检查活跃用户行为膨胀；若整体慢，优先保护数据库并控制流量。恢复后重放相同场景确认正常路径没有被降级掩盖。这里是排障方案，不能用模板虚构自己处理过某次线上故障。

**依据：** E14/E22/E23/E25。

### Q114：再给一周优化时间，你会先做什么，如何验收？

**回答模板：** 我会先补会直接违背用户意图的边界：最终推荐输出统一负反馈过滤、点赞乱序和丢失窗口的复现实验，以及删除后缓存可见性；对外部署前还应优先补齐 Q122 的内部接口鉴权遗漏。并行规划性能上剩余的综合 100 RPS 和三十分钟稳定性测试，但不在正确性没明确前只追求更漂亮 P95。

每项都有具体验收：被屏蔽作者不能从辅助源漏出；like/unlike 重复乱序后最终状态符合最新版本；预热后删除的内容在规定时限内不可见；稳定性测试中资源、积压和错误不持续增长。若只优化性能，则根据来源指标挑新的最大成本，例如协同过滤，而不是继续堆缓存层。

**依据：** E02/E07/E12/E14/E25；上述修复是建议，不是本次已实施内容。

## 十三、播放、上传、账号与消息：Q115–Q124

### Q115：上传视频做了哪些校验？为什么扩展名校验还不够？

**回答模板：** 上传先通过 MaxBytesReader 限制请求体，再限制文件种类、扩展名和嗅探 MIME；视频还落临时业务文件后用 ffprobe 检查是否有视频流、时长、分辨率以及音视频编码。当前请求上限约 1 GiB，视频 512 MiB、图片 20 MiB，视频最长十分钟、单边最大 3840。

扩展名由客户端给出，改名不代表内容合法；MIME 嗅探也不能代替容器和编解码检查。文件名由时间戳和随机后缀生成，kind 经过枚举处理，减少路径拼接风险。探测或 faststart 失败会删除已保存文件，但仍缺用户存储配额和完整孤儿文件回收，不能说有大小限制就不存在磁盘耗尽风险。

**依据：** E21 `Create/validateUploadFile/validateVideoMetadata`。

### Q116：faststart 优化了什么？是不是重新压缩视频？

**回答模板：** 对 MP4/MOV 路径调用 ffmpeg，使用 `-c copy -movflags +faststart` 并输出 MP4，主要重新安排容器元数据，帮助播放器更早获取播放信息，不是重新编码压缩视频。处理写到临时文件，成功后 rename 替换，失败清理，并有两分钟子进程超时。

它可能改善需要先获取元数据的首帧体验，但不能保证所有设备首帧按固定比例下降；网络、文件大小和编码支持仍然影响播放。当前同步在上传请求里执行，长视频会占请求时间和磁盘 I/O。若负载增大，可改异步处理状态机，但发布接口必须等资源可用或明确展示处理中，不能提前返回不可播放地址。

**依据：** E21 `shouldFaststart/faststartVideo`。

### Q117：Range 请求为什么对视频播放重要？你自己实现了吗？

**回答模板：** 拖动进度或分段加载时，浏览器可请求文件的部分字节，而不必每次从头下载。项目通过 Gin 的静态文件路由服务 uploads，复用底层 HTTP 文件服务的 Range 能力，并有范围读取测试；不是手写了一套媒体流协议。范围响应涉及 206、Content-Range，不可满足范围通常是 416，可对照 [HTTP Semantics RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-14)。

验证时应检查返回字节与范围是否一致，而非只检查 200。Range 也不等于 HLS、自适应码率或 CDN；本地文件服务在多副本、带宽和存储上仍有限制。迁移对象存储后，要重新验证代理和存储端是否正确转发范围请求及缓存响应。

**依据：** E01 `g.Static`；E21 Range 测试。

### Q118：预加载怎样知道“下一个视频”？它与推荐列表一致吗？

**回答模板：** 当前预加载仓储根据 currentVideoID 找其发布时间，然后按时间线顺序查询更早的公开视频，返回媒体和封面地址。它没有拿推荐会话的候选序列，因此在推荐页上，“时间线的下一条”不一定是用户实际即将刷到的那条。

播放配置按 platform/network_type 查询，缺记录时先回平台默认网络配置，再用领域默认；数据库查询错误则返回错误，并非所有故障都能默认成功。更准确的预加载应使用当前 Feed 后续 ID，并结合网络类型限制数量，测首帧收益和浪费流量。不能把当前基础预加载讲成已实现智能预测下载。

**依据：** E18 `GetConfig/ListPreloadVideos`。

### Q119：播放 QoS 收集什么？怎么判断 faststart 或预加载真有收益？

**回答模板：** QoS 保存首帧耗时、卡顿次数、观看时长以及用户和视频，使用用户加幂等键避免重复报告。首帧字段用可空值表达，没采集到与耗时为零不同；服务端会校验基础参数，但数据仍依赖客户端测量口径。

验证优化时我会在相同资源、设备和网络条件下比较首帧分位数、卡顿率、播放失败率及额外下载量，区分缓存冷暖和自动播放失败。当前仓库有上报接口与测试，没有足够实测材料证明 faststart 或预加载带来具体百分比提升。不能把后端 Feed P95 的改善直接当作播放器首帧改善。

**依据：** E18 QoS Service/Repository；E21；播放 API 测试。

### Q120：JWT 登录和登出怎么实现？退出后旧 token 还能用吗？

**回答模板：** 注册时密码只保存 bcrypt 哈希，登录校验后签发 HS256 access token，包含 uid、role、类型、iat、exp 和 jti。鉴权中间件验证允许的算法、签名、类型和用户 ID，再把身份放到请求上下文。业务访问的身份来自验证后的 token，不该由用户任意传入。

当前登出只是无状态模式下返回成功，让客户端删除 token，没有服务端撤销表；未过期旧 token 在相同密钥下仍可能使用。角色变更也不会自动改已签声明。若需要即时失效可设计版本号或撤销机制，但需额外查询、缓存和生命周期管理。回答不能把“有登出接口”说成“令牌已作废”。

**依据：** E20；`domain/account/entity.go`、`interfaces/http/account/handler.go`。

### Q121：Feed 为什么用可选 JWT？无效 token 是拒绝还是当游客？

**回答模板：** 时间线和热榜允许游客看，登录用户则额外获得自己的点赞收藏状态；Following 和 Recommend 需要 ViewerID。可选中间件在无 Authorization、格式不对或解析失败时继续按游客处理，只有验证通过才注入身份，所以它与必须登录的写接口中间件语义不同。

这样公共页面不会因一个旧 token 完全打不开，但用户可能突然失去个性化，需要客户端清理登录态或提示重登。它不意味着游客能操作关注和点赞，因为那些路由仍使用强制鉴权。测试应覆盖无 token、坏 token、过期 token 和有效 token，并检查最终场景是否正确返回需登录错误。

**依据：** E20 `NewOptionalJWTAuth/NewJWTAuth`；E01/E02。

### Q122：简历说内部接口有 X-Internal-Token，实际每条都保护了吗？

**回答模板：** 不能照搬旧描述。中间件本身实现了共享令牌和常量时间比较，空配置会拒绝；治理开关、内部消息、内部 QoS、Outbox 查询路由确实挂了它。但 `/internal/recommendation-candidates`、`/internal/exposure-decisions`、`/internal/exposures` 没有挂该中间件，路由组和对应 Handler 也没有等价检查。

这意味着若 API 端口可访问，调用者可直接向这些入口提交 user_id，产生跨用户候选查询或曝光写入风险。“internal” 路径名不提供鉴权。我会优先在该路由组统一施加认证，补无令牌、错误令牌、正确令牌测试，再讨论私网、TLS 和密钥轮换。本文只是指出现状，未实施修复；相关简历亮点应该改成“部分内部接口已鉴权，存在待补齐入口”。

**依据：** E01 `router.go:220` 起的注册；E20；`interfaces/http/recommendation/handler.go`。

### Q123：消息中心如何避免重复通知，点赞取消再点赞会再次通知吗？

**回答模板：** 消息仓储按接收用户与事件 ID/幂等键做唯一约束，重复插入后查回已有记录。点赞通知的业务事件 ID 由视频和点赞用户组成，评论通知由 comment_id 组成，所以同一次评论重试不会多条通知；对同视频取消后再点赞，也可能复用原点赞通知身份。

这是一种“同用户对同视频最多保留一次点赞通知”的语义，不是每次动作变化都必发一条。更关键的是通知调用错误被忽略，数据库点赞或评论成功不等于消息必达。若要可靠通知，应让明确的业务事件驱动消息消费者，并保留幂等键；不能仅凭名字 CreateFromEvent 就说它已是 MQ 消费链。

**依据：** E12 `notifyLike/notifyComment/createInteractionMessage`；E19。

### Q124：消息已读、未读统计和分页怎样防越权与并发重复更新？

**回答模板：** 消息列表按当前用户和 `(created_at,id)` 游标查询；CountUnread 按 user_id 与 is_read=false 统计；MarkRead 同样带接收用户条件，只更新未读记录。即使客户端提交别人的 message_id，也不应修改那条消息；重复标记已读后第二次更新数为零。

空 messageIDs 在当前协议中表示全部已读，这是有业务含义的约定，前端不能把意外空数组当“什么也不做”。批量全已读与新消息同时到达时，结果取决于数据库语句实际覆盖的记录；若需要“只标记点击时已看到的消息”，可以使用时间或最大 ID 截止点，避免误处理随后到达的消息。

**依据：** E19 `List/CountUnread/MarkRead` 及 Repository。

## 十四、治理、工程验证与压力追问：Q125–Q132

### Q125：限流算法是什么？部署三个 API 后配额会自动共享吗？

**回答模板：** 当前是进程内按 IP 的 Token Bucket，用当前时间与上次时间差补充 token，上限为 burst；够一个 token 就扣减放行，否则返回 429 和 Retry-After。客户端表最多 10000 项，满时任意淘汰一个，健康检查与 metrics 不受这个限流影响。

三个副本各有自己的桶，不会自动形成全局统一配额；同一个 NAT 出口的大量用户也可能共用 IP 配额。代理部署还要确认 ClientIP 的可信来源。若需要全局预算，优先在网关统一限流或设计共享状态，但应该先明确按用户、IP 还是资源成本计费。测试应覆盖突发、补充、淘汰和多实例分摊，不能只看单次 429。

**依据：** E22 `tokenBucket/NewRateLimit`。

### Q126：有 context 超时为什么仍可能晚于 deadline 才返回？

**回答模板：** HTTP 中间件把 deadline 放到 request context，下游数据库、Redis 等需要配合检查它；它不会强行中止任意 Go 代码。推荐主召回另有默认 500 ms 预算，最多一次重试，并在同一个预算里等待默认 25 ms 退避。若阻塞在普通互斥锁或不响应 context 的操作上，返回仍可能晚。

上传和静态媒体路由被排除在普通请求超时之外，ffprobe/ffmpeg 自己有不同上限，符合资源处理更长的特点。重试只适合可安全重复的操作，不能看到 500 就对所有写请求无限重试。验证需要慢依赖、父请求取消和锁等待场景，并测实际返回时间及资源释放，而非只断言 context.Err 非空。

**依据：** E22 `NewRequestTimeout`；E03 `governedCandidateSource`；E21。

### Q127：隔舱和熔断分别保护什么？半开如何避免所有请求同时探测？

**回答模板：** 隔舱用容量 32 的 channel 限制同时进入推荐主召回的请求，满了立即尝试兜底；熔断记录失败次数，达到阈值 5 后打开十秒，冷却结束允许一个请求进入半开，其他请求继续拒绝。成功重置为关闭，失败再次打开，参数游标错误按依赖可达处理，不当作依赖故障。

这些都是进程内状态，并非集群全局隔离；兜底 Hot/Timeline 也会用数据库，所以主源被隔离不代表数据库总负载有上限。并发在途结果还可能影响熔断状态，需要真实时序测试，不能只画三态图。应同时验证冷却探测、slot 释放、父 context 取消和手工降级开关，避免泄露并发名额。

**依据：** E22 `sourceGovernor`；E03 `governedCandidateSource.Load`。

### Q128：Prometheus、日志和 Trace 如何配合？Outbox 的 Trace 连续吗？

**回答模板：** Metrics 用于看整体趋势，例如场景延迟、缓存命中、来源耗时、治理决策、Worker 任务与 Outbox；结构化访问日志保存 request_id，存在有效 span 时记录 trace_id；HTTP 和 Kafka 有 OTel 埋点，Kafka 通过 record headers 注入和提取传播信息。配置 OTLP endpoint 才会启用实际导出。

但 Outbox Event/表没有保存原始请求的 Trace 上下文，调度 Worker 从自己的 context 发消息，因此跨持久化边界不能默认认为还是原 HTTP trace。事件 ID 仍可关联事实。后续若需要完整因果链，可保存传播字段或使用 span link；还要控制指标标签基数，不把任意 user_id、event_id 都塞进 Prometheus 标签。

**依据：** E23；E08 Event；E09 Kafka headers；E10 Outbox。

### Q129：有 Compose 和 Kubernetes YAML，就代表可以生产高可用部署吗？

**回答模板：** 它们证明项目有可复用的编排配置，不代表部署和故障恢复已通过生产验收。Compose 可启动完整开发依赖，K8s 清单包含 Namespace、配置 Secret、Service、PVC 和若干探针，但主要还是 API、Worker、Web、MySQL、Redis、RabbitMQ 的单副本配置，没有完整同步当前 Kafka/Tempo 拓扑。

而 API 的 /health 固定返回成功，只说明进程能处理这条 HTTP 请求，不证明数据库、事件通道或 Worker 已准备好。生产还要区分存活与就绪、存储备份恢复、资源限制和媒体共享方案。不能直接把数据库 Deployment replicas 从一改三就称为复制集；这些都需要组件自己的数据一致性协议。

**依据：** E24；E01 `HealthCheck`；部署清单。

### Q130：启动自动迁移和优雅退出有哪些实际边界？

**回答模板：** API/Worker 启动时会迁移模型，补缺视频统计并创建时间线索引；迁移对部分并发 DDL 错误做有限重试。这对本地开发方便，但多副本同时升级可能竞争元数据锁，大表变更也会拖慢启动。生产更适合可审查、可回退的独立迁移步骤，并保持应用版本兼容。

API 收到终止信号后使用 http.Server.Shutdown 等待在途请求，默认退出预算有限；Worker 取消 context 后关闭总线，但没有统一等待所有业务 goroutine 完成的显式任务组，未完成任务仍需靠重新投递恢复。因此部署时要让 termination grace period 与处理上限配合，不能把“监听 SIGTERM”直接等同于所有任务一定完成。

**依据：** E24 迁移；E23 `Run`；E01 Worker main；E09 `Close`。

### Q131：你有哪些测试？这次说测试通过，具体意味着什么？

**回答模板：** 项目有 Feed 路由分页、推荐多源过滤、缓存与计数、关系消息、上传 Range、Fanout、向量、Outbox、Kafka 重试与治理等测试，很多 API 测试通过内存仓储和假缓存隔离用例。它们能验证规则和调用约定，但不能证明 MySQL 锁竞争、Redis 事务冲突或 broker 崩溃恢复都正确。

本次用 Go 1.25.4 执行 `env -u KAFKA_BROKERS go test ./...`，命令成功，多数包复用了测试缓存；真实 Kafka 集成测试因没有该环境变量不在实测范围，也没有重新跑历史 k6。这是已有测试的校验，不是生产稳定性证明。下一批值得增加的是跨层反例测试：辅助源漏过滤、点赞乱序、缓存热态删除和真实数据库并发。

**依据：** E27 及各包 `_test.go`；本次工具执行记录。

### Q132：只有 20 RPS、用伪 embedding、又基于开源项目，你的贡献是什么？

**回答模板：** 我会直接接受这些事实，并把贡献范围讲具体：这是对已有业务基线的工程改造，最有证据的是能沿读路径定位重复热度排序，在不放宽推荐截止时间和四类治理验收条件的前提下，用有限快照显著降低限定实验中的延迟与 CPU。代码还展示了混合分发、事件可靠性和推荐反馈，但个人完成哪些部分要对应实际提交与测试记录。

我不会把单机 20 RPS 包装成大厂规模，也不会把 hash 向量说成算法创新。真正学到的是，先定义排序、事实、缓存和异步的语义，再谈优化；例如“有幂等表”不等于 exactly-once，“有 TTL”不等于自动一致。后续会优先补已识别的正确性窗口和效果验证，再扩大容量。这样回答能把项目能力、个人成长和局限放在同一套证据上。

**依据：** README 来源说明；E14/E25；个人贡献必须自行核实，不由模板代填。

## 十五、面试前的事实纠偏与验证记录

### 不要照搬的旧表述

| 容易被追问击穿的说法 | 建议改成 |
| --- | --- |
| 项目只使用 RabbitMQ | 支持 Kafka 优先初始化与 RabbitMQ 兼容路径，实际以后端配置和初始化结果为准 |
| 全链路事件不丢、严格 exactly-once | Kafka 模式下发布等写入有事务 Outbox，至少一次投递配合业务幂等；存在覆盖和原子性边界 |
| 所有 Feed 都有同样的页缓存和自动回源 | Timeline 接通通用页缓存，其他场景各有候选索引；Hot 的 Redis 故障没有同样的 SQL 自动回退 |
| 稳定游标保证热榜、推荐永不重复遗漏 | 时间线复合游标明确；动态榜单和推荐缺会话级长期固定快照 |
| 缓存 15 秒后必然与数据库一致 | JSON TTL 为 15 秒，base/shards 生命周期更长，需要明确对账与切换机制 |
| 完成了深度语义推荐/全库 ANN | 用 128 维本地 hash n-gram 在有界候选中做相似度，打通推荐工程链路 |
| 所有来源统一过滤屏蔽作者 | 推荐 Service 内六路统一过滤；Feed 外层首屏辅助来源仍需补出口约束 |
| Bloom 是精确最近 30 天去重 | 固定 Bitmap、整个 key 续期，存在误判及时间窗口差异 |
| 消息中心是可靠异步通知消费者 | 消息存储和幂等已实现，当前多处通知由应用直接调用且忽略错误 |
| 内部推荐和曝光接口均有共享令牌鉴权 | 中间件存在，但三条内部推荐/曝光路由未挂载，必须逐入口补齐并验证 |
| JWT 登出后令牌立即失效 | 当前无服务端撤销机制，客户端删除令牌，旧令牌可能在过期前仍有效 |
| Kubernetes 清单证明高可用生产落地 | 有基础单副本清单，仍缺完整新栈部署和生产恢复验证 |
| 推荐性能优化证明用户效果更好 | 已有延迟与资源对照，尚无点击、完播、留存 A/B 证据 |

### 本次核查与历史实验分开记录

- **本次已做：** 读取入口装配、核心业务调用链、持久化和缓存实现、测试、部署及压测脚本；检索 TODO/Mock/Stub 等并区分测试替身与真实空壳；核对仓库简历描述；读取保存的历史 k6 JSON。
- **本次测试：** 在 `apps/api` 用 Go 1.25.4 执行 `env -u KAFKA_BROKERS go test ./...`，退出码 0；输出包含缓存结果。没有新增业务代码和测试。
- **没有在本次执行：** 启动服务或部署、真实 Kafka 集成测试、重新生成压测快照、k6 性能复测、故障注入、线上推荐效果实验。
- **历史证据：** 5 RPS Before/After、20 RPS 三轮及混合 50 RPS 的 JSON 已在本机核对，位于 Git 忽略的 `build/performance/medium-v1/`。不要认为这些文件一定随仓库克隆携带；分享项目时应另行保留去敏后的性能材料。
- **已有工作区修改：** 核查时 `apps/docker-compose.yml` 和 `docs/performance-testing.md` 已有修改，本次文档未改动这两个文件。
- **代码风险表述：** 文中崩溃窗口、死锁可能性、乱序与过滤缺口来自静态调用链分析；除明确引用测试或历史报告之处外，没有宣称已经动态复现。

### 建议练习路线

第一次按 Q001 → Q009 → Q016 → Q019 → Q031 → Q046 → Q069 → Q088 → Q103 讲清主线，再沿每章连续追问。第二次重点练 Q018、Q027、Q029、Q042、Q047、Q049、Q060、Q064、Q080、Q096、Q111，训练在面试官指出边界时如何承认现状并给出可验证修复。第三次才练 Q087、Q092、Q102、Q114、Q132 的方案与价值挑战，确保把“已做”和“准备做”分清。
