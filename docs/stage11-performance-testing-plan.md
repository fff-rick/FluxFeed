# FluxFeed Stage 11 压测与性能优化详细设计

> 文档定位：为 FluxFeed Stage 11 提供可复现、可比较、可解释的压测方案。
>
> 基准环境：当前单机 Docker Compose 部署。
>
> 工具边界：复用 k6、Prometheus、Grafana、Tempo、Docker 和 MySQL，不新增压测平台。
>
> 结果原则：README 和简历只记录真实运行结果，不使用本文的目标值冒充实测数据。

## 1. 背景与目标

FluxFeed 已完成 Hybrid Feed、Redis 多级缓存、Kafka 事件总线、Transactional
Outbox、推荐反馈闭环、系统治理和可观测性建设。Stage 11 不再以“接口能够被请求”
为目标，而是回答以下问题：

1. Timeline、Hot、Recommend 三类读链路在固定环境中的吞吐和延迟是多少？
2. Publish、Like、Follow 等写请求增加时，Outbox 和 Kafka 消费能否及时收敛？
3. 热点缓存、推荐召回、MySQL 连接池或异步消费者中，谁是当前容量上限？
4. 突发流量和长时间运行时，限流、超时、熔断、降级是否符合预期？
5. 一项优化是否在相同输入下带来可复现的收益，并且没有把压力转移到其他组件？

本方案的最终产物不是一个孤立的 QPS 数字，而是同一环境下可回放的
`Baseline -> 瓶颈证据 -> 优化 -> Retest` 对比记录。

### 1.1 压测范围

| 类型 | 覆盖能力 | 主要入口 |
| --- | --- | --- |
| Feed 读取 | Timeline、Hot、Recommend、游标分页、卡片组装 | `GET /api/feed-items`、`POST /api/feed-queries` |
| 反馈写入 | 曝光、点赞、关注以及缓存/画像失效 | `POST /api/video-view-events`、Like、Follow API |
| 内容供给 | 视频发布、Outbox、Kafka、Fanout 和 Embedding Worker | `POST /api/videos` |
| 缓存 | L1、Redis、热点 Key、分散 Key、冷启动和回源 | Feed API、`scripts/feed-cache-load.js` |
| 异步链路 | Outbox 发布、消费者重试、DLQ、Consumer Lag | Worker、Kafka、MySQL |
| 治理 | 限流、超时、隔舱、熔断、推荐降级 | HTTP Middleware、Recommend Pipeline |
| 资源 | API/Worker CPU、内存、GC、MySQL 连接池 | Docker、Prometheus、MySQL |

### 1.2 非目标

- 不用单机结果推导生产集群容量，也不声称获得跨机房或多副本结论。
- 不在本阶段引入分布式压测控制面、服务网格或新的监控组件。
- 不把媒体文件上传和转码纳入主业务混合流量；该链路有独立的两分钟超时和大文件变量，
  应作为媒体专项测试。
- 不通过关闭校验、删除业务逻辑或无限扩大连接池来制造更高 QPS。
- 不在容量测试中混入预期的 429，再把它统计为服务容量错误。

## 2. 从阶段能力到压测证据

| 已完成阶段 | 压测关注点 | 可证明的工程能力 |
| --- | --- | --- |
| Stage 3 Hybrid Feed | 普通作者 Publish 后 Inbox 写扩散；大 V 走 Pull | Fanout 延迟、失败量、关注流读取延迟 |
| Stage 4 Redis Cache | 冷/热缓存、热点与分散 Key、Redis 故障 | 命中率、回源比例、热点 P95、SWR 行为 |
| Stage 5 Kafka | 发布与多消费组处理 | 事件吞吐、Consumer Lag、处理耗时 |
| Stage 6 可靠消息 | 写请求后的 Outbox、幂等、重试和 DLQ | Pending/Failed、最老积压、重复请求结果 |
| Stage 7 推荐系统 | 六路召回、预排、精排和重排 | 各召回源候选数、召回耗时、Recommend P95 |
| Stage 8 反馈闭环 | 曝光和正负反馈写入 | 写延迟、反馈事件消费、Seen/画像更新 |
| Stage 9 系统治理 | 限流、超时、隔舱、熔断、降级 | 429、治理决策计数、降级可用性 |
| Stage 10 可观测性 | Metrics、Logs、Traces | 从慢请求定位到组件和依赖的证据链 |

## 3. 基准环境

### 3.1 拓扑

```text
Host k6
   |
   v
API :8080 --------------> MySQL
   |  \-----------------> Redis
   |  \-----------------> Kafka
   |                       |
   |                       v
   +-------------------- Worker

Prometheus :9090 <------- API /metrics
Grafana :13000 ---------- Prometheus + Tempo
Tempo :3200 <------------ API / Worker Trace
```

k6 在宿主机运行，API 和全部依赖运行于当前 `apps/docker-compose.yml`。这种部署
不能消除压测端与被测端争抢宿主机资源的影响，因此每份报告必须记录宿主机配置，
结果只用于同机 Before/After 比较。

### 3.2 每次测试必须记录

```bash
git rev-parse HEAD
git status --short
docker version
docker compose version
k6 version
docker compose -f apps/docker-compose.yml images
```

报告还需人工记录：

- 测试开始和结束时间、时区。
- CPU 型号、逻辑核数、物理内存、操作系统。
- Docker 可用 CPU/内存及是否存在其他高负载进程。
- Compose 服务镜像和容器重启次数。
- API 配置、数据快照版本、缓存状态和 k6 参数。
- Git 工作区是否有未提交变更；有变更时附 `git diff --stat`。

### 3.3 两套治理配置

容量测试和治理测试不得共用限流口径。

| 配置 | `access_ttl` | `rate_limit_rps` | `rate_limit_burst` | 用途 |
| --- | ---: | ---: | ---: | --- |
| 容量配置 | `2h` | `10000` | `20000` | 基线、阶梯、突发、稳定性和综合流量 |
| 默认配置 | `15m` | `50` | `100` | 验证按来源 IP 的 Token Bucket 限流 |

容量配置需要在压测前写入被 API 实际加载的配置并重启 API。配置变化必须进入报告，
测试结束后恢复默认值。`2h` JWT 用于避免 30 分钟稳定性测试中 Token 到期；不改变
认证逻辑。容量配置仍保留 3 秒普通请求截止时间和 500 ms 推荐主召回超时。

> 当前限流按客户端 IP 生效。宿主机单实例 k6 的请求通常共享一个来源 IP，默认
> 50 RPS 配置天然会截断容量曲线，因此容量测试必须使用上述高限流配置。

### 3.4 启动与健康检查

```bash
cd apps
docker compose up -d --build
docker compose ps

curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/metrics >/dev/null
curl -fsS http://127.0.0.1:9090/-/ready
curl -fsS http://127.0.0.1:13000/api/health
```

进入正式压测前，所有服务应稳定运行至少 2 分钟，API 和 Worker 不得处于重启循环，
Outbox Pending、Kafka Lag 和错误率应处于静止或稳定状态。

## 4. 基准数据设计

### 4.1 固定数据集

默认本机基准数据集命名为 `medium-v1`：

| 数据 | 规模 | 分布要求 |
| --- | ---: | --- |
| 用户 | 12,000 | 200 个压测账号、普通作者、活跃消费者和大 V |
| 视频 | 100,000 | 状态有效；发布时间覆盖最近 30 天，最近 24 小时保留足量内容 |
| 关注关系 | 1,000,000 级 | 长尾分布；至少 2 个作者拥有不低于 10,000 名粉丝 |
| 点赞/收藏 | 1,000,000 级 | 20% 热门视频承载约 80% 行为，保留长尾样本 |
| 观看/曝光 | 1,000,000 级 | 包含 exposed、valid_play、finish 和少量负反馈 |
| 视频向量 | 覆盖推荐候选 | 热门、最近、关注和相似召回均不能长期返回空集合 |

大 V 数据用于越过当前 10,000 粉丝阈值并触发 Pull 分支；普通作者用于验证 Push
Fanout。数据量是默认值，不是生产规模声明。

### 4.2 账号池与目标池

- 建立 200 个独立压测账号，拆分为互不重叠的只读池与写入池，凭据保存于不提交 Git 的
  本地数据文件。
- setup 阶段分别登录并构建 Token 池；VU 通过 `(__VU - 1) % pool_size` 选择身份。
- Timeline、Hot、Recommend 使用只读池；Exposure、Like、Follow、Publish 使用写入池。
  `mixed`、`capacity`、`spike`、`soak` 会拒绝账号重叠，避免写行为改变读基线画像。
- 视频目标池排除已删除、未发布和作者自己的非法目标；关注目标池排除当前用户自身。
- 写请求按 VU 与迭代号轮换目标，避免所有请求只竞争同一数据库行；缓存专项除外。

### 4.3 幂等与业务副作用

写请求必须使用唯一且可追踪的键：

```text
Idempotency-Key: perf-<run_id>-<scenario>-<vu>-<iter>
request_id:       perf-<run_id>-<scenario>-<vu>-<iter>
```

- Publish 每次创建新视频，会增加 `video`、`video_stat`、`outbox_event` 以及异步数据。
- Like 使用 PUT 设置最终状态；重复点赞不会产生稳定的持续写压力。目标池应轮换，必要时在
  独立迭代中配对 Unlike，但 Unlike 不计入规定的 Like RPS。
- Follow 同样是状态设置操作，应轮换用户与目标；清理用 Unfollow 放在测量窗口之后。
- Exposure 使用 `event_type=exposed`，成功应返回 201，并校验响应中的 `event`；同一
  `request_id` 只用于一次业务请求。
- Publish、Like 和 Follow 的异步副作用必须在流量结束后继续观察 5 分钟。

Baseline 与 Retest 必须从相同 MySQL 快照开始。Redis 缓存包含 TTL，不复制 RDB；每次恢复
后清空性能专用 Redis、重启 API 清除 L1，再执行同一预热步骤。写场景结束后还原快照或为
下一轮使用新的隔离数据分片，不能直接在被上一轮污染的数据上比较。

### 4.4 数据就绪检查

数据生成不计入压测时间。开始前至少确认：

```sql
SELECT COUNT(*) FROM account;
SELECT COUNT(*) FROM video WHERE status = 2;
SELECT COUNT(*) FROM user_follow WHERE status = 1;
SELECT COUNT(*) FROM interaction_action WHERE status = 1;
SELECT COUNT(*) FROM video_view_events;
SELECT COUNT(*) FROM video_embedding;
SELECT status, COUNT(*) FROM outbox_event GROUP BY status;
```

同时用压测账号各请求一次 Timeline、Hot、Following 和 Recommend。任何核心场景长期
返回空列表，都视为数据准备失败，而不是一次有效的低延迟结果。

## 5. 请求模型

### 5.1 接口与断言

| 业务 | 请求 | 鉴权 | 成功状态 | 最小业务断言 | 主要副作用 |
| --- | --- | --- | ---: | --- | --- |
| Timeline | `GET /api/feed-items?scene=timeline&limit=20` | 可选 | 200 | `items` 为数组，存在 `next_cursor` 字段 | Feed/卡片缓存读取 |
| Hot | `GET /api/feed-items?scene=hot&limit=20` | 可选 | 200 | `items` 为数组 | 热榜与卡片缓存读取 |
| Recommend | `POST /api/feed-queries`，`scene=recommend` | Bearer | 200 | `items` 为数组，响应 `scene` 正确 | 多路召回、Seen Filter |
| Exposure | `POST /api/video-view-events` | Bearer | 201 | `event.video_id` 与请求一致 | 观看流水、曝光聚合、Outbox |
| Like | `PUT /api/videos/{videoId}/like` | Bearer | 200 | `active=true`、`action_type=LIKE` | Redis 计数、事件发布/回落 |
| Follow | `PUT /api/users/me/following/{targetUserId}` | Bearer | 200 | `following=true` | 关系、缓存失效、Outbox |
| Publish | `POST /api/videos` | Bearer | 201 | 返回正整数 `id`，作者正确 | 视频、统计、Outbox、Fanout |

Publish 使用预先准备的可访问媒体 URL 与封面 URL，不在主压测中重复上传媒体。所有请求
均设置独立 k6 `tags`，至少包含 `endpoint`、`scene` 和 `operation`，从而为每类接口
单独设置阈值，不能只看聚合后的 `http_req_duration`。

### 5.2 游标模型

- 单接口首屏基线固定空游标，用于衡量热点路径。
- 综合流量中，每个 VU 保存自己的 `next_cursor`，80% 请求继续翻页、20% 回到首屏。
- `has_more=false`、游标为空或目标数据不足时回到首屏，不能继续发送无效游标。
- Recommend 辅助召回只参与首屏，首屏和后续页必须分开打 Tag，避免平均值掩盖差异。

### 5.3 缓存状态

每个读场景分别记录两种状态：

1. **Cold**：重启 API 清空 L1；清理仅限本次测试前缀或恢复 Redis 测试快照，然后立即
   执行一次固定流量。不得对未知环境运行无范围的 `FLUSHALL`。
2. **Warm**：用 5 VU 对目标场景预热 5 分钟，确认缓存命中率稳定后开始正式测量。

Cold 和 Warm 结果分别报告，不取平均值。综合、阶梯、突发和稳定性场景默认使用 Warm。

## 6. 场景矩阵

除冒烟外，使用 k6 `constant-arrival-rate` 或 `ramping-arrival-rate` 控制到达率，避免
响应变慢后闭环 VU 自动降低请求量。`preAllocatedVUs` 应覆盖目标 RPS，运行中出现
`dropped_iterations` 即说明压测端供给不足，结果无效并需要增加 VU 后重跑。

### 6.1 场景总表

| ID | 场景 | 负载模型 | 持续时间 | 核心判定 |
| --- | --- | --- | --- | --- |
| S0 | 冒烟 | 1 VU，顺序覆盖全部接口 | 1 分钟 | 无意外 4xx/5xx，断言全部通过 |
| S1 | 单接口基线 | Timeline/Hot/Recommend 各 10 RPS，独立运行 | 各 5 分钟 | 得到 Cold/Warm 延迟和组件基线 |
| S2 | 写链路 | Publish 2、Like 10、Follow 5 RPS | 10 分钟 + 5 分钟排空 | HTTP 成功，Outbox/Kafka 收敛 |
| S3 | 综合流量 | 固定 100 RPS | 15 分钟 | 比例准确，无明显资源或异步回退 |
| S4 | 容量阶梯 | 25/50/100/200 RPS | 每档 3 分钟 | 找到首个稳定性红线和上一稳定档 |
| S5 | 突发恢复 | 20 -> 200 -> 20 RPS | 2 + 1 + 3 分钟 | 突发期间受控，恢复后积压收敛 |
| S6 | 稳定性 | 固定 50 RPS | 30 分钟 + 5 分钟观察 | 无泄漏、持续等待或积压增长 |
| S7 | 缓存专项 | Hot 40 VU + Spread 10 VU | 60 秒起，可扩至 2 分钟 | 复用现有脚本阈值 |
| S8 | 治理专项 | 默认配置，60/100/200 RPS 分档 | 每档 2 分钟 | 429 和治理计数符合 Token Bucket |

### 6.2 S0 冒烟

顺序执行登录、三类 Feed、Exposure、Like、Follow 和 Publish。该场景检查数据池、Token、
URL、响应 JSON 和权限，不用于给出性能结论。任何检查失败都应停止后续场景。

### 6.3 S1 单接口基线

Timeline、Hot、Recommend 不并行运行，每个接口分别完成 Cold 和 Warm 两轮。每轮前记录：

- Feed 请求计数与延迟。
- 缓存命中/未命中计数。
- 推荐各召回源候选数和耗时。
- MySQL 连接等待计数。

Warm 轮正式计时前预热 5 分钟。Recommend 同时拆分首屏和翻页结果。该场景作为后续优化
最小对比单元，避免综合流量只能发现“系统慢”却无法归因。

### 6.4 S2 写链路

三种写请求并行运行，目标到达率分别为 Publish 2 RPS、Like 10 RPS、Follow 5 RPS。
测量窗口为 10 分钟，结束后停止写流量并观察 5 分钟：

- Publish 返回后 `outbox_event` 应出现并最终发布。
- 普通作者事件应由 Fanout Worker 消费；大 V 发布应保持 Pull 语义。
- Like/Follow 返回状态必须与目标状态一致，重复或冲突不得计作成功。
- Kafka Lag、Outbox Pending 和最老 Pending 年龄应回落至开始前水平。
- DLQ 新增量和 Outbox Failed 必须为 0。

### 6.5 S3 综合流量

总负载固定 100 RPS，按多个 k6 scenario 分配固定到达率：

| 操作 | 比例 | RPS |
| --- | ---: | ---: |
| Timeline | 45% | 45 |
| Hot | 15% | 15 |
| Recommend | 20% | 20 |
| Exposure | 10% | 10 |
| Like | 5% | 5 |
| Follow | 3% | 3 |
| Publish | 2% | 2 |

先 Warm 5 分钟，再正式运行 15 分钟，结束后观察异步链路 5 分钟。固定到达率保证比例不因
某一接口变慢而漂移。综合结果用于呈现系统整体行为，接口验收仍按 Tag 分开计算。

### 6.6 S4 容量阶梯

使用与综合流量相同的业务比例，依次运行 25、50、100、200 RPS，每档 3 分钟。任何一档
触发以下条件时停止继续升压：

- 连续 1 分钟业务错误率不低于 1%。
- 任一读接口连续 1 分钟 P95 超过其阈值。
- API CPU 连续 2 分钟不低于 85%。
- MySQL 连接等待速率持续上升且无回落。
- Kafka Lag 或 Outbox Pending 连续两个观测窗口增长。
- k6 出现 `dropped_iterations`；此时判定为压测端不足，而不是服务端容量上限。

首个越过红线的档位是“不稳定档”，上一档才是当前环境下的“已验证稳定档”。若 200 RPS
仍稳定，只能记录“稳定容量不低于 200 RPS”，不能外推真实上限。

### 6.7 S5 突发恢复

以综合比例运行 20 RPS 2 分钟，1 分钟内提升到 200 RPS并保持 1 分钟，再降回 20 RPS
观察 3 分钟。记录突发阶段的错误、P99、治理决策和异步积压，并验证恢复阶段：

- P95 在 2 分钟内回到突发前范围。
- Kafka Lag 和 Outbox Pending 不再增长。
- API/Worker 没有重启，熔断器没有长期停留在 Open。

### 6.8 S6 稳定性

用综合比例固定运行 50 RPS、30 分钟，随后观察 5 分钟。每分钟采集容器 CPU/内存，比较
第 5～10 分钟与第 25～30 分钟：

- API 和 Worker 内存增长不超过 10%，并且不存在单调无回收趋势。
- GC P95 没有持续恶化，Goroutine 数没有单调增长。
- MySQL 等待、Kafka Lag、Outbox Pending 不形成持续正斜率。
- 错误率和 P95 没有随时间分段恶化。

30 分钟只能证明本机短时稳定性；如需要发布前稳定性结论，再把相同场景延长到 2 小时，
不修改其他参数。

### 6.9 S7 缓存专项

首先原样执行仓库已有脚本：

```bash
BASE_URL=http://127.0.0.1:8080 \
SCENE=timeline HOT_VUS=40 SPREAD_VUS=10 DURATION=60s \
k6 run scripts/feed-cache-load.js
```

随后分别完成 Cold、Warm 和 Redis 故障三轮。Redis 故障轮只在隔离的本机环境执行，记录
操作时间，停止 Redis 后以低流量验证 L1/DB 回源，再恢复 Redis并等待健康。故障轮的目标
是可用性和恢复行为，不与正常轮 QPS 横向比较。

### 6.10 S8 治理专项

恢复默认 `50 RPS / 100 Burst`，重启 API 后以 60、100、200 RPS 各运行 2 分钟：

- 前 100 个左右的瞬时请求可能由 Burst 接纳，稳态接受速率应趋近 50 RPS。
- 超出配额的请求应返回 429，并携带 `Retry-After: 1`。
- `gcfeed_governance_events_total{component="http",decision="rate_limited"}` 增量应与
  观察到的 429 数量一致或仅存在采样边界差异。
- 429 是该场景的预期结果，单独计入 `rate_limited`，不得混入业务容量错误率。

推荐降级通过内部开关单独验证。开启 `recommendation_degraded` 后，Recommend 仍应返回
可用 Feed，并出现 `manual_degrade` 决策；测试结束立即关闭开关。该操作使用现有 Internal
Token，不把 Token 写进脚本、报告或 Git。

## 7. 指标、查询与采集

### 7.1 k6 指标

每个场景至少保留：

- `http_reqs`、`http_req_failed`、`http_req_duration` 的 P50/P95/P99。
- 每个 endpoint/scene 的请求数、失败率和延迟。
- 自定义 `business_success_rate`。
- 推荐场景的 `recommendation_degradation` 必须为 0；该 Counter 汇总测试期间新增的
  `fallback`、`circuit_open`、`bulkhead_full` 和 `partial_recall`。
- 预期 429 的独立计数 `rate_limited`。
- `dropped_iterations` 和实际迭代速率。

业务成功必须同时满足 HTTP 状态和响应结构断言。连接失败、超时、意外 4xx/5xx、JSON
解析失败及业务字段不一致均计入失败。

### 7.2 Prometheus 查询

HTTP QPS 与错误率：

```promql
sum(rate(gcfeed_http_requests_total[1m])) by (method, route, status)
```

```promql
sum(rate(gcfeed_http_requests_total{status=~"5.."}[1m]))
/
clamp_min(sum(rate(gcfeed_http_requests_total[1m])), 0.001)
```

HTTP 与 Feed P95：

```promql
histogram_quantile(
  0.95,
  sum(rate(gcfeed_http_request_duration_seconds_bucket[5m])) by (le, method, route)
)
```

```promql
histogram_quantile(
  0.95,
  sum(rate(gcfeed_feed_request_duration_seconds_bucket[5m])) by (le, scene)
)
```

缓存命中率：

```promql
sum(rate(gcfeed_feed_cache_requests_total{result="hit"}[5m])) by (area)
/
clamp_min(
  sum(rate(gcfeed_feed_cache_requests_total{result=~"hit|miss"}[5m])) by (area),
  0.001
)
```

推荐召回：

```promql
histogram_quantile(
  0.95,
  sum(rate(gcfeed_recommendation_recall_duration_seconds_bucket[5m])) by (le, source)
)
```

```promql
histogram_quantile(
  0.50,
  sum(rate(gcfeed_recommendation_recall_candidate_count_bucket[5m])) by (le, source)
)
```

Kafka、Outbox 与治理：

```promql
max(gcfeed_kafka_consumer_lag) by (group, topic, partition)
```

```promql
sum(gcfeed_outbox_events) by (status)
```

```promql
gcfeed_outbox_oldest_pending_age_seconds
```

```promql
sum(increase(gcfeed_kafka_dlq_events_total[5m])) by (group, event_type, result)
```

```promql
sum(rate(gcfeed_governance_events_total[1m])) by (component, decision)
```

MySQL 连接池：

```promql
gcfeed_mysql_connections_in_use
```

```promql
sum(rate(gcfeed_mysql_connection_wait_total[5m])) by (job)
```

```promql
sum(rate(gcfeed_mysql_connection_wait_duration_seconds_total[5m])) by (job)
```

GC 使用 Prometheus Go Collector 的 `go_gc_duration_seconds`、`go_memstats_*` 和
`go_goroutines`。若当前运行版本没有对应时间序列，应在报告中标为“未采集”，不能填 0。

### 7.3 CPU、内存与 MySQL QPS

Prometheus 当前未采集宿主机/容器 CPU 和 MySQL 语句 QPS，因此使用现有能力补齐：

```bash
docker stats fluxfeed-api fluxfeed-worker fluxfeed-mysql fluxfeed-redis fluxfeed-kafka
```

每分钟记录一次 CPU、内存使用量和内存百分比。Before/After 必须用相同采样方法。

MySQL 在测试前后分别记录：

```sql
SHOW GLOBAL STATUS WHERE Variable_name IN (
  'Queries',
  'Questions',
  'Threads_connected',
  'Threads_running',
  'Slow_queries'
);
```

`QPS = (结束 Queries - 开始 Queries) / 测量秒数`。报告必须注明这是整个 MySQL 实例的
近似值；测试期间不得运行额外管理查询或后台任务，以免混入结果。

### 7.4 Trace 与日志

发现 P99 尖峰时，在 Grafana Tempo 中用同一时间窗口抽取慢 Trace，至少判断时间消耗位于：

- Feed Pipeline/推荐召回。
- Redis 读取或缓存回填。
- MySQL 查询/连接等待。
- Kafka 发布或 Worker 处理。

Trace 用于定位而不是逐请求全量导出。报告引用 Trace ID、时间和结论，不复制敏感 Header。
日志通过 Request ID 与 Trace ID 关联，重点检查超时、熔断、回源和重试。

## 8. 验收标准

### 8.1 最低稳定性红线

| 维度 | 验收值 |
| --- | --- |
| 非预期 HTTP/业务失败 | 错误率 `< 1%`，业务成功率 `> 99%` |
| Hot Key | P95 `< 100 ms` |
| 分散 Key | P95 `< 300 ms` |
| Timeline / Hot | 各场景 P95 `< 300 ms` |
| Recommend | P95 `< 500 ms` |
| 普通 API | P99 `< 3 s` |
| API CPU | 稳态持续 `< 85%` |
| 稳定性内存 | 后段相对前段增长 `< 10%` 且无持续单调上涨 |
| MySQL | 连接等待不持续增长 |
| Kafka | 测试后 5 分钟内 Lag 回到开始前水平 |
| Outbox | Failed 为 0；Pending 和最老年龄在 5 分钟内回落 |
| DLQ | 测量窗口新增量为 0 |

阈值只定义最低可接受状态，不代表已经达到生产 SLA。推荐 P95 与当前 500 ms 主召回超时
相同，若大量请求依靠降级才能达标，仍判定 Recommend 原路径未达标。

### 8.2 Before/After 规则

1. Baseline 和 Retest 使用同一宿主机、Git 基线外的单一优化差异、相同 Compose 配置、
   `medium-v1` 数据快照、缓存状态和 k6 参数。
2. 每组连续运行 3 次，报告中保留每次值，并使用中位数作结论。
3. 任意一次出现压测端 `dropped_iterations`、容器重启、数据异常或监控缺口，该次作废重跑。
4. 目标指标必须改善；错误率、P99、CPU、内存、MySQL 等待、Kafka Lag、Outbox 和 DLQ
   不得回退。
5. 三次结果的离散程度明显超过 10% 时，先排查环境噪声，不宣称优化有效。
6. 只报告测到的稳定档。例如最高验证 100 RPS 时，写“100 RPS 下满足红线”，不写
   “系统最大支持 100 RPS”。

## 9. 执行顺序与停止规则

### 9.1 已实现入口

仓库提供统一脚本 `scripts/performance-load.js`，通过 `PROFILE` 选择场景：

```text
smoke
baseline-timeline
baseline-hot
baseline-recommend
write
mixed
capacity
spike
soak
governance
```

容量测试使用独立配置和 Compose 覆盖文件，不需要修改默认配置：

```bash
docker compose \
  -f apps/docker-compose.yml \
  -f apps/docker-compose.performance.yml \
  up -d --build
```

性能覆盖文件默认仍暴露 API `8080`。若该端口不可用，可统一指定备用端口：

```bash
export PERFORMANCE_API_PORT=18081
export PERFORMANCE_BASE_URL=http://127.0.0.1:18081
export BASE_URL=$PERFORMANCE_BASE_URL
docker compose \
  -f apps/docker-compose.yml \
  -f apps/docker-compose.performance.yml \
  up -d --build
```

`PERFORMANCE_API_PORT` 控制 Compose 端口映射，`PERFORMANCE_BASE_URL` 用于快照恢复后的
健康检查，`BASE_URL` 用于 k6；三者必须指向同一个 API 实例。

确认 Outbox 已排空后创建只读快照；创建期间脚本会短暂停止 API/Worker，且只允许操作挂载
`performance_*` 数据卷的容器。快照文件保存在已被 Git 忽略的
`build/performance/snapshots/`：

```bash
scripts/performance-snapshot.sh create small-diagnostic-v1
scripts/performance-snapshot.sh verify small-diagnostic-v1

# 每轮 Before/After 开始前执行；该操作会覆盖性能 MySQL 并清空性能 Redis。
scripts/performance-snapshot.sh restore small-diagnostic-v1
```

P0 小数据回归完成后，可从 `small-diagnostic-v1` 生成固定的 `medium-v1`。脚本会先恢复小型
快照，只允许操作性能专用数据卷；历史数据使用集合 SQL 写入，不制造百万条 Outbox/Kafka
事件。密码只会写入 Git 忽略的本地账号池文件：

```bash
export PERFORMANCE_API_PORT=18081
export PERFORMANCE_BASE_URL=http://127.0.0.1:18081
export PERFORMANCE_PASSWORD='<local-random-password>'
scripts/performance-seed-medium.sh
```

生成结果位于 `build/performance/medium-v1/`：

- `read-accounts.json`：100 个只读账号；
- `write-accounts.json`：100 个写入账号；
- `video-ids.txt`、`target-user-ids.txt`：k6 目标池；
- `smoke*.json`、`recommend*.json`：本机诊断结果，不提交 Git。

同名 `medium-v1` 快照已存在时脚本会拒绝覆盖。需要重建时应先人工归档旧快照，不能让脚本
静默删除已有基准。

先用 k6 静态解析场景：

```bash
k6 inspect \
  -e PROFILE=mixed \
  -e READ_ACCOUNTS_JSON='[{"account":"perf-reader-001","password":"<local-password>"}]' \
  -e WRITE_ACCOUNTS_JSON='[{"account":"perf-writer-001","password":"<local-password>"}]' \
  -e VIDEO_IDS=1,2,3 \
  -e TARGET_USER_IDS=101,102,103 \
  scripts/performance-load.js
```

单账号适合冒烟；正式混合测试必须分别传入互不重叠的 `READ_ACCOUNTS_JSON` 和
`WRITE_ACCOUNTS_JSON`。凭据只保存在本机环境变量，不得写入仓库或报告：

```bash
export READ_ACCOUNTS_JSON='[
  {"account":"perf-reader-001","password":"<local-password>"},
  {"account":"perf-reader-002","password":"<local-password>"}
]'
export WRITE_ACCOUNTS_JSON='[
  {"account":"perf-writer-001","password":"<local-password>"},
  {"account":"perf-writer-002","password":"<local-password>"}
]'
export VIDEO_IDS='1,2,3'
export TARGET_USER_IDS='101,102,103'

PROFILE=smoke DURATION=1m k6 run scripts/performance-load.js
PROFILE=baseline-timeline k6 run scripts/performance-load.js
PROFILE=baseline-hot k6 run scripts/performance-load.js
PROFILE=baseline-recommend k6 run scripts/performance-load.js
PROFILE=write k6 run scripts/performance-load.js
PROFILE=mixed k6 run scripts/performance-load.js
PROFILE=capacity k6 run scripts/performance-load.js
PROFILE=spike k6 run scripts/performance-load.js
PROFILE=soak k6 run scripts/performance-load.js
```

`write`、`mixed`、`capacity`、`spike`、`soak` 和完整 `smoke` 必须提供视频与关注目标池；
其中四个混合场景还会强制校验读写账号不重叠。兼容变量 `ACCOUNTS_JSON` 仅用于冒烟或单一
读/写场景，缺少参数时 setup 会直接终止。默认治理配置下单独运行：

```bash
PROFILE=governance k6 run scripts/performance-load.js
```

推荐场景会在 setup/teardown 通过 Prometheus 记录治理 Counter，并等待一个 15 秒采集周期
后计算增量；任一推荐降级事件都会使 k6 阈值失败。Prometheus 非默认地址时设置
`PROMETHEUS_URL`。

可通过 `BASE_URL`、`PROMETHEUS_URL`、`LIMIT`、`RUN_ID`、`DURATION`、`RPS`、`STAGE_DURATION`、
`PRE_ALLOCATED_VUS` 和 `MAX_VUS` 覆盖默认值。Publish 的 `MEDIA_URL` 和 `COVER_URL`
默认引用本地测试路径，也可通过同名环境变量替换。

### 9.2 推荐顺序

推荐固定顺序：

```text
环境与数据检查
  -> S0 冒烟
  -> Cold/Warm 预热确认
  -> S1 单接口基线
  -> S7 缓存专项
  -> S2 写链路
  -> S3 综合流量
  -> S4 容量阶梯
  -> S5 突发恢复
  -> S6 稳定性
  -> S8 治理专项
  -> 恢复默认配置并归档结果
```

任一阶段出现以下情况立即停止，不继续用更高流量放大故障：

- 数据错误、意外大量 4xx、业务断言持续失败。
- API、Worker、MySQL、Redis 或 Kafka 重启/不可用。
- 错误率连续 1 分钟不低于 5%。
- CPU 接近 100% 且服务失去响应，或内存接近宿主机/容器上限。
- Outbox Failed、DLQ 开始持续增长。
- 测试可能影响非本次测试数据。

停止后保存 k6 输出、Prometheus 时间范围、容器日志和环境记录，再恢复服务与默认配置。

## 10. 瓶颈分析与优化闭环

按证据选择优化点，一次只验证一类变化：

| 现象 | 优先核对 | 可能方向 |
| --- | --- | --- |
| 首屏慢、缓存命中率低 | `page_l1`/L2 命中、MySQL QPS | Key/TTL、预热、批量读取 |
| 热点快、分散 Key 慢 | Card/Stat Miss、连接等待 | MGET/Pipeline、批量水合、索引 |
| Recommend P95 高 | 各 Recall P95、候选数、治理决策 | 慢召回、超时预算、候选规模 |
| HTTP 已返回但 Lag 增长 | Consumer 处理耗时、重试、Worker CPU | 消费并发、批处理、慢处理器 |
| Outbox Pending 增长 | 发布错误、Kafka 可用性、最老年龄 | 轮询批次、重试、连接问题 |
| MySQL Wait 增长 | In-use、Wait Duration、慢 Trace | 慢查询、索引、批量访问；最后才调连接池 |
| 长测内存上涨 | Go Heap、Goroutine、L1 项数 | 泄漏、无界集合、过期清理 |
| 429 提前出现 | 实际加载配置、Client IP | 区分容量配置与治理配置 |

优化后先运行对应的最小单接口场景，确认收益再回归综合、突发和稳定性场景。不得只通过
提高超时阈值或关闭治理功能让请求“成功”。

## 11. 压测报告模板

```markdown
# FluxFeed 性能测试报告：<优化主题>

## 1. 结论

- 测试日期：
- Git Baseline / Retest SHA：
- 数据快照：medium-v1
- 核心结论：
- 已验证稳定档：
- 未覆盖事项：

## 2. 环境

| 项目 | 值 |
| --- | --- |
| CPU / 内存 / OS | |
| Docker / Compose / k6 | |
| API 配置 | |
| 服务镜像 | |
| 缓存状态 | Cold / Warm |

## 3. 变更与假设

- 瓶颈证据：
- 唯一优化变量：
- 预期影响：

## 4. 三次实测

| 指标 | Baseline-1 | Baseline-2 | Baseline-3 | Baseline 中位数 | Retest-1 | Retest-2 | Retest-3 | Retest 中位数 | 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| QPS | | | | | | | | | |
| P50 | | | | | | | | | |
| P95 | | | | | | | | | |
| P99 | | | | | | | | | |
| Error Rate | | | | | | | | | |
| API CPU | | | | | | | | | |
| API Memory | | | | | | | | | |
| Redis Hit Rate | | | | | | | | | |
| MySQL QPS / Wait | | | | | | | | | |
| Kafka Max Lag | | | | | | | | | |
| Outbox Peak / Drain Time | | | | | | | | | |

## 5. 可观测性证据

- Grafana 时间范围：
- PromQL：
- 慢 Trace ID：
- 日志/异常：

## 6. 验收与回归

- 最低红线：通过 / 不通过
- 目标指标：改善 / 无显著变化 / 回退
- 其他指标是否回退：
- 结论与下一步：

## 7. 面试摘要

在固定的单机 Compose 环境和 medium-v1 数据集上，使用 k6 对 <场景> 连续进行
3 轮 Before/After 测试；通过 <指标/Trace> 定位 <瓶颈>，采用 <优化> 后，P95 从
<真实值> 降至 <真实值>，吞吐从 <真实值> 提升至 <真实值>，同时错误率、P99、
Kafka Lag 和 MySQL 连接等待未回退。
```

模板中的尖括号必须替换为真实数据。若优化无显著收益，应如实记录，并保留其排除了一条
错误假设的价值。

## 12. 完成标准

Stage 11 的压测设计在满足以下条件后视为完成：

- 所有场景都有确定的请求、身份、负载、持续时间、断言、副作用和停止条件。
- Cold/Warm、容量/治理、同步响应/异步收敛均被分开评价。
- 指标能够从当前 k6、Prometheus、Grafana、Tempo、Docker 或 MySQL 中实际取得；
  缺失指标明确标注，不用推测值填充。
- Baseline 与 Retest 能用固定数据快照和相同参数重复三次。
- 结论包含系统瓶颈证据和无回退检查，而不只展示单一 QPS。
- README、简历或面试材料仅在真实执行后引用报告中的实测中位数。
