# FluxFeed 性能测试报告：Stage 11 首轮真实基线

## 1. 结论

- 测试日期：2026-09-28（Asia/Shanghai）。
- Git SHA：`b25aa6aa0370562efe3408c9234e65e18a391e0e`，工作区包含本轮尚未提交的压测实现。
- 环境：单机 Docker Compose，k6 在同一宿主机运行。
- 数据集：本轮使用 API 动态构造的小数据集，不是最终 `medium-v1` 基准。
- Timeline、Hot、Recommend 单接口 10 RPS 基线全部通过；Publish/Like/Follow 共 17 RPS、
  10 分钟写链路通过。
- 综合 100 RPS 下 HTTP 和业务断言成功率为 100%，但 Recommend P95 为 506.87 ms，
  超过 500 ms 红线，且大量请求触发 fallback/circuit open，因此综合场景判定不通过。
- 50 RPS 诊断回测期间仍出现近乎全量推荐降级和 MySQL 高 CPU，已按停止条件中断，
  不能认定为稳定档。
- 当前瓶颈位于推荐多路召回的数据库路径，不在 HTTP、Redis、Outbox 或 Kafka 消费积压。

本报告只能证明当前小数据集和单机环境下的结果，不代表生产容量，也不满足三轮中位数的
最终 Before/After 口径。

## 2. 环境

| 项目 | 实测值 |
| --- | --- |
| CPU | Intel Core i7-10750H，4 核 8 线程 |
| 内存 | 15 GiB，测试前可用约 10 GiB |
| Docker | Client/Server 29.8.0，Compose 5.5.1 |
| k6 | 2.2.0，Linux amd64 |
| API 配置 | JWT 2h；限流 10,000 RPS / Burst 20,000 |
| MySQL 连接池 | MaxOpen 50、MaxIdle 10 |
| 普通请求超时 | 3s |
| 推荐主召回超时 | 500ms |
| 部署 | API、Worker、MySQL、Redis、Kafka、RabbitMQ、Prometheus、Tempo、Grafana 单机容器 |

为避免破坏已有开发数据，本轮通过 `apps/docker-compose.performance.yml` 使用独立的
`performance_*` 数据卷。Redis 不暴露宿主机端口，API/Worker 仍通过 Compose 网络访问。

### 2.1 最终数据状态

| 数据 | 数量 |
| --- | ---: |
| 账号 | 31，其中 30 个 `perf*` 压测账号 |
| 已发布视频 | 3,879 |
| 视频向量 | 3,879 |
| 有效关注 | 762 |
| 有效点赞 | 8,680 |
| Outbox Published | 19,875 |
| Outbox Pending / Failed | 0 / 0 |

测试中的 Publish 会持续增加视频与向量，因此 100 RPS 综合测试后期的数据规模大于初始
单接口基线。该变化是本轮探索性测试的限制，正式 Before/After 必须恢复同一数据快照。

## 3. 启动阶段发现的问题

### 3.1 API 与 Worker 并发迁移竞态

全新数据库首次启动时，API 与 Worker 同时执行 AutoMigrate。Worker 收到 MySQL 1050
`Table already exists` 后退出，原实现只把 1060/1061 视为可重试的并发迁移错误。

本轮处理：

- API 完成迁移后重启 Worker，使测试继续。
- 将 MySQL 1050 纳入现有有限重试，并补充单元测试。
- 1062 重复数据错误仍不重试，避免掩盖真实数据冲突。

### 3.2 Kafka Topic 未初始化

Kafka 首次启动没有业务 Topic，Outbox 发布返回 `UNKNOWN_TOPIC_OR_PARTITION`，758 条记录
进入 Pending 并按指数退避。创建四个 Topic 后，现有重试机制将全部记录发布成功，Failed
为 0，各消费组 Lag 回到 0。

本轮处理：性能 Compose 增加一次性 `kafka-init`，创建 `fluxfeed.video`、
`fluxfeed.interaction`、`fluxfeed.exposure`、`fluxfeed.relation`，API 和 Worker 等待其成功。

### 3.3 压测断言校准

首轮 smoke 的 Like HTTP 状态均为 200，但脚本错误地断言 `action_type=like`；真实协议值为
`LIKE`。修正后重新执行完整 1 分钟 smoke，所有检查通过。首轮失败是测试代码问题，不计入
服务性能结果。

## 4. 有效实测结果

### 4.1 冒烟

| 指标 | 结果 |
| --- | ---: |
| 持续时间 / VU | 1 分钟 / 1 VU |
| HTTP 请求 | 410 |
| 业务检查 | 350 / 350 成功 |
| HTTP 错误率 | 0% |
| Dropped Iterations | 0 |
| Timeline P95 / P99 | 14.02 / 18.71 ms |
| Hot P95 / P99 | 11.55 / 12.52 ms |
| Recommend P95 / P99 | 192.21 / 200.72 ms |

冒烟同时覆盖 Timeline、Hot、Recommend、Exposure、Like、Follow 和 Publish。

### 4.2 三类 Feed 单接口基线

| 场景 | 负载 | 业务请求 | 成功率 | P50 | P95 | P99 | Max | 结论 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Timeline | 10 RPS × 5m | 3,000 | 100% | 1.06 ms | 4.96 ms | 8.34 ms | 17.26 ms | 通过 |
| Hot | 10 RPS × 5m | 3,001 | 100% | 1.39 ms | 7.60 ms | 11.32 ms | 14.91 ms | 通过 |
| Recommend | 10 RPS × 5m | 3,000 | 100% | 59.69 ms | 99.73 ms | 197.95 ms | 321.04 ms | 通过 |

三个场景 HTTP 错误率均为 0，Dropped Iterations 均为 0。Timeline 测试结束时缓存命中率：

- `card`、`card_l1`、`page_l1`、`page_stale`、`stat_l1`：1.0。
- `stat`：约 0.859。

Hot 报告出现单个约 `-0.19 ms` 的负最小值，属于本机/k6 计时异常；P50/P95/P99 与最大值
正常，报告保留该事实，不用最小值形成结论。

### 4.3 写链路

负载：Publish 2 RPS、Like 10 RPS、Follow 5 RPS，持续 10 分钟。

| 操作 | P95 | P99 | Max |
| --- | ---: | ---: | ---: |
| Publish | 9.75 ms | 15.87 ms | 71.25 ms |
| Like | 11.92 ms | 15.97 ms | 133.77 ms |
| Follow | 34.97 ms | 42.92 ms | 115.78 ms |

- 10,201 / 10,201 业务请求成功，HTTP 错误率 0。
- 实际 HTTP 吞吐约 17.05 RPS，无 Dropped Iterations。
- 压测中 Outbox 通常只有 1～3 条瞬时 Pending，最老约 0.32 秒。
- 结束时 Outbox Pending/Failed 为 0，Kafka Lag 为 0，DLQ 无新增。
- 发布视频数与视频向量数完全一致，说明 Embedding Consumer 已收敛。

### 4.4 综合 100 RPS

负载比例：Timeline 45%、Hot 15%、Recommend 20%、Exposure 10%、Like 5%、Follow 3%、
Publish 2%，持续 15 分钟。

| 操作 | P95 | P99 | Max | 红线 |
| --- | ---: | ---: | ---: | --- |
| Timeline | 13.56 ms | 35.57 ms | 470.99 ms | 通过 |
| Hot | 16.84 ms | 41.62 ms | 401.15 ms | 通过 |
| Recommend | **506.87 ms** | 531.80 ms | 712.20 ms | **P95 不通过** |
| Exposure | 75.09 ms | 199.33 ms | 939.26 ms | P99 通过 |
| Like | 50.96 ms | 115.31 ms | 433.65 ms | P99 通过 |
| Follow | 118.45 ms | 269.21 ms | 1.02 s | P99 通过 |
| Publish | 46.91 ms | 114.04 ms | 547.81 ms | P99 通过 |

- 90,005 / 90,005 业务请求成功，HTTP 错误率 0。
- 实际 HTTP 吞吐约 99.86 RPS，无 Dropped Iterations。
- Recommend P95 超过 500 ms，因此整体判定不通过。
- 20 分钟 Prometheus 窗口估算出现约 17,100 次 fallback、15,849 次 circuit open；计数含
  Prometheus 区间外推误差，但足以证明大量成功响应来自降级路径。
- 综合测试结束时 Outbox Pending/Failed、Kafka Lag、DLQ 新增均为 0。

### 4.5 综合资源观察

| 组件 | CPU 典型采样 | 内存范围 | 观察 |
| --- | --- | --- | --- |
| API | 约 24%～28% | 31.5～36.4 MiB | 无 CPU 饱和；内存随数据/缓存增长 |
| Worker | 约 8%～10% | 18.7～19.3 MiB | 稳定，Lag 为 0 |
| MySQL | 约 21%～33% | 611～701 MiB | 出现轻微连接等待趋势 |
| Redis | 约 4%～8% | 13.2～15.2 MiB | 无明显瓶颈 |
| Kafka | 约 3%～213% | 923～1,010 MiB | 有约 2 核 CPU 峰值，Lag 仍为 0 |

API MySQL 连接等待速率从约 0.21 次/秒升至 0.55 次/秒，对应等待时长约 6～12 ms/秒；
尚未形成主要延迟来源，但应在后续稳定性测试继续观察。

## 5. 50 RPS 诊断回测

100 RPS 不通过后，冷却熔断 15 秒，以当前更大的数据集运行 50 RPS 综合回测。约 145 秒时：

- 推荐 fallback 约 10 次/秒，接近该轮全部 Recommend 流量。
- circuit open 约 9.5 次/秒。
- MySQL 单次资源采样约 746% CPU，触发停止条件。
- 已完成的 7,136 个业务迭代均成功，Recommend P95 70.90 ms、P99 514.98 ms；P95 很低
  是熔断后快速降级造成，不能作为主推荐链路性能。
- Ctrl-C 中断瞬间 API 日志出现 2 个 Recommend 500，未进入 k6 已完成迭代统计。

因此该轮是中断的诊断样本，不是通过的 50 RPS 基线。停止 10 秒后 MySQL 回落到约 0.5%
CPU，没有残留执行查询，说明高占用与推荐负载直接相关。

## 6. 瓶颈证据

100 RPS 测试后，推荐六路召回 P95：

| Recall Source | P95 |
| --- | ---: |
| Hot | 954 ms |
| Interest | 951 ms |
| Latest | 899 ms |
| Similar | 937 ms |
| Following | 915 ms |
| Collaborative | 38 ms |

当前候选查询为每个用户执行曝光排除与 30 天负反馈 `NOT EXISTS`；Interest/Similar 在线读取
最多 500 个向量并在应用层计算，Collaborative 使用 30 天窗口在线共现聚合。并发增加后多路
查询超过 500 ms 总预算，触发推荐熔断和 Hot/Latest 降级。

该证据说明首要优化对象是推荐召回数据库访问和候选计算，而不是扩大 HTTP 超时或 MySQL
连接池。直接提高超时只会延长尾延迟，扩大连接池可能进一步放大数据库 CPU。

## 7. 下一轮优化与复测

1. 对六路召回 SQL 分别执行 `EXPLAIN ANALYZE`，记录扫描行数、临时表、排序和负反馈子查询
   开销；先处理 Hot/Latest/Following 的公共过滤查询。
2. 避免每个 Recall Source 重复执行相同的 Seen/负反馈过滤，评估先取候选再统一过滤。
3. 为 Interest/Similar 引入有界候选缓存或预计算；数据进一步增长时再采用 ANN，不在当前
   小规模下直接引入额外服务。
4. 将 Collaborative 30 天在线共现物化为 item-item 结果，保留异步更新与降级路径。
5. 使用固定数据库快照依次运行 Recommend 1/5/10/20 RPS，找到主链路不 fallback 的边界。
6. 优化后重复三轮单接口 Recommend 和 50/100 RPS 综合场景，以中位数做 Before/After；
   然后再进行 30 分钟稳定性测试。

## 8. 结果文件

k6 原始 Summary JSON 位于本地构建目录 `build/performance/`：

```text
smoke-r2-20260928.json
baseline-timeline-20260928.json
baseline-hot-20260928.json
baseline-recommend-20260928.json
write-20260928.json
mixed-100rps-20260928.json
mixed-50rps-20260928.json
```

`mixed-50rps-20260928.json` 来自人工中断场景，只能用于诊断。`smoke-20260928.json` 是断言
修复前的无效结果，不用于本报告结论。

## 9. 面试摘要（基于本轮真实数据）

在 8 线程、15 GiB 内存的单机 Compose 环境中，使用 k6 对 FluxFeed 建立真实压测基线：
Timeline、Hot、Recommend 在 10 RPS 下 P95 分别为 4.96 ms、7.60 ms、99.73 ms，17 RPS
写链路连续运行 10 分钟成功率 100%，Outbox 与 Kafka Lag 均可及时归零。综合 100 RPS 下
系统仍保持 100% HTTP 成功，但 Recommend P95 升至 506.87 ms，并因多路数据库召回超时
大量触发熔断降级；通过 Prometheus 指标将瓶颈定位到在线召回查询，而不是缓存或异步链路。
