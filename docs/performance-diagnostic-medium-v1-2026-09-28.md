# FluxFeed medium-v1 容量数据与诊断记录

## 1. 结论

- 已从固定的 `small-diagnostic-v1` 构造并固化本机 `medium-v1`，读写账号池互不重叠。
- 数据规模达到 Stage 11 设计目标；P1 优化前 Recommend 在 1 RPS 下仍触发
  fallback/circuit open，优化后的同档回归已消除治理降级并通过延迟阈值。
- 中型数据暴露两个新瓶颈：账号查询对百万关注关系做全表聚合，以及推荐召回缺少与查询
  形状匹配的观看事件索引。
- 账号查询、Collaborative 和 P1 有界候选均取得明确改善；正式容量结论仍需完成
  5/10/20 RPS 分档及三轮中位数验证。

## 2. 固定数据集

| 数据 | 实际数量 |
| --- | ---: |
| 用户 | 12,000 |
| 已发布视频 / 视频向量 | 100,000 / 100,000 |
| 有效关注 | 1,002,762 |
| 有效点赞与收藏 | 1,008,680 |
| 观看事件 | 1,009,827 |
| 曝光聚合 | 504,127 |
| Feed Inbox | 979,838 |
| 粉丝数不低于 10,000 的作者 | 2 |

行为数据按约 80/20 热点分布生成，观看事件包含 `exposed`、`valid_play`、`finish` 和 5%
负反馈。普通作者的最新视频进入 Feed Inbox，大 V 保留 Pull 路径。账号池、目标池和快照均
保存在 Git 忽略的 `build/performance/` 下。

## 3. 账号查询问题

原 `FindByAccount` 和 `FindByID` 每次查询都会对 `user_follow` 做两次全表 `GROUP BY`，并对
`video` 再做一次全表分组。在 medium-v1 上，k6 setup 的请求中位数约 2.06 秒，200 个账号
无法在合理时间内完成登录。

修复后：

- 登录只读取认证所需字段，不加载无用统计；
- 资料查询将关注、粉丝和作品统计改为绑定当前用户的索引相关子查询，不再全表分组。

单账号实测登录为 55.9 ms，`/api/users/me` 为 15.4 ms，返回的关注、粉丝和作品数分别为
83、11,081 和 9。

## 4. 推荐诊断

### 4.1 初始 1 RPS

固定快照、单只读账号、30 秒：

| 指标 | 结果 |
| --- | ---: |
| Recommend P95 / P99 | 519.20 / 527.56 ms |
| fallback / circuit open | 31 / 24 |
| HTTP / 业务请求成功率 | 100% / 100% |
| 场景结论 | 不通过 |

HTTP 200 来自快速降级，不能视为推荐主链路通过。

### 4.2 复合索引证据

Collaborative 使用 `(user_id,event_type,created_at,video_id)` 与
`(video_id,event_type,created_at)` 复合索引后，同一用户的 `EXPLAIN ANALYZE` 从 388 ms
降至 34.3 ms，Prometheus 中 Collaborative P95 约 24 ms。账号与协同查询均不需要引入
新服务或物化 item-item 表。

负反馈子查询在复合查询中仍可能被 MySQL 错误估算为全表扫描，因此查询显式指定已验证的
覆盖索引。Hot 单独执行由约 1.68 秒降至约 418 ms，但六路并发后仍超过总预算。

### 4.3 当前剩余慢源

复合索引后再次运行 1 RPS × 30 秒：

| 指标 | 结果 |
| --- | ---: |
| Recommend P95 / P99 | 505.54 / 512.01 ms |
| fallback / circuit open | 31 / 24 |
| Collaborative P95 | 约 24 ms |
| Following / Latest P95 | 约 10 ms |
| Hot / Interest / Similar | 触发 500 ms 截止时间 |

这满足 PERF-03 的触发条件：多个候选源重复执行 Seen、负反馈、统计和向量连接。下一批应让
各 Source 先读取有界候选，再统一批量加载 Seen/负反馈并过滤；不得通过提高 500 ms 超时或
扩大连接池掩盖问题。

### 4.4 P1 有界候选与统一过滤结果

本批次将六路召回统一限制为每路最多 200 条候选（目标 100 条的 2 倍），Source 查询不再
重复连接 `exposures` 或执行负反馈反连接。候选 Video/Author ID 去重后，各执行一次最近曝光
查询和负反馈查询，再在应用层按原规则过滤。Interest/Similar 同时固定从视频发布时间索引
开始，再按主键读取 embedding，避免 MySQL 从约 5 万条同模型向量开始扫描。

固定 `medium-v1`、只读账号池、1 RPS × 30 秒复测：

| 指标 | P1 前 | P1 后 |
| --- | ---: | ---: |
| Recommend P95 | 505.54 ms | 274.32 ms |
| Recommend P99 | 512.01 ms | 297.98 ms |
| fallback | 31 | 0 |
| circuit open | 24 | 0 |
| partial recall | 有截止时间失败 | 0 |
| HTTP / 业务请求成功率 | 100% / 100% | 100% / 100% |

P1 后 31 次 Recommend 请求全部走完整主链路，未出现 fallback、circuit open、bulkhead full
或 partial recall，且未提高 500 ms 推荐超时。结果文件保存在本机 Git 忽略目录
`build/performance/medium-v1/recommend-1rps-index-first.json`。

随后执行 5 RPS × 30 秒短档诊断，150 次 Recommend 的 P95/P99 为
289.22/323.40 ms，业务成功率 100%，无丢弃迭代，四类推荐治理事件增量仍全部为 0。
慢查询日志中剩余超过 200 ms 的查询均为 Hot 排序，Interest/Similar 的全量向量起扫已消失。
该短档用于确认轻并发回归，不替代计划要求的 5 分钟正式分档。

## 5. 当前判定

`medium-v1` 数据准备、可重复快照和 P1 的 1/5 RPS 短档验证已经完成，尚不能外推容量；
下一步按 5/10/20 RPS 各 5 分钟正式分档回归，重点观察 Hot 排序，全部通过后再执行三轮
中位数和 30 分钟稳定性测试。
