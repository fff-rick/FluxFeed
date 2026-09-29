# FluxFeed Stage 11 性能优化与问题修复方案

> 制定依据：[首轮真实压测报告](performance-test-report-2026-09-28.md) 与
> [Stage 11 压测设计](stage11-performance-testing-plan.md)。
>
> 目标：先恢复推荐主链路在综合流量下的稳定性，再扩大数据规模和容量；不以延长超时、
> 扩大连接池或隐藏降级来制造“通过”。

## 1. 结论与实施优先级

首轮测试已经证明 Timeline、Hot、写链路、Outbox 和 Kafka 不是当前容量短板。100 RPS
综合流量失败的直接原因是 Recommend 主召回超过 500 ms，随后触发 fallback 和 circuit
open；HTTP 仍返回 200，因此仅看成功率会得到错误结论。

本轮优化按以下顺序实施：

| 优先级 | 工作项 | 预期结果 | 是否阻塞下一轮容量测试 |
| --- | --- | --- | --- |
| P0 | 拆分负反馈相关子查询 | 消除六路召回重复扫描用户事件的主要 SQL 开销 | 是 |
| P0 | 画像缺失时计算并持久化 | 避免同一用户每次 Recommend 重算四类行为画像 | 是 |
| P0 | 把推荐降级纳入压测失败判定 | 防止快速 fallback 掩盖主链路故障 | 是 |
| P0 | 固定数据快照与只读/写入账号池 | Before/After 使用相同输入，可重复比较 | 是 |
| P1 | 分档回归并定位剩余慢源 | 判断 P0 是否足够，避免过早改造召回架构 | 否 |
| P1 | 必要时统一 Seen/负反馈过滤 | 六路只取候选，一次批量过滤，降低重复 DB 工作 | 条件实施 |
| P1 | 限制画像刷新风暴 | 仅当 P0 后仍出现同用户并发重算时实施 | 条件实施 |
| P2 | Interest/Similar 候选快照或 ANN | 数据规模达到 `medium-v1` 且向量计算成为新瓶颈时实施 | 否 |
| P2 | Collaborative item-item 物化 | 在线共现实际成为慢源时实施 | 否 |

P0 完成并通过 50 RPS 主链路验证前，不进行 100/200 RPS 容量结论和 30 分钟稳定性测试。

## 2. 问题清单与根因

### 2.1 PERF-01：负反馈相关子查询导致召回 SQL 放大

**现象**

- 100 RPS 综合测试中 Hot、Interest、Latest、Similar、Following 五路召回 P95 为
  899～954 ms，均超过推荐主召回 500 ms 预算。
- 50 RPS 诊断时 MySQL 曾达到约 746% CPU；停止负载后迅速回落。
- Collaborative P95 仅 38 ms，说明不能把所有问题归因于协同过滤在线聚合。

**代码根因**

`apps/api/internal/infra/persistence/recommendation/gorm.go` 的候选查询对每一条视频执行同一个
负反馈 `NOT EXISTS`。该子查询把“屏蔽视频”和“屏蔽作者”放在一个 `OR` 中，MySQL 无法按
关联视频或作者有效收敛，只能对候选行重复扫描当前用户的事件。六个 Source 并发执行相同
公共过滤，进一步放大数据库工作量。

**补充实证**

在本轮数据状态（3,879 视频、用户 18 有 323 条观看事件）执行 `EXPLAIN ANALYZE`：

| SQL | 实测耗时 | 关键执行特征 |
| --- | ---: | --- |
| 原 Hot 候选 SQL | 约 1,429 ms | 3,661 个候选循环，每次扫描约 323 条用户事件 |
| 拆分后的等价 SQL | 约 18 ms | 两个负反馈集合各物化一次，再按 video/author 反连接 |

这组数字用于确认根因，不是优化后的正式压测结果。正式收益仍须从固定快照运行三轮取得。

**修复方案**

将公共负反馈条件拆成两个语义独立的反连接：

```sql
AND NOT EXISTS (
  SELECT 1
  FROM video_view_events AS negative_video_event
  WHERE negative_video_event.user_id = ?
    AND negative_video_event.video_id = v.id
    AND negative_video_event.created_at >= ?
    AND negative_video_event.event_type IN ('skip', 'not_interested')
)
AND v.author_id NOT IN (
  SELECT hidden_video.author_id
  FROM video_view_events AS hidden_event
  JOIN video AS hidden_video ON hidden_video.id = hidden_event.video_id
  WHERE hidden_event.user_id = ?
    AND hidden_event.created_at >= ?
    AND hidden_event.event_type = 'hide_author'
)
```

落点：

- 替换 `feedbackFilter` 常量，使画像行为、评论和关注信号查询共用修复后的条件。
- 替换 `ListCandidatesBySource` 中重复内嵌的旧条件，确保六路召回同时生效。
- 保持 30 天窗口和负反馈语义不变，不更改 API、DTO 或推荐排序权重。
- 先不新增索引。现有数据上的拆分已经显著改变执行计划；只有 `medium-v1` 的
  `EXPLAIN ANALYZE` 仍显示事件扫描过大时，再评估
  `(user_id, event_type, created_at, video_id)`，避免为每次写事件提前增加无证据的索引成本。

**验证**

1. 构造同一作者多个视频，分别写入 `skip`、`not_interested`、`hide_author`，验证过滤语义。
2. 对 Hot、Latest、Following、Interest、Similar 分别保存 `EXPLAIN ANALYZE`。
3. 固定快照运行 Recommend 1/5/10/20 RPS；20 RPS 下每个主召回 Source P95 应小于
   400 ms，为 500 ms 总预算保留余量。
4. 任何被明确负反馈的视频或作者重新出现，立即回滚 SQL 改动，不以性能收益换正确性。

### 2.2 PERF-02：兴趣画像缺失时反复在线重算

**现象与根因**

当前数据库 `user_interest_embedding` 为 0 行。`LoadUserInterestVector` 在未找到画像时会调用
`calculateUserInterestVector`，但不会保存结果；一次计算包含观看、点赞/收藏、评论和关注
四组查询。Like、Follow 等状态变化又会删除已有画像。因此同一用户后续每次 Recommend 都
可能重复计算，综合读写场景会形成额外数据库放大。

**修复方案**

- 抽取并复用现有 Upsert 逻辑；画像缺失时计算一次后写入 `user_interest_embedding`。
- 即使用户没有正向信号，也写入 `dimension=0`、`embedding_json=[]` 的空画像作为负缓存；
  读取到该记录时返回“无个性画像”，但不再次扫描行为表。
- `RefreshUserInterestVector` 同样写入空画像，不再以删除记录表示“计算成功但无信号”。
- Like、Comment、Follow 的真实状态变化仍执行现有失效逻辑；幂等重复请求不失效。
- 首版依赖数据库唯一键与 Upsert 处理并发，不增加分布式锁或新缓存组件。若复测证明同一用户
  首次请求存在明显并发重算，再在单 API 进程内增加按用户 `singleflight`。

**验证**

1. 新用户连续请求两次 Recommend，第一次生成空或非空画像，第二次不再执行画像重算 SQL。
2. 点赞或关注状态真实变化后画像被失效；下一次 Recommend 只重建一次，后续复用。
3. 重复相同幂等写请求不得反复删除画像。
4. Recommend 返回内容、默认画像降级和向量维度校验保持原语义。

### 2.3 TEST-01：HTTP 200 掩盖推荐主链路降级

**现象**

100 RPS 时 90,005 个业务请求全部成功，但约有 17,100 次 fallback 和 15,849 次 circuit
open；50 RPS 诊断中 Recommend P95 降到 70.90 ms，实际是熔断后的快速降级，不是性能改善。

**修复方案**

不改变对外响应协议，直接复用现有
`gcfeed_governance_events_total{component="recommendation",decision=...}`：

- 每轮测试前后记录 `fallback`、`circuit_open`、`bulkhead_full`、`partial_recall` 的 Counter。
- 基线和容量场景要求 `fallback=0`、`circuit_open=0`、`bulkhead_full=0`；出现任一增量即判定
  Recommend 主链路不通过，即使 HTTP、业务断言和延迟阈值均通过。
- `partial_recall` 单独报告。首轮目标为 0；后续如要允许局部召回失败，必须先定义占比红线。
- 治理专项允许并期待 fallback/circuit open，但结果与业务容量分开记录。

建议在压测报告采集命令中加入以下查询，不为此新增监控系统：

```promql
sum(increase(gcfeed_governance_events_total{
  component="recommendation",
  decision=~"fallback|circuit_open|bulkhead_full|partial_recall"
}[20m])) by (decision)
```

### 2.4 TEST-02：动态数据污染 Before/After

**现象**

首轮使用 31 个账号和 3,879 个视频，Publish 在测试期间持续增加视频和向量；50 RPS 回测
面对的数据量已大于单接口基线，因此不能直接比较。该数据也未达到规划的 `medium-v1`。

**修复方案**

- 将当前小数据集命名为 `small-diagnostic-v1`，仅用于修复回归，不再称为容量数据集。
- 每一轮 Before/After 从相同 MySQL 快照恢复；清空性能 Redis 和 API L1 后执行相同预热，
  账号池、目标池和请求选择规则保持一致。
- Recommend 基线使用只读账号池；Publish/Like/Follow 使用独立写账号和目标分片，避免在读基线
  中持续改变候选与画像。
- P0 修复在小数据集通过后再构造 `medium-v1`；不能用小数据集结果替代最终容量结论。

## 3. P1 条件优化

P1 不是默认实施项。完成 P0 并复测后，只有对应证据仍存在才进入。

### 3.1 PERF-03：六路重复 Seen/负反馈过滤

**触发条件**：拆分 SQL 后，两个及以上召回源 P95 仍超过 400 ms，且 Trace/SQL 证明公共
过滤占主要耗时。

**方案**：六路查询只返回有界候选，合并去重后调用现有 `ListRecentExposures` 一次批量过滤；
新增一次批量负反馈读取并在应用层过滤。每路先取目标数量的 2 倍，过滤后不足再按需补取，
防止提前过滤导致结果数量下降。

**风险**：过滤时点变化可能改变候选集合；必须增加候选数量、负反馈语义和分页测试。若 P0
已经达到目标，则不做该改造。

### 3.2 PERF-04：同用户画像并发重建

**触发条件**：画像持久化后，Trace 仍显示同一用户在短窗口内并发执行多次
`calculateUserInterestVector`。

**方案**：复用项目已有 `golang.org/x/sync`，在 API 进程内按用户合并首次重建请求；不引入
Redis 锁。多副本场景需要时再评估跨实例协调。

### 3.3 PERF-05：向量候选读取与计算

**触发条件**：`medium-v1` 下 Interest/Similar 明显慢于其他 Source，且 SQL 已不再是主因。

**方案顺序**：

1. 缓存短 TTL 的“最近 500 个视频及向量”快照，用户向量和 Seen 仍逐用户处理。
2. 记录命中率、重建耗时和内存；收益不足即删除缓存。
3. 只有 500 个候选的应用层余弦计算已被实测证明不可接受时，才评估 ANN/向量数据库。

### 3.4 PERF-06：Collaborative 在线共现

首轮 Collaborative P95 为 38 ms，并非当前瓶颈。本轮不物化 item-item 表。只有数据扩容后
它越过 400 ms 或占据主要 MySQL CPU，才进入异步物化设计，避免提前增加一致性与回填成本。

## 4. 已发现工程问题的修复状态

| 编号 | 问题 | 当前状态 | 后续动作 |
| --- | --- | --- | --- |
| FIX-01 | API/Worker 并发迁移遇到 MySQL 1050 后退出 | 已在本轮工作区修复 | 保留有限重试测试；1062 仍不得重试 |
| FIX-02 | 全新环境 Kafka Topic 不存在，Outbox Pending | 已在性能 Compose 修复 | 验证 `kafka-init` 幂等与依赖顺序 |
| FIX-03 | k6 Like 断言使用小写 `like` | 已修复 | 保持真实协议值 `LIKE` |
| FIX-04 | 推荐主链路 SQL 放大 | 已实施，待固定快照压测 | 已完成语义、执行计划和全量 Go 测试 |
| FIX-05 | 兴趣画像缺失反复重算 | 已实施，待固定快照压测 | 缺失/空画像均持久化，集成测试通过 |
| FIX-06 | 降级未进入业务容量判定 | 已实施，待固定快照压测 | k6 自动比较 Prometheus Counter，任一增量使阈值失败 |
| FIX-07 | 压测数据在轮次间漂移 | 已实施并验证 | 本机已固化 `small-diagnostic-v1`，提供安全恢复脚本并强制读写账号池隔离 |
| FIX-08 | 登录/资料查询全表聚合关注与作品 | 已实施并验证 | 登录约 2.06 s 降至 55.9 ms，资料查询 15.4 ms |
| FIX-09 | Collaborative 共现缺少复合索引 | 已实施并验证 | `EXPLAIN ANALYZE` 由 388 ms 降至 34.3 ms |
| FIX-10 | medium-v1 候选查询重复扫描 | 已实施并完成正式验收 | 与 FIX-11 联合完成三轮 20 RPS 和综合 50 RPS 验收 |
| FIX-11 | Hot 候选每请求重算导致 MySQL CPU 越线 | 已实施并完成正式验收 | 三轮 20 RPS P95 中位数 63.96 ms；综合 50 RPS Recommend P95 104.11 ms，无降级 |

## 5. 建议改动范围

首批代码改动控制在以下位置，不修改生产 API 和 DTO：

| 文件 | 改动 |
| --- | --- |
| `apps/api/internal/infra/persistence/recommendation/gorm.go` | 拆分负反馈条件；画像计算结果 Upsert；空画像持久化 |
| `apps/api/internal/infra/persistence/recommendation/*_test.go` | 负反馈语义、画像首次生成/失效/复用测试 |
| `scripts/performance-load.js` | 仅在需要时调整各 Profile 阈值，不把治理场景的 429 混入容量失败率 |
| `docs/performance-test-report-*.md` | 记录治理 Counter 差值、三轮结果和中位数 |

不应在首批改动中扩大 MySQL 连接池、提高 500 ms 推荐超时、增加微服务、引入 ANN 或新缓存
平台。这些措施不能修复已确认的重复扫描，并可能放大尾延迟和数据库压力。

## 6. 实施与复测步骤

### 批次 A：SQL 根因修复

1. 为负反馈视频和屏蔽作者补语义测试。
2. 替换公共过滤 SQL，运行推荐仓储与全量 Go 测试。
3. 对五个慢 Source 保存修复前后 `EXPLAIN ANALYZE`；确认结果集一致。
4. 在 `small-diagnostic-v1` 上运行 Recommend 1/5/10/20 RPS，每档 5 分钟。

### 批次 B：画像物化修复

1. 增加缺失画像、空画像、状态变化后失效三类测试。
2. 实现缺失时 Upsert，复查数据库中画像数量和重建频次。
3. 重复批次 A 的 10/20 RPS，并加入 Like 5 RPS 的读写组合，确认画像不会持续抖动。

### 批次 C：综合回归

每轮恢复相同快照、执行相同预热，连续运行三次并取中位数：

1. Recommend 20 RPS × 5 分钟。
2. 综合 50 RPS × 15 分钟。
3. 综合 100 RPS × 15 分钟；只有 50 RPS 完整通过才执行。
4. 100 RPS 通过后运行 50 RPS × 30 分钟稳定性测试。
5. 每轮结束继续观察 5 分钟，确认 Kafka Lag、Outbox Pending 回到测试前水平。

### 批次 D：固定基准数据

P0 回归通过后建立 `medium-v1` 数据快照，再从 Recommend 分档测试开始重复。小数据集和
`medium-v1` 的结果必须分表报告，不能合并中位数。

当前本机已完成 `medium-v1`：12,000 用户、100,000 已发布视频及向量、1,002,762 条有效
关注、1,008,680 条有效互动、1,009,827 条观看事件、504,127 条曝光聚合和 979,838 条
Feed Inbox；两个大 V 的粉丝数均超过 10,000。P1 后 1/5 RPS 诊断已不再出现推荐降级，
FIX-11 后 5/10/20 RPS 正式分档通过，随后三轮 20 RPS 的 P95/P99 中位数为
63.96/202.62 ms，综合 50 RPS × 15 分钟也已通过；综合 100 RPS 和 30 分钟稳定性测试尚未
完成，当前结果仍不能作为最终容量结论。详见
[验收报告](performance-acceptance-2026-09-29.md)。

## 7. 验收标准

### 7.1 功能正确性

- `skip`、`not_interested` 只排除对应视频；`hide_author` 排除该作者全部视频。
- Recent Exposure、游标、候选去重、排序权重与修复前语义一致。
- 画像失效后能够重建；无行为用户使用默认画像，不反复查询行为表。
- 全量 Go 测试和 k6 smoke 通过，无新增 4xx/5xx。

### 7.2 性能与稳定性

| 指标 | 通过条件 |
| --- | --- |
| Recommend 20 RPS 单接口 | P95 < 500 ms，P99 < 3 s，无 dropped iterations |
| 综合 50/100 RPS | Recommend P95 < 500 ms；Timeline/Hot P95 < 300 ms |
| 推荐治理事件 | fallback、circuit_open、bulkhead_full 增量均为 0 |
| 业务质量 | HTTP 非预期失败率 < 1%，业务检查成功率 > 99% |
| 数据库 | 稳态 CPU < 85%，连接池无持续等待增长，慢 SQL无持续堆积 |
| 异步链路 | DLQ 新增 0、Outbox Failed 0，Lag/Pending 5 分钟内恢复 |
| 稳定性 | 30 分钟内存增长不超过 10%，GC 和错误率无持续恶化 |

Before/After 必须使用同一 Git 基线之外的唯一变量、同一快照、同一流量与预热方式连续运行
三次。优化后的 Recommend P95 必须下降，且 P99、错误率、MySQL CPU、连接等待与异步积压
均不得回退。任何一项依赖 fallback 才达标，整体仍判定失败。

## 8. 回滚条件

- 负反馈、Seen 或分页语义发生变化：立即回滚对应 SQL。
- Recommend P95 改善但 P99、错误率或 MySQL CPU 中位数恶化超过 10%：不合并，继续定位。
- 画像持久化导致维度错误、旧画像无法失效或写竞争显著增加：回滚画像改动，保留 SQL 修复。
- 新增索引使行为写入 P95 明显回退：删除该索引，优先保留查询改写。
- 任何结果无法从固定快照复现三次：只记录为诊断数据，不写入最终性能结论或简历。

## 9. 完成定义

本优化阶段完成需要同时满足：

1. PERF-01、PERF-02、TEST-01、TEST-02 均已实现并有自动化或可重复验证证据。
2. `small-diagnostic-v1` 三轮 50 RPS 和 100 RPS 综合测试通过，推荐无主链路降级。
3. 建立 `medium-v1` 固定快照并产出新的三轮中位数报告。
4. 报告包含 SQL 执行计划、PromQL、容器资源、Before/After 中位数和回滚判断。
5. 若未达到任一条件，明确写成“未完成/未通过”，不使用 HTTP 100% 成功替代性能结论。
