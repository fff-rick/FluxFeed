# 系统治理模块设计

## 1. 模块职责

系统治理模块负责限流、降级开关和失败任务重试，保障高峰期服务可用。

当前第一批已实现：普通 API 三秒 Context 超时、按 IP 的进程内 Token Bucket
（默认 50 RPS／100 Burst，最多记录 10,000 个客户端）、HTTP Server 读写超时与
五秒优雅关闭，以及推荐主召回 500 ms 超时后的 `Hot -> Latest` 降级。推荐主召回同时使用
32 并发隔舱、连续 5 次失败熔断 10 秒和一次 25 ms 退避重试；熔断恢复期仅放行单个探测。
上传接口沿用媒体处理链路自身的两分钟超时。多实例全局限流和动态开关仍待后续批次。

配置位于 `governance`：

```yaml
request_timeout: "3s"
recommendation_timeout: "500ms"
shutdown_timeout: "5s"
rate_limit_rps: 50
rate_limit_burst: 100
```

治理动作通过 Prometheus 指标 `gcfeed_governance_events_total{component,decision}` 暴露，
覆盖限流、重试、隔舱拒绝、熔断、自动／手动降级、局部召回和默认画像等决策。

## 2. 接口设计

| 方法 | 接口路径 | 作用 | 鉴权 | 幂等键 |
| --- | --- | --- | --- | --- |
| POST | `/internal/rate-limit-decisions` | 判断当前请求是否放行 | 服务鉴权 | 支持 |
| GET | `/internal/governance/degrade-switches` | 获取降级开关状态 | 服务鉴权 | 无 |
| PATCH | `/internal/governance/degrade-switches/{key}` | 更新降级开关 | 服务鉴权 | 无 |
| POST | `/internal/dead-letter-retries` | 重试死信任务 | 服务鉴权 | 支持 |

## 3. 降级开关

当前实现支持 `recommendation_degraded`，状态保存在 API 进程内，重启自动恢复关闭；适合
单实例故障演练和紧急绕过个性化主召回。多实例部署需要共享配置时，再引入下述持久化模型
或配置中心，当前不提前增加每次请求的数据库查询。

更新示例：

```http
PATCH /internal/governance/degrade-switches/recommendation_degraded
X-Internal-Token: <token>
Content-Type: application/json

{"enabled": true}
```

### 3.1 规划中的持久化模型

`governance_degrade_switch`：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| `id` | BIGINT | PK | 记录 ID |
| `switch_key` | VARCHAR(64) | UNIQUE, NOT NULL | 开关键 |
| `enabled` | TINYINT | NOT NULL, DEFAULT 0 | 0 关 / 1 开 |
| `updated_by` | BIGINT | NOT NULL | 更新人 |
| `updated_at` | DATETIME | NOT NULL | 更新时间 |

索引建议：`uk_switch_key(switch_key)`。

### 3.2 `governance_dead_letter`

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| `id` | BIGINT | PK | 任务 ID |
| `task_type` | VARCHAR(64) | NOT NULL | 任务类型 |
| `payload` | JSON | NOT NULL | 任务参数 |
| `retry_count` | INT | NOT NULL, DEFAULT 0 | 重试次数 |
| `status` | TINYINT | NOT NULL, DEFAULT 1 | 1 待重试 / 2 已完成 / 3 放弃 |
| `last_error` | VARCHAR(512) | NULLABLE | 最后错误 |
| `updated_at` | DATETIME | NOT NULL | 更新时间 |
| `created_at` | DATETIME | NOT NULL | 创建时间 |

索引建议：`idx_status_updated(status, updated_at)`。

## 4. 业务规则

| 规则 | 说明 |
| --- | --- |
| 限流按资源维度决策 | 可按用户、IP、接口或场景组合判断 |
| 降级开关全局可查 | 内部服务读取开关后决定是否关闭非核心能力 |
| 内部服务更新开关 | 使用 Internal Token，仅接受已注册 key |
| 死信重试有次数上限 | 超过上限后任务进入放弃状态 |
| 重试操作保持幂等 | 同一死信任务重复重试不会重复产生业务事实 |

## 5. 测试建议

| 场景 | 期望 |
| --- | --- |
| 请求限流决策 | 返回 allow 和原因 |
| 更新降级开关 | 已注册 key 状态变化，未知 key 返回 404 |
| 查询降级开关 | 返回当前开关集合 |
| 重试死信任务 | retry_count 增加，成功后状态完成 |
| 超过重试上限 | 状态变为放弃 |

## 6. 前端接入点

| 页面 | 接入能力 |
| --- | --- |
| 治理控制台 | 查看和更新降级开关 |
| 死信任务页 | 查看失败任务和触发重试 |
| 监控看板 | 展示限流和降级状态 |
