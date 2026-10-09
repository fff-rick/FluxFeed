# FluxFeed 密钥管理与轮换

## 原则

- 仓库、镜像和普通 ConfigMap 不保存真实密钥。
- 本地 Docker Compose 从仓库根目录 `.env` 读取密钥；该文件已被 Git 忽略。
- Kubernetes 只引用预先创建的 `fluxfeed-secrets`，`apps/deploy.yaml` 不创建 Secret。
- 生产启动必须设置 `FLUXFEED_ENV=production`。应用会拒绝弱 JWT/内部 Token、空数据库或 Redis 密码、root 数据库账号、RabbitMQ guest 账号以及未启用 TLS 的外部依赖。
- 生产秘密应优先由云 Secret Manager、External Secrets Operator 或密封 Secret 系统管理；手工创建仅适合初次验证。

## 本地 Docker Compose

```bash
cp .env.example .env
openssl rand -hex 32
```

将生成值分别写入 `.env`，然后运行：

```bash
make up
```

不要把 `.env` 加入 Git，也不要在工单、聊天记录或构建日志中粘贴其内容。

## Kubernetes

部署清单要求命名空间中已经存在 `fluxfeed-secrets`，包含以下键：

| 键 | 用途 |
| --- | --- |
| `mysql-root-password` | MySQL 初始化管理密码 |
| `mysql-user` | 应用数据库账号，不能为 root |
| `mysql-password` | 应用数据库密码 |
| `redis-password` | Redis 认证密码 |
| `rabbitmq-user` | RabbitMQ 非 guest 账号 |
| `rabbitmq-password` | RabbitMQ 密码 |
| `rabbitmq-url` | 带认证信息的完整 AMQP URL |
| `jwt-secret` | JWT 签名密钥，至少 32 字符 |
| `internal-token` | 内部接口 Token，至少 32 字符 |

首次验证可以从受保护的终端执行以下命令。生产环境应由 Secret Manager 同步这些键：

```bash
kubectl create namespace fluxfeed --dry-run=client -o yaml | kubectl apply -f -
kubectl -n fluxfeed create secret generic fluxfeed-secrets \
  --from-literal=mysql-root-password="$MYSQL_ROOT_PASSWORD" \
  --from-literal=mysql-user="$MYSQL_USER" \
  --from-literal=mysql-password="$MYSQL_PASSWORD" \
  --from-literal=redis-password="$REDIS_PASSWORD" \
  --from-literal=rabbitmq-user="$RABBITMQ_USER" \
  --from-literal=rabbitmq-password="$RABBITMQ_PASSWORD" \
  --from-literal=rabbitmq-url="$RABBITMQ_URL" \
  --from-literal=jwt-secret="$JWT_SECRET" \
  --from-literal=internal-token="$INTERNAL_TOKEN"
kubectl apply -f apps/deploy.yaml
```

命令所引用的环境变量应由密码管理器临时注入，不应写入 shell 历史或仓库脚本。

## 生产连接加密

应用支持以下安全连接配置：

- MySQL：`FLUXFEED_DATABASE_TLS=true` 或驱动支持的已注册 TLS 配置名；
- Redis：`FLUXFEED_REDIS_TLS=true`，可选 `FLUXFEED_REDIS_TLS_SERVER_NAME`；
- Kafka：`FLUXFEED_KAFKA_USERNAME`、`FLUXFEED_KAFKA_PASSWORD`、`FLUXFEED_KAFKA_TLS=true`；
- RabbitMQ：使用 `amqps://` URL；
- OpenTelemetry：生产环境不要设置 `OTEL_EXPORTER_OTLP_INSECURE=true`。

仓库中的 Compose 和 `apps/deploy.yaml` 单机中间件明确使用 `development` 模式，仅用于开发与验证。跨节点生产部署应改用提供 TLS 的托管服务或挂载受信任证书的集群配置，并显式设置 `FLUXFEED_ENV=production`；缺少 TLS 时应用会拒绝启动。

## 轮换流程

### JWT 密钥

当前 JWT 实现只支持一个有效签名密钥，因此轮换会使已有访问令牌失效。访问令牌有效期为 15 分钟，推荐流程：

1. 选择低流量窗口并发布通知；
2. 在 Secret Manager 中生成并更新新密钥；
3. 滚动重启 API；
4. 确认登录和鉴权指标正常；
5. 观察至少一个旧 Token TTL 后关闭事件。

需要无感轮换时，再引入带 `kid` 的双密钥验证窗口。

### 内部 Token

当前实现同样只支持单 Token。轮换时先停止或协调内部调用方，再更新 Secret 并滚动重启调用方和 API。需要零停机轮换时，再扩展为新旧 Token 短期并存。

### 数据库、Redis 和 RabbitMQ

1. 在服务端创建新凭据，暂时保留旧凭据；
2. 更新 Secret Manager；
3. 滚动重启 API 和 Worker；
4. 验证健康检查、错误率、Kafka/Outbox 积压及业务读写；
5. 撤销旧凭据；
6. 记录轮换人、时间、影响范围和验证结果。

## 泄露应急

1. 立即吊销或替换泄露凭据，不等待代码发布；
2. 检查访问日志、审计日志和异常数据变更；
3. 如果进入 Git 历史，使用 `git filter-repo` 等工具清理，并通知所有协作者重新同步；
4. 仅清理 Git 历史不能恢复安全性，所有受影响凭据必须先轮换；
5. 记录事故时间线、影响范围、处置结果和防复发措施。
