# 审核模块

审核模块是视频进入公开 Feed 前的强制治理入口。当前采用“规则预审 + 人工终审”：规则只提供风险提示，不会自动发布或驳回视频。

## 审核流程

```text
用户提交视频
  → 创建 title / video / cover 三个预审项
  → Worker 执行规则预审
  → human_review
  → 审核员通过或驳回
  → published / rejected
```

`RuleProvider` 目前只检查审核项字符串中的演示关键词，不分析视频画面、音频或封面内容。接入外部内容安全服务时可通过 `/internal/moderation/results` 回传单项结果，但视频仍必须经过人工终审。

## 接口

| 方法 | 路径 | 权限 | 作用 |
| --- | --- | --- | --- |
| GET | `/api/admin/reviews?status=human_review` | `review.read` | 查询人工待审队列及三个预审项 |
| POST | `/api/admin/reviews/{reviewId}/decisions` | `review.decide` | 人工通过或驳回 |
| POST | `/internal/moderation/results` | Internal Token | 外部机审回传单项结果 |
| PUT | `/api/admin/users/{userId}/role` | `role.assign` | 将已注册用户设为 `reviewer` |

人工判定请求：

```json
{"decision":"pass","reason":"人工审核通过"}
```

驳回必须提供明确原因。只有 `human_review` 状态接受人工判定，避免预审尚未结束时提前发布。

## 数据与权限

- `content_review` 保存目标、版本、总状态和风险原因。
- `content_review_item` 保存 `title`、`video`、`cover` 的内容、预审来源、结果和时间。
- `content_state_history` 保存用户、系统和审核员触发的状态变化。
- `reviewer` 具有 `review.read` 与 `review.decide`，登录后进入 `/review` 专用工作台。
- 未审核媒体仅允许作者和具有 `review.read` 权限的用户访问。

项目根目录 `.env` 中的 `ADMIN`、`ADMIN_PW` 用于引导管理员账号。API 启动时会幂等创建该账号；若账号已存在，则同步密码、恢复正常状态并补齐 `super_admin` 权限。该账号登录后自动进入 `/admin`。

创建审核员时，先让目标用户正常注册，再由上述管理员在运营后台的“角色分配”页输入用户 ID。角色变更后，目标用户需重新登录以刷新 JWT 和前端资料。

评论继续使用现有自动规则审核，不进入视频人工终审队列。
