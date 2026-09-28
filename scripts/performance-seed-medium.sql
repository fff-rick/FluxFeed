SET SESSION time_zone = '+00:00';

CREATE TEMPORARY TABLE perf_digit (n TINYINT UNSIGNED PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO perf_digit VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TEMPORARY TABLE perf_digit_2 LIKE perf_digit;
CREATE TEMPORARY TABLE perf_digit_3 LIKE perf_digit;
CREATE TEMPORARY TABLE perf_digit_4 LIKE perf_digit;
CREATE TEMPORARY TABLE perf_digit_5 LIKE perf_digit;
CREATE TEMPORARY TABLE perf_digit_6 LIKE perf_digit;
INSERT INTO perf_digit_2 SELECT n FROM perf_digit;
INSERT INTO perf_digit_3 SELECT n FROM perf_digit;
INSERT INTO perf_digit_4 SELECT n FROM perf_digit;
INSERT INTO perf_digit_5 SELECT n FROM perf_digit;
INSERT INTO perf_digit_6 SELECT n FROM perf_digit;

DROP TABLE IF EXISTS _performance_seed_seq, _performance_seed_accounts, _performance_seed_videos;
CREATE TABLE _performance_seed_seq ENGINE=InnoDB AS
SELECT a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 + f.n * 100000 AS n
FROM perf_digit a
CROSS JOIN perf_digit_2 b
CROSS JOIN perf_digit_3 c
CROSS JOIN perf_digit_4 d
CROSS JOIN perf_digit_5 e
CROSS JOIN perf_digit_6 f;
ALTER TABLE _performance_seed_seq ADD PRIMARY KEY (n);

INSERT INTO account (account, password, nickname, avatar_url, bio, status, role, created_at, updated_at)
SELECT
  CONCAT('medium-user-', LPAD(s.n + 1, 5, '0')),
  source.password,
  CONCAT('Medium User ', LPAD(s.n + 1, 5, '0')),
  '',
  'Stage 11 medium-v1 performance account',
  1,
  'user',
  UTC_TIMESTAMP(3),
  UTC_TIMESTAMP(3)
FROM _performance_seed_seq s
CROSS JOIN (SELECT password FROM account WHERE account = @source_account LIMIT 1) source
WHERE s.n < 11969
ON DUPLICATE KEY UPDATE
  nickname = VALUES(nickname),
  status = VALUES(status),
  updated_at = VALUES(updated_at);

CREATE TABLE _performance_seed_accounts ENGINE=InnoDB AS
SELECT ROW_NUMBER() OVER (ORDER BY account) AS seq, id
FROM account
WHERE account LIKE 'medium-user-%';
ALTER TABLE _performance_seed_accounts ADD PRIMARY KEY (seq), ADD UNIQUE KEY (id);

INSERT INTO video (
  author_id, title, description, media_url, cover_url, status,
  published_at, idempotency_key, created_at, updated_at
)
SELECT
  a.id,
  CONCAT('Medium performance video ', LPAD(s.n + 1, 6, '0')),
  CONCAT('Stage 11 topic ', MOD(s.n, 128), ' deterministic capacity fixture'),
  '/uploads/performance.mp4',
  '/uploads/performance.webp',
  2,
  TIMESTAMP('2026-09-28 08:00:00') - INTERVAL MOD(s.n * 37, 2592000) SECOND,
  CONCAT('medium-v1-video-', LPAD(s.n + 1, 6, '0')),
  TIMESTAMP('2026-09-28 08:00:00') - INTERVAL MOD(s.n * 37, 2592000) SECOND,
  UTC_TIMESTAMP(3)
FROM _performance_seed_seq s
JOIN _performance_seed_accounts a ON a.seq = MOD(s.n, 11969) + 1
WHERE s.n < 96121
ON DUPLICATE KEY UPDATE
  title = VALUES(title),
  status = VALUES(status),
  published_at = VALUES(published_at),
  updated_at = VALUES(updated_at);

CREATE TABLE _performance_seed_videos ENGINE=InnoDB AS
SELECT ROW_NUMBER() OVER (ORDER BY idempotency_key) AS seq, id, author_id, published_at
FROM video
WHERE idempotency_key LIKE 'medium-v1-video-%';
ALTER TABLE _performance_seed_videos ADD PRIMARY KEY (seq), ADD UNIQUE KEY (id);

INSERT INTO video_embedding (
  video_id, model, dimension, embedding_json, text_hash, created_at, updated_at
)
SELECT
  v.id,
  'hash-ngram-v1',
  128,
  CAST(CONCAT('[1', REPEAT(',0', 127), ']') AS JSON),
  SHA2(CONCAT('medium-v1:', v.id), 256),
  UTC_TIMESTAMP(3),
  UTC_TIMESTAMP(3)
FROM _performance_seed_videos v
ON DUPLICATE KEY UPDATE
  dimension = VALUES(dimension),
  embedding_json = VALUES(embedding_json),
  text_hash = VALUES(text_hash),
  updated_at = VALUES(updated_at);

INSERT IGNORE INTO user_follow (
  user_id, target_user_id, status, idempotency_key, created_at, updated_at
)
SELECT follower.id, whale.id, 1, NULL, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)
FROM _performance_seed_accounts follower
JOIN _performance_seed_accounts whale ON whale.seq = 1
WHERE follower.seq BETWEEN 2 AND 11001;

INSERT IGNORE INTO user_follow (
  user_id, target_user_id, status, idempotency_key, created_at, updated_at
)
SELECT follower.id, whale.id, 1, NULL, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)
FROM _performance_seed_accounts follower
JOIN _performance_seed_accounts whale ON whale.seq = 2
WHERE follower.seq <= 11001 AND follower.seq <> 2;

INSERT IGNORE INTO user_follow (
  user_id, target_user_id, status, idempotency_key, created_at, updated_at
)
SELECT
  follower.id,
  target.id,
  1,
  NULL,
  TIMESTAMP('2026-09-28 08:00:00') - INTERVAL MOD(s.n * 17, 2592000) SECOND,
  UTC_TIMESTAMP(3)
FROM _performance_seed_seq s
JOIN _performance_seed_accounts follower ON follower.seq = MOD(s.n, 11969) + 1
JOIN _performance_seed_accounts target
  ON target.seq = MOD(MOD(s.n, 11969) + 4 + FLOOR(s.n / 11969), 11969) + 1
WHERE s.n < 980000;

INSERT INTO user_relation_stat (
  user_id, following_count, follower_count, created_at, updated_at
)
SELECT
  a.id,
  COALESCE(following.total, 0),
  COALESCE(followers.total, 0),
  UTC_TIMESTAMP(3),
  UTC_TIMESTAMP(3)
FROM _performance_seed_accounts a
LEFT JOIN (
  SELECT user_id, COUNT(*) AS total FROM user_follow WHERE status = 1 GROUP BY user_id
) following ON following.user_id = a.id
LEFT JOIN (
  SELECT target_user_id, COUNT(*) AS total FROM user_follow WHERE status = 1 GROUP BY target_user_id
) followers ON followers.target_user_id = a.id
ON DUPLICATE KEY UPDATE
  following_count = VALUES(following_count),
  follower_count = VALUES(follower_count),
  updated_at = VALUES(updated_at);

INSERT IGNORE INTO interaction_action (
  user_id, video_id, action_type, status, idempotency_key, created_at, updated_at
)
SELECT
  u.id,
  v.id,
  IF(MOD(s.n, 5) = 0, 'FAVORITE', 'LIKE'),
  1,
  NULL,
  TIMESTAMP('2026-09-28 08:00:00') - INTERVAL MOD(s.n * 19, 2592000) SECOND,
  UTC_TIMESTAMP(3)
FROM _performance_seed_seq s
JOIN _performance_seed_accounts u ON u.seq = MOD(s.n, 11969) + 1
JOIN _performance_seed_videos v ON v.seq = CASE
  WHEN s.n < 800000 THEN MOD((MOD(s.n, 11969) + 1) * 131 + FLOOR(s.n / 11969), 19224) + 1
  ELSE 19225 + MOD((MOD(s.n, 11969) + 1) * 131 + FLOOR((s.n - 800000) / 11969), 76897)
END
WHERE s.n < 1000000;

INSERT INTO video_stat (
  video_id, like_count, comment_count, favorite_count, created_at, updated_at
)
SELECT
  v.id,
  COALESCE(SUM(a.action_type = 'LIKE' AND a.status = 1), 0),
  0,
  COALESCE(SUM(a.action_type = 'FAVORITE' AND a.status = 1), 0),
  UTC_TIMESTAMP(3),
  UTC_TIMESTAMP(3)
FROM _performance_seed_videos v
LEFT JOIN interaction_action a ON a.video_id = v.id
GROUP BY v.id
ON DUPLICATE KEY UPDATE
  like_count = VALUES(like_count),
  comment_count = VALUES(comment_count),
  favorite_count = VALUES(favorite_count),
  updated_at = VALUES(updated_at);

INSERT IGNORE INTO video_view_events (
  user_id, video_id, scene, request_id, event_type, watch_ms, completed, created_at
)
SELECT
  u.id,
  v.id,
  CASE MOD(s.n, 3) WHEN 0 THEN 'recommend' WHEN 1 THEN 'timeline' ELSE 'hot' END,
  CONCAT('medium-v1-', LPAD(s.n + 1, 7, '0')),
  CASE
    WHEN MOD(s.n, 100) < 50 THEN 'exposed'
    WHEN MOD(s.n, 100) < 80 THEN 'valid_play'
    WHEN MOD(s.n, 100) < 95 THEN 'finish'
    WHEN MOD(s.n, 100) < 98 THEN 'skip'
    WHEN MOD(s.n, 100) < 99 THEN 'not_interested'
    ELSE 'hide_author'
  END,
  CASE WHEN MOD(s.n, 100) < 50 THEN 0 ELSE 3000 + MOD(s.n * 97, 57001) END,
  MOD(s.n, 100) >= 80 AND MOD(s.n, 100) < 95,
  TIMESTAMP('2026-09-28 08:00:00') - INTERVAL MOD(s.n * 23, 2592000) SECOND
FROM _performance_seed_seq s
JOIN _performance_seed_accounts u ON u.seq = MOD(s.n, 11969) + 1
JOIN _performance_seed_videos v
  ON v.seq = MOD((MOD(s.n, 11969) + 1) * 197 + FLOOR(s.n / 11969), 96121) + 1
WHERE s.n < 1000000;

INSERT IGNORE INTO exposures (
  user_id, video_id, first_exposed_at, last_exposed_at, exposure_count,
  last_scene, created_at, updated_at
)
SELECT user_id, video_id, created_at, created_at, 1, scene, created_at, created_at
FROM video_view_events
WHERE request_id LIKE 'medium-v1-%' AND event_type = 'exposed';

INSERT IGNORE INTO feed_inbox (user_id, video_id, author_id, published_at, created_at)
SELECT f.user_id, latest.id, latest.author_id, latest.published_at, UTC_TIMESTAMP(3)
FROM (
  SELECT
    v.id,
    v.author_id,
    v.published_at,
    ROW_NUMBER() OVER (PARTITION BY v.author_id ORDER BY v.published_at DESC, v.id DESC) AS row_num
  FROM video v
  WHERE v.idempotency_key LIKE 'medium-v1-video-%'
) latest
JOIN user_follow f ON f.target_user_id = latest.author_id AND f.status = 1
JOIN _performance_seed_accounts author ON author.id = latest.author_id AND author.seq > 2
WHERE latest.row_num = 1;

SELECT 'medium_accounts', COUNT(*) FROM _performance_seed_accounts
UNION ALL SELECT 'medium_videos', COUNT(*) FROM _performance_seed_videos
UNION ALL SELECT 'published_videos', COUNT(*) FROM video WHERE status = 2
UNION ALL SELECT 'video_embeddings', COUNT(*) FROM video_embedding
UNION ALL SELECT 'active_follows', COUNT(*) FROM user_follow WHERE status = 1
UNION ALL SELECT 'active_actions', COUNT(*) FROM interaction_action WHERE status = 1
UNION ALL SELECT 'view_events', COUNT(*) FROM video_view_events
UNION ALL SELECT 'exposures', COUNT(*) FROM exposures
UNION ALL SELECT 'feed_inbox', COUNT(*) FROM feed_inbox
UNION ALL SELECT 'whales_over_10000', COUNT(*) FROM user_relation_stat WHERE follower_count >= 10000;

DROP TABLE _performance_seed_seq, _performance_seed_accounts, _performance_seed_videos;
