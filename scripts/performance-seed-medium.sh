#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
source_account=${PERFORMANCE_SOURCE_ACCOUNT:-medium-user-00001}
password=${PERFORMANCE_PASSWORD:-}
api_port=${PERFORMANCE_API_PORT:-8080}
base_url=${PERFORMANCE_BASE_URL:-http://127.0.0.1:$api_port}
mysql_password=${MYSQL_ROOT_PASSWORD:-sealos123}
artifact_dir=${PERFORMANCE_ARTIFACT_DIR:-$repo_dir/build/performance/medium-v1}
snapshot_root=${PERFORMANCE_SNAPSHOT_DIR:-$repo_dir/build/performance/snapshots}
compose=(docker compose -f "$repo_dir/apps/docker-compose.yml" -f "$repo_dir/apps/docker-compose.performance.yml")

if [[ -z "$password" ]]; then
  echo "PERFORMANCE_PASSWORD is required and is only written to ignored local account-pool files" >&2
  exit 2
fi
if [[ ! "$source_account" =~ ^[a-zA-Z0-9._-]+$ ]]; then
  echo "invalid PERFORMANCE_SOURCE_ACCOUNT: $source_account" >&2
  exit 2
fi
if [[ -e "$snapshot_root/medium-v1/database.sql.gz" ]]; then
  echo "medium-v1 snapshot already exists: $snapshot_root/medium-v1" >&2
  exit 1
fi
command -v jq >/dev/null || { echo "jq is required to generate local k6 account pools" >&2; exit 1; }

PERFORMANCE_API_PORT="$api_port" PERFORMANCE_BASE_URL="$base_url" \
  "$script_dir/performance-snapshot.sh" restore small-diagnostic-v1

login_payload=$(jq -nc --arg account "$source_account" --arg password "$password" '{account:$account,password:$password}')
if ! curl -fsS -H 'Content-Type: application/json' -d "$login_payload" "$base_url/api/sessions" >/dev/null; then
  if [[ "$source_account" != "medium-user-00001" ]]; then
    echo "failed to authenticate PERFORMANCE_SOURCE_ACCOUNT at $base_url" >&2
    exit 1
  fi
  register_payload=$(jq -nc --arg account "$source_account" --arg password "$password" \
    '{account:$account,password:$password,nickname:"Medium Seed Source"}')
  status=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
    -d "$register_payload" "$base_url/api/users")
  if [[ "$status" != "201" ]]; then
    echo "failed to register PERFORMANCE_SOURCE_ACCOUNT at $base_url: status=$status" >&2
    exit 1
  fi
fi

stopped=1
restart_services() {
  if [[ "$stopped" -eq 1 ]]; then
    "${compose[@]}" up -d api worker >/dev/null
  fi
}
trap restart_services EXIT
"${compose[@]}" stop api worker >/dev/null

{ printf "SET @source_account = '%s';\n" "$source_account"; sed -n '1,$p' "$script_dir/performance-seed-medium.sql"; } | \
  "${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql mysql -uroot -N -B FluxFeed

"${compose[@]}" exec -T redis redis-cli FLUSHDB >/dev/null
"${compose[@]}" up -d api worker >/dev/null
stopped=0
trap - EXIT

for _ in $(seq 1 60); do
  if curl -fsS "$base_url/health" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "$base_url/health" >/dev/null || { echo "API did not become healthy after medium-v1 generation" >&2; exit 1; }

mkdir -p "$artifact_dir"
jq -n --arg password "$password" '
  def pad5: tostring | ("00000" + .)[-5:];
  [range(1; 101) | {account:("medium-user-" + (. | pad5)), password:$password}]
' >"$artifact_dir/read-accounts.json"
jq -n --arg password "$password" '
  def pad5: tostring | ("00000" + .)[-5:];
  [range(101; 201) | {account:("medium-user-" + (. | pad5)), password:$password}]
' >"$artifact_dir/write-accounts.json"
chmod 600 "$artifact_dir/read-accounts.json" "$artifact_dir/write-accounts.json"

"${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql mysql -uroot -N -B FluxFeed -e "
  SELECT GROUP_CONCAT(id ORDER BY id SEPARATOR ',')
  FROM (SELECT id FROM video WHERE idempotency_key LIKE 'medium-v1-video-%' ORDER BY id LIMIT 1000) targets;
" >"$artifact_dir/video-ids.txt"
"${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql mysql -uroot -N -B FluxFeed -e "
  SELECT GROUP_CONCAT(id ORDER BY id SEPARATOR ',')
  FROM (SELECT id FROM account WHERE account LIKE 'medium-user-%' ORDER BY account LIMIT 200 OFFSET 200) targets;
" >"$artifact_dir/target-user-ids.txt"

ready=$("${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql mysql -uroot -N -B FluxFeed -e "
  SELECT
    (SELECT COUNT(*) FROM account) >= 12000
    AND (SELECT COUNT(*) FROM video WHERE status = 2) >= 100000
    AND (SELECT COUNT(*) FROM video_embedding) >= 100000
    AND (SELECT COUNT(*) FROM user_follow WHERE status = 1) >= 1000000
    AND (SELECT COUNT(*) FROM interaction_action WHERE status = 1) >= 1000000
    AND (SELECT COUNT(*) FROM video_view_events) >= 1000000
    AND (SELECT COUNT(*) FROM user_relation_stat WHERE follower_count >= 10000) >= 2;
")
if [[ "$ready" != "1" ]]; then
  echo "medium-v1 readiness check failed" >&2
  exit 1
fi

PERFORMANCE_API_PORT="$api_port" PERFORMANCE_BASE_URL="$base_url" \
  "$script_dir/performance-snapshot.sh" create medium-v1
echo "created medium-v1 snapshot and local k6 inputs under: $artifact_dir"
