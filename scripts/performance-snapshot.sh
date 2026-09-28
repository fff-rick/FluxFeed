#!/usr/bin/env bash
set -euo pipefail

action=${1:-}
snapshot_name=${2:-small-diagnostic-v1}

if [[ ! "$snapshot_name" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]]; then
  echo "invalid snapshot name: $snapshot_name" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
snapshot_root=${PERFORMANCE_SNAPSHOT_DIR:-$repo_dir/build/performance/snapshots}
snapshot_dir=$snapshot_root/$snapshot_name
database_name=${MYSQL_DATABASE:-FluxFeed}
mysql_password=${MYSQL_ROOT_PASSWORD:-sealos123}
api_port=${PERFORMANCE_API_PORT:-8080}
health_url=${PERFORMANCE_BASE_URL:-http://127.0.0.1:$api_port}
compose=(docker compose -f "$repo_dir/apps/docker-compose.yml" -f "$repo_dir/apps/docker-compose.performance.yml")

usage() {
  echo "usage: $0 {create|restore|verify} [snapshot-name]" >&2
  exit 2
}

container_id() {
  local service=$1
  "${compose[@]}" ps -q "$service"
}

assert_performance_volume() {
  local service=$1 destination=$2 suffix=$3 id volume
  id=$(container_id "$service")
  if [[ -z "$id" ]]; then
    echo "$service container is not running; start the performance Compose stack first" >&2
    exit 1
  fi
  volume=$(docker inspect "$id" --format "{{range .Mounts}}{{if eq .Destination \"$destination\"}}{{.Name}}{{end}}{{end}}")
  if [[ "$volume" != *"_$suffix" ]]; then
    echo "refusing to continue: $service uses '$volume', expected a performance volume ending in '_$suffix'" >&2
    exit 1
  fi
}

assert_performance_stack() {
  assert_performance_volume mysql /var/lib/mysql performance_mysql_data
  assert_performance_volume redis /data performance_redis_data
}

database_counts() {
  "${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql \
    mysql -uroot -N -B "$database_name" -e "
      SELECT 'account', COUNT(*) FROM account
      UNION ALL SELECT 'video', COUNT(*) FROM video
      UNION ALL SELECT 'video_embedding', COUNT(*) FROM video_embedding
      UNION ALL SELECT 'user_follow', COUNT(*) FROM user_follow
      UNION ALL SELECT 'interaction_action', COUNT(*) FROM interaction_action
      UNION ALL SELECT 'video_view_events', COUNT(*) FROM video_view_events
      UNION ALL SELECT 'exposures', COUNT(*) FROM exposures
      UNION ALL SELECT 'user_interest_embedding', COUNT(*) FROM user_interest_embedding
      UNION ALL SELECT CONCAT('outbox_', status), COUNT(*) FROM outbox_event GROUP BY status;
    "
}

verify_snapshot() {
  [[ -f "$snapshot_dir/database.sql.gz" ]] || { echo "snapshot not found: $snapshot_dir/database.sql.gz" >&2; exit 1; }
  (cd "$snapshot_dir" && sha256sum --check database.sql.gz.sha256)
}

create_snapshot() {
  assert_performance_stack
  if [[ -e "$snapshot_dir/database.sql.gz" ]]; then
    echo "snapshot already exists, choose a new name: $snapshot_dir" >&2
    exit 1
  fi

  local stopped=1
  restart_services() {
    if [[ "$stopped" -eq 1 ]]; then
      "${compose[@]}" up -d api worker >/dev/null
    fi
  }
  trap restart_services EXIT
  "${compose[@]}" stop api worker >/dev/null

  local unsettled
  unsettled=$("${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql \
    mysql -uroot -N -B "$database_name" -e "SELECT COUNT(*) FROM outbox_event WHERE status <> 'published';")
  if [[ "$unsettled" != "0" ]]; then
    echo "refusing to snapshot with $unsettled unsettled outbox events" >&2
    exit 1
  fi
  mkdir -p "$snapshot_dir"
  local partial_dump=$snapshot_dir/database.sql.gz.partial
  "${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql \
    mysqldump -uroot --single-transaction --routines --triggers --events \
    --set-gtid-purged=OFF --no-tablespaces --databases "$database_name" | gzip -1 >"$partial_dump"
  mv "$partial_dump" "$snapshot_dir/database.sql.gz"
  (cd "$snapshot_dir" && sha256sum database.sql.gz >database.sql.gz.sha256)
  database_counts >"$snapshot_dir/counts.tsv"
  git -C "$repo_dir" rev-parse HEAD >"$snapshot_dir/git-sha.txt"
  date -u +%Y-%m-%dT%H:%M:%SZ >"$snapshot_dir/created-at.txt"
  "${compose[@]}" up -d api worker >/dev/null
  stopped=0
  trap - EXIT
  echo "created snapshot: $snapshot_dir"
}

restore_snapshot() {
  assert_performance_stack
  verify_snapshot

  local stopped=1
  restart_services() {
    if [[ "$stopped" -eq 1 ]]; then
      "${compose[@]}" up -d api worker >/dev/null
    fi
  }
  trap restart_services EXIT

  "${compose[@]}" stop api worker >/dev/null
  gzip -dc "$snapshot_dir/database.sql.gz" | \
    "${compose[@]}" exec -T -e MYSQL_PWD="$mysql_password" mysql mysql -uroot
  "${compose[@]}" exec -T redis redis-cli FLUSHDB >/dev/null
  "${compose[@]}" up -d api worker >/dev/null
  stopped=0
  trap - EXIT

  for _ in $(seq 1 60); do
    if curl -fsS "$health_url/health" >/dev/null; then
      database_counts | diff -u "$snapshot_dir/counts.tsv" -
      echo "restored snapshot: $snapshot_dir"
      echo "Redis and API L1 are cold; run the documented warm-up before measuring."
      return
    fi
    sleep 1
  done
  echo "API did not become healthy after snapshot restore" >&2
  exit 1
}

case "$action" in
  create) create_snapshot ;;
  restore) restore_snapshot ;;
  verify) verify_snapshot ;;
  *) usage ;;
esac
