#!/usr/bin/env bash
# M4 网关压测编排：种子用户 → qcache 预热 → 依次跑 4 个 k6 场景 → 结果落 bench/results/
# 前置：compose mysql/redis/kafka 已起，网关容器已跑（默认 http://127.0.0.1:8090，
#       容器需 GLOBAL_QPS 抬高——容量模式测网关天花板而非限流阈值）
set -e
BASE=${BASE:-http://127.0.0.1:8090}
OUT=bench/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

bash bench/seed_users.sh

# qcache 预热（问题串与 k6 场景经环境变量共享，保证 sha256 一致）
Q="k6 cache hit question?"
DB="california_schools"
HASH=$(printf '%s|%s' "$Q" "$DB" | sha256sum | cut -d' ' -f1)
docker exec deployment-redis-1 redis-cli SET "qcache:$HASH" \
  '{"status":"SUCCESS","sql":"SELECT 1","exec_result":{"row_count":1},"token_usage":{},"node_timings":{}}' \
  EX 7200 > /dev/null
export BASE BENCH_Q="$Q" BENCH_DB="$DB"

for s in submit_capacity status_poll qcache_hit reject_storm; do
  echo ""
  echo "════════ $s ════════"
  k6 run --summary-trend-stats "avg,min,med,p(90),p(95),p(99),max" "bench/k6/$s.js" --summary-export "$OUT/$s.json" 2>&1 | tee "$OUT/$s.log" | tail -25
  curl -s "$BASE/metrics" | grep -E "^gateway_" | sort > "$OUT/metrics-after-$s.txt"
done

echo ""
echo "results → $OUT"
