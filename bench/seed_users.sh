#!/usr/bin/env bash
# 压测用户种子（直插 MySQL，策略缓存首请求自动预热）：
#   bench_user   容量模式 —— 通配全库、qps 100000、日配额 1000 万（不构成瓶颈）
#   bench_abuser 防护模式 —— qps=1（验证限流优雅降级）
# 可选参数 $1：额外生成 N 个模拟用户（M4b 真实基准用，50 并发一人一号）
set -e
MYSQL_CONTAINER=${MYSQL_CONTAINER:-deployment-mysql-1}
N_USERS=${1:-0}

docker exec -i "$MYSQL_CONTAINER" mysql -uroot -pnl2sql_root nl2sql_ops <<'SQL'
INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status) VALUES
  ('bench_user',   'user', '["*"]', 100000, 10000000, 1),
  ('bench_abuser', 'user', '["*"]', 1,      100000,   1)
ON DUPLICATE KEY UPDATE qps_limit=VALUES(qps_limit), daily_quota=VALUES(daily_quota), status=1;
SQL

if [ "$N_USERS" -gt 0 ]; then
  values=""
  for i in $(seq 1 "$N_USERS"); do
    values="${values:+$values,}('bench_u$(printf '%03d' "$i")', 'user', '[\"*\"]', 50, 10000, 1)"
  done
  docker exec -i "$MYSQL_CONTAINER" mysql -uroot -pnl2sql_root nl2sql_ops \
    -e "INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status) VALUES $values ON DUPLICATE KEY UPDATE status=1;"
fi

docker exec "$MYSQL_CONTAINER" mysql -uroot -pnl2sql_root nl2sql_ops \
  -e "SELECT user_tag, role, qps_limit, daily_quota, status FROM user_policy WHERE user_tag LIKE 'bench%';"
