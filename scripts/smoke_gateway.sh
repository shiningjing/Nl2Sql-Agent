#!/usr/bin/env bash
# M1 冒烟：Java 网关健康、代理透传、traceId、（可选）引擎不可达降级
# 用法: bash scripts/smoke_gateway.sh [gateway_url]   默认 http://127.0.0.1:8080
set -e
GW="${1:-http://127.0.0.1:8080}"

echo "== 1. /health 聚合（engine 组件状态）=="
curl -s -m 15 "$GW/health" | grep -o '"engine":{"status":"[A-Z]*"' || echo "(未找到 engine 组件)"

echo "== 2. /metrics Prometheus =="
curl -s -m 15 "$GW/metrics" | head -2

echo "== 3. 代理透传（引擎 /api/v1/health 应 200）=="
curl -s -m 15 -o /dev/null -w "HTTP %{http_code}\n" "$GW/api/v1/health"

echo "== 4. X-Trace-Id 回写 =="
curl -s -m 15 -D - -o /dev/null "$GW/api/v1/health" | grep -i "x-trace-id"

echo "== 5. 引擎故障降级（手动步骤，默认跳过）=="
echo "   停掉引擎后: curl -m 5 $GW/api/v1/task/x/status 应快速 503"
echo "OK"
