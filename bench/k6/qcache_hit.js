import http from 'k6/http';
import { check } from 'k6';

// 容量模式：qcache 命中短路（Redis GET + SUCCESS 终态写 + taskowner，不触 Kafka/引擎）
// 问题串由 run_all.sh 预热进 qcache 后经环境变量传入（保证哈希一致）
export const options = {
  stages: [
    { duration: '15s', target: 50 },
    { duration: '60s', target: 50 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<100'],
  },
};

const BASE = __ENV.BASE || 'http://127.0.0.1:8090';
const Q = __ENV.BENCH_Q;
const DB = __ENV.BENCH_DB;

export default function () {
  const res = http.post(
    `${BASE}/api/v1/task/submit`,
    JSON.stringify({ question: Q, db_id: DB }),
    { headers: { 'Content-Type': 'application/json', 'X-User-Id': 'bench_user' } }
  );
  check(res, {
    'status 202': (r) => r.status === 202,
    'cache hit': (r) => r.status === 202 && r.json('cache_hit') === '1',
  });
}
