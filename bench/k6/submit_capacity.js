import http from 'k6/http';
import { check } from 'k6';

// 容量模式：submit 写路径全链（策略→权限→限流→配额→Redis 状态+Kafka 信封）
// 用户 bench_user 高限额，闭合回路压网关天花板
export const options = {
  stages: [
    { duration: '15s', target: 50 },
    { duration: '60s', target: 50 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<250'],
  },
};

const BASE = __ENV.BASE || 'http://127.0.0.1:8090';

export default function () {
  const res = http.post(
    `${BASE}/api/v1/task/submit`,
    JSON.stringify({ question: `k6 load iter=${__ITER}`, db_id: 'california_schools' }),
    { headers: { 'Content-Type': 'application/json', 'X-User-Id': 'bench_user' } }
  );
  check(res, { 'status 202': (r) => r.status === 202 });
}
