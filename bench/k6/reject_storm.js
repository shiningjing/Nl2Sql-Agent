import http from 'k6/http';
import { check } from 'k6';

// 防护模式：拒绝风暴 —— qps=1 用户被 100 并发打（Lua 限流快速 429）+ 无头请求 401
// 验证被滥用时网关以极低成本拒绝、正常延迟不受影响（429/401 是预期响应，不计失败）
export const options = {
  stages: [
    { duration: '10s', target: 100 },
    { duration: '30s', target: 100 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(99)<100'],
  },
};

const BASE = __ENV.BASE || 'http://127.0.0.1:8090';

export default function () {
  if (__ITER % 2 === 0) {
    // 超限用户：首请求/每秒 1 个 202，其余 429
    const res = http.post(
      `${BASE}/api/v1/task/submit`,
      JSON.stringify({ question: `abuse iter=${__ITER}`, db_id: 'california_schools' }),
      { headers: { 'Content-Type': 'application/json', 'X-User-Id': 'bench_abuser' } }
    );
    check(res, { '202 or 429': (r) => r.status === 202 || r.status === 429 });
  } else {
    // 无身份头：401 立即拒绝
    const res = http.post(
      `${BASE}/api/v1/task/submit`,
      JSON.stringify({ question: 'anon' }),
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(res, { 'status 401': (r) => r.status === 401 });
  }
}
