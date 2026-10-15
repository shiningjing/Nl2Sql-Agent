import http from 'k6/http';
import { check } from 'k6';

// 容量模式：status 读路径（纯 Redis GET，UI 轮询热路径）
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

export function setup() {
  const res = http.post(
    `${BASE}/api/v1/task/submit`,
    JSON.stringify({ question: 'k6 status poll target', db_id: 'california_schools' }),
    { headers: { 'Content-Type': 'application/json', 'X-User-Id': 'bench_user' } }
  );
  return { taskId: res.json('task_id') };
}

export default function (data) {
  const res = http.get(`${BASE}/api/v1/task/${data.taskId}/status`, {
    headers: { 'X-User-Id': 'bench_user' },
  });
  check(res, { 'status 200': (r) => r.status === 200 });
}
