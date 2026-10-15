# 压测工具集（M4）

- `seed_users.sh` — 压测用户种子：`bench_user`（容量模式，高限额）/ `bench_abuser`（qps=1）；参数 N 额外生成 N 个模拟用户（M4b 用）
- `k6/submit_capacity.js` — submit 写路径容量（校验链 + Redis + Kafka）
- `k6/status_poll.js` — status 读路径容量（Redis GET）
- `k6/qcache_hit.js` — qcache 命中短路容量
- `k6/reject_storm.js` — 防护模式：qps=1 用户 + 无头请求的拒绝风暴（429/401 为预期）
- `run_all.sh` — 编排：种子 → qcache 预热 → 依次跑 4 场景 → 结果+网关指标快照落 `results/`

前置：compose mysql/redis/kafka 已起；网关容器以 `GLOBAL_QPS=100000` 启动（容量模式）。
结果与结论见 `docs/benchmarks/2026-10-15-m4a-gateway-load.md`。
