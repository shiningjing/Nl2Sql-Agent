# M4a 网关高并发压测报告（k6，无真实 LLM）

- 日期：2026-10-15 · 工具：k6 v2.2.0 · 场景：`bench/k6/*.js`（编排 `bench/run_all.sh`）
- 被测：Spring Gateway 容器（单实例，Docker Desktop/WSL2，Windows 开发机）
- 依赖：Testcontainers 同款 compose 栈（Redis/Kafka/MySQL 容器）；引擎未启动——四条被压路径均不触 LLM/FastAPI
- 容量模式说明：容器以 `GLOBAL_QPS=100000` 启动、`bench_user` 限额 100000/日配额 1000 万，
  测的是**网关自身天花板**而非限流阈值（限流阈值单独由拒绝风暴场景验证）
- 环境噪音：与开发机共存（同机还有另一项目的 Flink/Zabbix 容器在运行），数字应视为**下界**

## 结果（50/100 并发，爬坡 15s + 稳态 60s）

| 场景 | 并发 | RPS | avg | P90 | P95 | P99 | max | 失败率 |
|---|---|---|---|---|---|---|---|---|
| submit（写：校验链+Redis+Kafka） | 50 | **714** | 59ms | 101ms | 123ms | **180ms** | 404ms | 0% |
| status（读：Redis GET） | 50 | **2372** | 18ms | 30ms | 36ms | **52ms** | 146ms | 0% |
| qcache 命中（Redis GET+终态写） | 50 | **1603** | 26ms | 43ms | 51ms | **67ms** | 140ms | 0% |
| 拒绝风暴（429 限流/401 无头各半） | 100 | **3391** | 23ms | 45ms | 55ms | **75ms** | 163ms | 预期拒绝 |

- 同日主机空闲时的第一轮采样 submit 达 1890 RPS / avg 22ms（背景负载差异，见下）
- 累计 56.8 万请求；Prometheus 侧对账：`gateway_cache_total{hit}=136280`、
  `gateway_ratelimit_rejected_total{user}=84764` 与场景计数吻合

## 压测抓出的缺陷（已修复 + 回归测试）

1. **Recorder 溢出 Bug（P0）**：qcache 风暴下 dirty 集合入队（~1600/s）远快于 flush 出队
   （当时 100 条/2s），超过 10000 上限后走溢出分支——`ConcurrentHashMap` 迭代器
   `remove()` 未先 `next()` 抛 `IllegalStateException`，**该场景全部 submit 500**。
   修复：`next()` 后再 `remove()`；flush 改为循环取批（批 500、单次预算 2s）。
   81 个集成测试未覆盖此路径（触发需要压测量级）——压测价值的最直接证明。
2. **访问日志 5xx 记 200（P2）**：异常穿透 filter 时状态捕获 wrapper 仍是默认 200。
   修复：捕获异常时先记 500 再抛出。两个修复各带回归测试，83/83 全绿。

## write-behind 落库吞吐实测（发现的系统天花板）

qcache 场景 136,280 次命中 → MySQL `task_record` 实际落库 33,370 行：
单线程调度 flush 的 MySQL upsert 持续吞吐约 **700 行/秒**，超出部分因 dirty 集合
10,000 上限被丢弃（有界积压 + 载荷卸除，属设计内降级）。生产意义：极端热点问题
的落库是最终一致而非完整；改进方向（未做）：独立消费线程 + 多值批量 upsert，
或让 Kafka 消费滞后本身作为缓冲（替代内存 dirty 集）。

## 复现

```bash
docker compose -f deployment/docker-compose.yml up -d mysql redis kafka
docker run -d --name m4-bench-gw --network deployment_default -p 8090:8080 \
  -e REDIS_HOST=redis -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e MYSQL_HOST=mysql -e MYSQL_PORT=3306 -e MYSQL_USER=root -e MYSQL_PASSWORD=nl2sql_root \
  -e ENGINE_BASE_URL=http://app:8000 -e GLOBAL_QPS=100000 deployment-gateway-java:latest
bash bench/run_all.sh        # 结果落 bench/results/<时间戳>/
```
