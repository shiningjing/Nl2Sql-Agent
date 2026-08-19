# Java 任务网关设计（Spring Boot 高并发入口层）

日期：2026-08-19
状态：已与用户确认设计方向，待实现

## 1. 背景与目标

nl2sql-agent 目前是 Python 单体：Streamlit UI → Go 网关(:8080) → FastAPI(:8000)，异步任务走 FastAPI → Kafka → Worker，Redis 做任务状态机与 token 流转发。全仓库无 Java 代码。

**目标**：引入一个 Spring Boot 服务替换 Go 网关，成为统一入口 + 任务高并发层，形成「Java 业务层 + Python 推理引擎」的分层架构。定位为**本地部署可用 + 求职作品集展示**双重目标，重点是任务链路的高并发、低延迟可演示、可压测、可讲故事。

**非目标（明确不做）**：

- 不做账户体系（无注册/登录/密码/JWT）——本地部署没有真实多用户
- 不上 Spring Cloud 微服务——两个服务（Java + Python 引擎）用不上注册中心/配置中心，假微服务在面试中是负资产
- 不做分库分表——数据量不支持
- 不重写 Python 侧任何核心逻辑——LangGraph 引擎、Worker、Redis/Kafka 协议全部保持原样

## 2. 核心思路

Java 服务接管 :8080 端口（UI 无需改配置），成为「任务网关层」：

- **异步任务热路径完全不经过 Python**：submit 直接写 Redis 初始状态、直接产 Kafka 消息；status 直读 Redis；SSE 订阅 Redis PubSub 转发。热路径只碰 Redis 和 Kafka，不碰 MySQL、不碰 FastAPI。
- **MySQL 只做异步冷存储**：Kafka consumer 消费任务事件，攒批写 `task_record`（write-behind 模式），用于压测分析与历史查询。
- **用户维度用纯标签实现**：请求带 `X-User-Id` 字符串标签，无账户；权限/限流/配额全部围绕标签做，评测脚本造模拟用户来压测。

```
Streamlit UI ──HTTP/SSE──▶ Spring Boot(:8080) ──REST──▶ FastAPI(:8000 引擎，同步查询路径)
                             │
                             │ /task/submit: 身份→数据权限→限流→幂等→配额 → Redis 初始状态 → Kafka → 202
                             │ /task/{id}/status: 直读 task:{id} (Redis)
                             │ /task/{id}/stream: 订阅 task:{id}:tokens (PubSub) → SSE 转发
                             │ /task/{id}/cancel|feedback: 权限校验后写 Redis / 产 Kafka
                             │ Kafka consumer(nl2sql.task.status/result) → 攒批 → MySQL task_record
                             └─ MySQL nl2sql_ops 库：user_policy + task_record（Flyway 管理）
```

## 3. 技术栈

| 选型 | 决定 | 理由 |
|---|---|---|
| 框架 | Spring Boot 3.3.x + JDK 21（LTS） | 虚拟线程处理 SSE 转发等 IO 密集场景，比 WebFlux 简单且面试可讲 |
| 构建 | Maven 多模块单模块均可（先单模块） | 国内主流，MyBatis-Plus 文档配套 |
| Web | Spring MVC + 虚拟线程 (`spring.threads.virtual.enabled=true`) | SSE 用 SseEmitter |
| ORM | MyBatis-Plus + HikariCP | 国内面试对口；池参数调优是素材 |
| 迁移 | Flyway | 建表 + 种子数据（local_user / bench_admin） |
| MQ | Spring Kafka | 产 task.request/feedback，消费 task.status/result |
| 缓存 | Spring Data Redis (Lettuce) | 复用现有 Redis 实例 |
| 熔断 | Resilience4j | 同步查询引擎路径的 timeout + circuit breaker |
| 指标 | Micrometer + Prometheus (`/metrics`) | 对齐现有网关习惯 |
| 安全 | 无 Spring Security；自定义注解 + HandlerInterceptor | 无登录体系时 Security 无用武之地；「为什么不用」是面试考点 |

## 4. 跨语言协议契约（Java 必须复刻的部分）

以 Python 侧现有实现为唯一事实来源，实现时逐字段对齐，并配 JSON golden file 契约测试。

### 4.1 Redis（来源：`infrastructure/task_store.py`）

| Key / Channel | 内容 | TTL |
|---|---|---|
| `task:{task_id}` | JSON 字符串（非 hash），字段：`task_id, status, question(截200), db_id, database_url, progress, node, sql, exec_result, token_usage, node_timings, retry_count, error, created_at, updated_at` | PENDING/RUNNING 2h；SUCCESS/FAILED/CANCELLED 24h；TIMEOUT 1h |
| `task:{task_id}:heartbeat` | ISO 时间戳（Worker 写，Java 不写） | 30s |
| `task:{task_id}:cancel` | "1" | 1h |
| `idempotent:{key_hash}` | task_id | 5min |
| PubSub `task:{task_id}:tokens` | **明文 token 文本**（非 JSON 信封） | — |

状态机：PENDING → RUNNING → SUCCESS/FAILED/TIMEOUT/CANCELLED；FAILED/TIMEOUT → PENDING（重试）。Java 的 submit 需按相同字段名/格式初始化 `task:{task_id}`，Worker 的 `task_get/task_update` 才能无缝工作。

### 4.2 Kafka（来源：`infrastructure/broker.py`）

- Topic：`nl2sql.task.request` / `nl2sql.task.status` / `nl2sql.task.result` / `nl2sql.task.dlq` / `nl2sql.task.feedback`（现有 1 分区，压测若到瓶颈再扩，注意 key=task_id 保序）
- 消息信封：`{"task_id": ..., "event": ..., "payload": {...}}`，UTF-8 JSON；producer key = task_id
- event 取值：`submitted | running | node_done | success | failed | timeout | cancelled`

### 4.3 SSE 事件（来源：`api/routes/task.py::task_stream`）

事件名：`token` / `status` / `node_done` / `complete` / `error` / `timeout`；流式超时 5 分钟；token 事件来自 PubSub 通道逐条透传，状态类事件来自轮询 `task:{task_id}` 状态变化。

## 5. API 设计（Java 提供，前缀 /api/v1，端口 8080）

| 端点 | 方法 | 说明 | 权限 |
|---|---|---|---|
| `/task/submit` | POST | 幂等键查重、策略校验、初始化 Redis 状态、产 Kafka、202 + task_id | 数据权限校验 db_id |
| `/task/{id}/status` | GET | 直读 Redis 状态 | owner 或 admin |
| `/task/{id}/cancel` | POST | 写 cancel 标记 | owner 或 admin |
| `/task/{id}/stream` | GET | SSE 转发（订阅 PubSub + 轮询状态） | owner 或 admin |
| `/task/{id}/feedback` | POST | 产 feedback topic | owner 或 admin |
| `/task/scan-stale` | POST | 复刻 scan_stale_tasks（SCAN + 心跳判死） | admin |
| `/query`、`/query/full`、`/query/full/stream` | POST/GET | 转发 FastAPI 同步路径，带超时/熔断，流式透传 | 仅需有效用户标识 |
| `/health` | GET | 聚合健康（引擎 + Redis + Kafka + MySQL） | 无 |
| `/metrics` | GET | Prometheus 指标 | 无 |
| `/admin/users/{tag}` | PUT | 更新策略并删 Redis 缓存（M3） | admin |
| `/admin/stats` | GET | 全局任务统计（M3） | admin |

## 6. 用户策略层

### 6.1 模型（一张 `user_policy` 表）

```sql
CREATE TABLE user_policy (
  user_tag       VARCHAR(64) PRIMARY KEY,
  role           VARCHAR(16) NOT NULL DEFAULT 'user',   -- user | admin
  allowed_db_ids JSON NOT NULL,                          -- ["*"] = 全库
  qps_limit      INT NOT NULL DEFAULT 10,
  daily_quota    BIGINT NOT NULL DEFAULT 1000,
  status         TINYINT NOT NULL DEFAULT 1,             -- 0 = 封禁
  created_at DATETIME(3), updated_at DATETIME(3)
);
```

`allowed_db_ids` 用 JSON 列不做关联表：只按 user_tag 正查，无反查需求。Flyway 种子：`local_user`（user，`["*"]`，高限额，给 Streamlit UI 用）、`bench_admin`（admin，给运维演示）。

### 6.2 策略加载与缓存

- Redis `policy:{user_tag}` → 策略 JSON，TTL 10min ± 随机抖动（防雪崩），cache-aside（miss 读 MySQL 回填）
- admin 更新策略后主动删 key（缓存一致性演示点）
- **未知 tag 默认拒绝：401**。白名单语义，压测用户由脚本预置，不存在漏建问题

### 6.3 校验点分布

```
HandlerInterceptor（所有请求）：
  读 X-User-Id → 查策略(Redis→MySQL) → 无记录 401 / status=0 403
  @RequireRole(ADMIN) 端点且角色不足 → 403

Service 层（需业务上下文）：
  submit：db_id ∈ allowed_db_ids（"*" 放行）→ 否则 403
  cancel/feedback/status/stream：task.user_tag == 本人或 admin → 否则 403（越权/IDOR 防护）

Redis Lua（submit 流量闸门）：
  限流：ZSET 滑动窗口 rl:{tag}，阈值 = 该用户 qps_limit → 429
  配额：quota:{tag}:{yyyyMMdd} INCR 对比 daily_quota → 429
```

Java 不需要知道全部合法 db_id——白名单语义，库是否存在由引擎层报错，避免同步 `databases.json`。

### 6.4 submit 校验顺序（含理由，面试细节）

**身份 → 数据权限 → 限流 → 幂等 → 配额 → 提交**

原则：永久性错误（403 客户端自己fix不了重试也没用）先于暂时性错误（429）；限流在幂等之前，让限流保护包括幂等 SETNX 在内的所有 Redis 操作。幂等键生成规则与 FastAPI 现有实现保持一致（实现时以 `api/routes/task.py` 为准）。

## 7. 高并发/低延迟设计

1. **提交热路径**：入口只碰 Redis + Kafka（策略缓存命中时 1 次 Redis 读 + 1 次 Lua + 1 次 SETNX + 1 次状态写 + 1 次 Kafka produce），无 Python、无 MySQL 网络调用。
2. **结果缓存**：`qcache:{sha256(question + '|' + db_id)}` → 结果 JSON，TTL 30min ± 抖动。submit 前检查，命中则直接初始化一个 SUCCESS 状态的任务（带 `cache_hit` 标记），毫秒级出结果、不产 Kafka。空值也缓存（防穿透）。
3. **幂等**：复用 `idempotent:{key_hash}` 约定，重复提交返回原 task_id。
4. **限流**：Redis + Lua 滑动窗口（ZADD/ZREMRANGEBYSCORE/ZCARD 原子脚本），用户级阈值来自策略表 + 全局兜底闸门（阈值均为配置项）；Lua 执行异常时 fail-open 放行（可用性优先，文档写明取舍）。
5. **异步批量落库（write-behind）**：submit 动作与 Kafka status/result consumer 都把事件投进内存队列，单一批量写线程每 2s 或满 100 条 flush 到 `task_record`（upsert by task_id）。MySQL 故障不阻塞请求路径，队列写满丢弃最旧事件并计数告警（压测分析容忍最终一致）。
6. **熔断降级**：Resilience4j 作用于转发 FastAPI 的同步路径——超时 + 失败率熔断，熔断打开时若有 `qcache` 命中则返回缓存结果，否则 503。异步路径天然削峰（Kafka 缓冲）。
7. **连接与线程**：HikariCP 池参数（maximumPoolSize 按压测调）、Kafka producer/consumer 并发、虚拟线程承担 SSE 长连接扇出、优雅停机（先拒绝新请求、drain 内存队列、关 producer）。
8. **指标**：各校验阶段耗时、限流/配额拒绝数、缓存命中率、Kafka 生产延迟、落库批次大小——全部出 Prometheus 指标。

## 8. 数据库设计（MySQL `nl2sql_ops` 库，与 BIRD 目标库隔离）

```sql
CREATE TABLE task_record (
  task_id         VARCHAR(64) PRIMARY KEY,
  user_tag        VARCHAR(64) NOT NULL,
  db_id           VARCHAR(128),
  question        TEXT,              -- 全文（Redis 侧截 200）
  status          VARCHAR(16),
  sql_text        MEDIUMTEXT,
  row_count       INT,
  retry_count     TINYINT DEFAULT 0,
  error           TEXT,
  cache_hit       TINYINT DEFAULT 0,
  submitted_at    DATETIME(3),
  started_at      DATETIME(3),
  finished_at     DATETIME(3),
  total_latency_ms INT,
  node_timings    JSON,              -- 各阶段耗时
  token_usage     JSON,
  created_at DATETIME(3), updated_at DATETIME(3),
  INDEX idx_user_time (user_tag, submitted_at),
  INDEX idx_status_time (status, submitted_at),
  INDEX idx_time (submitted_at)
);
```

单张宽表（不拆 query_log）：压测分析一条 SQL 即可出各阶段 P99；join 无收益。

## 9. Redis Key 总表（Java 侧）

| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `policy:{user_tag}` | string(JSON) | 10min±抖动 | 策略缓存 |
| `rl:{user_tag}` | ZSET | 窗口期 | 滑动窗口限流 |
| `quota:{user_tag}:{yyyyMMdd}` | counter | 48h | 日配额 |
| `qcache:{sha256}` | string(JSON) | 30min±抖动 | 结果缓存（含空值） |
| `task:{id}`、`idempotent:{key}` 等 | — | — | 复用 Python 侧（见 4.1） |

## 10. 评测/压测改造（模拟用户）

- 评测脚本（evaluation/）增加预置步骤 `seed_users.py`：生成 N 个（默认 20）`bench_u01~20` 写入 `user_policy`，**故意制造差异**——若干仅可查 1-2 库的受限用户、若干低 QPS 用户、1 个 status=0 被封用户；
- 压测请求随机携带 `X-User-Id`；
- 预期可验证：受限用户越库 → 403 有计数；低 QPS 用户 → 429；被封用户全程 403；幂等重复提交 → 同 task_id；缓存命中 → cache_hit=1 且延迟毫秒级。全部落在 `task_record`，压测报告一条 SQL 出结论。

## 11. 部署

- docker-compose 增加 `gateway-java` 服务：Maven 多阶段构建（宿主机无需 JDK）；占用 8080，与现 Go 网关互斥（Go 网关退役，目录保留至 M4 后删除）；
- Flyway 在现有 mysql:8.4 容器上自动建 `nl2sql_ops` 库与两张表 + 种子；
- 复用现有 redis、kafka 容器，不加新基础设施；
- 本地裸跑需 JDK 21 + Maven（可选，Docker 构建为主路径）。

## 12. 错误处理与降级

| 故障 | 行为 |
|---|---|
| 引擎 FastAPI 挂（同步路径） | 熔断打开 → qcache 命中返回缓存，否则 503 |
| Kafka 不可用 | submit 503（不静默降级——网关层要响亮失败，与 Python 侧 no-op 哲学的差异写入文档） |
| Redis 不可用 | submit/status 503（热路径强依赖，明确不降级） |
| MySQL 不可用 | 请求路径无感知（落库队列缓冲 + 告警） |
| 限流 Lua 异常 | fail-open 放行 + 计数 |
| SSE | 5 分钟流超时，与 Python 行为一致 |
| Worker 死亡 | 心跳判死机制不变（scan-stale 移到 Java admin 端点） |

**安全说明**：`X-User-Id` 自报身份仅适用于本地/可信内网；真实部署必须在入口换成认证身份（API Key/JWT）。写入文档，面试展示为安全意识。

## 13. 测试策略

- **单元测试**（JUnit 5 + Mockito）：策略解析、数据权限白名单、任务归属、限流窗口计算、幂等、缓存 key；
- **契约测试**：Kafka 信封 / Redis 状态 JSON 的 golden file，防止 Python 侧 schema 漂移；
- **集成测试**（Testcontainers：MySQL/Redis/Kafka）：submit→consume→落库全链路、Lua 脚本真实执行；
- **E2E**：compose 起全栈，seed 用户，提交 N 任务断言状态与 task_record；
- **压测**：evaluation 模拟用户模式 + wrk 打缓存命中路径。

## 14. 里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| M1 入口打通 | Spring Boot 骨架 + 同步查询转发（含流式）+ /health + compose 集成，Go 网关退役 | UI 经 8080 全功能可用（同步路径） |
| M2 任务链路 | submit(初始化 Redis + 产 Kafka) / status / stream(SSE) / cancel / feedback，协议契约测试 | UI 异步任务全流程走 Java 入口；Worker 无感知 |
| M3 高并发加固 | user_policy + 拦截器权限层 + 限流/配额/幂等 + 结果缓存 + Kafka consumer 异步落库 + 熔断 + 指标 + admin 端点 | 403/429/幂等/缓存全部可演示；task_record 有数据 |
| M4 压测收官 | seed_users + evaluation 模拟用户模式 + wrk/JMeter 压测 + 调参 + 简历素材文档 | 500 题 benchmark 全量走 Java 入口；50 并发无丢任务无重复；缓存命中 P99 < 50ms（本地） |

## 15. 风险与权衡

- **跨语言协议耦合**：Java 复刻 Python 的 Redis/Kafka/SSE 三套协议，Python 侧改动会破坏 Java → 用契约测试 + 本文档钉住字段清单缓解；
- **Kafka 单分区**：现有 1 partition 是吞吐上限，压测到瓶颈需扩分区（key=task_id 保序，扩分区对 worker 透明）；
- **内存落库队列丢数据**：MySQL 长时间故障时丢最旧事件——压测分析场景可接受最终一致，指标可见；
- **双入口并存期**：FastAPI 的 task 端点保留（Python 内部/调试用），Java 成为唯一对外入口，文档标注。

## 16. 成功标准

1. 500 题 BIRD benchmark 全量经 Java 入口跑通，结果与直接打 FastAPI 一致；
2. 50 并发提交：无丢任务、无重复任务（幂等可验证）、task_record 记录完整；
3. 限流/配额/越权/封禁/缓存命中全部有压测数据可证明（403/429 计数、cache_hit 率、延迟分布）；
4. 缓存命中路径本地 P99 < 50ms；
5. 核心模块（策略/权限/限流/幂等/缓存/落库）有单测与集成测试。
