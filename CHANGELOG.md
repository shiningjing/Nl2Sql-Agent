# Changelog

## v0.8.0 — M4b: 并行任务执行基准 (2026-10-26)

- **Kafka 分区横向扩展实证**：request topic 1→8 分区 + 1→4 Worker（消费组自动均分，Python/Java 零代码改动）
- **4 Worker × 50 真实任务**（DeepSeek 实调）：**成功率 100%、总墙钟 5 分 15 秒（≈9.5 任务/分钟）、加速比近线性 4×**；Worker 负载按 task_id 哈希均衡（node 事件 56/84/125/129）；端到端 P50 107s（长尾为排队等待，设计内形态）
- 冒烟先行：2 Worker 验证并行机制，揭穿哈希偏斜假串行（node 事件计数法）；Worker 内存实测 707MB/个（修正 1.5-2GB 预估，4 Worker ≈ 2.8GB 可与日常负载共存）
- 修复 apache/kafka 镜像随机 cluster id 导致的存储重格式化（compose 固定 KAFKA_CLUSTER_ID，分区数可跨容器重建持久化）
- 报告：`docs/benchmarks/2026-10-26-m4b-parallel-tasks.md`；与 M4a 网关压测构成完整高并发证据链

## v0.7.0 — M4a: 网关高并发压测 (2026-10-15)

- **k6 四场景压测**（不触真实 LLM，纯网关热路径，50/100 并发，累计 56.8 万请求，失败率 0%）：status 读路径 **2372 RPS / P99 52ms**、qcache 命中 **1603 RPS / P99 67ms**、submit 写路径 **714 RPS / P99 180ms**（主机空闲时 1890 RPS / avg 22ms）、拒绝风暴 **3391 RPS / P99 75ms**（100 并发滥用下优雅 429/401）
- **压测抓出 P0 缺陷并修复**：Recorder dirty 集合溢出时 `CHM iterator.remove()` 未先 `next()` → qcache 风暴下全量 500；flush 改循环取批（500/批、单次预算 2s）+ 修复访问日志异常穿透记 200 → 各带回归测试，83/83 全绿
- **write-behind 天花板实测**：MySQL upsert 持续 ~700 行/秒，超出走有界积压丢弃（设计内降级）
- `bench/` 压测工具集（种子用户 + 4 场景 + 编排）与报告 `docs/benchmarks/`
- 删除已退役的 Go 网关目录（M1 起由 Spring 网关替代）

## v0.6.2 — M3: 高并发加固 (2026-08-30)

- **策略层**：`X-User-Id` 身份闸门（未知 401/禁用 403/角色 403），策略 cache-aside（`policy:{tag}` 正缓存 600s±10% 抖动 + 未知名负缓存 60s 防穿透），admin 更新即删缓存下请求生效
- **submit 五级校验链**：数据权限 403（db 白名单，`*` 通配）→ 全局/用户限流 429 → SETNX 原子幂等 → 日配额 429 → qcache → 提交；任务归属（`taskowner:{id}`）非本人 403、admin 直通
- **限流/配额**：Lua 滑动窗口（ZSET 清窗→计数→占位单脚本原子，实测 50 并发限 10 恰好放行 10）+ 日配额 INCR；fail-open
- **qcache 结果缓存**：`sha256(question|db_id)`，命中短路直接 SUCCESS（实测 69s 任务 → 123ms，引擎/Kafka 零请求）；熔断/超时降级回退陈旧缓存（200 + `X-Cache: stale`）
- **write-behind 落库**：Kafka status/result 事件只作变脏信号，2s/100 条批量以 Redis 状态为准 upsert MySQL `task_record`（含 cache_hit/total_latency/node_timings/token_usage），SUCCESS 自动回写 qcache
- **MySQL**：Flyway V1（user_policy + task_record 宽表 + 种子用户），MyBatis-Plus upsert
- **Admin 端点**：`PUT /api/v1/admin/users/{tag}`（策略热更）+ `GET /api/v1/admin/stats`（按状态聚合）
- **指标四组**：`gateway_ratelimit_rejected` / `gateway_quota_rejected` / `gateway_cache` / `gateway_recorder_flush`
- UI 全部请求携带用户头；compose 网关接入 MySQL
- 测试 81 个全绿（Testcontainers：Redis+Kafka+MySQL）；期间修复测试基座 `@DynamicPropertySource` 基类同名属性覆盖子类的根因 bug

## v0.6.1 — M2: 任务链路 Java 化 (2026-08-20)

- 任务端点（submit/status/cancel/health/feedback/scan-stale/stream）由 Spring 原生实现，异步热路径不再经过 FastAPI
- 协议复刻三处 Python 契约：task_store.py Redis 状态机（key/TTL/16 字段 JSON）、broker.py Kafka 信封、task.py SSE 事件流——**Python Worker 零改动**
- 真实 Worker E2E 验证：Java 提交 → Worker 消费执行 → 状态/SSE 回传完整闭环（含幂等、心跳健康 worker_alive）
- 契约测试：sha256 幂等向量与 Python 对拍、golden JSON、Kafka 信封；Testcontainers（真 Redis/Kafka）集成测试
- compose：kafka 双 listener（容器间 29092 / 宿主机 9092）+ gateway 接入 Redis/Kafka
- 测试 44 个全绿；gateway-java 新增 8 主类

## v0.6.0 — M1: Java 网关上线 (2026-08-19)

- Spring Boot 3.3 + JDK 21（虚拟线程）替换 Go 网关接管 :8080，Go 网关退役（目录保留至 M4 后删除）
- `/api/v1/**` 透明代理（方法/路径/查询串/头/body/状态全透传）+ `X-Trace-Id` 贯穿
- `/stream` 端点 SSE 流式透传（边读边 flush，长连接不走熔断）
- Resilience4j：非流式路径超时（TimeLimiter）+ 熔断（CircuitBreaker），引擎故障快速 503（实测 21ms）
- actuator `/health` 聚合引擎状态 + Prometheus `/metrics`
- docker-compose 集成 `gateway-java` 服务（多阶段构建，宿主机无需 JDK）；16 个测试全绿
- 设计文档: `docs/superpowers/specs/2026-08-19-java-task-gateway-design.md`

## v0.4.x — W4: 部署 + Go 工具 + UI (2026-06-22 → 2026-06-24)

- **v0.4.6** — 移除 Thinking process expander，直接展示 SQL/Results/Trace
- **v0.4.5** — Thinking process 增强：展开显示 Pipeline 时间线 + 生成的 SQL
- **v0.4.4** — 修复侧边栏按钮字体过大 + 历史记录边框容器 + 浏览器标签页标题
- **v0.4.3** — Human-Feedback 多轮对话 + Streamlit UI ChatGPT 风格重构
- **v0.4.2** — Go API 网关 (go-chi): 反向代理 + 限流 + 聚合健康检查 + 结构化日志
- **v0.4.1** — Go MCP Server (vitess/sqlparser + database/sql): 15 测试通过
- **v0.4.0** — Docker Compose 一键启动 (6 services: app + redis + pg + mysql + kafka)

## v0.3.x — W3: 异步任务 + SSE + Human-Feedback (2026-06-18 → 2026-06-21)

- **v0.3.7** — W3 收尾: 集成测试 + BIRD 100 题 benchmark (EX 39.0%, 0 crash, $0.97)
- **v0.3.6** — Human-Feedback multi-turn: 10 轮修正, conversation history 持久化 Redis
- **v0.3.5** — Feedback API endpoint + feedback graph (Refiner→Generator→Guard→Voter→SemCheck)
- **v0.3.4** — UI 多轮对话: ChatGPT 风格气泡 + expander 折叠历史轮次
- **v0.3.3** — 重试/超时/取消完善: 协作式取消 + 超时递增 + DLQ
- **v0.3.2** — SSE token streaming via Redis Pub/Sub + Plotly waterfall 可视化
- **v0.3.1** — Redis 心跳 + 渐进式超时 + 僵尸任务扫描 + 差异化 TTL
- **v0.3.0** — Kafka 异步任务基础设施: Worker 进程 + 4-task endpoints + Redis 状态机

## v0.2.x — W1-W2: 重构 + AgentOps + MCP + 安全层 (2026-05-10 → 2026-06-18)

- **v0.2.7** — Voter 按需激活: 正常路径单候选 temp=0, 仅 Self-Correction 时多候选 + LLM tiebreak；CLAUDE.md W2 完成标记
- **v0.2.6** — 统一错误分类: ErrorType 10 种标准码 + 分类器映射 + executor/MCP 返回 error_type
- **v0.2.5** — SQL 安全层重构: 收敛到 guard/safety_rules.py (check_safety + check_hallucinations)
- **v0.2.4** — MCP 工具 execute_readonly_sql: 沙箱 SQL 执行, 正则+AST 双重校验, 自动 LIMIT, 超时, max_rows 硬上限
- **v0.2.3** — MCP 工具 validate_sql: L1 正则 + L3 AST SQL 校验 (fastmcp stdio)
- **v0.2.2** — AgentState 可观测性: TraceLogger 增强, OTel 桥接, reporter+CSV 评测报告
- **v0.2.0** — NL2SQL Agent: LangGraph 编排 + RAG 检索 + Self-Correction 循环 (BIRD EX 38.8%)

## v0.1.x — 早期原型 (2026-05 之前)

- 基础 LangGraph 管线 + sqlglot AST 校验 + SQLite 执行
- BIRD Mini-Dev 数据集集成 + ChromaDB 向量化
- Multi-DB 支持 (SQLite/MySQL/PostgreSQL) + 多模型 LLM 切换
- 项目结构扁平化重构 (nl2sql/ + src/ → 10 top-level modules)
