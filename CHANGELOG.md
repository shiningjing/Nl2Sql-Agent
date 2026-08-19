# Changelog

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
