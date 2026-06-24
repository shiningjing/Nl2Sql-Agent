# NL2SQL Agent — 简历项目描述

## 项目信息

**NL2SQL Agent：基于 LangGraph 的 RAG 增强 SQL 生成与自修复系统**　2026.3–2026.6

---

## 项目简介

从零构建 LangGraph 9 节点端到端 NL→SQL 系统，解决业务取数依赖硬编码 SQL、响应慢的痛点。RAG 分层检索注入 Schema 结构与业务语义，三层质量防线（Guard 硬校验 + Voter 执行投票 + SemanticCheck 语义兜底），执行错误精准分类回灌自修复。Kafka+Redis+SSE 异步流式架构支撑 Human-Feedback 多轮修正闭环。Go 实现 MCP Server 与 API 网关。DeepSeek V4 Pro BIRD Mini-Dev EX 39.0%（RAG），RAG 表召回 98.4%，200 次调用 0 crash。

---

## 核心技术栈

`Python` `LangGraph` `LangChain` `FastAPI` `Kafka` `Redis` `ChromaDB` `Docker Compose` `Go` `OpenTelemetry`

---

## 项目亮点

- **图式工程流水线**：LangGraph 状态机将检索、拆解、生成、校验、投票、语义判定、自修复全流程建模为可追溯有向图，节点独立调试。Voter 按需激活（正常路径单候选零成本，仅修正路径多候选投票），平票时 LLM 投票替代简单规则兜底。

- **多层质量防线**：Guard 零 LLM 成本拦截危险 SQL 和幻觉列名（9 规则统一入口，正则 + AST 双重校验），Voter 多候选执行投票淘汰错误结果，SemanticCheck 二元语义判定兜底。执行错误经 10 种标准错误码精准分类后回灌 Refiner 自修复，修复率 24-27%。

- **RAG 分层注入**：Schema 结构检索补齐 LLM 缺失的数据库元数据，列级剪枝按问题相关性筛选列压缩 prompt 长度；Domain 业务知识检索注入字段口径、业务同义映射和常见查询模式。BIRD 表召回率 98.4%，RAG 较裸 LLM 提升 +11pp。

- **异步平台化**：Kafka 消息解耦 + Redis 任务状态机（PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT/CANCELLED）+ SSE 流式 token 推送 + Human-Feedback 多轮修正（上限 10 轮，对话历史持久化）。

- **Go 技术栈**：MCP Server（vitess/sqlparser AST 校验 + database/sql 连接池，15 测试覆盖）+ API 网关（go-chi 滑动窗口限流 + 反向代理 + 聚合健康检查）。

- **全栈可观测**：OpenTelemetry 全链路 tracing + TraceLogger (jsonl) + 节点级耗时 + Token 用量追踪。

- **工程交付**：Docker Compose 一键启动 6 服务，16 个 API 端点，188+ 测试，Streamlit ChatGPT 风格对话 UI。

---

## 核心指标

| 指标 | 数值 |
|------|------|
| BIRD EX (DeepSeek V4 Pro, RAG) | **39.0%** |
| BIRD EX (DeepSeek V4 Pro, +Evidence) | **43.0%** |
| BIRD EX (Claude Opus 4.7, +Evidence) | **47.0%** |
| RAG Table Recall | **98.4%** |
| Self-Correction 修复率 | **24-27%** |
| 稳定性 | **200 次连续调用 0 crash** |
| 测试覆盖 | **188 tests** |
| 支持数据库 | SQLite / PostgreSQL / MySQL（11 DB） |
| 支持 LLM | DeepSeek / OpenAI / Claude（8 预置） |
| 单次成本 | ~$0.005（¥0.035）/ 100 题 $0.97（¥7） |
