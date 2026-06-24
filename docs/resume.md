# NL2SQL Agent — 简历项目描述

## 项目信息

**NL2SQL Agent：基于 LangGraph 的 RAG 增强 SQL 生成与自修复系统**　2026.3–2026.6

---

## 项目简介

针对业务取数依赖硬编码 SQL、响应慢的痛点，面向多 schema 业务库，从零构建 LangGraph 多节点端到端 NL→SQL 系统。通过 RAG 分层检索注入 Schema 结构与业务语义，三层质量防线（Guard 硬校验 + Voter 执行投票 + SemanticCheck 语义兜底），执行错误精准分类回灌自修复，降低自然语言取数门槛并提升可执行 SQL 稳定性。引入 Kafka+Redis+SSE 异步流式架构及 Human-Feedback 多轮修正闭环。BIRD benchmark 最优配置达 52.1% EX，较裸 LLM 基线提升 14 个百分点，200 次连续调用 0 crash。

---

## 核心技术栈

`Python` `LangGraph` `LangChain` `FastAPI` `Kafka` `Redis` `ChromaDB` `Docker Compose` `Go` `OpenTelemetry`

---

## 项目亮点

- **图式工程流水线**：LangGraph 状态机将检索、拆解、生成、校验、投票、语义判定、自修复全流程建模为可视化有向图，节点可独立调试，决策路径可追溯。Voter 按需激活（正常路径单候选零成本，仅修正路径多候选投票），平票时 LLM 投票替代简单规则兜底。

- **多层质量防线**：Guard 零 LLM 成本拦截危险 SQL 和幻觉列名（9 规则统一入口，正则 + AST 双重校验），Voter 多候选执行投票淘汰错误结果，SemanticCheck 二元语义判定兜底，语法→执行→语义逐层收敛。执行错误经 10 种标准错误码精准分类后回灌 Refiner 自修复，修复率 24-27%。

- **RAG 分层注入**：Schema 结构检索补齐 LLM 缺失的数据库元数据，列级剪枝按问题相关性筛选列以压缩 prompt 长度；Domain 业务知识检索注入字段口径、业务同义映射和常见查询模式，解决仅靠 DDL 无法理解的隐性业务逻辑。BIRD 表召回率 98.4%。

- **平台化与工程交付**：Streamlit 构建 ChatGPT 风格对话 UI，基于 Kafka + Redis 异步任务架构支撑 SSE 流式推送与 Human-Feedback 多轮修正闭环。Go 实现 MCP Server 与 API 网关，集成 OpenTelemetry 全链路追踪，Docker Compose 一键部署全服务。

---

## 核心指标

| 指标 | 数值 |
|------|------|
| BIRD EX（最优） | **52.1%**（+14pp vs 裸 LLM） |
| RAG Table Recall | **98.4%** |
| Self-Correction 修复率 | **24-27%** |
| 稳定性 | **200 次连续调用 0 crash** |
| 测试覆盖 | **188 tests** |
| 支持数据库 | SQLite / PostgreSQL / MySQL（11 DB） |
| 支持 LLM | DeepSeek / OpenAI / Claude（8 预置） |
