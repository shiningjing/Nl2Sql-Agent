# M2 任务链路 Java 化 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 任务端点（submit/status/cancel/stream/feedback/health/scan-stale）由 Java 原生实现，直接读写 Redis、直产 Kafka，异步任务路径不再经过 FastAPI，Python Worker 零改动。

**Architecture:** 复刻 Python 三处协议（`infrastructure/task_store.py` 的 Redis key/状态 JSON、`infrastructure/broker.py` 的 Kafka 信封、`api/routes/task.py` 的端点语义与 SSE 事件），Spring MVC + StringRedisTemplate + Spring Kafka + Lettuce PubSub（RedisMessageListenerContainer）实现；契约用 golden JSON 与 Testcontainers 真实 Redis/Kafka 集成测试钉住。

**Tech Stack:** Spring Kafka / Spring Data Redis (Lettuce) / Testcontainers (redis:7-alpine + KafkaContainer) / Mockito

**规格来源:** `docs/superpowers/specs/2026-08-19-java-task-gateway-design.md` §4 §5 §14-M2

---

## 协议契约（已逐行核对源码，实现不得偏离）

### Redis（task_store.py）
- `task:{task_id}` = JSON 字符串。taskCreate 初始字段（16 个，null 必须保留）：
  `task_id, status="PENDING", question(截200), db_id, database_url, progress=0, node=null, sql=null, exec_result=null, token_usage={}, node_timings={}, retry_count=0, error=null, created_at/updated_at=ISO8601UTC`
- TTL：PENDING/RUNNING 7200s；SUCCESS/FAILED/CANCELLED 86400s；TIMEOUT 3600s
- `task:{id}:cancel` = "1"（TTL 3600）；`task:{id}:heartbeat` = ISO 时间戳（Worker 写）
- `idempotent:{sha256(key:question[:80])[:32]}` = task_id（TTL 300s）
- PubSub 通道 `task:{task_id}:tokens`：**明文 token 字符串**（非 JSON）
- scan_stale：SCAN `task:*`（跳过 `:heartbeat`/`:cancel` 子键）找 RUNNING 且心跳缺失/超 60s 的，转 TIMEOUT

### Kafka（broker.py）
- Topic：`nl2sql.task.request` / `nl2sql.task.feedback`；消息 = `{"task_id","event","payload"}`，key=task_id
- submit：event="submitted"，payload 12 字段（question,db_id,database_url,rag_schema,rag_domain,multi_candidate,rag_k,rag_column_prune,rag_hybrid,rag_fk_expand,fewshot_enabled）
- feedback：event="feedback"，payload 10 字段（feedback,turn,question,db_id,database_url,sql,exec_result,conversation_turns,token_usage,node_timings）

### 端点语义（task.py）
| 端点 | 行为 |
|---|---|
| POST /task/submit | 幂等命中→202 返回已有 task_id；否则建状态+产 Kafka→202 `{task_id,status:"PENDING"}`；task_id=uuid hex 12 位 |
| GET /task/{id}/status | 直读 Redis；404 body `{"detail":"Task not found or expired"}`；200 返回 14 字段（**不含 database_url/conversation_turns**） |
| POST /task/{id}/cancel | 不存在→`{task_id,status:"not_found"}`；终态→返回该状态；否则写 cancel 标记→`status:"cancelled"` |
| GET /task/{id}/health | 返回 `{task_id,task_status,heartbeat,heartbeat_stale_s,healthy,worker_alive}` |
| POST /task/scan-stale | `{stale_count, stale_task_ids}` |
| GET /task/{id}/stream | SSE：token（PubSub，`{"text":...}`）/status（updated_at 变化时）/node_done（node 变化时）/complete（终态后 return）/error（任务不存在）/timeout（5 分钟）；状态轮询 500ms |
| POST /task/{id}/feedback | 404 同上；非 SUCCESS/FAILED→400；turns>10→400；产 Kafka+状态转 RUNNING→202 `{task_id,status:"accepted",turn}` |

注意：submit 请求含 `llm` 字段但 payload 不透传（Python 行为如此，Java 保持一致）。

---

## 文件结构

```
gateway-java/
  pom.xml                                          [改] +spring-kafka/data-redis/testcontainers
  src/main/resources/application.yml               [改] +redis/kafka 连接
  src/main/java/com/nl2sql/gateway/
    task/TaskState.java                            状态 JSON 模型（16 字段，null 保留）
    task/TaskStoreService.java                     Redis 读写：create/get/cancel/幂等/scanStale
    task/TaskIdempotency.java                      幂等键 sha256 计算（纯函数）
    task/KafkaTopics.java                          topic 常量 + NewTopic bean（幂等建 1 分区）
    task/KafkaPublisher.java                       信封序列化 + produce
    task/TaskController.java                       7 个端点
    task/TaskStreamService.java                    PubSub 订阅 + 500ms 轮询 → SSE 事件
  src/test/java/com/nl2sql/gateway/task/
    TaskStateContractTest.java                     golden JSON
    TaskIdempotencyTest.java                       sha256 向量（与 Python 对拍）
    TaskControllerTest.java                        Mockito 单测（端点语义）
    IntegrationTestBase.java                       Testcontainers: redis+kafka+MockWebServer(引擎)
    TaskSubmitIntegrationTest.java                 真实 Redis/Kafka 全链路 + "不代理"断言
    TaskStreamIntegrationTest.java                 真 PubSub→SSE 事件序列
deployment/docker-compose.yml                     [改] kafka 双 listener + gateway-java 环境变量
README.md / README_zh.md / CHANGELOG.md            [改] M2 收官
```

---

### Task 1: 依赖与配置

**Files:** Modify `gateway-java/pom.xml`, `gateway-java/src/main/resources/application.yml`；Create `task/KafkaTopics.java`

- [ ] **Step 1: pom.xml dependencies 增补**

```xml
        <!-- 在 resilience4j 依赖之后追加 -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>
        <!-- test 域增补 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: application.yml 增补**

```yaml
spring:
  data:
    redis:
      host: ${REDIS_HOST:127.0.0.1}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:127.0.0.1:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: "1"
```

- [ ] **Step 3: KafkaTopics.java**

```java
package com.nl2sql.gateway.task;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Topic 常量 + 幂等建Topic（与 broker.py 一致：1 分区、rf=1，已存在则跳过）。 */
@Configuration
public class KafkaTopics {

    public static final String TOPIC_REQUEST = "nl2sql.task.request";
    public static final String TOPIC_FEEDBACK = "nl2sql.task.feedback";

    @Bean
    public NewTopic taskRequestTopic() {
        return new NewTopic(TOPIC_REQUEST, 1, (short) 1);
    }

    @Bean
    public NewTopic taskFeedbackTopic() {
        return new NewTopic(TOPIC_FEEDBACK, 1, (short) 1);
    }
}
```

- [ ] **Step 4: 构建 + 存量测试全绿后提交**

```bash
export JAVA_HOME=~/scoop/apps/temurin21-jdk/current && cd gateway-java && mvn test
git add -A gateway-java && git commit -m "M2: 依赖与配置 — spring-kafka + data-redis + testcontainers + topic 常量"
```

---

### Task 2: TaskState 契约（golden JSON）

**Files:** Create `task/TaskState.java`；Test `task/TaskStateContractTest.java`

- [ ] **Step 1: 失败测试** — 序列化结果与 Python `task_create` 的 JSON 逐字段一致（null 保留、空对象保留）

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskStateContractTest {

    private final ObjectMapper om = new ObjectMapper();

    @Test
    void serializesExactlyLikePythonTaskCreate() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "abc123def456";
        s.status = "PENDING";
        s.question = "avg enrollment?";
        s.db_id = "california_schools";
        s.database_url = "";
        s.progress = 0;
        s.created_at = "2026-08-20T10:00:00+00:00";
        s.updated_at = "2026-08-20T10:00:00+00:00";

        Map<String, Object> map = om.readValue(om.writeValueAsString(s), Map.class);
        assertThat(map).containsOnlyKeys(
                "task_id", "status", "question", "db_id", "database_url",
                "progress", "node", "sql", "exec_result", "token_usage",
                "node_timings", "retry_count", "error", "created_at", "updated_at",
                "conversation_turns");
        assertThat(map.get("node")).isNull();
        assertThat(map.get("sql")).isNull();
        assertThat(map.get("exec_result")).isNull();
        assertThat(map.get("error")).isNull();
        assertThat(map.get("token_usage")).isEqualTo(Map.of());
        assertThat(map.get("node_timings")).isEqualTo(Map.of());
        assertThat(map.get("retry_count")).isEqualTo(0);
        assertThat(map.get("conversation_turns")).isEqualTo(Map.of()); // 反序列化兼容 Worker 写入的数组
    }

    @Test
    void toleratesWorkerWrittenFields() throws Exception {
        String workerJson = """
            {"task_id":"t1","status":"RUNNING","question":"q","db_id":"d","database_url":"",
             "progress":40,"node":"generator","sql":null,"exec_result":null,
             "token_usage":{"total":100},"node_timings":{"router":1.2},"retry_count":0,
             "error":null,"created_at":"c","updated_at":"u",
             "conversation_turns":[{"feedback":"add group by"}]}
            """;
        TaskState s = om.readValue(workerJson, TaskState.class);
        assertThat(s.status).isEqualTo("RUNNING");
        assertThat(s.node).isEqualTo("generator");
        om.writeValueAsString(s); // 必须可往返
    }
}
```

- [ ] **Step 2: 确认失败（类不存在）→ Step 3: 实现**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Redis task:{id} 状态 JSON——字段名与 infrastructure/task_store.py 逐字一致，null 必须保留。 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public class TaskState {

    public String task_id;
    public String status;
    public String question;
    public String db_id;
    public String database_url;
    public int progress;
    public String node;
    public String sql;
    public Object exec_result;
    public Map<String, Object> token_usage = new LinkedHashMap<>();
    public Map<String, Object> node_timings = new LinkedHashMap<>();
    public int retry_count;
    public Object error;
    public String created_at;
    public String updated_at;

    /** Worker 在 feedback 后写入的会话轮次；Python 端缺省为 []，Java 建任务时不写此键，
     *  反序列化时兼容 List，序列化时空值回写为空集合（Python 端 get 默认 []，语义等价）。 */
    @JsonProperty("conversation_turns")
    public Object conversation_turns = new LinkedHashMap<String, Object>();

    public Map<String, Object> toStatusResponse() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("task_id", task_id);
        m.put("status", status);
        m.put("question", question);
        m.put("db_id", db_id);
        m.put("progress", progress);
        m.put("node", node);
        m.put("sql", sql);
        m.put("exec_result", exec_result);
        m.put("token_usage", token_usage);
        m.put("node_timings", node_timings);
        m.put("error", error);
        m.put("retry_count", retry_count);
        m.put("created_at", created_at);
        m.put("updated_at", updated_at);
        return m;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> turns() {
        if (conversation_turns instanceof List<?> l) {
            return (List<Map<String, Object>>) (Object) l;
        }
        return List.of();
    }
}
```

注意：`toStatusResponse()` 输出 14 字段、不含 database_url/conversation_turns（与 `TaskStatusResponse(**state)` 的 pydantic 过滤一致）。

- [ ] **Step 4: 测试通过后提交**

```bash
mvn test -Dtest=TaskStateContractTest
git add -A gateway-java && git commit -m "M2: TaskState 契约 — 16 字段 golden JSON，与 task_store.py 对齐"
```

---

### Task 3: 幂等键 + TaskStoreService（Redis 读写）

**Files:** Create `task/TaskIdempotency.java`, `task/TaskStoreService.java`；Test `task/TaskIdempotencyTest.java`, `task/TaskStoreServiceTest.java`（Mockito）

- [ ] **Step 1: 幂等键向量对拍** — 先用 Python 生成期望值：

```bash
python -c "import hashlib; print(hashlib.sha256('test-key:What is the avg enrollment'.encode()).hexdigest()[:32])"
# 记下输出，写入测试向量
```

- [ ] **Step 2: TaskIdempotencyTest**

```java
package com.nl2sql.gateway.task;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TaskIdempotencyTest {

    @Test
    void matchesPythonSha256Rule() {
        // 向量 = Step 1 的 Python 输出（question 短于 80 不截断）
        assertThat(TaskIdempotency.key("test-key", "What is the avg enrollment"))
                .isEqualTo("<PYTHON_OUTPUT>");
    }

    @Test
    void truncatesQuestionTo80() {
        String q80 = "x".repeat(80) + "TRUNCATED";
        assertThat(TaskIdempotency.key("k", q80)).isEqualTo(TaskIdempotency.key("k", "x".repeat(80)));
    }

    @Test
    void newTaskIdIs12HexChars() {
        assertThat(TaskIdempotency.newTaskId()).matches("[0-9a-f]{12}");
    }
}
```

- [ ] **Step 3: 实现**

```java
package com.nl2sql.gateway.task;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

public final class TaskIdempotency {

    private TaskIdempotency() {
    }

    /** sha256(f"{key}:{question[:80]}")[:32] — 与 api/routes/task.py 逐字一致。 */
    public static String key(String idempotencyKey, String question) {
        String input = idempotencyKey + ":" + question.substring(0, Math.min(80, question.length()));
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(0, 32);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** uuid4().hex[:12] 等价实现。 */
    public static String newTaskId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
```

- [ ] **Step 4: TaskStoreService（真实实现）**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Redis 任务状态机——复刻 infrastructure/task_store.py 的 key 布局与 TTL。 */
@Service
public class TaskStoreService {

    public static final Duration TTL_RUNNING = Duration.ofSeconds(7200);
    public static final Duration TTL_TERMINAL_GOOD = Duration.ofSeconds(86400);
    public static final Duration TTL_TERMINAL_BAD = Duration.ofSeconds(3600);
    public static final Duration TTL_CANCEL = Duration.ofSeconds(3600);
    public static final Duration TTL_IDEMPOTENT = Duration.ofSeconds(300);
    public static final long HEARTBEAT_STALE_S = 60;

    private final StringRedisTemplate redis;
    private final ObjectMapper om = new ObjectMapper();

    public TaskStoreService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String nowIso() {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.now().atZone(java.time.ZoneOffset.UTC));
    }

    /** 初始化 PENDING 状态（字段与 task_store.task_create 一致）。 */
    public TaskState create(String taskId, String question, String dbId, String databaseUrl) {
        TaskState s = new TaskState();
        s.task_id = taskId;
        s.status = "PENDING";
        s.question = question.substring(0, Math.min(200, question.length()));
        s.db_id = dbId;
        s.database_url = databaseUrl;
        String now = nowIso();
        s.created_at = now;
        s.updated_at = now;
        save(s);
        return s;
    }

    public TaskState get(String taskId) {
        String raw = redis.opsForValue().get("task:" + taskId);
        if (raw == null) {
            return null;
        }
        try {
            return om.readValue(raw, TaskState.class);
        } catch (Exception e) {
            return null;
        }
    }

    public void save(TaskState s) {
        Duration ttl = switch (s.status) {
            case "PENDING", "RUNNING" -> TTL_RUNNING;
            case "TIMEOUT" -> TTL_TERMINAL_BAD;
            default -> TTL_TERMINAL_GOOD;
        };
        try {
            redis.opsForValue().set("task:" + s.task_id, om.writeValueAsString(s), ttl);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void requestCancel(String taskId) {
        redis.opsForValue().set("task:" + taskId + ":cancel", "1", TTL_CANCEL);
    }

    public String idempotentGet(String key) {
        return redis.opsForValue().get("idempotent:" + key);
    }

    public void idempotentSet(String key, String taskId) {
        redis.opsForValue().set("idempotent:" + key, taskId, TTL_IDEMPOTENT);
    }

    /** 心跳判死：RUNNING 且心跳缺失/超时 → TIMEOUT（复刻 scan_stale_tasks）。 */
    public List<String> scanStale() {
        List<String> stale = new ArrayList<>();
        Set<String> keys = redis.keys("task:*");
        if (keys == null) {
            return stale;
        }
        for (String key : keys) {
            if (key.endsWith(":heartbeat") || key.endsWith(":cancel")) {
                continue;
            }
            String taskId = key.substring("task:".length());
            TaskState s = get(taskId);
            if (s == null || !"RUNNING".equals(s.status)) {
                continue;
            }
            String hb = redis.opsForValue().get(key + ":heartbeat");
            boolean dead = true;
            if (hb != null) {
                try {
                    Instant t = Instant.parse(hb);
                    dead = t.plusSeconds(HEARTBEAT_STALE_S).isBefore(Instant.now());
                } catch (Exception ignore) {
                    // unparseable → dead
                }
            }
            if (dead) {
                s.status = "TIMEOUT";
                s.error = "Worker lost (heartbeat stale)";
                save(s);
                stale.add(taskId);
            }
        }
        return stale;
    }
}
```

（`Instant.parse` 要求 ISO-8601 带 Z；Python `_now_iso()` 输出 `+00:00` 后缀——实现时若解析失败则回退 `OffsetDateTime.parse`，两分支都要。）

- [ ] **Step 5: 单测（Mockito mock StringRedisTemplate 验证 key 格式/TTL/序列化调用）+ 全量绿 + 提交**

```bash
mvn test
git add -A gateway-java && git commit -m "M2: TaskStoreService — Redis 状态机复刻（key/TTL/幂等/心跳判死）"
```

---

### Task 4: KafkaPublisher（信封契约）

**Files:** Create `task/KafkaPublisher.java`；Test `task/KafkaPublisherTest.java`（Mockito：信封 JSON 精确断言）

- [ ] **Step 1: 失败测试**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final KafkaPublisher publisher = new KafkaPublisher(kafka, new ObjectMapper());

    @Test
    void publishesEnvelopeWithTaskIdKey() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishSubmitted("t1", Map.of("question", "q", "db_id", "d"));

        var envelopeCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("nl2sql.task.request"), eq("t1"), envelopeCaptor.capture());
        Map<String, Object> envelope = new ObjectMapper()
                .readValue(envelopeCaptor.getValue(), Map.class); // checked exception 见下方注
        assertThat(envelope.get("task_id")).isEqualTo("t1");
        assertThat(envelope.get("event")).isEqualTo("submitted");
        assertThat(((Map<String, Object>) envelope.get("payload"))).containsEntry("question", "q");
    }
}
```

（readValue 抛 checked 异常——测试方法签名加 `throws Exception`。）

- [ ] **Step 2: 实现**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/** Kafka 信封 {"task_id","event","payload"}，key=task_id — 与 broker.py TaskMessage 一致。 */
@Service
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;

    public KafkaPublisher(KafkaTemplate<String, String> kafka, ObjectMapper om) {
        this.kafka = kafka;
        this.om = om;
    }

    public void publishSubmitted(String taskId, Map<String, Object> payload) {
        publish(KafkaTopics.TOPIC_REQUEST, taskId, "submitted", payload);
    }

    public void publishFeedback(String taskId, Map<String, Object> payload) {
        publish(KafkaTopics.TOPIC_FEEDBACK, taskId, "feedback", payload);
    }

    private void publish(String topic, String taskId, String event, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("task_id", taskId);
        envelope.put("event", event);
        envelope.put("payload", payload);
        try {
            kafka.send(topic, taskId, om.writeValueAsString(envelope));
        } catch (Exception e) {
            throw new IllegalStateException("kafka publish failed", e);
        }
    }
}
```

- [ ] **Step 3: 绿后提交**

```bash
mvn test -Dtest=KafkaPublisherTest
git add -A gateway-java && git commit -m "M2: KafkaPublisher — 信封契约 {task_id,event,payload} + key=task_id"
```

---

### Task 5: TaskController（submit/status/cancel/health/scan-stale/feedback）

**Files:** Create `task/TaskController.java`；Test `task/TaskControllerTest.java`（Mockito）

- [ ] **Step 1: 失败测试（核心语义各一例）**

```java
package com.nl2sql.gateway.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest
@Import(TaskController.class)
class TaskControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    TaskStoreService store;
    @MockBean
    KafkaPublisher publisher;

    @Test
    void submitReturns202WithTaskId() throws Exception {
        when(store.idempotentGet(any())).thenReturn(null);
        mvc.perform(post("/api/v1/task/submit").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"q\",\"db_id\":\"california_schools\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.task_id").isNotEmpty())
                .andExpect(jsonPath("$.status").value("PENDING"));
        verify(store).create(anyString(), eq("q"), eq("california_schools"), eq(""));
        verify(publisher).publishSubmitted(anyString(), argThat(p -> "q".equals(p.get("question"))));
    }

    @Test
    void submitIdempotentHitReturnsExistingTaskId() throws Exception {
        when(store.idempotentGet(any())).thenReturn("existing123");
        mvc.perform(post("/api/v1/task/submit").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"q\",\"idempotency_key\":\"k1\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.task_id").value("existing123"));
        verify(publisher, never()).publishSubmitted(anyString(), any());
    }

    @Test
    void status404MatchesFastApiDetailShape() throws Exception {
        when(store.get("nope")).thenReturn(null);
        mvc.perform(get("/api/v1/task/nope/status"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Task not found or expired"));
    }

    @Test
    void cancelTerminalReturnsStateStatus() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "SUCCESS";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/cancel"))
                .andExpect(jsonPath("$.status").value("SUCCESS"));
        verify(store, never()).requestCancel(anyString());
    }

    @Test
    void feedbackOnRunningTaskReturns400() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "RUNNING";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/feedback").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"feedback\":\"add group by\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void taskEndpointsAreNotProxiedToEngine() throws Exception {
        // 说明：集成测试 TaskSubmitIntegrationTest 里用 MockWebServer 请求数断言；此处单测只验证映射存在
        when(store.get(any())).thenReturn(null);
        mvc.perform(get("/api/v1/task/any/status")).andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 2: 实现 TaskController**（要点：submit 的 payload 12 字段照抄 task.py L58-70；feedback 校验顺序 404→400 状态→400 turns；404/400 错误体用 `{"detail": "..."}` 对齐 FastAPI HTTPException）

```java
package com.nl2sql.gateway.task;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 任务端点——语义与 api/routes/task.py 逐条对齐；映射比 /api/v1/** 更具体，天然优先于透明代理。 */
@RestController
@RequestMapping("/api/v1/task")
public class TaskController {

    public static final int FEEDBACK_MAX_TURNS = 10;

    private final TaskStoreService store;
    private final KafkaPublisher publisher;

    public TaskController(TaskStoreService store, KafkaPublisher publisher) {
        this.store = store;
        this.publisher = publisher;
    }

    public record SubmitRequest(
            String question, Boolean rag_schema, Boolean rag_domain, Boolean multi_candidate,
            Integer rag_k, Boolean rag_column_prune, Boolean rag_hybrid, Boolean rag_fk_expand,
            Boolean fewshot_enabled, String database_url, String db_id, Object llm,
            String idempotency_key) {
    }

    @PostMapping("/submit")
    public ResponseEntity<Map<String, String>> submit(@RequestBody SubmitRequest req) {
        if (req.idempotency_key() != null && !req.idempotency_key().isBlank()) {
            String idemKey = TaskIdempotency.key(req.idempotency_key(), req.question());
            String existing = store.idempotentGet(idemKey);
            if (existing != null) {
                return ResponseEntity.accepted().body(Map.of("task_id", existing, "status", "PENDING"));
            }
            String taskId = TaskIdempotency.newTaskId();
            store.idempotentSet(idemKey, taskId);
            return accepted(taskId, req);
        }
        return accepted(TaskIdempotency.newTaskId(), req);
    }

    private ResponseEntity<Map<String, String>> accepted(String taskId, SubmitRequest req) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("question", req.question());
        payload.put("db_id", req.db_id() == null ? "" : req.db_id());
        payload.put("database_url", req.database_url() == null ? "" : req.database_url());
        payload.put("rag_schema", bool(req.rag_schema(), true));
        payload.put("rag_domain", bool(req.rag_domain(), true));
        payload.put("multi_candidate", bool(req.multi_candidate(), true));
        payload.put("rag_k", req.rag_k() == null ? 8 : req.rag_k());
        payload.put("rag_column_prune", bool(req.rag_column_prune(), false));
        payload.put("rag_hybrid", bool(req.rag_hybrid(), true));
        payload.put("rag_fk_expand", bool(req.rag_fk_expand(), true));
        payload.put("fewshot_enabled", bool(req.fewshot_enabled(), true));

        store.create(taskId, req.question(), req.db_id() == null ? "" : req.db_id(),
                req.database_url() == null ? "" : req.database_url());
        publisher.publishSubmitted(taskId, payload);
        return ResponseEntity.accepted().body(Map.of("task_id", taskId, "status", "PENDING"));
    }

    private boolean bool(Boolean v, boolean dflt) {
        return v == null ? dflt : v;
    }

    @GetMapping("/{task_id}/status")
    public ResponseEntity<?> status(@PathVariable String task_id) {
        TaskState s = store.get(task_id);
        if (s == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("detail", "Task not found or expired"));
        }
        return ResponseEntity.ok(s.toStatusResponse());
    }

    @PostMapping("/{task_id}/cancel")
    public Map<String, String> cancel(@PathVariable String task_id) {
        TaskState s = store.get(task_id);
        if (s == null) {
            return Map.of("task_id", task_id, "status", "not_found");
        }
        if ("SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "CANCELLED".equals(s.status) || "TIMEOUT".equals(s.status)) {
            return Map.of("task_id", task_id, "status", s.status);
        }
        store.requestCancel(task_id);
        return Map.of("task_id", task_id, "status", "cancelled");
    }

    @GetMapping("/{task_id}/health")
    public ResponseEntity<?> health(@PathVariable String task_id) {
        TaskState s = store.get(task_id);
        if (s == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("detail", "Task not found or expired"));
        }
        String hb = store.heartbeat(task_id);
        boolean healthy = false;
        Double staleS = null;
        if (hb != null) {
            try {
                long elapsed = java.time.Duration.between(
                        java.time.OffsetDateTime.parse(hb), java.time.OffsetDateTime.now()).toSeconds();
                healthy = elapsed < TaskStoreService.HEARTBEAT_STALE_S;
                staleS = (double) elapsed;
            } catch (Exception ignore) {
                // unparseable → unhealthy
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("task_id", task_id);
        body.put("task_status", s.status);
        body.put("heartbeat", hb);
        body.put("heartbeat_stale_s", staleS);
        body.put("healthy", healthy);
        body.put("worker_alive", healthy && "RUNNING".equals(s.status));
        return ResponseEntity.ok(body);
    }

    @PostMapping("/scan-stale")
    public Map<String, Object> scanStale() {
        List<String> stale = store.scanStale();
        return Map.of("stale_count", stale.size(), "stale_task_ids", stale);
    }

    public record FeedbackRequest(String feedback) {
    }

    @PostMapping("/{task_id}/feedback")
    public ResponseEntity<?> feedback(@PathVariable String task_id, @RequestBody FeedbackRequest req) {
        TaskState s = store.get(task_id);
        if (s == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("detail", "Task not found or expired"));
        }
        if (!"SUCCESS".equals(s.status) && !"FAILED".equals(s.status)) {
            return ResponseEntity.badRequest().body(Map.of("detail",
                    "Feedback only allowed on SUCCESS tasks (current: " + s.status + ")"));
        }
        List<Map<String, Object>> turns = s.turns();
        int turn = turns.size() + 1;
        if (turn > FEEDBACK_MAX_TURNS) {
            return ResponseEntity.badRequest().body(Map.of("detail",
                    "Maximum feedback turns (" + FEEDBACK_MAX_TURNS + ") reached"));
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("feedback", req.feedback());
        payload.put("turn", turn);
        payload.put("question", s.question);
        payload.put("db_id", s.db_id);
        payload.put("database_url", s.database_url);
        payload.put("sql", s.sql);
        payload.put("exec_result", s.exec_result);
        payload.put("conversation_turns", turns);
        payload.put("token_usage", s.token_usage);
        payload.put("node_timings", s.node_timings);

        publisher.publishFeedback(task_id, payload);
        s.status = "RUNNING";
        s.node = null;
        s.progress = 0;
        s.updated_at = TaskStoreService.nowIso();
        store.save(s);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("task_id", task_id);
        body.put("status", "accepted");
        body.put("turn", turn);
        return ResponseEntity.accepted().body(body);
    }
}
```

（`store.heartbeat(taskId)` 在 Task 3 的 TaskStoreService 里补：`redis.opsForValue().get("task:"+id+":heartbeat")`。）

- [ ] **Step 3: 绿后提交**

```bash
mvn test -Dtest=TaskControllerTest
git add -A gateway-java && git commit -m "M2: TaskController — 7 端点语义复刻（404/400 形状对齐 FastAPI）"
```

---

### Task 6: TaskStreamService（PubSub + 轮询 → SSE）

**Files:** Create `task/TaskStreamService.java`；Modify `TaskController`（stream 端点）；Test `task/TaskStreamIntegrationTest.java`

- [ ] **Step 1: 集成测试基座 IntegrationTestBase**

```java
package com.nl2sql.gateway.task;

import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTestBase {

    @Container
    @ServiceConnection
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.1");

    static final MockWebServer engine = new MockWebServer();

    @DynamicPropertySource
    static void engineProps(DynamicPropertyRegistry registry) throws IOException {
        engine.start();
        registry.add("engine.base-url", () -> engine.url("/").toString().replaceAll("/$", ""));
    }

    @AfterAll
    static void stopEngine() throws IOException {
        engine.shutdown();
    }

    @Autowired
    protected StringRedisTemplate redisTemplate;
}
```

- [ ] **Step 2: TaskSubmitIntegrationTest（真实 Redis/Kafka 全链路 + 不代理断言）**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskSubmitIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper om = new ObjectMapper();

    @Test
    void submitPersistsRedisStateAndProducesKafkaEnvelope() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        ResponseEntity<Map> resp = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                new HttpEntity<>("{\"question\":\"avg?\",\"db_id\":\"california_schools\"}", headers),
                Map.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        String taskId = (String) resp.getBody().get("task_id");

        // Redis 状态存在且为 PENDING
        String raw = redisTemplate.opsForValue().get("task:" + taskId);
        assertThat(raw).isNotNull();
        Map<String, Object> state = om.readValue(raw, Map.class);
        assertThat(state.get("status")).isEqualTo("PENDING");
        assertThat(state.get("db_id")).isEqualTo("california_schools");

        // Kafka 信封
        var props = new java.util.Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "it-" + System.nanoTime());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (var consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
            consumer.subscribe(List.of("nl2sql.task.request"));
            consumer.poll(Duration.ofSeconds(10)).records("nl2sql.task.request").stream()
                    .map(ConsumerRecord::value)
                    .map(v -> { try { return om.readValue(v, Map.class); } catch (Exception e) { throw new RuntimeException(e); } })
                    .filter(e -> taskId.equals(e.get("task_id")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("envelope not found"));
        }

        // 任务端点没有被代理到引擎（MockWebServer 零请求）
        assertThat(engine.getRequestCount()).isZero();
    }
}
```

- [ ] **Step 3: 实现 TaskStreamService（订阅 + 轮询循环，事件序列复刻 task.py L156-254）**

```java
package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** SSE 事件源：PubSub token + 500ms 状态轮询，事件名与载荷复刻 task.py task_stream。 */
@Service
public class TaskStreamService {

    private final TaskStoreService store;
    private final RedisMessageListenerContainer listenerContainer;
    private final ObjectMapper om = new ObjectMapper();

    public TaskStreamService(TaskStoreService store,
                             RedisMessageListenerContainer listenerContainer) {
        this.store = store;
        this.listenerContainer = listenerContainer;
    }

    /** 事件 = SSE name + JSON data。 */
    public record SseEvent(String event, String data) {
    }

    /** 拉取下一个事件；deadlineMs 内无事件返回 null（调用方结束流）。 */
    public SseEvent next(String taskId, EventCursor cursor, long deadlineMs) throws IOException {
        // 1) token 优先（500ms 超时后转状态轮询——与 python wait_for 语义一致）
        String token = cursor.tokens.poll();
        if (token == null) {
            pollStatus(taskId, cursor);
            try {
                token = cursor.tokens.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (token != null) {
                return tokenEvent(token);
            }
        } else {
            return tokenEvent(token);
        }

        // 2) 状态轮询结果
        if (cursor.error != null) {
            return new SseEvent("error", json(Map.of("error", "Task not found")));
        }
        SseEvent statusEvent = cursor.takeStatusEvent();
        if (statusEvent != null) {
            return statusEvent;
        }
        SseEvent nodeEvent = cursor.takeNodeEvent();
        if (nodeEvent != null) {
            return nodeEvent;
        }
        if (cursor.completeEvent != null) {
            return cursor.completeEvent;
        }
        if (System.currentTimeMillis() > deadlineMs) {
            return new SseEvent("timeout", json(Map.of("error", "Stream timeout (5 min)")));
        }
        return null; // 本轮无事件，继续循环（由调用方决定）
    }

    private SseEvent tokenEvent(String token) throws IOException {
        return new SseEvent("token", json(Map.of("text", token)));
    }

    private void pollStatus(String taskId, EventCursor cursor) throws IOException {
        TaskState s = store.get(taskId);
        if (s == null) {
            cursor.error = "not_found";
            return;
        }
        if (!s.updated_at.equals(cursor.lastUpdated)) {
            cursor.lastUpdated = s.updated_at;
            cursor.pendingStatus = new SseEvent("status", json(Map.of(
                    "status", s.status,
                    "progress", s.progress,
                    "node", s.node == null ? "" : s.node,
                    "sql_preview", s.sql == null ? "" : s.sql.substring(0, Math.min(200, s.sql.length())),
                    "error", s.error == null ? "" : s.error)));
        }
        if (s.node != null && !s.node.equals(cursor.lastNode)) {
            cursor.lastNode = s.node;
            cursor.pendingNode = new SseEvent("node_done", json(Map.of("node", s.node)));
        }
        if ("SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "TIMEOUT".equals(s.status) || "CANCELLED".equals(s.status)) {
            cursor.completeEvent = new SseEvent("complete", json(Map.of(
                    "status", s.status,
                    "sql", s.sql == null ? "" : s.sql,
                    "exec_result", s.exec_result,
                    "token_usage", s.token_usage,
                    "node_timings", s.node_timings,
                    "error", s.error == null ? "" : s.error)));
        }
    }

    /** 订阅 token 通道并把消息塞进队列；返回取消订阅句柄。 */
    public AutoCloseable subscribeTokens(String taskId, BlockingQueue<String> queue) {
        MessageListener listener = (Message message, byte[] pattern) ->
                queue.add(new String(message.getBody(), StandardCharsets.UTF_8));
        listenerContainer.addMessageListener(listener, new ChannelTopic("task:" + taskId + ":tokens"));
        return () -> listenerContainer.removeMessageListener(listener);
    }

    public String json(Object o) throws IOException {
        return om.writeValueAsString(o);
    }

    /** 每个流连接一份游标。 */
    public static class EventCursor {
        final BlockingQueue<String> tokens = new LinkedBlockingQueue<>();
        String lastUpdated = "";
        String lastNode = "";
        String error;
        SseEvent pendingStatus;
        SseEvent pendingNode;
        SseEvent completeEvent;

        SseEvent takeStatusEvent() {
            SseEvent e = pendingStatus;
            pendingStatus = null;
            return e;
        }

        SseEvent takeNodeEvent() {
            SseEvent e = pendingNode;
            pendingNode = null;
            return e;
        }
    }
}
```

（注意 complete 事件优先级最高：once set, next() 必须先发 pendingStatus/pendingNode 再发 complete，最后返回 null 结束——controller 循环收到 complete 后 break 并 close。）

- [ ] **Step 4: TaskController 增加 stream 端点**

```java
    private final TaskStreamService streamService; // 构造注入

    @GetMapping("/{task_id}/stream")
    public void stream(@PathVariable String task_id, jakarta.servlet.http.HttpServletResponse response)
            throws Exception {
        response.setContentType("text/event-stream");
        response.setCharacterEncoding("UTF-8");
        var cursor = new TaskStreamService.EventCursor();
        long deadline = System.currentTimeMillis() + 300_000;
        try (var sub = streamService.subscribeTokens(task_id, cursor.tokens);
             var out = response.getOutputStream()) {
            while (System.currentTimeMillis() < deadline) {
                TaskStreamService.SseEvent ev = streamService.next(task_id, cursor, deadline);
                if (ev == null) {
                    continue;
                }
                out.write(("event: " + ev.event() + "\ndata: " + ev.data() + "\n\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                out.flush();
                if ("complete".equals(ev.event()) || "error".equals(ev.event())
                        || "timeout".equals(ev.event())) {
                    return;
                }
            }
        }
    }
```

（null 语义改为"本轮无事件、继续"时注意：timeout 判定在 next() 内部用 deadlineMs，到达即返回 timeout 事件。实现时保证循环能退出。）

- [ ] **Step 5: TaskStreamIntegrationTest（真 PubSub → SSE 事件顺序）**

```java
package com.nl2sql.gateway.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TaskStreamIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    TaskStoreService store;

    @Test
    void emitsTokenStatusCompleteSequence() throws Exception {
        String taskId = TaskIdempotency.newTaskId();
        store.create(taskId, "q", "d", "");
        redisTemplate.opsForValue().set("task:" + taskId + ":heartbeat",
                java.time.OffsetDateTime.now().toString(), java.time.Duration.ofSeconds(30));

        // 异步推 token + 更新状态到 SUCCESS
        new Thread(() -> {
            try {
                Thread.sleep(400);
                redisTemplate.convertAndSend("task:" + taskId + ":tokens", "SEL");
                TaskState s = store.get(taskId);
                s.status = "RUNNING";
                s.node = "generator";
                s.updated_at = TaskStoreService.nowIso();
                store.save(s);
                Thread.sleep(400);
                TaskState done = store.get(taskId);
                done.status = "SUCCESS";
                done.sql = "SELECT 1";
                done.updated_at = TaskStoreService.nowIso();
                store.save(done);
            } catch (Exception ignore) {
            }
        }).start();

        ResponseEntity<Resource> resp = rest.getForEntity("/api/v1/task/" + taskId + "/stream", Resource.class);
        List<String> events = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(resp.getBody().getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.startsWith("event: ")) {
                    events.add(line.substring("event: ".length()));
                }
            }
        }
        assertThat(events).contains("status", "node_done", "token", "complete");
        assertThat(events.get(events.size() - 1)).isEqualTo("complete");
    }
}
```

- [ ] **Step 6: 全量绿后提交**

```bash
mvn test
git add -A gateway-java && git commit -m "M2: TaskStreamService — PubSub+轮询 SSE 事件流（token/status/node_done/complete）+ 集成测试"
```

---

### Task 7: compose kafka 双 listener + gateway 环境变量

**Files:** Modify `deployment/docker-compose.yml`

- [ ] **Step 1: kafka 服务环境变量替换**

```yaml
# 原：
#   KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
#   KAFKA_ADVERTISED_LISTERS: PLAINTEXT://127.0.0.1:9092
# 改为：
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: EXTERNAL://127.0.0.1:9092,INTERNAL://kafka:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@127.0.0.1:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

app 服务环境 `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` → `kafka:29092`。

- [ ] **Step 2: gateway-java 服务环境变量增补**

```yaml
    environment:
      - ENGINE_BASE_URL=${ENGINE_BASE_URL:-http://app:8000}
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

- [ ] **Step 3: 起 redis+kafka+gateway 验证连通（kafka 首次拉镜像）**

```bash
cd deployment && docker compose up -d redis kafka gateway-java
sleep 30 && curl -s http://127.0.0.1:8080/health | head -c 200
# 期望 engine DOWN（宿主机引擎未起）但应用 UP、Redis/Kafka bean 无报错
```

- [ ] **Step 4: 提交**

```bash
git add deployment/docker-compose.yml
git commit -m "M2: compose — kafka 双 listener(容器间 29092) + gateway Redis/Kafka 接入"
```

---

### Task 8: 真 Worker E2E + UI 冒烟

- [ ] **Step 1: 宿主机起引擎与 Worker**

```bash
cd F:\Experience\nl2sql-agent
python -m uvicorn api.app:app --host 0.0.0.0 --port 8000 &
python -m worker.main &
```

（Worker 自动 create_topics；消费 nl2sql.task.request。需 .env 的 LLM key 有效——**用户当前 key 失效，E2E 前需更新**。）

- [ ] **Step 2: 重建并重启 gateway（指宿主机 redis/kafka：本机 6379/9092）**

```bash
cd deployment
ENGINE_BASE_URL=http://10.10.64.198:8000 REDIS_HOST=127.0.0.1 KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
  docker compose up --no-deps -d --build gateway-java
```

- [ ] **Step 3: 全链路验证（提交→流式→完成→状态）**

```bash
TASK_ID=$(curl -s -X POST http://127.0.0.1:8080/api/v1/task/submit \
  -H "Content-Type: application/json" \
  -d '{"question":"What is the average enrollment in California?","db_id":"california_schools"}' \
  | python -c "import sys,json;print(json.load(sys.stdin)['task_id'])")
echo "task=$TASK_ID"
curl -s -N -m 180 http://127.0.0.1:8080/api/v1/task/$TASK_ID/stream | head -40
curl -s http://127.0.0.1:8080/api/v1/task/$TASK_ID/status | head -c 400
# 期望：SSE 出现 token/status/.../complete；status 最终 SUCCESS
```

- [ ] **Step 4: UI 冒烟（人工）**：`API_BASE=http://127.0.0.1:8080 streamlit run ui/app.py`，提交异步任务看打字机与结果。

---

### Task 9: 文档收官

**Files:** Modify `CHANGELOG.md`、`README.md`、`README_zh.md`（任务链路图改为 Java 原生端点）

- CHANGELOG 增 v0.6.1 M2 条目；README 架构图把 submit/stream 流向从 FastAPI 改为 Java→Kafka/Redis 直连；标注 FastAPI 剩余职责（同步查询/eval/元信息）。
- 提交 + 分支合并 master（沿用 M1 流程）。

---

## 验收清单（对照规格 §14 M2）

- [ ] UI 异步任务全流程经 Java 入口可用（submit→stream→complete→status），Worker 零改动
- [ ] 幂等：同 idempotency_key 重复提交返回同 task_id
- [ ] cancel/feedback/health/scan-stale 可用且响应形状与 FastAPI 一致
- [ ] 契约测试（golden JSON + sha256 向量 + Kafka 信封）全绿
- [ ] 集成测试（Testcontainers 真 Redis/Kafka）全绿；task 端点零代理断言通过

## Self-Review 记录

- 规格覆盖：§5 七端点 + §4 三协议 + §14 M2 验收 → Task 1-9 全对应 ✓
- 类型一致性：TaskState 字段被 TaskStoreService/TaskController/TaskStreamService 引用一致；`store.heartbeat()` 在 Task 5 标注补充到 Task 3 类 ✓
- 已知实现期决策点：IntegrationTestBase 的 KafkaContainer 镜像 tag；EventCursor 轮询节奏与 python 500ms 语义的等价实现；heartbeat ISO 解析双分支
