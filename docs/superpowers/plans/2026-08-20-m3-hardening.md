# M3 高并发加固 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用户策略层（身份/权限/归属）+ 限流/配额（Lua）+ SETNX 幂等升级 + qcache 结果缓存 + 熔断降级 + Kafka consumer 异步批量落库 MySQL + admin 端点，全部带指标与测试。

**Architecture:** 拦截器（身份/角色）→ TaskController submit 内联校验链（数据权限→限流→幂等→配额→缓存）→ 热路径仅 Redis/Kafka；MySQL 只被 Flyway（建表）、策略 miss（读库回填）、admin（写策略）、recorder（异步批量 upsert）触碰。recorder 消费 Worker 已有的 status/result topic（组 nl2sql-ops-recorder），事件只当"变脏信号"，flush 时以 Redis 状态为唯一事实源。

**Tech Stack:** MyBatis-Plus 3.5.7 / Flyway 10 (flyway-core + flyway-mysql) / mysql-connector-j / Testcontainers mysql

**规格来源:** `docs/superpowers/specs/2026-08-19-java-task-gateway-design.md` §6 §7 §8 §9 §12 §14-M3

**已核实事实:**
- Worker 持续发布 Kafka 事件：`nl2sql.task.status`（event=running/node_done/cancelled/failed/timeout）、`nl2sql.task.result`（event=success，payload 含 sql/exec_result/token_usage/node_timings/elapsed_s）——recorder 数据源成立
- UI 4 个 httpx 调用点（ui/app.py:125,142,215,235）需加 `X-User-Id` 头（默认 local_user，种子策略）
- compose MySQL 用户 nl2sql 仅对 demo 库有权限 → ops 库连接用 root（本地项目权衡，专属账号做法写入文档）；`createDatabaseIfNotExist=true` 免手动建库
- 现有 IntegrationTestBase（Redis+Kafka 容器 + MockWebServer）直接扩展 MySQL 容器

---

## 决策记录（实现时不得偏离）

1. **submit 校验链顺序**：身份(拦截器) → 数据权限 403 → 全局限流 → 用户限流 429 → 幂等 SETNX → 日配额 429 → qcache → 正常提交。限流在幂等前（保护所有 Redis 操作）；配额在幂等后（幂等命中不烧额度）
2. **幂等升级为 SETNX**：`SET idempotent:{hash} task_id NX EX 300`；失败方 GET 读走占位者的 task_id 返回。与 Python 契约 TTL 300s 不变
3. **任务归属**：submit 时写 `taskowner:{task_id}` = user_tag（TTL 86400s，Java 自有 key，非 Python 契约）。status/cancel/stream/feedback 校验 owner==本人 或 admin
4. **qcache 只缓存 SUCCESS 且 sql 非空**（含空结果集——空行结果就是"空值缓存"防穿透；失败不缓存：重放陈旧错误比慢更糟，属对规格 §7.2 空值条款的工程化解释）
5. **限流/配额 fail-open**：Redis/Lua 异常时放行+计数（可用性优先）；引擎不可用仍 503（硬依赖）——差异写进文档
6. **recorder 一致性**：Kafka 事件只标记 task_id 变脏（ConcurrentHashMap.newKeySet），flush（2s 或 ≥100）读 Redis 状态+owner 组装行、`INSERT ... ON DUPLICATE KEY UPDATE` upsert；cache_hit 任务不走 Kafka，由 submit 路径直接 markDirty
7. **拦截器范围**：`/api/v1/task/**`、`/api/v1/query/**`、`/api/v1/eval/**`；排除 `/api/v1/health`（冒烟/compose 探活）。eval 路径强制 ADMIN；scan-stale 用 @RequireRole(ADMIN)
8. **指标**：`gateway.ratelimit.rejected{type=user|global}`、`gateway.quota.rejected`、`gateway.cache{result=hit|miss}`、`gateway.recorder.flush{size}`（Timer）

---

## 文件结构

```
gateway-java/src/main/java/com/nl2sql/gateway/
  config/GatewayProperties.java          [新] gateway.* 配置（globalQps/cacheTtl 等）
  config/WebConfig.java                  [新] 注册拦截器
  policy/UserPolicy.java                 [新] 实体（allowed_db_ids 存 JSON 字符串列）
  policy/UserPolicyMapper.java           [新] MyBatis-Plus mapper + upsert SQL
  policy/UserPolicyService.java          [新] cache-aside + 负缓存 60s + admin 更新删缓存
  policy/RequireRole.java                [新] 注解 Role.USER/ADMIN
  policy/UserPolicyInterceptor.java      [新] 401/403/角色，policy 放请求属性
  policy/PermissionService.java          [新] dbAllowed/isOwnerOrAdmin
  ratelimit/RateLimiterService.java      [新] Lua 滑动窗口（用户+全局）
  ratelimit/QuotaService.java            [新] 日配额 Lua
  cache/ResultCacheService.java          [新] qcache 读写 + TTL 抖动
  recorder/TaskRecord.java               [新] 实体
  recorder/TaskRecordMapper.java         [新] upsert mapper
  recorder/TaskEventRecorder.java        [新] Kafka consumer + 批量 flusher
  admin/AdminController.java             [新] PUT /admin/users/{tag} + GET /admin/stats
  task/TaskStoreService.java             [改] +idempotentTryAcquire/taskownerGet/Set
  task/TaskController.java               [改] submit 链 + 归属校验 + scan-stale ADMIN
  task/KafkaTopics.java                  [改] +status/result/dlq NewTopic
  proxy/ProxyController.java             [改] 熔断打开→qcache 降级
  resources/application.yml              [改] datasource/flyway/mybatis-plus/gateway
  resources/db/migration/V1__init.sql    [新] 两表+种子
pom.xml                                  [改] mybatis-plus/flyway/mysql/testcontainers-mysql
ui/app.py                                [改] 4 个 httpx 调用点加 X-User-Id
deployment/docker-compose.yml            [改] gateway MySQL 环境变量
测试: policy/、ratelimit/、cache/、recorder/ 单测 + IntegrationTestBase 加 MySQL + SubmitChainIntegrationTest
```

---

### Task 1: 依赖 + Flyway + 实体 + 测试基座扩展

**Files:** pom.xml、application.yml、V1__init.sql、UserPolicy(.Mapper)、TaskRecord(.Mapper)、IntegrationTestBase

- [ ] **Step 1: pom.xml 增依赖**（mp 3.5.7 显式版本；flyway/mysql 由 boot 管理；test 加 org.testcontainers:mysql）

```xml
        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
            <version>3.5.7</version>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-mysql</artifactId>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <scope>runtime</scope>
        </dependency>
        <!-- test 域追加 -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mysql</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: application.yml 增段**

```yaml
spring:
  datasource:
    url: jdbc:mysql://${MYSQL_HOST:127.0.0.1}:${MYSQL_PORT:3306}/nl2sql_ops?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
    username: ${MYSQL_USER:root}
    password: ${MYSQL_PASSWORD:nl2sql_root}
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
  flyway:
    enabled: true
mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
gateway:
  ratelimit:
    global-qps: ${GLOBAL_QPS:200}
  cache:
    result-ttl-seconds: 1800
```

- [ ] **Step 3: V1__init.sql**（规格 §8 原样）

```sql
CREATE TABLE user_policy (
  user_tag       VARCHAR(64) PRIMARY KEY,
  role           VARCHAR(16) NOT NULL DEFAULT 'user',
  allowed_db_ids JSON NOT NULL,
  qps_limit      INT NOT NULL DEFAULT 10,
  daily_quota    BIGINT NOT NULL DEFAULT 1000,
  status         TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE task_record (
  task_id          VARCHAR(64) PRIMARY KEY,
  user_tag         VARCHAR(64) NOT NULL,
  db_id            VARCHAR(128),
  question         TEXT,
  status           VARCHAR(16),
  sql_text         MEDIUMTEXT,
  row_count        INT,
  retry_count      TINYINT DEFAULT 0,
  error            TEXT,
  cache_hit        TINYINT DEFAULT 0,
  submitted_at     DATETIME(3),
  started_at       DATETIME(3),
  finished_at      DATETIME(3),
  total_latency_ms INT,
  node_timings     JSON,
  token_usage      JSON,
  created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX idx_user_time (user_tag, submitted_at),
  INDEX idx_status_time (status, submitted_at),
  INDEX idx_time (submitted_at)
);

INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status) VALUES
  ('local_user',  'user',  '["*"]', 100, 100000, 1),
  ('bench_admin', 'admin', '["*"]', 100, 100000, 1);
```

- [ ] **Step 4: UserPolicy 实体 + Mapper**（allowedDbIds 用 String 列存 JSON，service 层解析）

```java
package com.nl2sql.gateway.policy;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("user_policy")
public class UserPolicy {
    @TableId(type = IdType.INPUT)
    public String userTag;
    public String role;
    public String allowedDbIds;   // JSON 数组字符串
    public Integer qpsLimit;
    public Long dailyQuota;
    public Integer status;
    public java.time.LocalDateTime createdAt;
    public java.time.LocalDateTime updatedAt;
}
```

```java
package com.nl2sql.gateway.policy;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface UserPolicyMapper extends BaseMapper<UserPolicy> {

    @Insert("""
            INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status)
            VALUES (#{p.userTag}, #{p.role}, #{p.allowedDbIds}, #{p.qpsLimit}, #{p.dailyQuota}, #{p.status})
            ON DUPLICATE KEY UPDATE role=VALUES(role), allowed_db_ids=VALUES(allowed_db_ids),
              qps_limit=VALUES(qps_limit), daily_quota=VALUES(daily_quota), status=VALUES(status)
            """)
    int upsert(@Param("p") UserPolicy p);
}
```

- [ ] **Step 5: TaskRecord 实体 + Mapper**（recorder 用；Task 6 消费）

```java
package com.nl2sql.gateway.recorder;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("task_record")
public class TaskRecord {
    @TableId(type = IdType.INPUT)
    public String taskId;
    public String userTag;
    public String dbId;
    public String question;
    public String status;
    public String sqlText;
    public Integer rowCount;
    public Integer retryCount;
    public String error;
    public Integer cacheHit;
    public java.time.LocalDateTime submittedAt;
    public java.time.LocalDateTime startedAt;
    public java.time.LocalDateTime finishedAt;
    public Integer totalLatencyMs;
    public String nodeTimings;    // JSON
    public String tokenUsage;     // JSON
}
```

```java
package com.nl2sql.gateway.recorder;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface TaskRecordMapper extends BaseMapper<TaskRecord> {

    @Insert("""
            INSERT INTO task_record (task_id, user_tag, db_id, question, status, sql_text, row_count,
              retry_count, error, cache_hit, submitted_at, started_at, finished_at, total_latency_ms,
              node_timings, token_usage)
            VALUES (#{r.taskId}, #{r.userTag}, #{r.dbId}, #{r.question}, #{r.status}, #{r.sqlText},
              #{r.rowCount}, #{r.retryCount}, #{r.error}, #{r.cacheHit}, #{r.submittedAt}, #{r.startedAt},
              #{r.finishedAt}, #{r.totalLatencyMs}, #{r.nodeTimings}, #{r.tokenUsage})
            ON DUPLICATE KEY UPDATE status=VALUES(status), sql_text=VALUES(sql_text),
              row_count=VALUES(row_count), retry_count=VALUES(retry_count), error=VALUES(error),
              cache_hit=VALUES(cache_hit), finished_at=VALUES(finished_at),
              total_latency_ms=VALUES(total_latency_ms), node_timings=VALUES(node_timings),
              token_usage=VALUES(token_usage)
            """)
    int upsert(@Param("r") TaskRecord r);
}
```

- [ ] **Step 6: IntegrationTestBase 加 MySQL 容器**

```java
    static final org.testcontainers.containers.MySQLContainer<?> mysql =
            new org.testcontainers.containers.MySQLContainer<>("mysql:8.4")
                    .withDatabaseName("nl2sql_ops").withUsername("root").withPassword("root");

    static {
        redis.start();
        kafka.start();
        mysql.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws IOException {
        // ...原有 engine/kafka/redis 三行保留...
        registry.add("spring.datasource.url", () -> mysql.getJdbcUrl() + "?useSSL=false&allowPublicKeyRetrieval=true");
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }
```

注意：MySQLContainer 的 withUsername("root") 不允许（保留名）——用 `withUsername("ops").withPassword("ops")` 并在 withDatabaseName("nl2sql_ops")；JDBC URL 直连容器库。Flyway 在测试上下文启动时自动跑 V1（验证迁移）。

- [ ] **Step 7: 跑全量（迁移在集成测试里自动验证）+ 提交**

```bash
export JAVA_HOME=~/scoop/apps/temurin21-jdk/current && cd gateway-java && mvn test
git add -A gateway-java && git commit -m "M3: 依赖+Flyway V1(两表+种子)+实体Mapper+集成基座MySQL"
```

---

### Task 2: 策略层（服务/注解/拦截器/权限）

**Files:** policy/UserPolicyService.java、RequireRole.java、UserPolicyInterceptor.java、PermissionService.java、config/WebConfig.java、config/GatewayProperties.java；测试 policy/UserPolicyServiceTest、UserPolicyInterceptorTest、PermissionServiceTest

- [ ] **Step 1: GatewayProperties**

```java
package com.nl2sql.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(Ratelimit ratelimit, Cache cache) {

    public record Ratelimit(int globalQps) {
    }

    public record Cache(int resultTtlSeconds) {
    }
}
```

- [ ] **Step 2: UserPolicyService**（cache-aside：`policy:{tag}` 正缓存 TTL 600s±60s 抖动；未知名负缓存 60s；admin 更新删 key）

```java
package com.nl2sql.gateway.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class UserPolicyService {

    public record Policy(String userTag, String role, List<String> allowedDbIds,
                         int qpsLimit, long dailyQuota, boolean enabled) {
        public boolean isAdmin() {
            return "admin".equals(role);
        }
    }

    private static final String NEGATIVE = "\u0000NEG";
    private final UserPolicyMapper mapper;
    private final StringRedisTemplate redis;
    private final ObjectMapper om;

    public UserPolicyService(UserPolicyMapper mapper, StringRedisTemplate redis, ObjectMapper om) {
        this.mapper = mapper;
        this.redis = redis;
        this.om = om;
    }

    /** 返回 null = 未知用户（调用方 401）。缓存 miss 读库回填；未知名负缓存 60s 防穿透。 */
    public Policy resolve(String userTag) {
        String key = "policy:" + userTag;
        String raw = redis.opsForValue().get(key);
        if (raw != null) {
            return NEGATIVE.equals(raw) ? null : parse(raw);
        }
        UserPolicy row = mapper.selectById(userTag);
        if (row == null) {
            redis.opsForValue().set(key, NEGATIVE, Duration.ofSeconds(60));
            return null;
        }
        Policy p = toPolicy(row);
        try {
            redis.opsForValue().set(key, om.writeValueAsString(p), jitter(Duration.ofSeconds(600), 0.1));
        } catch (Exception ignore) {
            // 缓存写失败不阻塞
        }
        return p;
    }

    /** admin 更新策略：upsert MySQL + 删缓存（下次请求读新值）。 */
    public Policy update(UserPolicy row) {
        mapper.upsert(row);
        redis.delete("policy:" + row.userTag);
        return toPolicy(row);
    }

    public void evict(String userTag) {
        redis.delete("policy:" + userTag);
    }

    private Policy parse(String json) {
        try {
            return om.readValue(json, Policy.class);
        } catch (Exception e) {
            return null;
        }
    }

    private Policy toPolicy(UserPolicy row) {
        List<String> dbs;
        try {
            dbs = om.readValue(row.allowedDbIds, new com.fasterxml.jackson.core.type.TypeReference<>() {
            });
        } catch (Exception e) {
            dbs = List.of();
        }
        return new Policy(row.userTag, row.role, dbs, row.qpsLimit, row.dailyQuota, row.status == 1);
    }

    static Duration jitter(Duration base, double ratio) {
        long millis = base.toMillis();
        long delta = (long) (millis * ratio);
        return Duration.ofMillis(Math.max(1, millis - delta + ThreadLocalRandom.current().nextLong(2 * delta)));
    }
}
```

- [ ] **Step 3: RequireRole + 拦截器 + WebConfig**

```java
package com.nl2sql.gateway.policy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequireRole {
    String value() default "user";   // "user" | "admin"
}
```

```java
package com.nl2sql.gateway.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

/** 身份闸门：X-User-Id → 策略；未知 401 / 禁用 403 / @RequireRole 不足 403；/api/v1/eval/** 强制 admin。 */
@Component
public class UserPolicyInterceptor implements HandlerInterceptor {

    public static final String POLICY_ATTR = "userPolicy";
    private final UserPolicyService policyService;
    private final ObjectMapper om = new ObjectMapper();

    public UserPolicyInterceptor(UserPolicyService policyService) {
        this.policyService = policyService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String tag = request.getHeader("X-User-Id");
        if (tag == null || tag.isBlank()) {
            return reject(response, 401, "missing X-User-Id header");
        }
        UserPolicyService.Policy policy = policyService.resolve(tag);
        if (policy == null) {
            return reject(response, 401, "unknown user: " + tag);
        }
        if (!policy.enabled()) {
            return reject(response, 403, "user disabled: " + tag);
        }
        String required = request.getRequestURI().startsWith("/api/v1/eval/") ? "admin" : null;
        if (required == null && handler instanceof HandlerMethod hm) {
            RequireRole rr = hm.getMethodAnnotation(RequireRole.class);
            if (rr != null) {
                required = rr.value();
            }
        }
        if ("admin".equals(required) && !policy.isAdmin()) {
            return reject(response, 403, "admin role required");
        }
        request.setAttribute(POLICY_ATTR, policy);
        return true;
    }

    private boolean reject(HttpServletResponse response, int code, String detail) throws Exception {
        response.setStatus(code);
        response.setContentType("application/json");
        response.getWriter().write(om.writeValueAsString(java.util.Map.of("detail", detail)));
        return false;
    }
}
```

```java
package com.nl2sql.gateway.config;

import com.nl2sql.gateway.policy.UserPolicyInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final UserPolicyInterceptor interceptor;

    public WebConfig(UserPolicyInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor)
                .addPathPatterns("/api/v1/task/**", "/api/v1/query/**", "/api/v1/eval/**", "/api/v1/admin/**")
                .excludePathPatterns("/api/v1/health");
    }
}
```

- [ ] **Step 4: PermissionService**

```java
package com.nl2sql.gateway.policy;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/** 数据权限（白名单语义，"*" 放行，库是否存在由引擎报错）+ 任务归属（taskowner 自有 key）。 */
@Service
public class PermissionService {

    private final StringRedisTemplate redis;

    public PermissionService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean dbAllowed(UserPolicyService.Policy policy, String dbId) {
        if (policy.allowedDbIds().contains("*")) {
            return true;
        }
        return dbId == null || dbId.isEmpty() || policy.allowedDbIds().contains(dbId);
    }

    public void setOwner(String taskId, String userTag) {
        redis.opsForValue().set("taskowner:" + taskId, userTag, Duration.ofSeconds(86400));
    }

    public boolean isOwnerOrAdmin(String taskId, UserPolicyService.Policy policy) {
        if (policy.isAdmin()) {
            return true;
        }
        String owner = redis.opsForValue().get("taskowner:" + taskId);
        return policy.userTag().equals(owner);
    }
}
```

- [ ] **Step 5: 单测**（UserPolicyServiceTest：miss 回填/负缓存/admin 更新删缓存，Mockito mock mapper+redis；PermissionServiceTest：通配/白名单/归属三态；拦截器用 MockHttpServletRequest 直测 preHandle：401 未知、403 禁用、403 非 admin 访问 eval、200 正常+属性注入）
- [ ] **Step 6: 全量绿 + 提交** `git commit -m "M3: 策略层 — 拦截器(401/403/角色)+cache-aside+数据权限/任务归属"`

---

### Task 3: 限流 + 配额（Lua）

**Files:** ratelimit/RateLimiterService.java、QuotaService.java；测试 ratelimit/RateLimiterServiceTest（集成：真 Redis）

- [ ] **Step 1: RateLimiterService**

```java
package com.nl2sql.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** 滑动窗口限流：ZSET + Lua 原子（清理窗口外→计数→未满则占位）。fail-open。 */
@Service
public class RateLimiterService {

    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>("""
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local member = ARGV[4]
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            if redis.call('ZCARD', key) < limit then
                redis.call('ZADD', key, now, member)
                redis.call('PEXPIRE', key, window)
                return 1
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Counter userRejected;
    private final Counter globalRejected;

    public RateLimiterService(StringRedisTemplate redis, MeterRegistry meters) {
        this.redis = redis;
        this.userRejected = Counter.builder("gateway.ratelimit.rejected").tag("type", "user")
                .register(meters);
        this.globalRejected = Counter.builder("gateway.ratelimit.rejected").tag("type", "global")
                .register(meters);
    }

    public boolean allowUser(String userTag, int qpsLimit) {
        return check("rl:" + userTag, 1000, qpsLimit, userRejected);
    }

    public boolean allowGlobal(int globalQps) {
        return check("rl:__global__", 1000, globalQps, globalRejected);
    }

    private boolean check(String key, long windowMs, int limit, Counter rejected) {
        try {
            String member = System.currentTimeMillis() + "-" + ThreadLocalRandom.current().nextLong(1L << 40);
            Long r = redis.execute(SCRIPT, List.of(key),
                    String.valueOf(System.currentTimeMillis()),
                    String.valueOf(windowMs), String.valueOf(limit), member);
            if (r != null && r == 1) {
                return true;
            }
            rejected.increment();
            return false;
        } catch (Exception e) {
            return true;   // fail-open：限流器故障不拒绝流量
        }
    }
}
```

- [ ] **Step 2: QuotaService**

```java
package com.nl2sql.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/** 日配额：quota:{tag}:{yyyyMMdd} INCR + 首次设 48h TTL。fail-open。 */
@Service
public class QuotaService {

    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>("""
            local cur = redis.call('INCR', KEYS[1])
            if cur == 1 then redis.call('EXPIRE', KEYS[1], 172800) end
            if cur > tonumber(ARGV[1]) then return 0 end
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Counter quotaRejected;

    public QuotaService(StringRedisTemplate redis, MeterRegistry meters) {
        this.redis = redis;
        this.quotaRejected = Counter.builder("gateway.quota.rejected").register(meters);
    }

    public boolean tryConsume(String userTag, long dailyQuota) {
        try {
            Long r = redis.execute(SCRIPT,
                    List.of("quota:" + userTag + ":" + LocalDate.now().toString().replace("-", "")),
                    String.valueOf(dailyQuota));
            if (r != null && r == 1) {
                return true;
            }
            quotaRejected.increment();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
```

- [ ] **Step 3: 集成测试**（继承 IntegrationTestBase，真 Redis）：
  - `allowUser` 限额 3：前 3 次 true、第 4 次 false
  - 并发 50 线程限额 10：通过数恰好 10（Lua 原子性验证——这是核心测试）
  - `tryConsume` 限额 2：两次 true 第三次 false；key TTL > 0
- [ ] **Step 4: 绿后提交** `git commit -m "M3: 限流+配额 — Lua 滑动窗口(并发恰好限额)+日配额+指标+fail-open"`

---

### Task 4: SETNX 幂等 + taskowner + submit 链 + 归属校验

**Files:** task/TaskStoreService.java（改）、TaskController.java（改）；测试 TaskControllerTest（改）、SubmitChainIntegrationTest（新）

- [ ] **Step 1: TaskStoreService 增方法**

```java
    /** SETNX 原子占位：成功=占住（返回 true）；失败=已被占（false，随后 GET 读走占位者 task_id）。 */
    public boolean idempotentTryAcquire(String key, String taskId) {
        return Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent("idempotent:" + key, taskId, TTL_IDEMPOTENT));
    }
```

- [ ] **Step 2: TaskController.submit 重构**（完整链，Policy 从拦截器属性取）

```java
    @PostMapping("/submit")
    public ResponseEntity<?> submit(@RequestBody SubmitRequest req, jakarta.servlet.http.HttpServletRequest http) {
        UserPolicyService.Policy policy =
                (UserPolicyService.Policy) http.getAttribute(UserPolicyInterceptor.POLICY_ATTR);

        if (!permission.dbAllowed(policy, req.db_id())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("detail", "db_id not allowed: " + req.db_id()));
        }
        if (!rateLimiter.allowGlobal(props.ratelimit().globalQps())
                || !rateLimiter.allowUser(policy.userTag(), policy.qpsLimit())) {
            return tooMany("rate limit exceeded");
        }
        String idemKey = null;
        if (req.idempotency_key() != null && !req.idempotency_key().isBlank()) {
            idemKey = TaskIdempotency.key(req.idempotency_key(), req.question());
            String existing = store.idempotentGet(idemKey);
            if (existing != null) {
                return ResponseEntity.accepted().body(Map.of("task_id", existing, "status", "PENDING"));
            }
            String candidate = TaskIdempotency.newTaskId();
            if (!store.idempotentTryAcquire(idemKey, candidate)) {
                String winner = store.idempotentGet(idemKey);
                return ResponseEntity.accepted().body(Map.of(
                        "task_id", winner != null ? winner : candidate, "status", "PENDING"));
            }
        }
        if (!quota.tryConsume(policy.userTag(), policy.dailyQuota())) {
            return tooMany("daily quota exceeded");
        }
        // qcache（Task 5 接入：命中→直接 SUCCESS 任务）
        String taskId = idemKey != null
                ? java.util.Optional.ofNullable(store.idempotentGet(idemKey)).orElse(TaskIdempotency.newTaskId())
                : TaskIdempotency.newTaskId();
        return accepted(taskId, req, policy);
    }

    private ResponseEntity<Map<String, String>> tooMany(String detail) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("detail", detail));
    }
```

`accepted(taskId, req, policy)` 在原 accepted 基础上增：`permission.setOwner(taskId, policy.userTag());`。

- [ ] **Step 3: 归属校验**（status/taskHealth/cancel/feedback/stream 五个端点开头统一加）

```java
    private ResponseEntity<?> requireOwner(String taskId, HttpServletRequest http) {
        UserPolicyService.Policy policy =
                (UserPolicyService.Policy) http.getAttribute(UserPolicyInterceptor.POLICY_ATTR);
        if (!permission.isOwnerOrAdmin(taskId, policy)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("detail", "not task owner"));
        }
        return null;
    }
```

（stream 端点为 void 返回，独立写 403 分支。）scan-stale 加 `@RequireRole("admin")`。

- [ ] **Step 4: 集成测试 SubmitChainIntegrationTest**（真 Redis/Kafka/MySQL + 已种子策略）：
  - 无 X-User-Id → 401；X-User-Id: ghost → 401（负缓存生效后仍 401）
  - local_user 提交 california_schools → 202 + Redis 状态 + Kafka 信封 + `taskowner:{id}`=local_user
  - 构造受限策略（直接 mapper.upsert 一个 bench_u01 只允许 imdb）→ bench_u01 提交 california_schools → 403
  - bench_u01 提交他人任务 status → 403；bench_admin → 200
  - 幂等并发：两线程同 key 同时提交 → 同一 task_id 且 Kafka 信封仅一条
- [ ] **Step 5: TaskControllerTest 适配**（mock policy 属性/permission/rateLimiter/quota）+ 全量绿 + 提交
  `git commit -m "M3: submit 校验链(权限→限流→SETNX 幂等→配额) + 任务归属 403 + eval/scan-stale admin"`

---

### Task 5: qcache + 熔断降级

**Files:** cache/ResultCacheService.java；TaskController（改）；ProxyController（改）；测试 cache/ResultCacheServiceTest、SubmitChainIntegrationTest（增两例）

- [ ] **Step 1: ResultCacheService**

```java
package com.nl2sql.gateway.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.config.GatewayProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;

/** qcache:{sha256(question|db_id)} → 完整结果 JSON；只缓存 SUCCESS 且 sql 非空；TTL±10% 抖动。 */
@Service
public class ResultCacheService {

    private final StringRedisTemplate redis;
    private final ObjectMapper om;
    private final GatewayProperties props;
    private final Counter hits;
    private final Counter misses;

    public ResultCacheService(StringRedisTemplate redis, ObjectMapper om,
                              GatewayProperties props, MeterRegistry meters) {
        this.redis = redis;
        this.om = om;
        this.props = props;
        this.hits = Counter.builder("gateway.cache").tag("result", "hit").register(meters);
        this.misses = Counter.builder("gateway.cache").tag("result", "miss").register(meters);
    }

    public static String key(String question, String dbId) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256")
                    .digest((question + "|" + (dbId == null ? "" : dbId)).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("qcache:");
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 命中返回完整结果 JSON（status/sql/exec_result/token_usage/node_timings），未命中 null。 */
    public String get(String question, String dbId) {
        try {
            String v = redis.opsForValue().get(key(question, dbId));
            if (v != null) {
                hits.increment();
            } else {
                misses.increment();
            }
            return v;
        } catch (Exception e) {
            return null;   // 缓存层故障降级为 miss
        }
    }

    public void put(String question, String dbId, String resultJson) {
        try {
            redis.opsForValue().set(key(question, dbId), resultJson,
                    com.nl2sql.gateway.policy.UserPolicyService.jitter(
                            Duration.ofSeconds(props.cache().resultTtlSeconds()), 0.1));
        } catch (Exception ignore) {
            // 写缓存失败不影响主流程
        }
    }
}
```

- [ ] **Step 2: submit 接入**（配额之后）

```java
        String cached = resultCache.get(req.question(), req.db_id());
        if (cached != null) {
            String taskId = TaskIdempotency.newTaskId();
            store.createFromCache(taskId, req.question(), req.db_id() == null ? "" : req.db_id(), cached);
            permission.setOwner(taskId, policy.userTag());
            recorder.markDirty(taskId);
            return ResponseEntity.accepted().body(Map.of("task_id", taskId, "status", "SUCCESS",
                    "cache_hit", "1"));
        }
```

TaskStoreService 增 `createFromCache`：反序列化 cached → 填 sql/exec_result/token_usage/node_timings + `cache_hit` 额外字段 + status=SUCCESS + 终态 TTL（不走 Kafka）。

- [ ] **Step 3: 回写**——TaskEventRecorder 的 flush 里，终态 SUCCESS 且 sql 非空时 `resultCache.put(state.question, state.db_id, 完整JSON)`（Task 6 实现时一并落）
- [ ] **Step 4: ProxyController 熔断降级**——`EngineUnavailableException` catch 处：若 POST 且路径 `/api/v1/query` 或 `/api/v1/query/full`，从 body 解析 question/db_id，`resultCache.get` 命中则 200 + 头 `X-Cache: stale` 返回；未命中维持 503
- [ ] **Step 5: 测试**：
  - ResultCacheServiceTest：key 稳定向量、put/get 往返、TTL 在 [0.9T, 1.1T] 区间（真 Redis TTL 断言）
  - 集成：预置 qcache → 提交同问题 → 202 cache_hit=1、Redis 状态 SUCCESS、**引擎(MockWebServer)与 Kafka 均零新请求**
  - ProxyControllerTest 增降级例：引擎断连 + qcache 预置 → 200 X-Cache: stale
- [ ] **Step 6: 提交** `git commit -m "M3: qcache 结果缓存 — submit 短路/回写/熔断降级(X-Cache: stale)+指标"`

---

### Task 6: Recorder（消费→批量落库）+ Admin

**Files:** recorder/TaskEventRecorder.java、admin/AdminController.java、KafkaTopics（改）；测试 recorder/TaskEventRecorderTest（集成）、AdminControllerTest

- [ ] **Step 1: KafkaTopics 增补 status/result/dlq 三个 NewTopic bean**（1 分区 rf 1，与 broker.py ALL_TOPICS 对齐）
- [ ] **Step 2: TaskEventRecorder**

```java
package com.nl2sql.gateway.recorder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.cache.ResultCacheService;
import com.nl2sql.gateway.task.TaskState;
import com.nl2sql.gateway.task.TaskStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** write-behind 落库：Kafka 事件只当变脏信号；flush 时以 Redis 状态为准组装 task_record 行。 */
@Service
public class TaskEventRecorder {

    private static final int MAX_PENDING = 10_000;
    private static final int BATCH = 100;

    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private final TaskStoreService store;
    private final TaskRecordMapper mapper;
    private final ResultCacheService resultCache;
    private final ObjectMapper om;
    private final Timer flushTimer;

    public TaskEventRecorder(TaskStoreService store, TaskRecordMapper mapper,
                             ResultCacheService resultCache, ObjectMapper om, MeterRegistry meters) {
        this.store = store;
        this.mapper = mapper;
        this.resultCache = resultCache;
        this.om = om;
        this.flushTimer = Timer.builder("gateway.recorder.flush").register(meters);
    }

    @KafkaListener(topics = {"nl2sql.task.status", "nl2sql.task.result"},
            groupId = "nl2sql-ops-recorder")
    public void onEvent(String raw) {
        try {
            JsonNode env = om.readTree(raw);
            String taskId = env.path("task_id").asText(null);
            if (taskId != null) {
                markDirty(taskId);
            }
        } catch (Exception ignore) {
            // 非 JSON 信封，忽略
        }
    }

    public void markDirty(String taskId) {
        if (dirty.size() >= MAX_PENDING && dirty.add(taskId)) {
            dirty.iterator().remove();   // 满则丢一个旧标记（最终一致容忍）
        } else {
            dirty.add(taskId);
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void flush() {
        if (dirty.isEmpty()) {
            return;
        }
        List<String> batch = new ArrayList<>(Math.min(BATCH, dirty.size()));
        for (String id : dirty) {
            if (batch.size() >= BATCH) {
                break;
            }
            if (dirty.remove(id)) {
                batch.add(id);
            }
        }
        flushTimer.record(batch.size(), java.util.concurrent.TimeUnit.MILLISECONDS, () -> {
            for (String id : batch) {
                try {
                    record(id);
                } catch (Exception e) {
                    // 单行失败不拖垮批次
                }
            }
        });
    }

    private void record(String taskId) throws Exception {
        TaskState s = store.get(taskId);
        if (s == null) {
            return;
        }
        String owner = store.owner(taskId);
        TaskRecord r = new TaskRecord();
        r.taskId = taskId;
        r.userTag = owner == null ? "unknown" : owner;
        r.dbId = s.db_id;
        r.question = s.question;
        r.status = s.status;
        r.sqlText = s.sql;
        Object exec = s.exec_result;
        if (exec instanceof java.util.Map<?, ?> m) {
            Object rc = m.get("row_count");
            r.rowCount = rc instanceof Number n ? n.intValue() : null;
        }
        r.retryCount = s.retry_count;
        r.error = s.error == null ? null : String.valueOf(s.error);
        r.cacheHit = "SUCCESS".equals(s.status) && s.sql != null && cachedFlag(s);
        r.submittedAt = parse(s.created_at);
        r.startedAt = parse(s.updated_at);
        r.finishedAt = "SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "TIMEOUT".equals(s.status) || "CANCELLED".equals(s.status) ? parse(s.updated_at) : null;
        if (r.submittedAt != null && r.finishedAt != null) {
            r.totalLatencyMs = (int) java.time.Duration.between(r.submittedAt, r.finishedAt).toMillis();
        }
        r.nodeTimings = om.writeValueAsString(s.node_timings);
        r.tokenUsage = om.writeValueAsString(s.token_usage);
        mapper.upsert(r);

        if ("SUCCESS".equals(s.status) && s.sql != null && !s.sql.isBlank()) {
            resultCache.put(s.question, s.db_id, om.writeValueAsString(java.util.Map.of(
                    "status", s.status, "sql", s.sql, "exec_result", s.exec_result,
                    "token_usage", s.token_usage, "node_timings", s.node_timings)));
        }
    }

    private boolean cachedFlag(TaskState s) {
        return Boolean.TRUE.equals(com.fasterxml.jackson.databind.node.JsonNodeFactory.instance
                .booleanNode(false).asBoolean()) && s.cacheHitFlag();  // 见 TaskState 增补
    }

    static OffsetDateTime parse(String iso) {
        try {
            return OffsetDateTime.parse(iso);
        } catch (Exception e) {
            return null;
        }
    }
}
```

（TaskState 增补可空 `Boolean cacheHitFlag`，@JsonIgnore 序列化时映射到 state JSON 的 `cache_hit` 字段——实现时用 `@JsonProperty("cache_hit")`。`cachedFlag` 直接返回 `Boolean.TRUE.equals(s.cacheHitFlag())`，上面那行占位写法作废。GatewayApplication 加 `@EnableScheduling`。）

- [ ] **Step 3: AdminController**

```java
package com.nl2sql.gateway.admin;

import com.nl2sql.gateway.policy.RequireRole;
import com.nl2sql.gateway.policy.UserPolicy;
import com.nl2sql.gateway.policy.UserPolicyService;
import com.nl2sql.gateway.recorder.TaskRecordMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final UserPolicyService policyService;
    private final TaskRecordMapper taskRecordMapper;

    public AdminController(UserPolicyService policyService, TaskRecordMapper taskRecordMapper) {
        this.policyService = policyService;
        this.taskRecordMapper = taskRecordMapper;
    }

    public record UpdatePolicyRequest(String role, List<String> allowed_db_ids,
                                      Integer qps_limit, Long daily_quota, Integer status) {
    }

    @PutMapping("/users/{tag}")
    @RequireRole("admin")
    public ResponseEntity<?> updateUser(@PathVariable String tag, @RequestBody UpdatePolicyRequest req) {
        UserPolicy row = new UserPolicy();
        row.userTag = tag;
        row.role = req.role() == null ? "user" : req.role();
        row.allowedDbIds = toJson(req.allowed_db_ids() == null ? List.of("*") : req.allowed_db_ids());
        row.qpsLimit = req.qps_limit() == null ? 10 : req.qps_limit();
        row.dailyQuota = req.daily_quota() == null ? 1000 : req.daily_quota();
        row.status = req.status() == null ? 1 : req.status();
        return ResponseEntity.ok(Map.of("user_tag", tag,
                "policy", policyService.update(row)));
    }

    @GetMapping("/stats")
    @RequireRole("admin")
    public Map<String, Object> stats() {
        List<Map<String, Object>> byStatus = taskRecordMapper.selectMaps(
                new com.baomidou.mybatisplus.core.conditions.query.QueryWrapper<com.nl2sql.gateway.recorder.TaskRecord>()
                        .select("status", "COUNT(*) AS cnt", "AVG(total_latency_ms) AS avg_ms",
                                "SUM(cache_hit) AS cache_hits")
                        .groupBy("status"));
        return Map.of("by_status", byStatus);
    }

    private static String toJson(List<String> l) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(l);
        } catch (Exception e) {
            return "[\"*\"]";
        }
    }
}
```

- [ ] **Step 4: 测试**（集成）：
  - 建 Redis 任务状态（模拟 Worker 写）→ 向 `nl2sql.task.result` 用 KafkaTemplate 发 success 事件 → 等待 flush（Awaitility 或 sleep 3s）→ MySQL `task_record` 断言行内容（status/sql/latency/owner）
  - qcache 回写断言：flush 后 `qcache:{sha256}` 存在
  - admin upsert 后 `policy:{tag}` 缓存被删（下一次请求生效）
- [ ] **Step 5: 提交** `git commit -m "M3: recorder(Kafka→批量upsert)+qcache回写+admin端点(upsert删缓存/stats)"`

---

### Task 7: 全链路集成测试 + UI/compose 接入

- [ ] **Step 1: ui/app.py 4 处调用加头**（模块顶部 `API_HEADERS = {"X-User-Id": os.getenv("NL2SQL_USER", "local_user")}`；4 个 httpx 调用传 `headers=API_HEADERS`）
- [ ] **Step 2: compose gateway-java 增 `MYSQL_HOST=mysql`、`MYSQL_PORT=3306`、`MYSQL_USER=root`、`MYSQL_PASSWORD=nl2sql_root`**
- [ ] **Step 3: 全量测试 + 提交** `git commit -m "M3: UI 用户头 + compose MySQL 接入"`

---

### Task 8: E2E 实测 + 文档 + 合并

- [ ] **Step 1: 起全栈（compose redis/kafka/mysql + 宿主引擎/Worker/网关）实测**：
  - 无头 401 / ghost 401 / local_user 202
  - admin 造受限用户 → 越库 403
  - 低 QPS 用户 curl 快发 → 429
  - 同问题二连发 → 第二次 `cache_hit:1` 且秒回
  - 跑 3 个真任务 → MySQL task_record 3 行、node_timings/token_usage 完整
  - `curl :8080/metrics | grep gateway_`
- [ ] **Step 2: CHANGELOG v0.6.2 + README 架构图补权限/缓存/落库标注**
- [ ] **Step 3: 合并 master + push**

---

## 验收清单（对照规格 §14 M3）

- [ ] 401（无头/未知）/ 403（封禁/越库/越权/非 admin）/ 429（限流/配额）全可演示
- [ ] 限流并发原子性：50 并发限 10 → 恰好 10 通过（测试钉死）
- [ ] 幂等并发：同 key 两线程 → 同 task_id、Kafka 仅一条
- [ ] 缓存命中路径不触引擎/Kafka，cache_hit=1 毫秒级；admin 改策略下请求即生效
- [ ] MySQL task_record 有完整生命周期（含 cache_hit 任务）；指标四组可查
- [ ] 测试全绿（Testcontainers：Redis+Kafka+MySQL）

## Self-Review 记录

- 规格覆盖：§6 策略/校验点/顺序→Task 2/4；§7.2-7.5 缓存/幂等/限流/write-behind→Task 3/4/5/6；§7.6 熔断降级→Task 5；§8 表→Task 1；§9 key→Task 2/3/5；§12 故障表→fail-open/503 已注；§10 seed_users 留 M4 ✓
- 占位符：Task 6 `cachedFlag` 的占位写法已显式标注作废与替代（TaskState.cacheHitFlag）✓
- 类型一致性：Policy record 字段（userTag/role/allowedDbIds/qpsLimit/dailyQuota/enabled）跨 Task 2/4/6 引用一致；`store.owner()` 需在 Task 4 一并实现（PermissionService 之外供 recorder 用）✓
- 已知实现期决策点：MySQLContainer 用户名不能用 root（容器保留名）→ ops/ops；@EnableScheduling 位置；Awaitility 是否引入（可 sleep 3s 替代避免新依赖）
