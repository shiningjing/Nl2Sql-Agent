# M1 Java 网关入口打通 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 Spring Boot 3.3 + JDK 21 服务替换 Go 网关接管 :8080 入口：`/api/v1/**` 透明代理到 FastAPI，同步路径带超时+熔断，SSE 流式透传，聚合健康与 Prometheus 指标，traceId 贯穿。

**Architecture:** 单体 Spring MVC 网关（虚拟线程），JDK HttpClient 做阻塞式转发（流式分支边读边 flush），Resilience4j CircuitBreaker + TimeLimiter 保护非流式路径，actuator 暴露 /health 与 /metrics（path-mapping 对齐 Go 网关习惯）。异步任务端点 M1 仅透传（Java 化在 M2）。

**Tech Stack:** Spring Boot 3.3.5 / JDK 21 / Resilience4j 2.2.0 / MockWebServer 4.12.0 / Maven / Docker multi-stage

**规格来源:** `docs/superpowers/specs/2026-08-19-java-task-gateway-design.md` §5 §7.6 §12 §14 M1 行

**已核实事实:**
- FastAPI 全部路由挂 `/api/v1` 前缀（`api/app.py:67-70`）；引擎健康路径 `GET /api/v1/health`
- UI 后端地址 = 环境变量 `API_BASE`（默认 `http://127.0.0.1:8000`，`ui/app.py:9`）→ 切网关零代码改动
- Go 网关行为（对齐基准）：`/health`、`/metrics` 本地处理，其余全部反代 + JSON 访问日志（`gateway/main.go`）
- SSE 端点特征：路径以 `/stream` 结尾（`/api/v1/query/full/stream`、`/api/v1/task/{id}/stream`）
- 本机现状：JDK 17（只有 17）、Maven 3.9.16、Docker 29.4 + Compose v5.1.1 → **JDK 21 需安装（scoop）或全程走 Docker 构建**

---

## 文件结构

```
gateway-java/
  pom.xml
  Dockerfile
  .gitignore
  src/main/resources/application.yml
  src/main/java/com/nl2sql/gateway/
    GatewayApplication.java          # 启动类 + @ConfigurationPropertiesScan
    config/ProxyProperties.java      # engine.* 配置（record）
    web/TraceIdFilter.java           # traceId 生成/MDC/X-Trace-Id 回写（最高优先级）
    web/AccessLogFilter.java         # JSON 访问日志（最低优先级，对齐 Go 格式）
    proxy/EngineResponse.java        # 转发结果 record
    proxy/HttpProxyService.java      # JDK HttpClient 转发（非流式 + 流式）
    proxy/ProxyController.java       # /api/v1/** 入口，分流 stream / 非stream
    health/EngineHealthIndicator.java# actuator 健康组件：探测引擎
  src/test/java/com/nl2sql/gateway/
    web/TraceIdFilterTest.java
    web/AccessLogFilterTest.java
    proxy/ProxyControllerTest.java   # @SpringBootTest + MockWebServer
    health/EngineHealthIndicatorTest.java
deployment/docker-compose.yml        # 增加 gateway-java 服务
```

职责边界：`ProxyController` 只做分流与错误映射；`HttpProxyService` 只管字节搬运；resilience 装饰在 Controller 层组装；filter 各管一件事。

---

### Task 0: JDK 21 前置

- [ ] **Step 1: 确认 JDK 21 可用性**

首选本地安装（scoop，不影响默认 JDK 17）：

```bash
scoop bucket list | grep -q java || scoop bucket add java
scoop install temurin21-jdk
ls ~/scoop/apps/temurin21-jdk/current/bin/java.exe && ~/scoop/apps/temurin21-jdk/current/bin/java.exe -version
```

Expected: `openjdk version "21.x.x"`

- [ ] **Step 2: 兜底方案（若 scoop 失败）**

全部构建/测试走 Docker（Task 1 起的 `mvn` 命令替换为）：

```bash
docker run --rm -v "F:\Experience\nl2sql-agent\gateway-java:/app" -v maven_repo:/root/.m2 -w /app maven:3.9-eclipse-temurin-21 mvn test
```

---

### Task 1: 工程骨架

**Files:**
- Create: `gateway-java/pom.xml`
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/GatewayApplication.java`
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/config/ProxyProperties.java`
- Create: `gateway-java/src/main/resources/application.yml`
- Create: `gateway-java/.gitignore`
- Create: `gateway-java/Dockerfile`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/GatewayApplicationTests.java`

- [ ] **Step 1: 写 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.5</version>
        <relativePath/>
    </parent>
    <groupId>com.nl2sql</groupId>
    <artifactId>gateway-java</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <name>nl2sql-gateway</name>
    <description>Spring Boot gateway for nl2sql-agent</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>io.github.resilience4j</groupId>
            <artifactId>resilience4j-spring-boot3</artifactId>
            <version>2.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>mockwebserver</artifactId>
            <version>4.12.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: 启动类与配置**

`GatewayApplication.java`:

```java
package com.nl2sql.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
```

`config/ProxyProperties.java`:

```java
package com.nl2sql.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "engine")
public record ProxyProperties(
        String baseUrl,
        String healthPath,
        int connectTimeoutMs,
        long responseTimeoutMs) {
}
```

`application.yml`:

```yaml
server:
  port: 8080

spring:
  application:
    name: gateway-java
  threads:
    virtual:
      enabled: true

engine:
  base-url: ${ENGINE_BASE_URL:http://127.0.0.1:8000}
  health-path: /api/v1/health
  connect-timeout-ms: 3000
  response-timeout-ms: 180000   # LLM 长流水线，超时交给 TimeLimiter 而不是连接层

management:
  endpoints:
    web:
      base-path: /
      exposure:
        include: health,prometheus
      path-mapping:
        prometheus: metrics
  endpoint:
    health:
      show-details: always

resilience4j:
  circuitbreaker:
    instances:
      engine:
        slidingWindowSize: 10
        failureRateThreshold: 50
        waitDurationInOpenState: 10s
        permittedNumberOfCallsInHalfOpenState: 3
  timelimiter:
    instances:
      engine:
        timeoutDuration: 180s
        cancelRunningFuture: true
```

`.gitignore`:

```
target/
*.iml
.idea/
```

- [ ] **Step 3: 上下文加载测试**

`GatewayApplicationTests.java`:

```java
package com.nl2sql.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "engine.base-url=http://127.0.0.1:1")
class GatewayApplicationTests {
    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 4: 构建验证（JAVA_HOME 指向 JDK 21）**

```bash
export JAVA_HOME=~/scoop/apps/temurin21-jdk/current
cd gateway-java && mvn -q test
```

Expected: `BUILD SUCCESS`，1 test passed。若 JDK 安装失败则用 Task 0 Step 2 的 Docker 命令。

- [ ] **Step 5: Dockerfile**

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/gateway-java-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

- [ ] **Step 6: Commit**

```bash
git add gateway-java
git commit -m "M1: gateway-java 工程骨架 — Spring Boot 3.3 + JDK 21 + 虚拟线程 + actuator/metrics 配置"
```

---

### Task 2: TraceIdFilter

**Files:**
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/web/TraceIdFilter.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/web/TraceIdFilterTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TraceIdFilterTest {

    private final TraceIdFilter filter = new TraceIdFilter();

    @Test
    void generatesTraceIdWhenAbsent() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/task");
        MockHttpServletResponse res = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);
        filter.doFilter(req, res, chain);
        String traceId = res.getHeader("X-Trace-Id");
        assertThat(traceId).hasSize(16).matches("[0-9a-f]+");
        assertThat(req.getAttribute("traceId")).isEqualTo(traceId);
        verify(chain).doFilter(any(), any());
        assertThat(MDC.get("traceId")).isNull(); // 已清理
    }

    @Test
    void reusesIncomingTraceId() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/task");
        req.addHeader("X-Trace-Id", "abc123def456abc7");
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, mock(FilterChain.class));
        assertThat(res.getHeader("X-Trace-Id")).isEqualTo("abc123def456abc7");
        assertThat(req.getAttribute("traceId")).isEqualTo("abc123def456abc7");
    }

    @Test
    void exposesTraceIdInMdcDuringChain() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/");
        MockHttpServletResponse res = new MockHttpServletResponse();
        final String[] seen = new String[1];
        filter.doFilter(req, res, (request, response) -> seen[0] = MDC.get("traceId"));
        assertThat(seen[0]).isNotBlank();
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=TraceIdFilterTest
```

Expected: COMPILATION ERROR（TraceIdFilter 不存在）

- [ ] **Step 3: 实现**

```java
package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/** 生成/复用 traceId：写入 MDC（日志用）、请求属性（代理转发用）、响应头（客户端可见）。 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter extends OncePerRequestFilter {

    public static final String TRACE_HEADER = "X-Trace-Id";
    public static final String TRACE_ATTR = "traceId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String incoming = request.getHeader(TRACE_HEADER);
        String traceId = (incoming != null && incoming.matches("[0-9a-zA-Z\\-]{4,64}"))
                ? incoming
                : UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        MDC.put(TRACE_ATTR, traceId);
        request.setAttribute(TRACE_ATTR, traceId);
        response.setHeader(TRACE_HEADER, traceId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(TRACE_ATTR);
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**

```bash
mvn -q test -Dtest=TraceIdFilterTest
```

Expected: PASS（3 tests）

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: TraceIdFilter — traceId 生成/复用 + MDC + X-Trace-Id 回写"
```

---

### Task 3: AccessLogFilter

**Files:**
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/web/AccessLogFilter.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/web/AccessLogFilterTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.nl2sql.gateway.web;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class AccessLogFilterTest {

    private final AccessLogFilter filter = new AccessLogFilter();
    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void attachAppender() {
        logger = (Logger) LoggerFactory.getLogger(AccessLogFilter.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
    }

    @Test
    void logsJsonAccessLine() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/v1/task/submit");
        req.setRemoteAddr("192.168.1.5");
        req.addHeader("X-Forwarded-For", "10.0.0.9");
        MockHttpServletResponse res = new MockHttpServletResponse();
        res.setStatus(202);
        filter.doFilter(req, res, (request, response) -> { });
        assertThat(appender.list).hasSize(1);
        String line = appender.list.get(0).getFormattedMessage();
        assertThat(line).contains("\"method\":\"POST\"")
                .contains("\"path\":\"/api/v1/task/submit\"")
                .contains("\"status\":202")
                .contains("\"elapsed_ms\":")
                .contains("\"client_ip\":\"10.0.0.9\"");
    }

    @Test
    void fallsBackToRemoteAddr() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/health");
        req.setRemoteAddr("127.0.0.1");
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, (request, response) -> { });
        String line = appender.list.get(0).getFormattedMessage();
        assertThat(line).contains("\"client_ip\":\"127.0.0.1\"");
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=AccessLogFilterTest
```

Expected: COMPILATION ERROR

- [ ] **Step 3: 实现**

```java
package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/** JSON 单行访问日志，格式对齐 Go 网关（method/path/status/elapsed_ms/client_ip + traceId）。 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class AccessLogFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(AccessLogFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long start = System.nanoTime();
        StatusCapture capture = new StatusCapture(response);
        try {
            filterChain.doFilter(request, capture);
        } finally {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            String clientIp = request.getHeader("X-Forwarded-For");
            if (clientIp == null || clientIp.isBlank()) {
                clientIp = request.getRemoteAddr();
            }
            log.info("{\"method\":\"{}\",\"path\":\"{}\",\"status\":{},\"elapsed_ms\":{},\"client_ip\":\"{}\",\"traceId\":\"{}\"}",
                    request.getMethod(), request.getRequestURI(), capture.status, elapsedMs,
                    clientIp, request.getAttribute(TraceIdFilter.TRACE_ATTR));
        }
    }

    private static final class StatusCapture extends jakarta.servlet.http.HttpServletResponseWrapper {
        private int status = 200;

        StatusCapture(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setStatus(int sc) {
            this.status = sc;
            super.setStatus(sc);
        }

        @Override
        public void sendError(int sc) throws IOException {
            this.status = sc;
            super.sendError(sc);
        }

        @Override
        public void sendError(int sc, String msg) throws IOException {
            this.status = sc;
            super.sendError(sc, msg);
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**

```bash
mvn -q test -Dtest=AccessLogFilterTest
```

Expected: PASS（2 tests）

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: AccessLogFilter — JSON 访问日志对齐 Go 网关格式"
```

---

### Task 4: 非流式透明代理

**Files:**
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/proxy/EngineResponse.java`
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/proxy/HttpProxyService.java`
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/proxy/ProxyController.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/proxy/ProxyControllerTest.java`

- [ ] **Step 1: 失败测试（透传：方法/路径/查询串/body/自定义头/状态码/响应体；X-Trace-Id 注入）**

```java
package com.nl2sql.gateway.proxy;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProxyControllerTest {

    static MockWebServer engine;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startEngine() throws IOException {
        engine = new MockWebServer();
        engine.start();
        System.setProperty("engine.base-url", engine.url("/").toString().replaceAll("/$", ""));
    }

    @AfterAll
    static void stopEngine() throws IOException {
        engine.shutdown();
    }

    @Test
    void proxiesPostWithBodyAndCustomHeader() throws Exception {
        engine.enqueue(new MockResponse().setResponseCode(202).setBody("{\"task_id\":\"t1\"}"));
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-User-Id", "bench_u01");
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> resp = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                new HttpEntity<>("{\"question\":\"q\"}", headers), String.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        assertThat(resp.getBody()).isEqualTo("{\"task_id\":\"t1\"}");

        RecordedRequest recorded = engine.takeRequest();
        assertThat(recorded.getMethod()).isEqualTo("POST");
        assertThat(recorded.getPath()).isEqualTo("/api/v1/task/submit");
        assertThat(recorded.getHeader("X-User-Id")).isEqualTo("bench_u01");
        assertThat(recorded.getHeader("X-Trace-Id")).isNotBlank();
        assertThat(recorded.getBody().readUtf8()).isEqualTo("{\"question\":\"q\"}");
    }

    @Test
    void proxiesGetWithQueryString() throws Exception {
        engine.enqueue(new MockResponse().setBody("[]"));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/task/abc/status?verbose=1", String.class);
        assertThat(resp.getBody()).isEqualTo("[]");
        assertThat(engine.takeRequest().getPath()).isEqualTo("/api/v1/task/abc/status?verbose=1");
    }

    @Test
    void passesThroughEngineErrorStatus() {
        engine.enqueue(new MockResponse().setResponseCode(500).setBody("{\"detail\":\"boom\"}"));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/health", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(500);
        assertThat(resp.getBody()).isEqualTo("{\"detail\":\"boom\"}");
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=ProxyControllerTest
```

Expected: COMPILATION ERROR（ProxyController 不存在）

- [ ] **Step 3: 实现**

`EngineResponse.java`:

```java
package com.nl2sql.gateway.proxy;

/** 非流式转发结果：状态码 + 响应头 + 响应体字节。 */
public record EngineResponse(int status, java.net.http.HttpHeaders headers, byte[] body) {
}
```

`HttpProxyService.java`:

```java
package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.config.ProxyProperties;
import com.nl2sql.gateway.web.TraceIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** JDK HttpClient 阻塞式转发（虚拟线程让阻塞 IO 成为合理选择）。 */
@Service
public class HttpProxyService {

    private static final Set<String> HOP_BY_HOP = Set.of("host", "connection", "content-length", "transfer-encoding", "keep-alive");

    private final ProxyProperties props;
    private final HttpClient httpClient;

    public HttpProxyService(ProxyProperties props) {
        this.props = props;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .build();
    }

    /** 非流式转发：完整读取引擎响应。 */
    public EngineResponse forward(HttpServletRequest request, byte[] body) throws IOException, InterruptedException {
        HttpRequest engineReq = buildRequest(request, body, false);
        HttpResponse<byte[]> engineResp =
                httpClient.send(engineReq, HttpResponse.BodyHandlers.ofByteArray());
        return new EngineResponse(engineResp.statusCode(), engineResp.headers(), engineResp.body());
    }

    /** 流式转发：只拿响应头 + InputStream，由调用方边读边写。 */
    public HttpResponse<java.io.InputStream> forwardStream(HttpServletRequest request, byte[] body)
            throws IOException, InterruptedException {
        return httpClient.send(buildRequest(request, body, true),
                HttpResponse.BodyHandlers.ofInputStream());
    }

    private HttpRequest buildRequest(HttpServletRequest request, byte[] body, boolean stream)
            throws IOException {
        StringBuilder uri = new StringBuilder(props.baseUrl()).append(request.getRequestURI());
        if (request.getQueryString() != null) {
            uri.append('?').append(request.getQueryString());
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uri.toString()));

        var names = new HashSet<String>();
        request.getHeaderNames().asIterator().forEachRemaining(names::add);
        for (String name : names) {
            if (HOP_BY_HOP.contains(name.toLowerCase())) {
                continue;
            }
            List<String> values = Collections.list(request.getHeaders(name));
            if (!values.isEmpty()) {
                builder.header(name, String.join(",", values));
            }
        }
        String traceId = (String) request.getAttribute(TraceIdFilter.TRACE_ATTR);
        if (traceId != null && names.stream().noneMatch(n -> n.equalsIgnoreCase(TraceIdFilter.TRACE_HEADER))) {
            builder.header(TraceIdFilter.TRACE_HEADER, traceId);
        }
        // 覆盖式设置：以网关侧收到的头为准
        builder.headers(); // no-op 语义占位

        String method = request.getMethod();
        java.net.http.HttpRequest.BodyPublisher publisher =
                (body == null || body.length == 0)
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofByteArray(body);
        switch (method) {
            case "GET" -> builder.GET();
            case "DELETE" -> builder.DELETE();
            default -> builder.method(method, publisher);
        }
        return builder.build();
    }

    private static final class Collections extends java.util.Collections {}
}
```

注意：上面 `Collections` 内部类是笔误占位——实现时直接 `import java.util.Collections;` 并调用 `Collections.list(...)`，删除该内部类。

`ProxyController.java`:

```java
package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.config.ProxyProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.http.HttpHeaders;
import java.util.List;

/** /api/v1/** 统一入口：非流式走熔断+超时装饰，流式直转（Task 5）。 */
@RestController
public class ProxyController {

    private final HttpProxyService proxyService;

    public ProxyController(HttpProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @RequestMapping(value = "/api/v1/**",
            method = {org.springframework.web.bind.annotation.RequestMethod.GET,
                    org.springframework.web.bind.annotation.RequestMethod.POST,
                    org.springframework.web.bind.annotation.RequestMethod.PUT,
                    org.springframework.web.bind.annotation.RequestMethod.DELETE,
                    org.springframework.web.bind.annotation.RequestMethod.PATCH})
    public ResponseEntity<byte[]> proxy(HttpServletRequest request, byte[] body)
            throws IOException, InterruptedException {
        EngineResponse resp = proxyService.forward(request, body);
        HttpHeaders headers = new HttpHeaders();
        resp.headers().map().forEach(headers::put);
        headers.remove(HttpHeaders.TRANSFER_ENCODING);
        headers.remove(HttpHeaders.CONTENT_LENGTH);
        MediaType contentType = headers.getContentType();
        return ResponseEntity.status(resp.status())
                .headers(headers)
                .contentType(contentType != null ? contentType : MediaType.APPLICATION_OCTET_STREAM)
                .body(resp.body());
    }
}
```

（`java.net.http.HttpHeaders` 与 Spring `HttpHeaders` 同名，实现时用完整限定名或重命名 import 消歧。）

- [ ] **Step 4: 运行确认通过**

```bash
mvn -q test -Dtest=ProxyControllerTest
```

Expected: PASS（3 tests）。若 `engine.base-url` 系统属性注入不生效，改为 `@SpringBootTest(properties = ...)` + `@DynamicPropertySource`（MockWebServer 先 start 再注入 URL）。

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: 透明代理 — /api/v1/** 转发 FastAPI（方法/路径/查询串/头/body/状态全透传 + X-Trace-Id 注入）"
```

---

### Task 5: SSE 流式透传

**Files:**
- Modify: `gateway-java/src/main/java/com/nl2sql/gateway/proxy/ProxyController.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/proxy/SseProxyTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.nl2sql.gateway.proxy;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SseProxyTest {

    static MockWebServer engine;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void startEngine() throws IOException {
        engine = new MockWebServer();
        engine.start();
        System.setProperty("engine.base-url", engine.url("/").toString().replaceAll("/$", ""));
    }

    @AfterAll
    static void stopEngine() throws IOException {
        engine.shutdown();
    }

    @Test
    void streamsSseBodyAndContentType() {
        String sse = "event: token\ndata: {\"t\":\"SEL\"}\n\n"
                + "event: complete\ndata: {\"sql\":\"SELECT 1\"}\n\n";
        engine.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody(sse));

        ResponseEntity<org.springframework.core.io.Resource> resp =
                rest.getForEntity("/api/v1/query/full/stream", org.springframework.core.io.Resource.class);

        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getHeaders().getContentType()).asString().contains("text/event-stream");

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resp.getBody().getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        assertThat(lines).contains("event: token", "event: complete");
        assertThat(String.join("\n", lines)).contains("SELECT 1");
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=SseProxyTest
```

Expected: FAIL（流式路径当前走非流式分支，内容虽可能到达但没有流式语义/出错——以实际失败为准）

- [ ] **Step 3: 实现 — ProxyController 增加流式分支**

```java
    // ProxyController 内新增（注入 ProxyProperties）：
    private final com.nl2sql.gateway.config.ProxyProperties props;

    // @RequestMapping 方法体首部分流：
    @RequestMapping(value = "/api/v1/**", method = { /* 同前 */ })
    public void proxy(jakarta.servlet.http.HttpServletRequest request,
                      jakarta.servlet.http.HttpServletResponse response,
                      java.io.InputStream rawBody) throws IOException, InterruptedException {
        byte[] body = rawBody.readAllBytes();
        if (request.getRequestURI().endsWith("/stream")) {
            streamToClient(request, response, body);
        } else {
            responseToClient(executeWithResilience(() -> {
                try {
                    return proxyService.forward(request, body);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }), response);
        }
    }

    private void streamToClient(HttpServletRequest request, HttpServletResponse response, byte[] body)
            throws IOException, InterruptedException {
        java.net.http.HttpResponse<java.io.InputStream> engineResp;
        try {
            engineResp = proxyService.forwardStream(request, body);
        } catch (IOException e) {
            response.setStatus(503);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"engine unavailable\",\"traceId\":\""
                    + request.getAttribute(TraceIdFilter.TRACE_ATTR) + "\"}");
            return;
        }
        response.setStatus(engineResp.statusCode());
        String contentType = engineResp.headers().firstValue("Content-Type").orElse("application/octet-stream");
        response.setContentType(contentType);
        try (java.io.InputStream in = engineResp.body();
             java.io.OutputStream out = response.getOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();   // SSE：每读到就推，绝不攒
            }
        }
    }

    private void responseToClient(EngineResponse resp, HttpServletResponse response) throws IOException {
        response.setStatus(resp.status());
        String contentType = resp.headers().firstValue("Content-Type").orElse("application/octet-stream");
        response.setContentType(contentType);
        try (java.io.OutputStream out = response.getOutputStream()) {
            out.write(resp.body());
        }
    }
```

（`executeWithResilience` 在 Task 6 实现；Task 5 先内联 `Supplier` 直接调用，Task 6 再替换为熔断装饰。原 `ResponseEntity<byte[]>` 版本删除，统一为 void +HttpServletResponse。相应地 Task 4 的三个测试断言不变——TestRestTemplate 对 void+response 写出的端点同样工作。）

- [ ] **Step 4: 运行全部代理测试确认通过**

```bash
mvn -q test -Dtest='ProxyControllerTest,SseProxyTest'
```

Expected: PASS（5 tests）

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: SSE 流式透传 — /stream 端点边读边 flush，长连接不走熔断（连接超时仍生效）"
```

---

### Task 6: 超时 + 熔断（Resilience4j）

**Files:**
- Modify: `gateway-java/src/main/java/com/nl2sql/gateway/proxy/ProxyController.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/proxy/ResilienceTest.java`

- [ ] **Step 1: 失败测试（引擎挂 → 503；连续失败 → 熔断打开快速失败；慢响应 → TimeLimiter 超时）**

```java
package com.nl2sql.gateway.proxy;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "resilience4j.circuitbreaker.instances.engine.slidingWindowSize=4",
                "resilience4j.circuitbreaker.instances.engine.failureRateThreshold=50",
                "resilience4j.circuitbreaker.instances.engine.waitDurationInOpenState=60s",
                "resilience4j.timelimiter.instances.engine.timeoutDuration=800ms"
        })
class ResilienceTest {

    static MockWebServer engine;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    CircuitBreakerRegistry breakerRegistry;

    @BeforeAll
    static void startEngine() throws IOException {
        engine = new MockWebServer();
        engine.start();
        System.setProperty("engine.base-url", engine.url("/").toString().replaceAll("/$", ""));
    }

    @AfterAll
    static void stopEngine() throws IOException {
        engine.shutdown();
    }

    @Test
    void engineDownReturns503() {
        System.setProperty("engine.base-url", "http://127.0.0.1:1"); // 不可达端口
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/task/t9/status", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(resp.getBody()).contains("engine unavailable");
        System.setProperty("engine.base-url", engine.url("/").toString().replaceAll("/$", ""));
    }

    @Test
    void breakerOpensAfterRepeatedFailures() throws Exception {
        System.setProperty("engine.base-url", "http://127.0.0.1:1");
        for (int i = 0; i < 6; i++) {
            rest.getForEntity("/api/v1/task/t9/status", String.class);
        }
        assertThat(breakerRegistry.circuitBreaker("engine").getState())
                .isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN);
        // OPEN 后快速失败：不再等待连接超时
        long start = System.nanoTime();
        ResponseEntity<String> fast = rest.getForEntity("/api/v1/task/t9/status", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(fast.getStatusCode().value()).isEqualTo(503);
        assertThat(elapsedMs).isLessThan(500);
        System.setProperty("engine.base-url", engine.url("/").toString().replaceAll("/$", ""));
    }

    @Test
    void slowEngineTimesOut() {
        engine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        long start = System.nanoTime();
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/task/t8/status", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(elapsedMs).isLessThan(5000); // 800ms TimeLimiter + 余量，远小于 180s 默认
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=ResilienceTest
```

Expected: FAIL（503 语义/熔断未实现）

- [ ] **Step 3: 实现 — ProxyController 加 resilience 装饰**

```java
    // 新增成员与构造注入：
    private final io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker;
    private final io.github.resilience4j.timelimiter.TimeLimiter timeLimiter;
    private final java.util.concurrent.ExecutorService virtualExecutor =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    // 构造函数追加（Spring 注入两个 Registry）：
    public ProxyController(HttpProxyService proxyService,
                           io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry breakerRegistry,
                           io.github.resilience4j.timelimiter.TimeLimiterRegistry timeLimiterRegistry) {
        this.proxyService = proxyService;
        this.circuitBreaker = breakerRegistry.circuitBreaker("engine");
        this.timeLimiter = timeLimiterRegistry.timeLimiter("engine");
    }

    /** 非流式路径统一装饰：TimeLimiter(虚拟线程) + CircuitBreaker。 */
    private EngineResponse executeWithResilience(
            java.util.function.Supplier<EngineResponse> supplier) throws IOException {
        try {
            java.util.concurrent.CompletableFuture<EngineResponse> future =
                    java.util.concurrent.CompletableFuture.supplyAsync(supplier, virtualExecutor);
            java.util.concurrent.Callable<EngineResponse> timed =
                    timeLimiter.decorateFutureSupplier(() -> future);
            java.util.concurrent.Callable<EngineResponse> guarded =
                    circuitBreaker.decorateCallable(timed);
            return guarded.call();
        } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
            throw new EngineUnavailableException("circuit breaker open", e);
        } catch (io.github.resilience4j.timelimiter.TimeoutException e) {
            throw new EngineUnavailableException("engine timeout", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new EngineUnavailableException("engine unavailable", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EngineUnavailableException("interrupted", e);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException io) {
                throw new EngineUnavailableException("engine unavailable", io);
            }
            throw new EngineUnavailableException("engine unavailable", e);
        }
    }
```

新增异常与 503 处理：

```java
// proxy/EngineUnavailableException.java
package com.nl2sql.gateway.proxy;

public class EngineUnavailableException extends RuntimeException {
    public EngineUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

```java
// web/EngineUnavailableExceptionHandler.java（@RestControllerAdvice）
package com.nl2sql.gateway.web;

import com.nl2sql.gateway.proxy.EngineUnavailableException;
import com.nl2sql.gateway.web.TraceIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class EngineUnavailableExceptionHandler {

    @ExceptionHandler(EngineUnavailableException.class)
    public ResponseEntity<Map<String, String>> engineUnavailable(EngineUnavailableException e,
                                                                 HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "engine unavailable",
                        "detail", String.valueOf(e.getMessage()),
                        "traceId", String.valueOf(request.getAttribute(TraceIdFilter.TRACE_ATTR))));
    }
}
```

`breakerRegistry.circuitBreaker("engine")` 会拿 yml 中 `resilience4j.circuitbreaker.instances.engine` 的配置（测试属性覆盖生效）。注意 `forward` 里的 IOException 需包成 RuntimeException 传入 supplier（Task 5 已有 `try/catch` 包裹）。

- [ ] **Step 4: 运行全部测试**

```bash
mvn -q test
```

Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: Resilience4j — 非流式路径超时(TimeLimiter)+熔断(CircuitBreaker)，引擎故障快速 503"
```

---

### Task 7: 引擎健康探测 + /metrics

**Files:**
- Create: `gateway-java/src/main/java/com/nl2sql/gateway/health/EngineHealthIndicator.java`
- Test: `gateway-java/src/test/java/com/nl2sql/gateway/health/EngineHealthIndicatorTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.nl2sql.gateway.health;

import com.nl2sql.gateway.config.ProxyProperties;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class EngineHealthIndicatorTest {

    static MockWebServer engine;

    @BeforeAll
    static void start() throws IOException {
        engine = new MockWebServer();
        engine.start();
    }

    @AfterAll
    static void stop() throws IOException {
        engine.shutdown();
    }

    private EngineHealthIndicator indicator(String baseUrl) {
        return new EngineHealthIndicator(new ProxyProperties(
                baseUrl, "/api/v1/health", 500, 2000));
    }

    @Test
    void engineUpReportsUp() {
        engine.enqueue(new MockResponse().setBody("{\"status\":\"ok\"}"));
        Health health = indicator(engine.url("/").toString().replaceAll("/$", "")).health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void engineDownReportsDown() {
        Health health = indicator("http://127.0.0.1:1").health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void engineNon200ReportsDown() {
        engine.enqueue(new MockResponse().setResponseCode(500));
        Health health = indicator(engine.url("/").toString().replaceAll("/$", "")).health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }
}
```

- [ ] **Step 2: 运行确认失败**

```bash
mvn -q test -Dtest=EngineHealthIndicatorTest
```

Expected: COMPILATION ERROR

- [ ] **Step 3: 实现**

```java
package com.nl2sql.gateway.health;

import com.nl2sql.gateway.config.ProxyProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** actuator /health 的 engine 组件：GET {base-url}{health-path}，2s 超时。 */
@Component("engine")
public class EngineHealthIndicator implements HealthIndicator {

    private final ProxyProperties props;
    private final HttpClient httpClient;

    public EngineHealthIndicator(ProxyProperties props) {
        this.props = props;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .build();
    }

    @Override
    public Health health() {
        try {
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder(URI.create(props.baseUrl() + props.healthPath())).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                return Health.up().withDetail("httpStatus", 200).build();
            }
            return Health.down().withDetail("httpStatus", resp.statusCode()).build();
        } catch (Exception e) {
            return Health.down().withDetail("error", String.valueOf(e.getMessage())).build();
        }
    }
}
```

- [ ] **Step 4: 运行测试 + 启动验证 actuator 路径**

```bash
mvn -q test
mvn -q spring-boot:run &   # JAVA_HOME=21
sleep 20
curl -s http://127.0.0.1:8080/health | head -c 300   # 期望 JSON 含 "engine":{"status":"UP"} 或 DOWN
curl -s http://127.0.0.1:8080/metrics | head -5      # 期望 Prometheus 文本
kill %1
```

Expected: `/health` 返回聚合 JSON；`/metrics` 返回 `# HELP ...` 开头的 Prometheus 格式（含 jvm/tomcat/hikari 指标；engine 调用指标在 M3 补业务维度）

- [ ] **Step 5: Commit**

```bash
git add -A gateway-java
git commit -m "M1: EngineHealthIndicator + actuator — /health 聚合引擎状态，/metrics Prometheus 输出"
```

---

### Task 8: docker-compose 集成 + E2E 冒烟

**Files:**
- Modify: `deployment/docker-compose.yml`（app 服务后追加 gateway-java 服务）
- Create: `scripts/smoke_gateway.sh`

- [ ] **Step 1: compose 增加 gateway-java 服务（services: 下、volumes: 前）**

```yaml
  gateway-java:
    build:
      context: ../gateway-java
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - ENGINE_BASE_URL=http://app:8000
    depends_on:
      - app
    restart: unless-stopped
```

- [ ] **Step 2: 构建并起服务**

```bash
cd deployment && docker compose up -d --build gateway-java
docker compose ps gateway-java   # 期望 Up
```

- [ ] **Step 3: 冒烟脚本 `scripts/smoke_gateway.sh`**

```bash
#!/usr/bin/env bash
# M1 冒烟：网关健康、代理透传、SSE、引擎故障降级
set -e
GW=http://127.0.0.1:8080

echo "== /health 聚合（engine 组件状态）=="
curl -s $GW/health | grep -o '"engine":{"status":"[A-Z]*"'

echo "== /metrics Prometheus =="
curl -s $GW/metrics | head -3

echo "== 代理透传（引擎 /api/v1/health）=="
curl -s -o /dev/null -w "%{http_code}\n" $GW/api/v1/health   # 期望 200

echo "== X-Trace-Id 回写 =="
curl -s -D - -o /dev/null $GW/api/v1/health | grep -i x-trace-id

echo "== 引擎不可达降级（绕过 compose 直连测试需停 app，冒烟默认跳过，手动验证）=="
echo "OK"
```

```bash
chmod +x scripts/smoke_gateway.sh && bash scripts/smoke_gateway.sh
```

Expected: engine status UP、200、X-Trace-Id 头存在

- [ ] **Step 4: UI 切换冒烟（本地，compose 的 app 已起时）**

```bash
# Streamlit 指向 Java 网关
API_BASE=http://127.0.0.1:8080 streamlit run ui/app.py
# 手动验证：提交一个异步任务 + 一个同步查询，流式打字机正常、结果正常
```

Expected: UI 全功能正常（人工验证，M1 验收项）

- [ ] **Step 5: Commit**

```bash
git add deployment/docker-compose.yml scripts/smoke_gateway.sh
git commit -m "M1: docker-compose 集成 gateway-java(:8080) + 冒烟脚本"
```

---

### Task 9: 文档与 Go 网关退役标注

**Files:**
- Modify: `README.md`（架构图中 Go 网关替换为 Java 网关）
- Modify: `README_zh.md`（同上）
- Modify: `CHANGELOG.md`
- Modify: `gateway/main.go`（文件头注释标注退役）

- [ ] **Step 1: 三处文档更新**

- README.md / README_zh.md 架构图与描述：`Go Gateway(:8080)` → `Spring Gateway(:8080, gateway-java/)`；技术栈行加 `Java 21 / Spring Boot 3.3`
- CHANGELOG.md 顶部新增：

```markdown
## v0.6.0 (2026-08-XX)
- M1: Java 网关上线 — Spring Boot 3.3 + JDK 21 虚拟线程替换 Go 网关(:8080)
- /api/v1/** 透明代理 + SSE 流式透传 + Resilience4j 超时熔断 + traceId 贯穿
- Go 网关退役（目录保留至 M4 后删除）
```

- gateway/main.go 顶部加注释：

```go
// DEPRECATED (M1, 2026-08): 已被 gateway-java (Spring Boot) 替代，仅为参照保留，M4 后删除。
```

- [ ] **Step 2: Commit**

```bash
git add README.md README_zh.md CHANGELOG.md gateway/main.go
git commit -m "M1: 文档更新 — Java 网关上线、Go 网关标注退役"
```

---

## 验收清单（对照规格 §14 M1）

- [ ] UI 设 `API_BASE=http://127.0.0.1:8080` 后全功能可用（同步查询/流式/异步任务透传）
- [ ] 停掉 FastAPI → 同步查询快速 503（熔断打开 <500ms），恢复后自动闭合（waitDuration 10s）
- [ ] `/health` 返回 engine 组件状态；`/metrics` Prometheus 格式
- [ ] `docker compose up -d --build gateway-java` 一键起，宿主机无需 JDK
- [ ] `mvn test` 全绿

## Self-Review 记录

- 规格覆盖：M1 七项任务 → Task 1-9 全对应（骨架/代理/加固/健康/日志/部署/测试）✓
- 占位符：Task 4 Step 3 的 `Collections` 内部类与 `builder.headers()` 占位行已在文中显式标注为实现时清理项 ✓
- 类型一致性：`EngineResponse(status, headers, body)`、`forward/forwardStream`、`TraceIdFilter.TRACE_ATTR` 跨任务引用一致 ✓
- 已知实现期决策点（允许现场调整）：Task 4 Step 4 的属性注入方式（系统属性 vs DynamicPropertySource）；Task 5 对 Task 4 方法签名的 void 化重构
