package com.nl2sql.gateway.task;

import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;

/** M2 集成测试基座：真实 Redis + Kafka（Testcontainers）+ MockWebServer 充当引擎。
 *  容器在静态块启动（先于 Spring 上下文），连接信息经 DynamicPropertySource 手动接线
 *  （Boot 3.3 不识别新版 org.testcontainers.kafka.KafkaContainer 的 @ServiceConnection）。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTestBase {

    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.7.1");

    static final org.testcontainers.containers.MySQLContainer<?> mysql =
            new org.testcontainers.containers.MySQLContainer<>("mysql:8.4")
                    .withDatabaseName("nl2sql_ops").withUsername("ops").withPassword("ops");

    static {
        redis.start();
        kafka.start();
        mysql.start();
    }

    public static final MockWebServer engine = new MockWebServer();

    /** 当前生效的引擎。engine.base-url 只在 props() 注册一次，supplier 惰性读此字段——
     *  若基类与子类都 registry.add 同名属性，后注册者胜，子类覆盖会被基类吃掉。 */
    protected static volatile MockWebServer activeEngine = engine;

    /** 子类在自己的 @DynamicPropertySource 方法里换成专属引擎（supplier 解析晚于全部注册，赋值即生效）。 */
    protected static void useEngine(MockWebServer server) throws IOException {
        if (server.getPort() == -1) {
            server.start();
        }
        activeEngine = server;
    }

    /** 子类 @AfterAll 释放专属引擎并恢复默认——否则后续无覆盖类的新上下文会解析到已关闭的引擎。 */
    protected static void releaseEngine(MockWebServer server) {
        try {
            server.shutdown();
        } catch (Exception ignore) {
            // 已关闭
        }
        activeEngine = engine;
    }

    private static final java.util.concurrent.atomic.AtomicBoolean ENGINE_STARTED =
            new java.util.concurrent.atomic.AtomicBoolean();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws IOException {
        if (ENGINE_STARTED.compareAndSet(false, true)) {
            engine.start();
        }
        registry.add("engine.base-url", () -> activeEngine.url("/").toString().replaceAll("/$", ""));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> String.valueOf(redis.getMappedPort(6379)));
        registry.add("spring.datasource.url", () -> mysql.getJdbcUrl());
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    @Autowired
    protected StringRedisTemplate redisTemplate;

    @Autowired
    protected TestRestTemplate rest;

    /** 策略闸门要求所有 /api/v1/** 请求带 X-User-Id；local_user 为 V1 种子用户（user 角色，通配全库）。 */
    @BeforeEach
    void attachUserHeader() {
        if (rest.getRestTemplate().getInterceptors().stream()
                .noneMatch(UserHeaderInterceptor.class::isInstance)) {
            rest.getRestTemplate().getInterceptors().add(new UserHeaderInterceptor());
        }
    }

    static final class UserHeaderInterceptor implements ClientHttpRequestInterceptor {
        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body,
                                           ClientHttpRequestExecution execution) throws IOException {
            if (!request.getHeaders().containsKey("X-User-Id")) {
                request.getHeaders().add("X-User-Id", "local_user");
            }
            return execution.execute(request, body);
        }
    }
}
