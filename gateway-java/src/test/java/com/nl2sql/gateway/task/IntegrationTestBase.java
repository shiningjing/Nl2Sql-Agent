package com.nl2sql.gateway.task;

import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
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

    static {
        redis.start();
        kafka.start();
    }

    static final MockWebServer engine = new MockWebServer();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws IOException {
        engine.start();
        registry.add("engine.base-url", () -> engine.url("/").toString().replaceAll("/$", ""));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> String.valueOf(redis.getMappedPort(6379)));
    }

    @AfterAll
    static void stopEngine() throws IOException {
        engine.shutdown();
    }

    @Autowired
    protected StringRedisTemplate redisTemplate;
}
