package com.nl2sql.gateway.proxy;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "resilience4j.circuitbreaker.instances.engine.slidingWindowSize=4",
                "resilience4j.circuitbreaker.instances.engine.failureRateThreshold=50",
                "resilience4j.circuitbreaker.instances.engine.waitDurationInOpenState=60s",
                "resilience4j.circuitbreaker.instances.engine.minimumNumberOfCalls=4",
                "resilience4j.timelimiter.instances.engine.timeoutDuration=800ms"
        })
class ResilienceTest {

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
    TestRestTemplate rest;

    @Autowired
    CircuitBreakerRegistry breakerRegistry;

    @Test
    void engineDisconnectReturns503() {
        engine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/query/t9-status", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(resp.getBody()).contains("engine unavailable");
    }

    @Test
    void breakerOpensAfterRepeatedFailuresThenFailsFast() {
        CircuitBreaker breaker = breakerRegistry.circuitBreaker("engine");
        // 6 次连接失败（窗口 4、阈值 50% → 达到 OPEN）
        for (int i = 0; i < 6; i++) {
            engine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
            rest.getForEntity("/api/v1/query/t9-status", String.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // OPEN 后快速失败：不等连接超时直接 503
        // （注：不用引擎请求计数证明——JDK HttpClient 对被重置的 GET 有透明重试，计数不可靠）
        long start = System.nanoTime();
        ResponseEntity<String> fast = rest.getForEntity("/api/v1/query/t9-status", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(fast.getStatusCode().value()).isEqualTo(503);
        assertThat(fast.getBody()).contains("circuit breaker open");
        assertThat(elapsedMs).isLessThan(500);
    }

    @Test
    void slowEngineTimesOut() {
        engine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        long start = System.nanoTime();
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/query/t8-status", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(elapsedMs).isLessThan(5000); // 800ms TimeLimiter + 余量
    }
}
