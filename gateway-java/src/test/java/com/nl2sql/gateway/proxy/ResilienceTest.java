package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.task.IntegrationTestBase;
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
class ResilienceTest extends IntegrationTestBase {

    /** 独立引擎：disconnect/NO_RESPONSE 残留响应不污染共享引擎队列（JDK HttpClient 重试会多消费队列项）。 */
    static final MockWebServer resilienceEngine = new MockWebServer();

    @DynamicPropertySource
    static void overrideEngine(DynamicPropertyRegistry registry) throws IOException {
        useEngine(resilienceEngine);
    }

    @AfterAll
    static void stopResilienceEngine() {
        releaseEngine(resilienceEngine);
    }

    @Autowired
    TestRestTemplate rest;

    @Autowired
    CircuitBreakerRegistry breakerRegistry;

    @Autowired
    com.nl2sql.gateway.cache.ResultCacheService resultCache;

    @Test
    void engineUnavailableFallsBackToStaleCache() {
        String cached = "{\"sql\":\"SELECT stale\",\"status\":\"SUCCESS\"}";
        resultCache.put("boom q?", "california_schools", cached);
        resilienceEngine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-User-Id", "bench_admin");
        headers.set("Content-Type", "application/json");
        ResponseEntity<String> resp = rest.exchange("/api/v1/query", org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(
                        "{\"question\":\"boom q?\",\"db_id\":\"california_schools\"}", headers),
                String.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getHeaders().getFirst("X-Cache")).isEqualTo("stale");
        assertThat(resp.getBody()).isEqualTo(cached);
    }

    @Test
    void engineDisconnectReturns503() {
        resilienceEngine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/query/t9-status", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(resp.getBody()).contains("engine unavailable");
    }

    @Test
    void breakerOpensAfterRepeatedFailuresThenFailsFast() {
        CircuitBreaker breaker = breakerRegistry.circuitBreaker("engine");
        // 6 次连接失败（窗口 4、阈值 50% → 达到 OPEN）
        for (int i = 0; i < 6; i++) {
            resilienceEngine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
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
        resilienceEngine.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        long start = System.nanoTime();
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/query/t8-status", String.class);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(resp.getStatusCode().value()).isEqualTo(503);
        assertThat(elapsedMs).isLessThan(5000); // 800ms TimeLimiter + 余量
    }
}
