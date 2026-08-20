package com.nl2sql.gateway.proxy;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProxyControllerTest {

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

    @Test
    void proxiesPostWithBodyAndCustomHeader() throws Exception {
        engine.enqueue(new MockResponse().setResponseCode(202).setBody("{\"task_id\":\"t1\"}"));
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-User-Id", "bench_u01");
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> resp = rest.exchange("/api/v1/query", HttpMethod.POST,
                new HttpEntity<>("{\"question\":\"q\"}", headers), String.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        assertThat(resp.getBody()).isEqualTo("{\"task_id\":\"t1\"}");

        RecordedRequest recorded = engine.takeRequest();
        assertThat(recorded.getMethod()).isEqualTo("POST");
        assertThat(recorded.getPath()).isEqualTo("/api/v1/query");
        assertThat(recorded.getHeader("X-User-Id")).isEqualTo("bench_u01");
        assertThat(recorded.getHeader("X-Trace-Id")).isNotBlank();
        assertThat(recorded.getBody().readUtf8()).isEqualTo("{\"question\":\"q\"}");
    }

    @Test
    void proxiesGetWithQueryString() throws Exception {
        engine.enqueue(new MockResponse().setBody("[]"));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/databases?verbose=1", String.class);
        assertThat(resp.getBody()).isEqualTo("[]");
        assertThat(engine.takeRequest().getPath()).isEqualTo("/api/v1/databases?verbose=1");
    }

    @Test
    void passesThroughEngineErrorStatus() throws Exception {
        engine.enqueue(new MockResponse().setResponseCode(500).setBody("{\"detail\":\"boom\"}"));
        ResponseEntity<String> resp = rest.getForEntity("/api/v1/health", String.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(500);
        assertThat(resp.getBody()).isEqualTo("{\"detail\":\"boom\"}");
        // 消费掉本测试的请求记录，避免污染共享 MockWebServer 队列
        assertThat(engine.takeRequest().getPath()).isEqualTo("/api/v1/health");
    }
}
