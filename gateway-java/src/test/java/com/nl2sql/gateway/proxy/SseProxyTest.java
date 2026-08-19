package com.nl2sql.gateway.proxy;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SseProxyTest {

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
    void streamsSseBodyAndContentType() throws Exception {
        String sse = "event: token\ndata: {\"t\":\"SEL\"}\n\n"
                + "event: complete\ndata: {\"sql\":\"SELECT 1\"}\n\n";
        engine.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody(sse));

        ResponseEntity<Resource> resp =
                rest.getForEntity("/api/v1/query/full/stream", Resource.class);

        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getHeaders().getContentType().toString())
                .contains("text/event-stream");

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
        // 消费请求记录，保持与其他测试的队列隔离
        assertThat(engine.takeRequest().getPath()).isEqualTo("/api/v1/query/full/stream");
    }
}
