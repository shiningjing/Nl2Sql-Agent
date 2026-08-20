package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.task.IntegrationTestBase;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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

class SseProxyTest extends IntegrationTestBase {

    /** 独立引擎：流式挂起诊断要求引擎队列严格独占（经 useEngine 生效）。 */
    static final MockWebServer sseEngine = new MockWebServer();

    @DynamicPropertySource
    static void overrideEngine(DynamicPropertyRegistry registry) throws IOException {
        useEngine(sseEngine);
    }

    @AfterAll
    static void stopSseEngine() {
        releaseEngine(sseEngine);
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void streamsSseBodyAndContentType() throws Exception {
        String sse = "event: token\ndata: {\"t\":\"SEL\"}\n\n"
                + "event: complete\ndata: {\"sql\":\"SELECT 1\"}\n\n";
        sseEngine.enqueue(new MockResponse()
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
        assertThat(sseEngine.takeRequest().getPath()).isEqualTo("/api/v1/query/full/stream");
    }
}
