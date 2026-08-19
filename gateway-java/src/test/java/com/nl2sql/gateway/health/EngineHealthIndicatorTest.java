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
