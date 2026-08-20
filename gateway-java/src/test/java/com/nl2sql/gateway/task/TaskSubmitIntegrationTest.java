package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskSubmitIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper om = new ObjectMapper();

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void submitPersistsRedisStateAndProducesKafkaEnvelope() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        ResponseEntity<Map> resp = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                new HttpEntity<>("{\"question\":\"avg?\",\"db_id\":\"california_schools\"}", headers),
                Map.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        String taskId = (String) resp.getBody().get("task_id");

        // Redis 状态存在且为 PENDING（key 布局与 Python 一致）
        String raw = redisTemplate.opsForValue().get("task:" + taskId);
        assertThat(raw).isNotNull();
        Map<String, Object> state = om.readValue(raw, Map.class);
        assertThat(state.get("status")).isEqualTo("PENDING");
        assertThat(state.get("db_id")).isEqualTo("california_schools");
        assertThat(state.get("progress")).isEqualTo(0);

        // Kafka 信封到达 request topic，key=task_id
        Map<String, Object> props = new java.util.HashMap<>();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "it-" + System.nanoTime());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        boolean found;
        try (var consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
            consumer.subscribe(List.of("nl2sql.task.request"));
            List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recs =
                    new java.util.ArrayList<>();
            consumer.poll(Duration.ofSeconds(15)).records("nl2sql.task.request").forEach(recs::add);
            found = recs.stream()
                    .map(ConsumerRecord::value)
                    .map(v -> {
                        try {
                            return om.readValue(v, Map.class);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(e -> taskId.equals(e.get("task_id")))
                    .peek(e -> {
                        assertThat(e.get("event")).isEqualTo("submitted");
                        assertThat((Map<String, Object>) e.get("payload"))
                                .containsEntry("question", "avg?")
                                .containsEntry("db_id", "california_schools");
                    })
                    .findFirst()
                    .isPresent();
        }
        assertThat(found).as("kafka envelope for %s", taskId).isTrue();

        // 任务端点没有被代理到引擎（MockWebServer 零请求）
        assertThat(engine.getRequestCount()).isZero();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void idempotentResubmitReturnsSameTaskId() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        String body = "{\"question\":\"dup?\",\"idempotency_key\":\"k-integration\"}";

        ResponseEntity<Map> first = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                new HttpEntity<>(body, headers), Map.class);
        ResponseEntity<Map> second = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                new HttpEntity<>(body, headers), Map.class);

        assertThat(first.getBody().get("task_id")).isEqualTo(second.getBody().get("task_id"));
    }
}
