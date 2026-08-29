package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.policy.UserPolicy;
import com.nl2sql.gateway.policy.UserPolicyMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** submit 校验链 + 任务归属的端到端验证（真 Redis/Kafka/MySQL + V1 种子策略）。 */
class SubmitChainIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    UserPolicyMapper policyMapper;

    @LocalServerPort
    int port;

    private final ObjectMapper om = new ObjectMapper();

    private HttpEntity<String> json(String user, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        if (user != null) {
            headers.set("X-User-Id", user);
        }
        return new HttpEntity<>(body, headers);
    }

    private void upsertPolicy(String tag, String role, String dbsJson) {
        UserPolicy row = new UserPolicy();
        row.userTag = tag;
        row.role = role;
        row.allowedDbIds = dbsJson;
        row.qpsLimit = 100;
        row.dailyQuota = 100000L;
        row.status = 1;
        policyMapper.upsert(row);
        redisTemplate.delete("policy:" + tag);   // upsert 后清缓存，避免读到旧值
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Map> kafkaEnvelopesFor(String taskId) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "it-" + System.nanoTime());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        List<Map> hits = new ArrayList<>();
        try (var consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
            consumer.subscribe(List.of("nl2sql.task.request"));
            consumer.poll(Duration.ofSeconds(15)).forEach(r -> {
                try {
                    Map env = om.readValue(r.value(), Map.class);
                    if (taskId.equals(env.get("task_id"))) {
                        hits.add(env);
                    }
                } catch (Exception ignore) {
                    // 无关消息
                }
            });
        }
        return hits;
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void missingHeaderRejected401() {
        TestRestTemplate bare = new TestRestTemplate();
        ResponseEntity<Map> resp = bare.postForEntity(
                "http://localhost:" + port + "/api/v1/task/submit",
                json(null, "{\"question\":\"q\"}"), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat((String) resp.getBody().get("detail")).contains("X-User-Id");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void unknownUserRejected401WithNegativeCache() {
        ResponseEntity<Map> first = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                json("ghost-user", "{\"question\":\"q\"}"), Map.class);
        assertThat(first.getStatusCode().value()).isEqualTo(401);
        assertThat((String) first.getBody().get("detail")).contains("unknown user");

        // 负缓存写入且第二次仍 401（不再查库）
        assertThat(redisTemplate.opsForValue().get("policy:ghost-user")).isNotBlank();
        ResponseEntity<Map> second = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                json("ghost-user", "{\"question\":\"q\"}"), Map.class);
        assertThat(second.getStatusCode().value()).isEqualTo(401);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void localUserSubmitPersistsStateOwnerAndEnvelope() throws Exception {
        ResponseEntity<Map> resp = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                json("local_user", "{\"question\":\"chain?\",\"db_id\":\"california_schools\"}"), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        String taskId = (String) resp.getBody().get("task_id");

        assertThat(redisTemplate.opsForValue().get("task:" + taskId)).isNotNull();
        assertThat(redisTemplate.opsForValue().get("taskowner:" + taskId)).isEqualTo("local_user");

        List<Map> envelopes = kafkaEnvelopesFor(taskId);
        assertThat(envelopes).hasSize(1);
        assertThat(envelopes.get(0).get("event")).isEqualTo("submitted");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void restrictedUserForbiddenForNonWhitelistedDb() {
        upsertPolicy("bench_u01", "user", "[\"imdb\"]");

        ResponseEntity<Map> resp = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                json("bench_u01", "{\"question\":\"q\",\"db_id\":\"california_schools\"}"), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(403);
        assertThat((String) resp.getBody().get("detail")).contains("not allowed");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void ownershipStrangerForbiddenAdminAllowed() throws Exception {
        ResponseEntity<Map> created = rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                json("local_user", "{\"question\":\"owner?\"}"), Map.class);
        String taskId = (String) created.getBody().get("task_id");

        upsertPolicy("bench_u01", "user", "[\"*\"]");

        ResponseEntity<Map> stranger = rest.exchange("/api/v1/task/" + taskId + "/status",
                HttpMethod.GET, json("bench_u01", null), Map.class);
        assertThat(stranger.getStatusCode().value()).isEqualTo(403);
        assertThat((String) stranger.getBody().get("detail")).contains("not task owner");

        ResponseEntity<Map> admin = rest.exchange("/api/v1/task/" + taskId + "/status",
                HttpMethod.GET, json("bench_admin", null), Map.class);
        assertThat(admin.getStatusCode().value()).isEqualTo(200);
        assertThat(admin.getBody().get("task_id")).isEqualTo(taskId);

        ResponseEntity<Map> owner = rest.exchange("/api/v1/task/" + taskId + "/status",
                HttpMethod.GET, json("local_user", null), Map.class);
        assertThat(owner.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void concurrentSameKeyReturnsSameTaskIdWithSingleEnvelope() throws Exception {
        String body = "{\"question\":\"race?\",\"idempotency_key\":\"race-key-1\"}";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<Map>>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return rest.exchange("/api/v1/task/submit", HttpMethod.POST,
                        json("local_user", body), Map.class);
            }));
        }
        start.countDown();
        ResponseEntity<Map> a = futures.get(0).get(20, TimeUnit.SECONDS);
        ResponseEntity<Map> b = futures.get(1).get(20, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(a.getStatusCode().value()).isEqualTo(202);
        assertThat(b.getStatusCode().value()).isEqualTo(202);
        assertThat(a.getBody().get("task_id")).isEqualTo(b.getBody().get("task_id"));

        List<Map> envelopes = kafkaEnvelopesFor((String) a.getBody().get("task_id"));
        assertThat(envelopes).hasSize(1);   // SETNX：并发同 key 只产生一条任务信封
    }
}
