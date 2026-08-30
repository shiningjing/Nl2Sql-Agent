package com.nl2sql.gateway.admin;

import com.nl2sql.gateway.task.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AdminControllerTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    private HttpEntity<String> as(String user, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-User-Id", user);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void adminUpsertEvictsPolicyCache() {
        // 预热正缓存，upsert 后必须被删（下一次请求读新值）
        redisTemplate.opsForValue().set("policy:it_user", "{\"userTag\":\"it_user\"}",
                java.time.Duration.ofSeconds(60));

        ResponseEntity<Map> resp = rest.exchange("/api/v1/admin/users/it_user", HttpMethod.PUT,
                as("bench_admin", "{\"role\":\"user\",\"allowed_db_ids\":[\"imdb\"],"
                        + "\"qps_limit\":5,\"daily_quota\":9,\"status\":1}"),
                Map.class);

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        Map<String, Object> policy = (Map<String, Object>) resp.getBody().get("policy");
        assertThat(policy.get("qpsLimit")).isEqualTo(5);
        assertThat(redisTemplate.opsForValue().get("policy:it_user")).isNull();   // 缓存已删
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void nonAdminForbidden() {
        ResponseEntity<Map> resp = rest.exchange("/api/v1/admin/users/it_user", HttpMethod.PUT,
                as("local_user", "{\"role\":\"user\"}"), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(403);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void statsRequiresAdminAndReturnsByStatus() {
        ResponseEntity<Map> admin = rest.exchange("/api/v1/admin/stats", HttpMethod.GET,
                as("bench_admin", null), Map.class);
        assertThat(admin.getStatusCode().value()).isEqualTo(200);
        assertThat(admin.getBody()).containsKey("by_status");

        ResponseEntity<Map> user = rest.exchange("/api/v1/admin/stats", HttpMethod.GET,
                as("local_user", null), Map.class);
        assertThat(user.getStatusCode().value()).isEqualTo(403);
    }
}
