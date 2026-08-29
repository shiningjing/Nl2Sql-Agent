package com.nl2sql.gateway.cache;

import com.nl2sql.gateway.task.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** 真 Redis：key 稳定性、put/get 往返、TTL 抖动区间。 */
class ResultCacheServiceTest extends IntegrationTestBase {

    @Autowired
    ResultCacheService cache;

    @Test
    void keyIsDeterministicDbSensitiveAndNamespaced() {
        assertThat(ResultCacheService.key("q", "db1"))
                .isEqualTo(ResultCacheService.key("q", "db1"))
                .isNotEqualTo(ResultCacheService.key("q", "db2"))
                .isNotEqualTo(ResultCacheService.key("q2", "db1"))
                .startsWith("qcache:")
                .hasSize("qcache:".length() + 64);   // sha256 hex
    }

    @Test
    void putGetRoundtripWithJitteredTtl() {
        String json = "{\"status\":\"SUCCESS\",\"sql\":\"SELECT 1\"}";
        cache.put("roundtrip?", "california_schools", json);

        assertThat(cache.get("roundtrip?", "california_schools")).isEqualTo(json);
        assertThat(cache.get("roundtrip?", "other_db")).isNull();   // db 维度隔离

        Long ttl = redisTemplate.getExpire(ResultCacheService.key("roundtrip?", "california_schools"),
                TimeUnit.SECONDS);
        // 1800s ±10% 抖动（yml 默认 result-ttl-seconds=1800）
        assertThat(ttl).isBetween(1620L, 1980L);
    }
}
