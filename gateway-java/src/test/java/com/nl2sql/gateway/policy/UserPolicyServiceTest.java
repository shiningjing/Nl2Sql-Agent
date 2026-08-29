package com.nl2sql.gateway.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserPolicyServiceTest {

    /** 与 UserPolicyService 的负缓存哨兵一致（NUL+NEG），用 (char)0 构造避免源码控制字符。 */
    private static final String NEG = String.valueOf((char) 0) + "NEG";

    @Mock
    UserPolicyMapper mapper;

    @Mock
    StringRedisTemplate redis;

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);

    private UserPolicyService service;
    private final ObjectMapper om = new ObjectMapper();

    @BeforeEach
    void wire() {
        lenient().when(redis.opsForValue()).thenReturn(valueOps);
        service = new UserPolicyService(mapper, redis, om);
    }

    private UserPolicy row(String tag, String dbsJson) {
        UserPolicy row = new UserPolicy();
        row.userTag = tag;
        row.role = "user";
        row.allowedDbIds = dbsJson;
        row.qpsLimit = 50;
        row.dailyQuota = 1000L;
        row.status = 1;
        return row;
    }

    @Test
    void cacheMissLoadsDbAndBackfillsWithJitteredTtl() {
        when(valueOps.get("policy:alice")).thenReturn(null);
        when(mapper.selectById("alice")).thenReturn(row("alice", "[\"db1\"]"));

        UserPolicyService.Policy p = service.resolve("alice");

        assertThat(p.userTag()).isEqualTo("alice");
        assertThat(p.allowedDbIds()).containsExactly("db1");
        assertThat(p.enabled()).isTrue();

        ArgumentCaptor<Duration> ttl = ArgumentCaptor.forClass(Duration.class);
        verify(valueOps).set(eq("policy:alice"), anyString(), ttl.capture());
        assertThat(ttl.getValue().toSeconds()).isBetween(540L, 660L);   // 600s ± 10%
    }

    @Test
    void unknownUserGetsNegativeCacheAndSecondResolveSkipsDb() {
        when(valueOps.get("policy:ghost")).thenReturn(null, NEG);
        when(mapper.selectById("ghost")).thenReturn(null);

        assertThat(service.resolve("ghost")).isNull();
        verify(valueOps).set(eq("policy:ghost"), eq(NEG), eq(Duration.ofSeconds(60)));

        assertThat(service.resolve("ghost")).isNull();
        verify(mapper).selectById("ghost");   // 仍只查过一次库
    }

    @Test
    void cacheHitSkipsDatabase() throws Exception {
        String cached = om.writeValueAsString(
                new UserPolicyService.Policy("bob", "user", List.of("*"), 10, 5L, true));
        when(valueOps.get("policy:bob")).thenReturn(cached);

        UserPolicyService.Policy p = service.resolve("bob");

        assertThat(p.qpsLimit()).isEqualTo(10);
        verify(mapper, never()).selectById(anyString());
    }

    @Test
    void updateUpsertsAndEvictsCache() {
        UserPolicy row = row("carol", "[\"*\"]");

        UserPolicyService.Policy p = service.update(row);

        verify(mapper).upsert(row);
        verify(redis).delete("policy:carol");
        assertThat(p.allowedDbIds()).containsExactly("*");
    }
}
