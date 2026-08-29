package com.nl2sql.gateway.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PermissionServiceTest {

    @Mock
    StringRedisTemplate redis;

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);

    private PermissionService service;

    private final UserPolicyService.Policy user = new UserPolicyService.Policy(
            "u1", "user", List.of("db_bird", "db_sales"), 100, 1000L, true);
    private final UserPolicyService.Policy admin = new UserPolicyService.Policy(
            "root", "admin", List.of("db_bird"), 100, 1000L, true);

    @BeforeEach
    void setUp() {
        service = new PermissionService(redis);
    }

    @Test
    void wildcardPolicyAllowsAnyDb() {
        UserPolicyService.Policy wildcard = new UserPolicyService.Policy(
                "w", "user", List.of("*"), 100, 1000L, true);
        assertThat(service.dbAllowed(wildcard, "db_anything")).isTrue();
    }

    @Test
    void whitelistAllowsListedAndRejectsOthers() {
        assertThat(service.dbAllowed(user, "db_bird")).isTrue();
        assertThat(service.dbAllowed(user, "db_secret")).isFalse();
    }

    @Test
    void emptyDbIdAllowed() {
        assertThat(service.dbAllowed(user, null)).isTrue();
        assertThat(service.dbAllowed(user, "")).isTrue();
    }

    @Test
    void setOwnerWritesTaskownerKeyWithDailyTtl() {
        when(redis.opsForValue()).thenReturn(valueOps);

        service.setOwner("t1", "u1");

        verify(valueOps).set("taskowner:t1", "u1", Duration.ofSeconds(86400));
    }

    @Test
    void ownershipThreeStatesOwnerStrangerAdmin() {
        when(redis.opsForValue()).thenReturn(valueOps);
        when(valueOps.get("taskowner:t1")).thenReturn("u1", "someone-else", null);

        assertThat(service.isOwnerOrAdmin("t1", user)).isTrue();      // 本人
        assertThat(service.isOwnerOrAdmin("t1", user)).isFalse();     // 陌生人
        assertThat(service.isOwnerOrAdmin("t1", user)).isFalse();     // 无归属记录
    }

    @Test
    void adminBypassesOwnershipWithoutRedisRead() {
        assertThat(service.isOwnerOrAdmin("t1", admin)).isTrue();
        verify(redis, never()).opsForValue();
        verify(valueOps, never()).get(anyString());
    }
}
