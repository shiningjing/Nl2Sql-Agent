package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskStoreServiceTest {

    @Mock
    StringRedisTemplate redis;
    @Mock
    ValueOperations<String, String> valueOps;

    private TaskStoreService store;
    private final ObjectMapper om = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redis.opsForValue()).thenReturn(valueOps);
        store = new TaskStoreService(redis);
    }

    @Test
    void createWritesPendingStateWithRunningTtl() throws Exception {
        store.create("t1", "q".repeat(300), "db1", "url1");

        var captor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq("task:t1"), captor.capture(), eq(TaskStoreService.TTL_RUNNING));
        Map<String, Object> state = om.readValue(captor.getValue(), Map.class);
        assertThat(state.get("status")).isEqualTo("PENDING");
        assertThat(state.get("question")).isEqualTo("q".repeat(200)); // 截 200
        assertThat(state.get("db_id")).isEqualTo("db1");
        assertThat(state.get("created_at")).isEqualTo(state.get("updated_at"));
    }

    @Test
    void saveTerminalStatusUsesGoodTtl() {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "SUCCESS";
        store.save(s);
        verify(valueOps).set(eq("task:t1"), anyString(), eq(TaskStoreService.TTL_TERMINAL_GOOD));
    }

    @Test
    void saveTimeoutUsesBadTtl() {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "TIMEOUT";
        store.save(s);
        verify(valueOps).set(eq("task:t1"), anyString(), eq(TaskStoreService.TTL_TERMINAL_BAD));
    }

    @Test
    void idempotentKeysUseContractPrefixAndTtl() {
        store.idempotentSet("abc123", "t9");
        verify(valueOps).set(eq("idempotent:abc123"), eq("t9"),
                eq(Duration.ofSeconds(300)));
    }

    @Test
    void cancelFlagUsesContractTtl() {
        store.requestCancel("t9");
        verify(valueOps).set(eq("task:t9:cancel"), eq("1"),
                eq(Duration.ofSeconds(3600)));
    }

    @Test
    void getReturnsNullOnMissingOrCorrupt() {
        when(valueOps.get("task:missing")).thenReturn(null);
        when(valueOps.get("task:corrupt")).thenReturn("not-json{");

        assertThat(store.get("missing")).isNull();
        assertThat(store.get("corrupt")).isNull();
    }
}
