package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Redis 任务状态机——复刻 infrastructure/task_store.py 的 key 布局与 TTL。 */
@Service
public class TaskStoreService {

    public static final Duration TTL_RUNNING = Duration.ofSeconds(7200);
    public static final Duration TTL_TERMINAL_GOOD = Duration.ofSeconds(86400);
    public static final Duration TTL_TERMINAL_BAD = Duration.ofSeconds(3600);
    public static final Duration TTL_CANCEL = Duration.ofSeconds(3600);
    public static final Duration TTL_IDEMPOTENT = Duration.ofSeconds(300);
    public static final long HEARTBEAT_STALE_S = 60;

    private final StringRedisTemplate redis;
    private final ObjectMapper om;

    public TaskStoreService(StringRedisTemplate redis, ObjectMapper om) {
        this.redis = redis;
        this.om = om;
    }

    /** ISO-8601 UTC，与 Python datetime.now(timezone.utc).isoformat() 等价（Worker 侧 fromisoformat 兼容 Z 后缀）。 */
    public static String nowIso() {
        return OffsetDateTime.now(java.time.ZoneOffset.UTC).toString();
    }

    /** 初始化 PENDING 状态（字段与 task_store.task_create 一致，question 截 200）。 */
    public TaskState create(String taskId, String question, String dbId, String databaseUrl) {
        TaskState s = new TaskState();
        s.task_id = taskId;
        s.status = "PENDING";
        s.question = question.substring(0, Math.min(200, question.length()));
        s.db_id = dbId;
        s.database_url = databaseUrl;
        String now = nowIso();
        s.created_at = now;
        s.updated_at = now;
        save(s);
        return s;
    }

    public TaskState get(String taskId) {
        String raw = redis.opsForValue().get("task:" + taskId);
        if (raw == null) {
            return null;
        }
        try {
            return om.readValue(raw, TaskState.class);
        } catch (Exception e) {
            return null;
        }
    }

    public String heartbeat(String taskId) {
        return redis.opsForValue().get("task:" + taskId + ":heartbeat");
    }

    public void save(TaskState s) {
        Duration ttl = switch (s.status) {
            case "PENDING", "RUNNING" -> TTL_RUNNING;
            case "TIMEOUT" -> TTL_TERMINAL_BAD;
            default -> TTL_TERMINAL_GOOD;
        };
        try {
            redis.opsForValue().set("task:" + s.task_id, om.writeValueAsString(s), ttl);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void requestCancel(String taskId) {
        redis.opsForValue().set("task:" + taskId + ":cancel", "1", TTL_CANCEL);
    }

    public String idempotentGet(String key) {
        return redis.opsForValue().get("idempotent:" + key);
    }

    public void idempotentSet(String key, String taskId) {
        redis.opsForValue().set("idempotent:" + key, taskId, TTL_IDEMPOTENT);
    }

    /** SETNX 原子占位：true=占住；false=已被并发请求占用（GET 读走占位者 task_id）。 */
    public boolean idempotentTryAcquire(String key, String taskId) {
        return Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent("idempotent:" + key, taskId, TTL_IDEMPOTENT));
    }

    /** qcache 命中短路：不走 Kafka，直接落 SUCCESS 终态（cache_hit=true，终态 TTL）。 */
    public TaskState createFromCache(String taskId, String question, String dbId, String resultJson) {
        TaskState s = new TaskState();
        s.task_id = taskId;
        s.status = "SUCCESS";
        s.question = question.substring(0, Math.min(200, question.length()));
        s.db_id = dbId == null ? "" : dbId;
        s.progress = 100;
        try {
            com.fasterxml.jackson.databind.JsonNode n = om.readTree(resultJson);
            if (n.hasNonNull("sql")) {
                s.sql = n.get("sql").asText();
            }
            if (n.has("exec_result")) {
                s.exec_result = om.readValue(n.get("exec_result").toString(), Object.class);
            }
            if (n.has("token_usage")) {
                s.token_usage = om.readValue(n.get("token_usage").toString(),
                        new com.fasterxml.jackson.core.type.TypeReference<>() {
                        });
            }
            if (n.has("node_timings")) {
                s.node_timings = om.readValue(n.get("node_timings").toString(),
                        new com.fasterxml.jackson.core.type.TypeReference<>() {
                        });
            }
        } catch (Exception ignore) {
            // 缓存载荷残缺：保留 SUCCESS 骨架
        }
        s.created_at = nowIso();
        s.updated_at = nowIso();
        s.cacheHitFlag = true;
        save(s);
        return s;
    }

    /** 心跳判死：RUNNING 且心跳缺失/超 60s → TIMEOUT（复刻 scan_stale_tasks）。 */
    public List<String> scanStale() {
        List<String> stale = new ArrayList<>();
        Set<String> keys = redis.keys("task:*");
        if (keys == null) {
            return stale;
        }
        for (String key : keys) {
            if (key.endsWith(":heartbeat") || key.endsWith(":cancel")) {
                continue;
            }
            String taskId = key.substring("task:".length());
            if (taskId.startsWith("idempotent:")) {
                continue;
            }
            TaskState s = get(taskId);
            if (s == null || !"RUNNING".equals(s.status)) {
                continue;
            }
            String hb = heartbeat(taskId);
            boolean dead = true;
            if (hb != null) {
                try {
                    Instant t = OffsetDateTime.parse(hb).toInstant();
                    dead = t.plusSeconds(HEARTBEAT_STALE_S).isBefore(Instant.now());
                } catch (Exception ignore) {
                    // unparseable → dead
                }
            }
            if (dead) {
                s.status = "TIMEOUT";
                s.error = "Worker lost (heartbeat stale)";
                s.updated_at = nowIso();
                save(s);
                stale.add(taskId);
            }
        }
        return stale;
    }
}
