package com.nl2sql.gateway.recorder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.cache.ResultCacheService;
import com.nl2sql.gateway.task.TaskState;
import com.nl2sql.gateway.task.TaskStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** write-behind 落库：Kafka 事件只当变脏信号；flush 时以 Redis 状态为准组装 task_record 行。 */
@Service
public class TaskEventRecorder {

    private static final int MAX_PENDING = 10_000;
    private static final int BATCH = 100;

    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private final TaskStoreService store;
    private final TaskRecordMapper mapper;
    private final ResultCacheService resultCache;
    private final ObjectMapper om;
    private final Timer flushTimer;

    public TaskEventRecorder(TaskStoreService store, TaskRecordMapper mapper,
                             ResultCacheService resultCache, ObjectMapper om, MeterRegistry meters) {
        this.store = store;
        this.mapper = mapper;
        this.resultCache = resultCache;
        this.om = om;
        this.flushTimer = Timer.builder("gateway.recorder.flush").register(meters);
    }

    @KafkaListener(topics = {"nl2sql.task.status", "nl2sql.task.result"},
            groupId = "nl2sql-ops-recorder")
    public void onEvent(String raw) {
        try {
            JsonNode env = om.readTree(raw);
            String taskId = env.path("task_id").asText(null);
            if (taskId != null && !taskId.isBlank()) {
                markDirty(taskId);
            }
        } catch (Exception ignore) {
            // 非 JSON 信封，忽略
        }
    }

    public void markDirty(String taskId) {
        dirty.add(taskId);
        if (dirty.size() > MAX_PENDING) {
            dirty.iterator().remove();   // 满则丢旧标记（最终一致容忍）
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void flush() {
        if (dirty.isEmpty()) {
            return;
        }
        List<String> batch = new ArrayList<>(Math.min(BATCH, dirty.size()));
        for (String id : dirty) {
            if (batch.size() >= BATCH) {
                break;
            }
            if (dirty.remove(id)) {
                batch.add(id);
            }
        }
        flushTimer.record(() -> {
            for (String id : batch) {
                try {
                    record(id);
                } catch (Exception e) {
                    // 单行失败不拖垮批次
                }
            }
        });
    }

    private void record(String taskId) throws Exception {
        TaskState s = store.get(taskId);
        if (s == null) {
            return;
        }
        String owner = store.owner(taskId);
        TaskRecord r = new TaskRecord();
        r.taskId = taskId;
        r.userTag = owner == null ? "unknown" : owner;
        r.dbId = s.db_id;
        r.question = s.question;
        r.status = s.status;
        r.sqlText = s.sql;
        Object exec = s.exec_result;
        if (exec instanceof Map<?, ?> m) {
            Object rc = m.get("row_count");
            r.rowCount = rc instanceof Number n ? n.intValue() : null;
        }
        r.retryCount = s.retry_count;
        r.error = s.error == null ? null : String.valueOf(s.error);
        r.cacheHit = Boolean.TRUE.equals(s.cacheHitFlag) ? 1 : 0;
        OffsetDateTime submitted = parse(s.created_at);
        boolean terminal = "SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "TIMEOUT".equals(s.status) || "CANCELLED".equals(s.status);
        OffsetDateTime finished = terminal ? parse(s.updated_at) : null;
        // DATETIME 无时区——统一存 UTC 朴素时间
        OffsetDateTime started = parse(s.updated_at);
        r.submittedAt = submitted == null ? null : submitted.toLocalDateTime();
        r.startedAt = started == null ? null : started.toLocalDateTime();
        r.finishedAt = finished == null ? null : finished.toLocalDateTime();
        if (submitted != null && finished != null) {
            r.totalLatencyMs = (int) java.time.Duration.between(submitted, finished).toMillis();
        }
        r.nodeTimings = om.writeValueAsString(s.node_timings);
        r.tokenUsage = om.writeValueAsString(s.token_usage);
        mapper.upsert(r);

        // qcache 回写：只缓存 SUCCESS 且 sql 非空
        if ("SUCCESS".equals(s.status) && s.sql != null && !s.sql.isBlank()) {
            resultCache.put(s.question, s.db_id, om.writeValueAsString(java.util.Map.of(
                    "status", s.status, "sql", s.sql, "exec_result", s.exec_result == null
                            ? java.util.Map.of() : s.exec_result,
                    "token_usage", s.token_usage, "node_timings", s.node_timings)));
        }
    }

    static OffsetDateTime parse(String iso) {
        try {
            return OffsetDateTime.parse(iso);
        } catch (Exception e) {
            return null;
        }
    }
}
