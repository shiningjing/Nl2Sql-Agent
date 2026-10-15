package com.nl2sql.gateway.recorder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.cache.ResultCacheService;
import com.nl2sql.gateway.task.IntegrationTestBase;
import com.nl2sql.gateway.task.TaskIdempotency;
import com.nl2sql.gateway.task.TaskState;
import com.nl2sql.gateway.task.TaskStoreService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** write-behind 全链路：Kafka 事件→变脏→定时 flush→MySQL 行 + qcache 回写。 */
class TaskEventRecorderTest extends IntegrationTestBase {

    @Autowired
    TaskRecordMapper taskRecordMapper;

    @Autowired
    TaskStoreService store;

    @Autowired
    TaskEventRecorder recorder;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ResultCacheService resultCache;

    private final ObjectMapper om = new ObjectMapper();

    @Test
    void resultEventFlushesTaskRecordAndWritesBackQcache() throws Exception {
        String taskId = TaskIdempotency.newTaskId();
        store.create(taskId, "rec q?", "california_schools", "");
        redisTemplate.opsForValue().set("taskowner:" + taskId, "local_user", Duration.ofSeconds(60));

        TaskState s = store.get(taskId);
        s.status = "SUCCESS";
        s.sql = "SELECT 42";
        s.exec_result = Map.of("row_count", 3);
        s.node_timings = Map.of("generator", 120);
        s.token_usage = Map.of("total_tokens", 99);
        store.save(s);

        kafka.send(com.nl2sql.gateway.task.KafkaTopics.TOPIC_RESULT, taskId, om.writeValueAsString(
                Map.of("task_id", taskId, "event", "success", "payload", Map.of()))).get();

        // 监听器消费 + 2s 定时 flush：轮询等待行落库（上限 30s，容忍慢宿主机上的消费协调）
        TaskRecord row = null;
        for (int i = 0; i < 150 && row == null; i++) {
            Thread.sleep(200);
            row = taskRecordMapper.selectById(taskId);
        }
        assertThat(row).as("task_record row flushed").isNotNull();
        assertThat(row.status).isEqualTo("SUCCESS");
        assertThat(row.sqlText).isEqualTo("SELECT 42");
        assertThat(row.userTag).isEqualTo("local_user");
        assertThat(row.rowCount).isEqualTo(3);
        assertThat(row.cacheHit).isZero();
        assertThat(row.totalLatencyMs).isGreaterThanOrEqualTo(0);
        assertThat(row.nodeTimings).contains("generator");
        assertThat(row.tokenUsage).contains("total_tokens");

        String cached = redisTemplate.opsForValue()
                .get(ResultCacheService.key("rec q?", "california_schools"));
        assertThat(cached).isNotNull().contains("SELECT 42");
    }

    @Test
    void markDirtyBeyondMaxPendingDoesNotThrowAndStaysBounded() {
        // 回归：qcache 压测曾触发 iterator.remove() 未先 next() → IllegalStateException → 全量 500
        for (int i = 0; i < 12_000; i++) {
            recorder.markDirty("overflow-" + i);
        }
        org.assertj.core.api.Assertions.assertThat(recorder.pendingSize())
                .isLessThanOrEqualTo(10_000);
    }

    @Test
    void cachedTaskMarkedDirtyFlushesWithCacheHitFlag() throws Exception {
        String taskId = TaskIdempotency.newTaskId();
        store.createFromCache(taskId, "cached rec?", "california_schools",
                "{\"status\":\"SUCCESS\",\"sql\":\"SELECT c\",\"exec_result\":{},"
                        + "\"token_usage\":{},\"node_timings\":{}}");
        redisTemplate.opsForValue().set("taskowner:" + taskId, "local_user", Duration.ofSeconds(60));

        recorder.markDirty(taskId);
        recorder.flush();   // 直接触发，确定性验证

        TaskRecord row = taskRecordMapper.selectById(taskId);
        assertThat(row).isNotNull();
        assertThat(row.cacheHit).isEqualTo(1);
        assertThat(row.status).isEqualTo("SUCCESS");
        assertThat(row.sqlText).isEqualTo("SELECT c");
    }
}
