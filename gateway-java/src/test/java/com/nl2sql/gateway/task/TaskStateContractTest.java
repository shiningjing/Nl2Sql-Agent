package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskStateContractTest {

    private final ObjectMapper om = new ObjectMapper();

    @Test
    void serializesExactlyLikePythonTaskCreate() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "abc123def456";
        s.status = "PENDING";
        s.question = "avg enrollment?";
        s.db_id = "california_schools";
        s.database_url = "";
        s.progress = 0;
        s.created_at = "2026-08-20T10:00:00+00:00";
        s.updated_at = "2026-08-20T10:00:00+00:00";

        Map<String, Object> map = om.readValue(om.writeValueAsString(s), Map.class);
        assertThat(map).containsOnlyKeys(
                "task_id", "status", "question", "db_id", "database_url",
                "progress", "node", "sql", "exec_result", "token_usage",
                "node_timings", "retry_count", "error", "created_at", "updated_at",
                "conversation_turns");
        assertThat(map.get("node")).isNull();
        assertThat(map.get("sql")).isNull();
        assertThat(map.get("exec_result")).isNull();
        assertThat(map.get("error")).isNull();
        assertThat(map.get("token_usage")).isEqualTo(Map.of());
        assertThat(map.get("node_timings")).isEqualTo(Map.of());
        assertThat(map.get("retry_count")).isEqualTo(0);
    }

    @Test
    void toleratesWorkerWrittenFields() throws Exception {
        String workerJson = """
            {"task_id":"t1","status":"RUNNING","question":"q","db_id":"d","database_url":"",
             "progress":40,"node":"generator","sql":null,"exec_result":null,
             "token_usage":{"total":100},"node_timings":{"router":1.2},"retry_count":0,
             "error":null,"created_at":"c","updated_at":"u",
             "conversation_turns":[{"feedback":"add group by"}]}
            """;
        TaskState s = om.readValue(workerJson, TaskState.class);
        assertThat(s.status).isEqualTo("RUNNING");
        assertThat(s.node).isEqualTo("generator");
        assertThat(s.turns()).hasSize(1);
        om.writeValueAsString(s); // 必须可往返
    }
}
