package com.nl2sql.gateway.task;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TaskStreamIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    TaskStoreService store;

    @Test
    void emitsTokenStatusNodeDoneCompleteSequence() throws Exception {
        String taskId = TaskIdempotency.newTaskId();
        store.create(taskId, "q", "d", "");

        // 异步推 token + 推进状态到 SUCCESS（模拟 Worker 行为）
        Thread simulator = new Thread(() -> {
            try {
                Thread.sleep(500);
                redisTemplate.convertAndSend("task:" + taskId + ":tokens", "SEL");
                TaskState running = store.get(taskId);
                running.status = "RUNNING";
                running.node = "generator";
                running.updated_at = TaskStoreService.nowIso();
                store.save(running);
                Thread.sleep(500);
                TaskState done = store.get(taskId);
                done.status = "SUCCESS";
                done.sql = "SELECT 1";
                done.updated_at = TaskStoreService.nowIso();
                store.save(done);
            } catch (Exception ignore) {
                // 测试线程自己兜底
            }
        });
        simulator.start();

        ResponseEntity<Resource> resp =
                rest.getForEntity("/api/v1/task/" + taskId + "/stream", Resource.class);
        List<String> events = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(resp.getBody().getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                lines.add(line);
                if (line.startsWith("event: ")) {
                    events.add(line.substring("event: ".length()));
                }
            }
        }
        simulator.join();

        assertThat(events).contains("status", "node_done", "token", "complete");
        assertThat(events.get(events.size() - 1)).isEqualTo("complete");
        // complete 载荷含 sql
        assertThat(String.join("\n", lines)).contains("SELECT 1");
    }

    @Test
    void streamOfMissingTaskEmitsError() throws Exception {
        ResponseEntity<Resource> resp =
                rest.getForEntity("/api/v1/task/neverexists/stream", Resource.class);
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(resp.getBody().getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        assertThat(sb.toString()).contains("event: error").contains("Task not found");
    }
}
