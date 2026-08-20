package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * SSE 事件源：PubSub token + 500ms 状态轮询。
 * 事件名与载荷复刻 api/routes/task.py::task_stream——
 * token({"text"}) / status(updated_at 变化) / node_done(node 变化) / complete(终态) / error(任务不存在) / timeout(5min)。
 */
@Service
public class TaskStreamService {

    private final TaskStoreService store;
    private final RedisMessageListenerContainer listenerContainer;
    private final ObjectMapper om = new ObjectMapper();

    public TaskStreamService(TaskStoreService store,
                             RedisMessageListenerContainer listenerContainer) {
        this.store = store;
        this.listenerContainer = listenerContainer;
    }

    public record SseEvent(String event, String data) {
    }

    /**
     * 拉取下一个事件（与 python 循环等价）：
     * 先取 token（无则轮询一次状态后再等 500ms）；然后按 status → node_done → complete 顺序吐出轮询结果。
     * 返回 null 表示本轮无事件，调用方继续循环；error/complete/timeout 为终态事件。
     */
    public SseEvent nextEvent(String taskId, EventCursor cursor) {
        String token = cursor.tokens.poll();
        if (token == null) {
            pollStatus(taskId, cursor);
            try {
                token = cursor.tokens.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        if (token != null) {
            return new SseEvent("token", json(Map.of("text", token)));
        }
        if (cursor.notFound) {
            return new SseEvent("error", json(Map.of("error", "Task not found")));
        }
        SseEvent status = cursor.takeStatus();
        if (status != null) {
            return status;
        }
        SseEvent node = cursor.takeNode();
        if (node != null) {
            return node;
        }
        return cursor.completeEvent;
    }

    private void pollStatus(String taskId, EventCursor cursor) {
        TaskState s = store.get(taskId);
        if (s == null) {
            cursor.notFound = true;
            return;
        }
        if (!s.updated_at.equals(cursor.lastUpdated)) {
            cursor.lastUpdated = s.updated_at;
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("status", s.status);
            body.put("progress", s.progress);
            body.put("node", s.node == null ? "" : s.node);
            body.put("sql_preview", s.sql == null ? "" : s.sql.substring(0, Math.min(200, s.sql.length())));
            body.put("error", s.error);
            cursor.pendingStatus = new SseEvent("status", json(body));
        }
        if (s.node != null && !s.node.equals(cursor.lastNode)) {
            cursor.lastNode = s.node;
            cursor.pendingNode = new SseEvent("node_done", json(Map.of("node", s.node)));
        }
        if ("SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "TIMEOUT".equals(s.status) || "CANCELLED".equals(s.status)) {
            Map<String, Object> body = new java.util.LinkedHashMap<>();
            body.put("status", s.status);
            body.put("sql", s.sql == null ? "" : s.sql);
            body.put("exec_result", s.exec_result);
            body.put("token_usage", s.token_usage);
            body.put("node_timings", s.node_timings);
            body.put("error", s.error);
            cursor.completeEvent = new SseEvent("complete", json(body));
        }
    }

    /** 订阅 token 通道，消息塞进队列；返回取消订阅句柄。 */
    public AutoCloseable subscribeTokens(String taskId, BlockingQueue<String> queue) {
        MessageListener listener = (message, pattern) ->
                queue.add(new String(message.getBody(), StandardCharsets.UTF_8));
        ChannelTopic topic = new ChannelTopic("task:" + taskId + ":tokens");
        listenerContainer.addMessageListener(listener, topic);
        return () -> listenerContainer.removeMessageListener(listener);
    }

    public String json(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 每个流连接一份游标。 */
    public static class EventCursor {

        final BlockingQueue<String> tokens = new LinkedBlockingQueue<>();
        String lastUpdated = "";
        String lastNode = "";
        boolean notFound;
        SseEvent pendingStatus;
        SseEvent pendingNode;
        SseEvent completeEvent;

        SseEvent takeStatus() {
            SseEvent e = pendingStatus;
            pendingStatus = null;
            return e;
        }

        SseEvent takeNode() {
            SseEvent e = pendingNode;
            pendingNode = null;
            return e;
        }
    }
}
