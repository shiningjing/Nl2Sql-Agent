package com.nl2sql.gateway.task;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 任务端点——语义与 api/routes/task.py 逐条对齐；映射比 /api/v1/** 更具体，天然优先于透明代理。 */
@RestController
@RequestMapping("/api/v1/task")
public class TaskController {

    public static final int FEEDBACK_MAX_TURNS = 10;

    private final TaskStoreService store;
    private final KafkaPublisher publisher;
    private final TaskStreamService streamService;

    public TaskController(TaskStoreService store, KafkaPublisher publisher,
                          TaskStreamService streamService) {
        this.store = store;
        this.publisher = publisher;
        this.streamService = streamService;
    }

    public record SubmitRequest(
            String question, Boolean rag_schema, Boolean rag_domain, Boolean multi_candidate,
            Integer rag_k, Boolean rag_column_prune, Boolean rag_hybrid, Boolean rag_fk_expand,
            Boolean fewshot_enabled, String database_url, String db_id, Object llm,
            String idempotency_key) {
    }

    @PostMapping("/submit")
    public ResponseEntity<Map<String, String>> submit(@RequestBody SubmitRequest req) {
        if (req.idempotency_key() != null && !req.idempotency_key().isBlank()) {
            String idemKey = TaskIdempotency.key(req.idempotency_key(), req.question());
            String existing = store.idempotentGet(idemKey);
            if (existing != null) {
                return ResponseEntity.accepted().body(Map.of("task_id", existing, "status", "PENDING"));
            }
            String taskId = TaskIdempotency.newTaskId();
            store.idempotentSet(idemKey, taskId);
            return accepted(taskId, req);
        }
        return accepted(TaskIdempotency.newTaskId(), req);
    }

    private ResponseEntity<Map<String, String>> accepted(String taskId, SubmitRequest req) {
        String dbId = req.db_id() == null ? "" : req.db_id();
        String databaseUrl = req.database_url() == null ? "" : req.database_url();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("question", req.question());
        payload.put("db_id", dbId);
        payload.put("database_url", databaseUrl);
        payload.put("rag_schema", bool(req.rag_schema(), true));
        payload.put("rag_domain", bool(req.rag_domain(), true));
        payload.put("multi_candidate", bool(req.multi_candidate(), true));
        payload.put("rag_k", req.rag_k() == null ? 8 : req.rag_k());
        payload.put("rag_column_prune", bool(req.rag_column_prune(), false));
        payload.put("rag_hybrid", bool(req.rag_hybrid(), true));
        payload.put("rag_fk_expand", bool(req.rag_fk_expand(), true));
        payload.put("fewshot_enabled", bool(req.fewshot_enabled(), true));

        store.create(taskId, req.question(), dbId, databaseUrl);
        publisher.publishSubmitted(taskId, payload);
        return ResponseEntity.accepted().body(Map.of("task_id", taskId, "status", "PENDING"));
    }

    private boolean bool(Boolean v, boolean dflt) {
        return v == null ? dflt : v;
    }

    @GetMapping("/{task_id}/status")
    public ResponseEntity<?> status(@PathVariable("task_id") String taskId) {
        TaskState s = store.get(taskId);
        if (s == null) {
            return notFound();
        }
        return ResponseEntity.ok(s.toStatusResponse());
    }

    @PostMapping("/{task_id}/cancel")
    public Map<String, String> cancel(@PathVariable("task_id") String taskId) {
        TaskState s = store.get(taskId);
        if (s == null) {
            return Map.of("task_id", taskId, "status", "not_found");
        }
        if ("SUCCESS".equals(s.status) || "FAILED".equals(s.status)
                || "CANCELLED".equals(s.status) || "TIMEOUT".equals(s.status)) {
            return Map.of("task_id", taskId, "status", s.status);
        }
        store.requestCancel(taskId);
        return Map.of("task_id", taskId, "status", "cancelled");
    }

    @GetMapping("/{task_id}/health")
    public ResponseEntity<?> taskHealth(@PathVariable("task_id") String taskId) {
        TaskState s = store.get(taskId);
        if (s == null) {
            return notFound();
        }
        String hb = store.heartbeat(taskId);
        boolean healthy = false;
        Double staleS = null;
        if (hb != null) {
            try {
                long elapsed = java.time.Duration.between(
                        java.time.OffsetDateTime.parse(hb),
                        java.time.OffsetDateTime.now()).toSeconds();
                healthy = elapsed < TaskStoreService.HEARTBEAT_STALE_S;
                staleS = (double) elapsed;
            } catch (Exception ignore) {
                // unparseable → unhealthy
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("task_id", taskId);
        body.put("task_status", s.status);
        body.put("heartbeat", hb);
        body.put("heartbeat_stale_s", staleS);
        body.put("healthy", healthy);
        body.put("worker_alive", healthy && "RUNNING".equals(s.status));
        return ResponseEntity.ok(body);
    }

    @PostMapping("/scan-stale")
    public Map<String, Object> scanStale() {
        List<String> stale = store.scanStale();
        return Map.of("stale_count", stale.size(), "stale_task_ids", stale);
    }

    public record FeedbackRequest(String feedback) {
    }

    @PostMapping("/{task_id}/feedback")
    public ResponseEntity<?> feedback(@PathVariable("task_id") String taskId,
                                      @RequestBody FeedbackRequest req) {
        TaskState s = store.get(taskId);
        if (s == null) {
            return notFound();
        }
        if (!"SUCCESS".equals(s.status) && !"FAILED".equals(s.status)) {
            return badRequest("Feedback only allowed on SUCCESS tasks (current: " + s.status + ")");
        }
        List<Map<String, Object>> turns = s.turns();
        int turn = turns.size() + 1;
        if (turn > FEEDBACK_MAX_TURNS) {
            return badRequest("Maximum feedback turns (" + FEEDBACK_MAX_TURNS + ") reached");
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("feedback", req.feedback());
        payload.put("turn", turn);
        payload.put("question", s.question);
        payload.put("db_id", s.db_id);
        payload.put("database_url", s.database_url);
        payload.put("sql", s.sql);
        payload.put("exec_result", s.exec_result);
        payload.put("conversation_turns", turns);
        payload.put("token_usage", s.token_usage);
        payload.put("node_timings", s.node_timings);

        publisher.publishFeedback(taskId, payload);
        s.status = "RUNNING";
        s.node = null;
        s.progress = 0;
        s.updated_at = TaskStoreService.nowIso();
        store.save(s);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("task_id", taskId);
        body.put("status", "accepted");
        body.put("turn", turn);
        return ResponseEntity.accepted().body(body);
    }

    /** SSE 流：token 实时 + 状态轮询，事件序列复刻 task_stream；5 分钟上限。 */
    @GetMapping("/{task_id}/stream")
    public void stream(@PathVariable("task_id") String taskId,
                       jakarta.servlet.http.HttpServletResponse response) throws Exception {
        response.setContentType("text/event-stream");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("X-Accel-Buffering", "no");
        TaskStreamService.EventCursor cursor = new TaskStreamService.EventCursor();
        long deadline = System.currentTimeMillis() + 300_000;
        try (var sub = streamService.subscribeTokens(taskId, cursor.tokens);
             var out = response.getOutputStream()) {
            while (System.currentTimeMillis() < deadline) {
                TaskStreamService.SseEvent ev = streamService.nextEvent(taskId, cursor);
                if (ev == null) {
                    continue;
                }
                out.write(("event: " + ev.event() + "\ndata: " + ev.data() + "\n\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                out.flush();
                if ("complete".equals(ev.event()) || "error".equals(ev.event())) {
                    return;
                }
            }
            out.write(("event: timeout\ndata: " + streamService.json(
                    java.util.Map.of("error", "Stream timeout (5 min)")) + "\n\n")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            out.flush();
        }
    }

    private static ResponseEntity<Map<String, String>> notFound() {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("detail", "Task not found or expired"));
    }

    private static ResponseEntity<Map<String, String>> badRequest(String detail) {
        return ResponseEntity.badRequest().body(Map.of("detail", detail));
    }
}
