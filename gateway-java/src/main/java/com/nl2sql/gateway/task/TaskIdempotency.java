package com.nl2sql.gateway.task;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

/** 幂等键与新任务 ID 生成——与 api/routes/task.py 的规则逐字一致。 */
public final class TaskIdempotency {

    private TaskIdempotency() {
    }

    /** sha256(f"{key}:{question[:80]}")[:32]。 */
    public static String key(String idempotencyKey, String question) {
        String input = idempotencyKey + ":"
                + question.substring(0, Math.min(80, question.length()));
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(0, 32);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** uuid4().hex[:12] 等价实现。 */
    public static String newTaskId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
