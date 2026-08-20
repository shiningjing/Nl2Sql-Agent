package com.nl2sql.gateway.task;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskIdempotencyTest {

    @Test
    void matchesPythonSha256Rule() {
        // 向量与 Python 对拍：python -c "import hashlib; print(hashlib.sha256('test-key:What is the avg enrollment'.encode()).hexdigest()[:32])"
        assertThat(TaskIdempotency.key("test-key", "What is the avg enrollment"))
                .isEqualTo("cbed037109e06f6e1eb73725f574e8d4");
    }

    @Test
    void truncatesQuestionTo80() {
        String q80 = "x".repeat(80) + "TRUNCATED";
        assertThat(TaskIdempotency.key("k", q80))
                .isEqualTo(TaskIdempotency.key("k", "x".repeat(80)));
    }

    @Test
    void newTaskIdIs12HexChars() {
        assertThat(TaskIdempotency.newTaskId()).matches("[0-9a-f]{12}");
    }
}
