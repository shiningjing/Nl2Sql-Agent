package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final KafkaPublisher publisher = new KafkaPublisher(kafka, new ObjectMapper());

    @Test
    void publishesEnvelopeWithTaskIdKey() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishSubmitted("t1", Map.of("question", "q", "db_id", "d"));

        ArgumentCaptor<String> envelopeCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("nl2sql.task.request"), eq("t1"), envelopeCaptor.capture());
        Map<String, Object> envelope = new ObjectMapper()
                .readValue(envelopeCaptor.getValue(), Map.class);
        assertThat(envelope.get("task_id")).isEqualTo("t1");
        assertThat(envelope.get("event")).isEqualTo("submitted");
        assertThat(asMap(envelope.get("payload"))).containsEntry("question", "q");
    }

    @Test
    void publishesFeedbackToFeedbackTopic() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishFeedback("t2", Map.of("feedback", "add group by", "turn", 1));

        ArgumentCaptor<String> envelopeCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("nl2sql.task.feedback"), eq("t2"), envelopeCaptor.capture());
        Map<String, Object> envelope = new ObjectMapper()
                .readValue(envelopeCaptor.getValue(), Map.class);
        assertThat(envelope.get("event")).isEqualTo("feedback");
        assertThat(asMap(envelope.get("payload"))).containsEntry("turn", 1);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }
}
