package com.nl2sql.gateway.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/** Kafka 信封 {"task_id","event","payload"}，key=task_id — 与 broker.py TaskMessage 一致。 */
@Service
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper om;

    public KafkaPublisher(KafkaTemplate<String, String> kafka, ObjectMapper om) {
        this.kafka = kafka;
        this.om = om;
    }

    public void publishSubmitted(String taskId, Map<String, Object> payload) {
        publish(KafkaTopics.TOPIC_REQUEST, taskId, "submitted", payload);
    }

    public void publishFeedback(String taskId, Map<String, Object> payload) {
        publish(KafkaTopics.TOPIC_FEEDBACK, taskId, "feedback", payload);
    }

    private void publish(String topic, String taskId, String event, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("task_id", taskId);
        envelope.put("event", event);
        envelope.put("payload", payload);
        try {
            kafka.send(topic, taskId, om.writeValueAsString(envelope));
        } catch (Exception e) {
            throw new IllegalStateException("kafka publish failed", e);
        }
    }
}
