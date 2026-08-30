package com.nl2sql.gateway.task;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Topic 常量 + 幂等建Topic（与 broker.py ALL_TOPICS 一致：1 分区、rf=1，已存在则跳过）。 */
@Configuration
public class KafkaTopics {

    public static final String TOPIC_REQUEST = "nl2sql.task.request";
    public static final String TOPIC_STATUS = "nl2sql.task.status";
    public static final String TOPIC_RESULT = "nl2sql.task.result";
    public static final String TOPIC_DLQ = "nl2sql.task.dlq";
    public static final String TOPIC_FEEDBACK = "nl2sql.task.feedback";

    @Bean
    public NewTopic taskRequestTopic() {
        return new NewTopic(TOPIC_REQUEST, 1, (short) 1);
    }

    @Bean
    public NewTopic taskStatusTopic() {
        return new NewTopic(TOPIC_STATUS, 1, (short) 1);
    }

    @Bean
    public NewTopic taskResultTopic() {
        return new NewTopic(TOPIC_RESULT, 1, (short) 1);
    }

    @Bean
    public NewTopic taskDlqTopic() {
        return new NewTopic(TOPIC_DLQ, 1, (short) 1);
    }

    @Bean
    public NewTopic taskFeedbackTopic() {
        return new NewTopic(TOPIC_FEEDBACK, 1, (short) 1);
    }
}
