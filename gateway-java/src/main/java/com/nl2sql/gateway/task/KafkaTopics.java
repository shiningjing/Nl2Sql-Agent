package com.nl2sql.gateway.task;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Topic 常量 + 幂等建Topic（与 broker.py 一致：1 分区、rf=1，已存在则跳过）。 */
@Configuration
public class KafkaTopics {

    public static final String TOPIC_REQUEST = "nl2sql.task.request";
    public static final String TOPIC_FEEDBACK = "nl2sql.task.feedback";

    @Bean
    public NewTopic taskRequestTopic() {
        return new NewTopic(TOPIC_REQUEST, 1, (short) 1);
    }

    @Bean
    public NewTopic taskFeedbackTopic() {
        return new NewTopic(TOPIC_FEEDBACK, 1, (short) 1);
    }
}
