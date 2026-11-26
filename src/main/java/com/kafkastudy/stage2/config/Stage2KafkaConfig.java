package com.kafkastudy.stage2.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Stage2KafkaConfig {

    @Bean
    public NewTopic stage2MessagesTopic() {
        return TopicBuilder.name("stage2-messages")
                .partitions(5)
                .replicas(1)
                .build();
    }
}
