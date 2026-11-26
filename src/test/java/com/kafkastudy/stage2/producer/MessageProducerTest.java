package com.kafkastudy.stage2.producer;

import org.apache.kafka.common.internals.Topic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class MessageProducerTest {

    @Autowired
    MessageProducer producer;

    @Test
    void sendTest() {

        String topic = "stage2-messages";
        String key = "user%d";
        String message = "item%d";


        producer.send(topic, key.formatted(88), message.formatted(88))
                .thenAccept(result -> {
                    assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
                    assertThat(result.getRecordMetadata().partition()).isGreaterThanOrEqualTo(0);
                }).join();

    }
}