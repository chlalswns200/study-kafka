package com.kafkastudy.stage2.producer;

import org.apache.kafka.common.internals.Topic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class MessageProducerTest {

    @Autowired
    MessageProducer producer;

    @Test
    void sendTest() {

        String topic = "stage2-messages";
        String key = "user%d";
        String message = "item%d";

        for (int i = 0; i < 6; i++) {
            producer.send(topic, key.formatted(i), message.formatted(i));
        }


    }
}