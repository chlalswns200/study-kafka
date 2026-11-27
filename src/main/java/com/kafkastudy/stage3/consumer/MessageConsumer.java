package com.kafkastudy.stage3.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageConsumer {

    @KafkaListener(topics = "stage2-messages",groupId = "stage3-group-a")
    public void stage2ConsumerGroupA(ConsumerRecord<String, String> record) {
        String key = record.key();
        int partition = record.partition();
        long offset = record.offset();
        String value = record.value();

        log.info("[MessageConsumer.stage2ConsumerGroupA] key={}, partition={}, offset={}, value={}", key, partition, offset, value);
    }

    @KafkaListener(topics = "stage2-messages",groupId = "stage3-group-b")
    public void stage2ConsumerGroupB(ConsumerRecord<String, String> record) {
        String key = record.key();
        int partition = record.partition();
        long offset = record.offset();
        String value = record.value();

        log.info("[MessageConsumer.stage2ConsumerGroupB] key={}, partition={}, offset={}, value={}", key, partition, offset, value);
    }
}
