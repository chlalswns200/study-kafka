package com.kafkastudy.stage2.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String key, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.whenComplete((result, ex) -> {
            log.info("[MessageProducer send Thread Info] ={}", Thread.currentThread().getName());
            if (ex == null) {
                ProducerRecord<String, String> producerRecord = result.getProducerRecord();
                RecordMetadata recordMetadata = result.getRecordMetadata();
                log.info("key={} partition={} offset={}",producerRecord.key(),recordMetadata.partition(),recordMetadata.offset());
            } else {
                log.error("[MessageProducer send Error]",ex);
            }
        });

    }


}
