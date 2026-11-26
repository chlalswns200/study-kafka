package com.kafkastudy.stage2.controller;

import com.kafkastudy.stage2.dto.SendRequest;
import com.kafkastudy.stage2.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping("/messages")
    public CompletableFuture<ResponseEntity<Void>> send(@RequestBody SendRequest request) {
        CompletableFuture<SendResult<String, String>> future = messageProducer.send("stage2-messages", request.key(), request.value());
        return future.thenApply(result -> ResponseEntity.accepted().<Void>build())
                .exceptionally(ex -> ResponseEntity.internalServerError().build());
    }
}