package com.kafkastudy.stage2.controller;

import com.kafkastudy.stage2.dto.SendRequest;
import com.kafkastudy.stage2.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping("/messages")
    public ResponseEntity<String> send(@RequestBody SendRequest request) {
        messageProducer.send("stage2-messages",request.key(),request.value());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("success");
    }
}