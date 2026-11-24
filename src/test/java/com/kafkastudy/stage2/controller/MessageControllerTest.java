package com.kafkastudy.stage2.controller;

import com.kafkastudy.stage2.dto.SendRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;

class MessageControllerTest {

    RestClient restClient = RestClient.builder()
            .baseUrl("http://localhost:8080")
            .build();

    @Test
    void sendTest() {
        SendRequest sendRequest = new SendRequest("user99", "value99");
        ResponseEntity<String> entity = restClient.post()
                .uri("/messages")
                .body(sendRequest)
                .retrieve()
                .toEntity(String.class);

        assertThat(entity.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(entity.getBody()).isEqualTo("success");
    }

}