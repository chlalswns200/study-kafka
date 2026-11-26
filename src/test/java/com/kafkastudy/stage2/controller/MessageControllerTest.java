package com.kafkastudy.stage2.controller;

import com.kafkastudy.stage2.dto.SendRequest;
import com.kafkastudy.stage2.producer.MessageProducer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import java.util.concurrent.CompletableFuture;

import static java.beans.Beans.isInstanceOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MessageControllerTest {

    @LocalServerPort
    private int port;

    private RestClient restClient;

    @MockitoBean
    private MessageProducer producer;

    @BeforeEach
    void setUp() {
        this.restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
    }


    @Test
    @DisplayName("카프카 전송 성공")
    void sendSuccessTest() {
        SendRequest sendRequest = new SendRequest("user99", "value999");
        ResponseEntity<Void> entity = restClient.post()
                .uri("/messages")
                .body(sendRequest)
                .retrieve()
                .toBodilessEntity();

        assertThat(entity.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @DisplayName("카프카 전송 실패 - 500")
    void sendFailTest() {

        // given
        given(producer.send(anyString(), anyString(), anyString()))
                .willReturn(CompletableFuture.failedFuture(new RuntimeException("카프카 전송 실패")));

        // when&then
        SendRequest sendRequest = new SendRequest("user99", "value999");
        assertThatThrownBy(()->
            restClient.post()
                    .uri("/messages")
                    .body(sendRequest)
                    .retrieve()
                    .toBodilessEntity())
                .isInstanceOf(HttpServerErrorException.InternalServerError.class);
    }

}