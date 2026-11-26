# Kafka 1:1 과외 커리큘럼

> 스택: Spring Boot 4.1 · Spring for Apache Kafka · JPA · MySQL · Docker
> Kafka 버전: 4.x (ZooKeeper 없이 **KRaft 모드**만 사용)

---

## 수업 진행 방식

매 단계(Stage)는 같은 사이클로 돌아간다.

```
① 개념 설명 (선생)  →  ② 코드 적용 (학생)  →  ③ 동작 확인 / 코드 리뷰 (함께)  →  ④ 문제 풀이 (학생)  →  ⑤ 채점·피드백 (선생)
```

**문제 유형**

| 유형 | 예시 |
|---|---|
| 🛠 구현 과제 | "주문 이벤트를 JSON으로 발행하고, 다른 그룹 두 개가 각각 받게 해봐" |
| 🔮 예측 후 실험 | "컨슈머 3개인데 파티션이 2개면 어떻게 될까? 먼저 예측하고 실행해서 확인" |
| 💥 장애 시나리오 | "브로커 1대를 죽였을 때 메시지가 유실되는 설정 조합을 찾아봐" |
| 🐛 버그 찾기 | "이 코드는 메시지가 중복 처리된다. 왜?" |
| 💬 개념/면접 질문 | "at-least-once와 exactly-once 차이를 설명하고, 각각 언제 쓰나?" |

**원칙**
- 코드는 학생이 직접 쓴다. 선생은 설명·힌트·리뷰만 한다.
- 막히면 힌트를 단계적으로 준다 (방향 → 키워드 → 코드 일부).
- 각 Stage의 "완료 조건"을 통과해야 다음 단계로 넘어간다.

---

## 로드맵 한눈에 보기

| Stage | 주제 | 핵심 키워드 |
|---|---|---|
| 0 | 환경 구축 & 첫 만남 | Docker Compose, KRaft, CLI, Kafka UI |
| 1 | 핵심 개념 | Broker, Topic, Partition, Offset, Retention |
| 2 | Producer 기초 | KafkaTemplate, Serializer, Key, Partitioner |
| 3 | Consumer 기초 | @KafkaListener, Consumer Group, Rebalance, Offset Commit |
| 4 | 메시지 포맷 & 테스트 | JSON 직렬화, 타입 매핑, EmbeddedKafka / Testcontainers |
| 5 | 클러스터 & Producer 신뢰성 | Replication, ISR, acks, idempotence, min.insync.replicas |
| 6 | Consumer 신뢰성 & 에러 처리 | 전달 보장, 재시도, DLT, 멱등 컨슈머 |
| 7 | 순서 보장 & 파티션 설계 | Key 설계, 파티션 수, Hot Partition |
| 8 | 트랜잭션 & DB 연동 | Kafka Transaction, 이중 쓰기 문제, Outbox 패턴 |
| 9 | 미니 프로젝트 | 주문-결제-재고 이벤트 기반 시스템 (Saga) |
| 10 | 심화 (선택) | Kafka Streams, Schema Registry, 모니터링, 성능 튜닝 |

---

## Stage 0. 환경 구축 & 첫 만남

**목표**: Kafka를 띄우고, 코드 없이 CLI로 메시지를 주고받아 본다.

**개념**
- Kafka는 무엇인가: 메시지 큐(RabbitMQ 등)와 "분산 커밋 로그"의 차이
- 왜 쓰나: 서비스 간 결합도 감소, 비동기 처리, 이벤트 재처리(replay)
- KRaft: Kafka 4.0부터 ZooKeeper가 제거되고 Kafka가 스스로 메타데이터를 관리

**실습**
- `docker-compose.yml` 작성: Kafka 브로커 1대(KRaft) + Kafka UI
- CLI로 토픽 생성 → console-producer로 발행 → console-consumer로 수신
- Kafka UI에서 토픽·메시지 확인
- 프로젝트 빌드 정상 확인 (JDK 25 툴체인, MySQL 연결)

**완료 조건**: 터미널 두 개 띄워서 한쪽에서 친 메시지가 다른 쪽에 실시간으로 뜬다.

---

## Stage 1. 핵심 개념

**목표**: Kafka 내부 구조를 머릿속에 그릴 수 있다.

**개념**
- Broker / Cluster / Controller
- Topic → Partition → Segment(실제 로그 파일)
- Offset: 파티션 안에서의 메시지 위치, 컨슈머가 "어디까지 읽었는지"
- 메시지는 읽어도 지워지지 않는다 → Retention(시간/크기 기준 삭제), Log Compaction
- Record 구조: key, value, headers, timestamp

**실습**
- 파티션 3개짜리 토픽 생성, key 유무에 따라 어느 파티션에 쌓이는지 관찰
- `--from-beginning`, 특정 offset부터 다시 읽기 (replay)
- 컨테이너 안에서 실제 로그 파일(`.log`, `.index`) 구경하기

**완료 조건**: "토픽-파티션-오프셋" 관계를 그림으로 설명할 수 있다.

---

## Stage 2. Producer 기초 (Spring)

**목표**: Spring 애플리케이션에서 메시지를 발행한다.

**개념**
- `KafkaTemplate`과 `ProducerFactory`
- `application.yaml`의 `spring.kafka.producer.*` 설정
- Serializer (String → 나중에 JSON)
- Key의 역할과 기본 Partitioner (key 해시 / Sticky Partitioner)
- `send()`는 비동기다 → `CompletableFuture<SendResult>`로 결과 확인

**실습**
- REST API(`POST /messages`) → Kafka로 발행
- 발행 성공 시 partition/offset 로그 출력, 실패 시 에러 로그
- 토픽을 코드로 생성 (`NewTopic` Bean)

**완료 조건**: API 호출 → Kafka UI에서 메시지 확인.

---

## Stage 3. Consumer 기초 (Spring)

**목표**: 메시지를 소비하고, 컨슈머 그룹의 동작 원리를 체감한다.

**개념**
- `@KafkaListener`, `ConsumerFactory`, `ListenerContainer`
- Consumer Group: 같은 그룹 = 파티션 나눠 가짐 / 다른 그룹 = 각자 전부 받음
- 파티션 수 vs 컨슈머 수 (남는 컨슈머는 논다)
- Rebalance: 언제 일어나고 무슨 문제가 생기나
- Offset Commit: auto commit vs manual (`AckMode`), `auto.offset.reset`
- `concurrency` 설정

**실습**
- Stage 2의 메시지를 받아 로그 출력
- 같은 그룹 / 다른 그룹 리스너를 만들어 차이 비교
- 앱을 2개 띄워(포트 다르게) 리밸런싱 관찰
- 수동 커밋으로 바꾸고, 커밋 전에 앱을 죽여보기

**완료 조건**: 컨슈머 그룹과 오프셋 커밋이 어떻게 맞물리는지 설명할 수 있다.

---

## Stage 4. 메시지 포맷 & 테스트

**목표**: 실무형 메시지(객체)를 주고받고, 테스트 코드로 검증한다.

**개념**
- JSON 직렬화/역직렬화, 타입 정보 헤더와 trusted packages
- 이벤트 설계: 이벤트 이름, 필드, 버전, eventId
- 테스트 전략: `@EmbeddedKafka` vs Testcontainers
- 비동기 테스트 검증 방법 (Awaitility 등)

**실습**
- `OrderCreatedEvent` 같은 DTO를 발행/수신
- Producer → Consumer 흐름을 통합 테스트로 작성

**완료 조건**: `./gradlew test`로 Kafka 송수신 테스트가 통과한다.

---

## Stage 5. 클러스터 & Producer 신뢰성

**목표**: "메시지가 유실되지 않으려면?"에 답할 수 있다.

**개념**
- Replication: Leader / Follower, ISR(In-Sync Replicas)
- `replication.factor`, `min.insync.replicas`
- `acks=0 / 1 / all` 의 의미와 트레이드오프
- `retries`, `delivery.timeout.ms`, `enable.idempotence` (중복 발행 방지)
- 성능 설정: `linger.ms`, `batch.size`, `compression.type`

**실습**
- Docker Compose를 브로커 3대 클러스터로 확장
- 리더 브로커를 죽이고 리더 선출 관찰
- 설정 조합별로 "유실되는 경우 / 안 되는 경우" 실험

**완료 조건**: 유실 없는 Producer 설정 조합을 근거와 함께 말할 수 있다.

---

## Stage 6. Consumer 신뢰성 & 에러 처리

**목표**: 처리 실패, 중복, 이상한 메시지(poison pill)에 대응한다.

**개념**
- 전달 보장: at-most-once / at-least-once / exactly-once
- `DefaultErrorHandler` + BackOff (재시도)
- Dead Letter Topic: `DeadLetterPublishingRecoverer`, `@RetryableTopic` (Non-blocking retry)
- 역직렬화 실패: `ErrorHandlingDeserializer`
- 멱등 컨슈머: eventId + DB(MySQL) 유니크 키로 중복 처리 방지
- `max.poll.interval.ms`와 처리 시간이 긴 컨슈머 문제

**실습**
- 일부러 예외를 던지는 컨슈머 → 재시도 → DLT 이동 확인
- 깨진 JSON을 발행해서 컨슈머가 멈추지 않게 만들기
- 같은 메시지를 두 번 보내도 DB에 한 번만 반영되게 만들기

**완료 조건**: 실패한 메시지가 "어디로 가고, 어떻게 다시 처리할지" 설계할 수 있다.

---

## Stage 7. 순서 보장 & 파티션 설계

**목표**: 순서가 중요한 비즈니스에서 올바르게 설계한다.

**개념**
- Kafka의 순서 보장은 **파티션 단위**
- Key 설계 (예: orderId, userId)
- 재시도 시 순서 꼬임과 `max.in.flight.requests.per.connection` + idempotence
- 파티션 수 결정 기준, 파티션 증설 시 key 매핑이 바뀌는 문제
- Hot Partition (특정 key 쏠림)

**실습**
- 같은 주문의 "생성 → 결제 → 배송" 이벤트 순서가 보장되는지 검증
- key 없이 보냈을 때 순서가 깨지는 상황 재현

**완료 조건**: 주어진 요구사항에 맞춰 토픽/파티션/key를 설계할 수 있다.

---

## Stage 8. 트랜잭션 & DB 연동

**목표**: DB 저장과 메시지 발행의 정합성을 지킨다. (JPA + MySQL 본격 활용)

**개념**
- 이중 쓰기(Dual Write) 문제: DB 커밋은 됐는데 발행 실패 / 그 반대
- Kafka 트랜잭션: `transactional.id`, `isolation.level=read_committed`
- consume-transform-produce 에서의 exactly-once
- `@Transactional`과 Kafka 트랜잭션은 하나로 묶이지 않는다 (왜?)
- **Transactional Outbox 패턴**: outbox 테이블 + 릴레이(polling)
- `@TransactionalEventListener(AFTER_COMMIT)` 방식과의 비교

**실습**
- 주문 저장 + 이벤트 발행을 Outbox 패턴으로 구현
- 중간에 장애를 넣어도 이벤트가 결국 발행되는지 확인

**완료 조건**: Outbox 패턴이 왜 필요하고 어떻게 동작하는지 설명 + 구현.

---

## Stage 9. 미니 프로젝트

**목표**: 지금까지 배운 걸 엮어 하나의 이벤트 기반 시스템을 만든다.

**주제 (안)**: 주문 → 결제 → 재고 차감 (Choreography Saga)
- 결제 실패 / 재고 부족 시 보상 트랜잭션(주문 취소 이벤트)
- 멱등성, DLT, Outbox, 순서 보장 전부 적용
- 같은 프로젝트 안에서 패키지로 서비스 분리 (나중에 멀티 모듈로 분리해도 됨)

**완료 조건**: 정상/실패 시나리오 모두 테스트로 검증.

---

## Stage 10. 심화 (선택)

관심 있는 것만 골라서 진행.
- **Kafka Streams**: 실시간 집계 (예: 분당 주문 수), KTable, 윈도우
- **Schema Registry + Avro/Protobuf**: 스키마 진화와 호환성
- **모니터링**: Consumer Lag, JMX 메트릭, Prometheus/Grafana
- **성능 튜닝**: 처리량 vs 지연시간, 배치 리스너
- **Kafka 4.x 신기능**: 새 컨슈머 그룹 프로토콜(KIP-848), Share Group(큐처럼 쓰기, KIP-932)

---

## 진도 체크

- [x] Stage 0. 환경 구축 & 첫 만남
- [x] Stage 1. 핵심 개념
- [x] Stage 2. Producer 기초
- [ ] Stage 3. Consumer 기초
- [ ] Stage 4. 메시지 포맷 & 테스트
- [ ] Stage 5. 클러스터 & Producer 신뢰성
- [ ] Stage 6. Consumer 신뢰성 & 에러 처리
- [ ] Stage 7. 순서 보장 & 파티션 설계
- [ ] Stage 8. 트랜잭션 & DB 연동
- [ ] Stage 9. 미니 프로젝트
- [ ] Stage 10. 심화 (선택)
