# 진도 기록

> PC를 바꾸면 Claude 세션이 이어지지 않기 때문에, 수업 내용을 여기에 남긴다.
> 새 세션에서는 `docs/CURRICULUM.md`와 이 파일을 먼저 읽고 이어서 진행한다.

## 현재 위치

- **진행 중**: Stage 3. Consumer 기초 (① 개념 설명 전)
- **다음 할 일**: Stage 3 개념 설명 → `@KafkaListener` 구현
- **Stage 2 남은 정리(선택)**: `MessageControllerTest.sendTest()`(옛 "success" 바디 기대) 삭제 → 현재 유일한 실패 테스트. `sendSuccessTest`에 명시적 stub 추가

---

## Stage 2. Producer 기초 — ✅ 완료 (2026-11-26)

**결과물**: `stage2/config/KafkaConfig`(NewTopic), `stage2/producer/MessageProducer`(whenComplete 콜백 로그), `stage2/controller/MessageController`(`POST /messages` → 202), `stage2/dto/SendRequest(key, value)`, 테스트 2개. 과제 4: 브로커에 `stage2-messages` 13건(p0:8, p1:2, p2:3) 확인.

**코드 리뷰 (2026-11-26)**
- 잘한 점: `whenComplete`로 성공/실패 분기, 실패 시 예외 객체까지 로깅, `ProducerRecord`에서 key를 꺼낸 점, DTO를 record로, 202 사용
- 🔴 필수: 빈 이름 충돌 위험 — `KafkaConfig` 클래스명/`topic()` 메서드명이 너무 일반적. Stage 3+에서 같은 이름 쓰면 `ConflictingBeanDefinitionException`/빈 오버라이딩 금지로 기동 실패 → `Stage2KafkaConfig`, `stage2MessagesTopic()` 등으로
- 🟡 권장: 토픽명 문자열이 KafkaConfig·Controller·Test 3곳에 중복 → 상수 하나로
- 🟡 권장: `MessageControllerTest`는 앱을 따로 띄워야 동작하는 수동 테스트 → Stage 4에서 제대로
- ⚪ 사소: 테스트 미사용 import, DB 비밀번호 환경변수화(`${DB_PASSWORD:1234}`) 미적용

**문제 1차 채점 (2026-11-26)**
| # | 질문 | 결과 | 피드백 |
|---|---|---|---|
| Q1(a) | 콜백 스레드 | ○ | HTTP 스레드와 다름 정답. 정확한 이름: `kafka-producer-network-thread \| producer-1` = Sender I/O 스레드(프로듀서당 1개). 보충: future가 이미 완료된 뒤 `whenComplete`를 걸면 호출한 스레드에서 실행됨 |
| Q1(b) | 콜백에서 2초 작업 | △ | "메인 스레드 점유"는 오답 → 막히는 건 **I/O 스레드 1개**. 다른 배치 전송·ack 처리 전부 정지 → 버퍼(`buffer.memory` 32MB) 가득 → `send()`가 `max.block.ms`까지 블로킹 → HTTP 스레드도 막힘. "서버 전체가 나빠진다"는 결론은 경로는 다르지만 맞음. 콜백은 가볍게, 무거운 일은 별도 Executor |
| Q2(a) | 테스트가 검증하는 것 | ✗ | assert가 없어 **아무것도 검증 안 함**. 확인되는 건 "send() 호출이 동기 예외를 안 던진다"뿐. 결과는 테스트 끝난 뒤 콜백 로그로만 |
| Q2(b) | 브로커 끄면? | ○(정정) | 1차 채점에서 선생이 "통과"라고 했으나 **틀렸음** → R1 실험 결과 **실패**가 맞음(예측 정답). 이유: 메타데이터 60초 대기 후 `KafkaTemplate`이 **즉시 실패(immediate failure)**를 감지해 호출 스레드(main)에 `KafkaException: Send failed` 동기 throw. 실패 경로 2개: ① send() 시점 즉시 실패(메타데이터 없음, 직렬화 오류) → 동기 예외 ② 전송 후 실패(메타데이터는 있는데 브로커가 나중에 죽음) → future/콜백으로만, `delivery.timeout.ms`(120초) 후. ②번이면 옛 테스트는 초록 |
| Q2(c) | 검증하게 만들려면 | ○ | 방향 정답: future 반환 → 테스트에서 `get()`/`join()` 후 partition/offset assert. 실패 시 `ExecutionException` → R2로 구현 |
| Q3(a) | "success"는 사실? | ○ | 정답. 더 근본적으로 컨트롤러는 결과가 오기 **전에** 응답함 → 의미상 "accepted(접수)"가 맞음 |
| Q3(b) | 실패를 알려야 한다면 | △ | 설계(결과 반환/예외 전파 후 에러 코드)는 맞음. **대가 누락**: HTTP 스레드가 브로커 ack까지 대기 → 응답 지연↑, 처리량↓, 브로커 느리면 스레드 풀 고갈. 대안: 컨트롤러가 `CompletableFuture`를 그대로 반환(Spring MVC 비동기 요청) |
| Q4 | 파티션 3→5 | ○(실험) | 예측은 빗나갔지만 실험으로 원인까지 찾음. `hash(key) % 3` → `% 5`로 매핑 변경. 기존 메시지는 **이동 안 함** → user1 메시지가 두 파티션에 나뉘어 순서 깨짐. 파티션은 늘리기만 가능. Stage 1 Q3(d) "`__consumer_offsets`가 처음부터 50개인 이유"와 같은 원리 |

**복습 과제 채점 (2026-11-26)**
| # | 과제 | 결과 | 피드백 |
|---|---|---|---|
| R1 | 브로커 끄고 테스트 | ○ | 실험 완료. 반복 로그는 메시지 재전송이 아니라 **메타데이터를 얻으려는 재접속**(`reconnect.backoff.ms` 50ms → 최대 1초, Kafka 4.x rebootstrap). 레코드는 파티션을 못 정해서 버퍼에도 못 들어감. ERROR 로그 스레드가 `[main]` = 동기 예외. `max.block.ms=3000`은 미적용(60000ms) |
| R2 | future 반환 + assert | ○+ | `send()`가 future 반환, 테스트에서 `thenAccept(assert).join()` → assert 실패 시 `CompletionException`으로 테스트 실패, 정상 동작. 보완: 타임아웃 없음(`get(10, SECONDS)` / `orTimeout`). **요구 이상 구현**: 컨트롤러가 `CompletableFuture<ResponseEntity>` 반환(Spring MVC 비동기) + `exceptionally`로 500, `@MockitoBean`·RANDOM_PORT 테스트 |
| 필수 | 빈 이름 | ○ | `Stage2KafkaConfig` / `stage2MessagesTopic()` 완료 |

**R2 코드 리뷰 (선생이 `./gradlew test --tests 'com.kafkastudy.stage2.*'` 실행: 4개 중 1개 실패)**
- 🔴 `MessageControllerTest.sendTest()` 실패 — 컨트롤러가 이제 바디 없이 202를 주는데 여전히 `"success"` 바디를 기대. 옛 테스트라 삭제 대상
- 🟡 `sendSuccessTest`는 stub 없이 통과 → Mockito 기본값이 완료된 future를 돌려줘서 우연히 성공. `given(...).willReturn(completedFuture(...))`로 의도를 명시
- 🟡 컨트롤러의 `exceptionally`는 ②(비동기 실패)만 잡음. ①(동기 `KafkaException`)은 메서드 밖으로 던져져 Spring 기본 에러 처리로 500 → 결과는 같지만 경로가 다름. 테스트도 ②만 검증
- ⚪ 이제 브로커 ack 후 응답하므로 "Kafka 저장 완료"까지 보장. 202 유지해도 무방(컨슈머 처리는 아직)

---

## Stage 1. 핵심 개념 — ✅ 완료 (2026-11-23)

**실습**: 실습 1~4 및 Q1·Q2 실험은 다른 PC에서 수행 완료(사용자 보고).

**문제 1차 채점 (2026-11-23)**
| # | 질문 | 결과 | 피드백 |
|---|---|---|---|
| Q1 | key 없이 보내면 골고루 퍼질까? | ○ | 결론(안 퍼짐) 정답. 이유 보완: key가 null이라 해시 불가 → **Sticky Partitioner**가 배치가 찰 때(batch.size 16KB)까지 한 파티션에 몰아 보냄(배치 효율). 데이터가 많아지면 장기적으로는 분산됨. 옛날(2.4 이전)엔 라운드로빈 |
| Q2 | `--from-beginning` 유무 | △ | offset 관련이라는 방향은 맞음. 오개념 2개: ① console-consumer는 `--group` 없으면 **매번 새 랜덤 그룹** → 책갈피가 없음 → `auto.offset.reset=latest`(지금부터 오는 것만) → 사과/바나나/포도 안 뜸 ② `--from-beginning` = `earliest` = 로그에 남은 **맨 처음 offset부터 전부**("안 읽힌 것부터"가 아님). 또 책갈피가 이미 있는 그룹이면 `--from-beginning`은 무시됨 |
| Q3 | `__consumer_offsets` | △ | (a)○ 그룹별·토픽별·파티션별 committed offset. (b)△ 토픽마다가 아니라 클러스터에 하나, **처음으로 컨슈머 그룹이 코디네이터를 찾을 때** 자동 생성. (c)✗ **compact** — key(그룹+토픽+파티션)별 최신 offset만 의미 있음. (그룹 offset 만료 `offsets.retention.minutes`=7일은 tombstone으로 처리). (d)✗ `offsets.topic.num.partitions` 기본 50 → hash(group.id)%50 파티션의 리더 브로커가 그 그룹의 **코디네이터** → 부하 분산. 나중에 바꾸면 매핑이 깨지니 처음부터 넉넉히 |
| Q4 | retention 지났는데 남은 메시지 | ✗ | 삭제는 메시지 단위가 아니라 **세그먼트 단위**, 그리고 **active segment는 절대 삭제 안 됨**. 세그먼트 안의 가장 최신 메시지가 retention을 넘어야 그 세그먼트 통째로 삭제. 오개념: "offset에서 사라진다" → offset 번호는 절대 재배치 안 됨, log start offset만 앞으로 이동 |

**복습 문제 채점**
| # | 질문 | 결과 | 피드백 |
|---|---|---|---|
| R1 | `--group study` 재실행 시 `--from-beginning` | ○ | 정답. 그룹에 committed offset이 있으면 `auto.offset.reset`(=from-beginning)은 무시 → 딸기·수박만. Q2 오개념 해소 |
| R2 | 세그먼트 7일 롤 + retention 7일 최악의 경우 | ○ | 정답 약 14일(+ retention 체크 주기 5분). 보충: 시간 기반 롤은 새 메시지가 append될 때 판단되므로 트래픽이 끊기면 더 길어질 수 있음 |

**복습 포인트**: auto.offset.reset(latest/earliest)과 committed offset의 우선순위, 세그먼트 단위 삭제, compaction 용도 → Stage 3(Consumer)에서 다시 나옴

---

## Stage 0. 환경 구축 & 첫 만남 — ✅ 완료 (2026-11-18)

**결과물**
- `docker-compose.yaml`: KRaft 브로커 1대(`apache/kafka:4.1.0`, combined mode) + Kafka UI(8989)
- 리스너: `INTERNAL://kafka:29092`(컨테이너 간), `EXTERNAL://localhost:9092`(호스트), `CONTROLLER://:9093`

**실습**: `stage0-hello` 토픽 생성 → console-producer/consumer로 송수신 → Kafka UI에서 offset 0~2 확인. 통과.

**개념 질문 채점**
| # | 질문 | 결과 | 피드백 |
|---|---|---|---|
| 1 | 메시지 큐 vs 분산 커밋 로그 | △ | 읽어도 안 지워짐/replay는 맞음. "전부 영구 보관"은 오답 → Retention으로 삭제됨. 핵심 누락: offset을 컨슈머 쪽이 관리해서 여러 그룹이 독립적으로 읽음 (MQ는 ack 시 삭제, 한 컨슈머만 수신) |
| 2 | KRaft / `broker,controller` 의미 | △ | ZooKeeper 대체는 맞음. 컨트롤러 = 클러스터 메타데이터(브로커 생존, 토픽/파티션, 리더) 관리. `broker,controller` = 한 노드가 두 역할(combined mode)이라는 후반부 답변 누락 |
| 3 | kafka-ui가 `kafka:29092`를 쓰는 이유 | ○ | 리스너 구분 정확. 예측 보정: "반영 안 됨"이 아니라 "연결 자체 실패"(컨테이너 안 localhost = 자기 자신). advertised 주소로 재접속한다는 점은 Stage 5에서 다시 다룸 |

- 3번 "예측 후 실험"(kafka-ui bootstrap을 `localhost:9092`로 바꿔보기)은 건너뜀 → 나중에 해볼 수 있음

**복습 포인트**: Retention, 컨슈머가 offset을 관리한다는 점 (Stage 1·3에서 다시 나옴)

---

## 환경 메모

- 이 PC(Windows, 사용자 폴더 한글): JDK는 IntelliJ가 받은 `~/.jdks/openjdk-25.0.2`
- `gradlew test` 시 `byte-buddy-agent ... Error opening zip file` 경고 → Gradle 캐시 경로의 한글 때문.
  빌드는 성공하지만 Stage 4(Mockito)에서 문제될 수 있음. 해결: 사용자 환경변수 `GRADLE_USER_HOME=C:\gradle-home`
