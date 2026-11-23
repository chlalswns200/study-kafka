# 진도 기록

> PC를 바꾸면 Claude 세션이 이어지지 않기 때문에, 수업 내용을 여기에 남긴다.
> 새 세션에서는 `docs/CURRICULUM.md`와 이 파일을 먼저 읽고 이어서 진행한다.

## 현재 위치

- **진행 중**: Stage 2. Producer 기초 (① 개념 설명 전)
- **다음 할 일**: Stage 2 개념 설명 → Spring `KafkaTemplate`으로 REST API → Kafka 발행 구현

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
