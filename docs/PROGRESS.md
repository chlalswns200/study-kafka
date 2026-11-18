# 진도 기록

> PC를 바꾸면 Claude 세션이 이어지지 않기 때문에, 수업 내용을 여기에 남긴다.
> 새 세션에서는 `docs/CURRICULUM.md`와 이 파일을 먼저 읽고 이어서 진행한다.

## 현재 위치

- **진행 중**: Stage 1. 핵심 개념 (① 개념 설명 단계)
- **다음 할 일**: Stage 1 실습 과제 수행 → 개념 질문 답변

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
