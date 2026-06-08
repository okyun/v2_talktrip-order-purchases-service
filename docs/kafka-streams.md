# Kafka Streams 정리 (talktrip-order-purchases-service)

## 0) 이 서비스에서의 상태(중요)

[`talktrip-order-purchases-service`](../build.gradle)는 `kafka-streams` 의존성이 포함되어 있지만, **현재 코드에는 Streams 토폴로지 구성(`KStream`, `StreamsBuilder`)이 없습니다.**

즉, “Kafka Streams를 쓰는 서비스”가 아니라 **일반 Kafka Consumer 기반의 이벤트 수신/감사로그 적재 서비스**입니다.

---

## 1) Kafka Streams 한 줄 정의

Kafka Streams는 Kafka 토픽을 입력으로 받아 **필터/조인/집계/윈도우 같은 스트림 처리**를 수행하고, 결과를 다른 토픽/상태 저장소로 내보내는 라이브러리입니다.

## 2) Streams가 필요한 상황

- “이벤트를 단순히 받아서 저장/호출”이 아니라,
  - 일정 시간 윈도우 집계(예: 30분 Top N)
  - 사용자/상품 단위 상태를 유지하며 연산
  - 여러 토픽 조인
  - out-of-order 이벤트 처리
  같은 **연속 처리(continuous processing)**가 필요할 때 적합합니다.

## 3) 핵심 개념(용어만)

- **Topology**: 처리 그래프(소스 → 변환 → 싱크)
- **State Store**: 집계/조인에 필요한 로컬 상태 저장소(RocksDB 등)
- **Windowing**: 시간 단위 집계(텀블링/호핑/세션 윈도우)
- **Exactly-once**: 설정/구성에 따라 중복 없이 처리(트랜잭션 기반)

## 4) 이 프로젝트 내 참고(연관 서비스)

Kafka Streams 집계 예시는 이 레포의 [`talktrip-stats-service`](../../talktrip-stats-service/)에서 실제로 사용합니다(TopN 산출) — [`StatisticsTopology`](../../talktrip-stats-service/src/main/java/org/example/talktripstatsservice/stream/topology/StatisticsTopology.java), [`KafkaStreamsConfig`](../../talktrip-stats-service/src/main/java/org/example/talktripstatsservice/stream/config/KafkaStreamsConfig.java).

