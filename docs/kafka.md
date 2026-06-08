# Kafka 정리 (talktrip-order-purchases-service)

이 문서는 Kafka를 “이 서비스가 실제로 사용하는 방식(Consumer)” 관점에서 핵심만 정리합니다.

## 1) Kafka 한 줄 정의

Kafka는 **이벤트를 토픽(topic)에 append-only 로그로 쌓고**, 컨슈머가 **offset을 기준으로 읽는** 분산 메시징 시스템입니다.

## 2) 기본 용어

- **topic**: 이벤트가 쌓이는 스트림(논리적 채널)
- **partition**: topic을 쪼갠 단위(병렬성/순서 보장 단위)
- **offset**: partition 내에서 메시지의 위치(읽기 진행도를 나타냄)
- **consumer group**: 같은 groupId를 가진 컨슈머들이 **파티션을 분담**해서 처리(스케일 아웃)

## 3) “순서 보장”의 범위

- Kafka는 **같은 partition 안에서는 순서를 보장**합니다.
- topic 전체의 “전역 순서”는 없습니다.
- 따라서 “주문 단위 순서”가 필요하면 보통 **key를 orderId 같은 값으로 고정**하여 같은 partition으로 가도록 설계합니다.

## 4) 재처리(replay)와 중복 처리

- 컨슈머는 offset 기반으로 읽기 때문에, 운영/디버그 목적의 재처리는 “offset을 되돌려 다시 읽기”로 가능합니다. (발행 측: [`OrderService`](../../tt/back_end/src/main/java/com/talktrip/talktrip/domain/order/service/OrderService.java) → [`OrderEventPublisher.publishOrderCreated`](../../tt/back_end/src/main/java/com/talktrip/talktrip/domain/event/order/OrderEventPublisher.java#L52) → [`KafkaEventProducer.publishOrderCreated`](../../tt/back_end/src/main/java/com/talktrip/talktrip/domain/messaging/avro/KafkaEventProducer.java#L56))
- 대신 “적어도 한 번(at-least-once)” 처리에서 **중복 처리**가 생길 수 있습니다.
  - 예: DB 저장이 성공했는데 커밋 전에 장애 → 재시작 후 같은 메시지 재처리
- 이 서비스처럼 “감사 로그 적재”가 있다면,
  - **idempotent 키(예: topic+partition+offset, 또는 비즈니스 키)**로 중복 삽입을 막거나
  - unique constraint를 두는 방식이 일반적입니다.

## 5) 이 서비스에서의 적용 포인트(현재 코드)

- [`@KafkaListener`](../src/main/java/org/example/talktriporderpurchasesservice/messaging/consumer/OrderCreatedDebugConsumer.java)로 다음 토픽을 구독합니다.
  - `order-created` ([`kafka.topics.order-created`](../src/main/resources/application.yaml))
    - groupId `debug-order-created-consumer`: [`listenOrderCreated`](../src/main/java/org/example/talktriporderpurchasesservice/messaging/consumer/OrderCreatedDebugConsumer.java#L39) — 로그만 남김(디버그)
    - groupId `audit-order-created-consumer`: [`listenOrderCreatedAudit`](../src/main/java/org/example/talktriporderpurchasesservice/messaging/consumer/OrderCreatedDebugConsumer.java#L57) → [`OrderCreatedLogService.saveFromPayload`](../src/main/java/org/example/talktriporderpurchasesservice/domain/order/service/OrderCreatedLogService.java#L24) → [`OrderCreatedLog`](../src/main/java/org/example/talktriporderpurchasesservice/domain/order/entity/OrderCreatedLog.java) / [`OrderCreatedLogRepository`](../src/main/java/org/example/talktriporderpurchasesservice/domain/order/repository/OrderCreatedLogRepository.java)

컨슈머 그룹을 분리한 이유(코드 주석 요약):
- debug 그룹과 audit 그룹이 “같은 데이터를 각자 목적대로” 소비하되,
- **audit 저장은 audit 그룹에서만** 수행해 “이중 적재”를 피하려는 의도입니다.

## 6) 자주 보는 설정 의미

[`spring.kafka.consumer.auto-offset-reset=earliest`](../src/main/resources/application.yaml):
- 컨슈머 그룹이 처음 생성되어 offset이 없을 때, **기존에 쌓인 메시지부터** 읽기 시작합니다.

