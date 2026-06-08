# talktrip-order-purchases-service 문서 인덱스

이 폴더는 `talktrip-order-purchases-service` 안에서 **메시징/스트리밍 구성요소를 빠르게 찾아볼 수 있게** 쪼개 정리한 문서들입니다.

## 이 서비스에서 실제로 쓰는 것(현재 코드 기준)

- **Kafka(Consumer)**: `order-created` 토픽을 구독해 로그를 남기거나(order-created debug), 감사 로그를 DB에 적재합니다(audit). — [`OrderCreatedDebugConsumer`](../src/main/java/org/example/talktriporderpurchasesservice/messaging/consumer/OrderCreatedDebugConsumer.java), [`application.yaml`](../src/main/resources/application.yaml)
- **Kafka Streams**: [`build.gradle`](../build.gradle)에 의존성은 있으나, 현재 `src/main/java` 코드에서는 Streams 토폴로지(`KStream`, `StreamsBuilder`)를 사용하지 않습니다. (참고 구현: [`talktrip-stats-service`](../../talktrip-stats-service/src/main/java/org/example/talktripstatsservice/stream/topology/StatisticsTopology.java))
- **Redis / Redis Streams**: 이 서비스에는 Redis 설정/사용 코드가 없습니다. (레포 내 참고: [DOC/REDIS](../../DOC/REDIS/REDIS_STREAM.md), [talktrip-chatting-service `pubsubTOstreams.md`](../../talktrip-chatting-service/docs/pubsubTOstreams.md))

## 문서 목록

- [`flash-sale-traffic.md`](flash-sale-traffic.md): **특가·몰림 트래픽** 시 파이프라인 분석, 기존 병목, 추가 개선(concurrency·rate limit·FE 대기 UI), 미적용 항목
- [`kafka.md`](kafka.md): Kafka(Producer/Consumer, consumer group, offset, ack 등) 핵심 개념과 이 서비스 적용 포인트
- [`kafka-streams.md`](kafka-streams.md): Kafka Streams(상태 저장, 윈도우, exactly-once 등) 개념과 “이 서비스에는 아직 미적용” 메모
- [`redis.md`](redis.md): Redis 기본 개념(키/자료구조/TTL/캐시 주의점)
- [`redis-streams.md`](redis-streams.md): Redis Streams(consumer group, pending, ack, id) 개념과 “이 서비스에는 미사용” 메모

