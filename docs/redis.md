# Redis 정리 (talktrip-order-purchases-service)

## 0) 이 서비스에서의 상태(중요)

[`talktrip-order-purchases-service`](../src/main/resources/application.yaml)는 **Redis를 사용하지 않습니다.**  
다만 레포 전체에서는 Redis가 다른 서비스(채팅, 통계 ZINCRBY 버킷, 캐시/락 등)에서 쓰이기 때문에, “Redis가 뭔지”를 빠르게 참고할 수 있도록 기본 개념을 정리합니다. (참고: [DOC/REDIS](../../DOC/REDIS/REDIS_CACHING.md), [`talktrip-stats-service` Redis 소비](../../talktrip-stats-service/src/main/java/org/example/talktripstatsservice/messaging/consumer/KafkaStatsRedisConsumer.java), [`ChatRoomRedisSummaryService`](../../talktrip-chatting-service/src/main/java/org/example/talktripchattingservice/chat/redis/ChatRoomRedisSummaryService.java), [`tt/back_end` CacheConfig](../../tt/back_end/src/main/java/com/talktrip/talktrip/global/config/CacheConfig.java))

---

## 1) Redis 한 줄 정의

Redis는 메모리 기반의 **Key-Value 데이터 스토어**로, 캐시/세션/락/큐/랭킹/ZSET 등 다양한 용도로 사용됩니다.

## 2) 자주 쓰는 자료구조

- **String**: 가장 기본(카운터, 토큰 등)
- **Hash**: 객체 필드 저장(예: `user:1`의 `name`, `age`)
- **List**: 순서 있는 목록(단순 큐/스택)
- **Set**: 중복 없는 집합
- **ZSET(Sorted Set)**: score로 정렬되는 집합(랭킹, 최신순 인덱스, Top N)

## 3) TTL(만료)과 캐시 운영 포인트

- 캐시 키에는 보통 **TTL**을 둬서 메모리/정합성 문제를 줄입니다.
- TTL이 없으면 키가 계속 쌓여서 메모리 압박이 생길 수 있습니다.
- 캐시는 “원본(Source of truth)이 DB/이벤트 로그에 있다”는 전제에서,
  - **미스 시 복구 경로**(DB 재조회 등)
  - **갱신 전략**(write-through / write-behind / invalidate)
  를 같이 설계하는 게 중요합니다.

## 4) Redis와 메시징의 차이

- Redis Pub/Sub: 단순 브로드캐스트(내구성 약함, 재처리 어려움)
- Redis Streams: 로그 기반(Consumer Group, ACK, 재처리 가능)
- Kafka: 분산 로그(대규모 처리/내구성/재처리 강함)

