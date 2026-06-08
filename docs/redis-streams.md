# Redis Streams 정리 (talktrip-order-purchases-service)

## 0) 이 서비스에서의 상태(중요)

[`talktrip-order-purchases-service`](../src/main/resources/application.yaml)는 **Redis Streams를 사용하지 않습니다.**  
레포 내 다른 서비스(예: 채팅 브로드캐스트, 결제 성공 워커)에서 “Pub/Sub vs Streams” 논의가 있어, Streams 개념을 참고용으로 정리합니다. (참고: [DOC/REDIS/REDIS_STREAM.md](../../DOC/REDIS/REDIS_STREAM.md), [`talktrip-chatting-service` `pubsubTOstreams.md`](../../talktrip-chatting-service/docs/pubsubTOstreams.md), [`PaymentSuccessStreamWorker`](../../tt/back_end/src/main/java/com/talktrip/talktrip/domain/order/service/PaymentSuccessStreamWorker.java))

---

## 1) Redis Streams 한 줄 정의

Redis Streams는 Redis 안에 **append-only 로그 스트림**을 저장하고, Consumer Group으로 **분산 소비 + ACK + 재처리**를 지원하는 자료구조입니다.

## 2) Pub/Sub과의 핵심 차이

- **Pub/Sub**
  - 구독자가 연결되어 있어야만 받음(내구성/재처리 약함)
  - 메시지 저장/ACK 개념 없음
- **Streams**
  - 스트림에 저장되고, 컨슈머가 ID 기준으로 읽음(로그)
  - Consumer Group + ACK로 “처리 완료”를 추적
  - 장애 시 pending 메시지 재처리 가능

## 3) 주요 개념

- **Stream key**: 스트림이 저장되는 Redis 키
- **Entry ID**: 스트림 메시지의 ID(시간 기반 + 시퀀스)
- **Consumer Group**: 여러 컨슈머가 작업을 나눠 처리
- **Pending Entries List(PEL)**: 읽었지만 ACK 안 된 항목 목록(재처리 포인트)

## 4) 언제 Streams를 고려하나

- Kafka까지는 과하고, Redis를 이미 쓰고 있는데
- “유실 없는 큐/작업 분배/재처리”가 필요할 때

