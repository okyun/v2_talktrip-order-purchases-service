package org.example.talktriporderpurchasesservice.messaging.consumer;

import org.example.talktriporderpurchasesservice.domain.common.PayloadEventTimeUtils;
import org.example.talktriporderpurchasesservice.domain.order.service.OrderCreatedLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * order-created 토픽 디버깅/감사용 Consumer.
 *
 * - back_end의 디버그 Consumer에서 order-created 구독을 분리 이관하는 용도.
 * - order-created (debug 그룹): 애플리케이션 로그만.
 * - order-created (audit 그룹): orderDB `order_created_log` 적재 — debug 그룹과 이중 저장 방지.
 */
@Component
public class OrderCreatedDebugConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderCreatedDebugConsumer.class);

    private final OrderCreatedLogService orderCreatedLogService;

    public OrderCreatedDebugConsumer(
            OrderCreatedLogService orderCreatedLogService
    ) {
        this.orderCreatedLogService = orderCreatedLogService;
    }

    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "debug-order-created-consumer",
            concurrency = "3" // order-created 토픽 파티션 3개 → @KafkaListener concurrency=3
    )
    public void listenOrderCreated(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Object orderId = payload != null ? payload.get("orderId") : null;
        Object memberId = payload != null ? payload.get("memberId") : null;
        Instant createdAt = PayloadEventTimeUtils.resolveCreatedAt(payload);
        logger.info("order-created 수신: orderId={}, memberId={}, createdAt={}, topic={}, partition={}, offset={}, key={}",
                orderId, memberId, createdAt, topic, partition, offset, key);
    }

    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "audit-order-created-consumer",
            concurrency = "3" // order-created 토픽 파티션 3개 → @KafkaListener concurrency=3
    )
    public void listenOrderCreatedAudit(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Object orderId = payload != null ? payload.get("orderId") : null;
        Object memberId = payload != null ? payload.get("memberId") : null;
        Instant createdAt = PayloadEventTimeUtils.resolveCreatedAt(payload);
        logger.debug("order-created 수신(audit): orderId={}, memberId={}, createdAt={}, topic={}, partition={}, offset={}, key={}",
                orderId, memberId, createdAt, topic, partition, offset, key);
        try {
            orderCreatedLogService.saveFromPayload(payload, topic, partition, offset, key);
        } catch (Exception e) {
            logger.error("order-created orderDB(order_created_log) 저장 실패: orderId={}, err={}",
                    orderId, e.getMessage(), e);
        }
    }
}

