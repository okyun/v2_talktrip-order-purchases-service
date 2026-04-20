package org.example.talktriporderpurchasesservice.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * order-created 토픽 디버깅/감사용 Consumer.
 *
 * - back_end의 디버그 Consumer에서 order-created 구독을 분리 이관하는 용도.
 * - 이 서비스에서는 DB 조회 없이 payload의 주요 필드만 로그로 남깁니다.
 */
@Component
public class OrderCreatedDebugConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderCreatedDebugConsumer.class);

    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "debug-order-created-consumer",
            concurrency = "1"
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
        logger.info("order-created 수신: orderId={}, memberId={}, topic={}, partition={}, offset={}, key={}",
                orderId, memberId, topic, partition, offset, key);
    }

    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "audit-order-created-consumer",
            concurrency = "1"
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
        logger.debug("order-created 수신(audit): orderId={}, memberId={}, topic={}, partition={}, offset={}, key={}",
                orderId, memberId, topic, partition, offset, key);
    }

    @KafkaListener(
            topics = "${kafka.topics.payment-success:payment-success}",
            groupId = "talktrip-order-purchases-service",
            concurrency = "1"
    )
    public void onPaymentSuccess(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Object orderId = payload != null ? payload.get("orderId") : null;
        Object orderCode = payload != null ? payload.get("orderCode") : null;
        logger.info("payment-success 수신(purchases): orderId={}, orderCode={}, topic={}, partition={}, offset={}, key={}",
                orderId, orderCode, topic, partition, offset, key);
    }
}

