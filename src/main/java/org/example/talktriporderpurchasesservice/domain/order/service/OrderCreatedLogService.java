package org.example.talktriporderpurchasesservice.domain.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.talktriporderpurchasesservice.domain.common.PayloadEventTimeUtils;
import org.example.talktriporderpurchasesservice.domain.order.entity.OrderCreatedLog;
import org.example.talktriporderpurchasesservice.domain.order.repository.OrderCreatedLogRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

@Service
public class OrderCreatedLogService {

    private final OrderCreatedLogRepository repository;
    private final ObjectMapper objectMapper;

    public OrderCreatedLogService(OrderCreatedLogRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void saveFromPayload(
            Map<String, Object> payload,
            String topic,
            int partition,
            long offset,
            String key
    ) throws JsonProcessingException {
        Instant createdAt = PayloadEventTimeUtils.resolveCreatedAt(payload);
        Map<String, Object> payloadToStore = PayloadEventTimeUtils.enrichCreatedAt(payload, createdAt);
        if (createdAt == null) {
            createdAt = Instant.now();
            payloadToStore.put("createdAt", createdAt.toString());
        }

        String json = objectMapper.writeValueAsString(payloadToStore);
        OrderCreatedLog row = OrderCreatedLog.builder()
                .createdAt(createdAt)
                .orderId(longValue(payload, "orderId"))
                .orderCode(str(payload, "orderCode"))
                .memberId(longValue(payload, "memberId"))
                .totalPrice(intValue(payload, "totalPrice"))
                .orderStatus(str(payload, "orderStatus"))
                .payloadJson(json)
                .kafkaTopic(topic)
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .kafkaKey(key)
                .build();
        repository.save(row);
    }

    private static String str(Map<String, Object> payload, String field) {
        if (payload == null) {
            return null;
        }
        Object v = payload.get(field);
        if (v == null) {
            return null;
        }
        return v.toString();
    }

    private static Long longValue(Map<String, Object> payload, String field) {
        if (payload == null) {
            return null;
        }
        Object v = payload.get(field);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer intValue(Map<String, Object> payload, String field) {
        if (payload == null) {
            return null;
        }
        Object v = payload.get(field);
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
