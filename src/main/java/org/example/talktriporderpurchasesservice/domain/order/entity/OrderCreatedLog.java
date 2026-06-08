package org.example.talktriporderpurchasesservice.domain.order.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * 주문 생성(order-created) 이벤트 감사 로그.
 */
@Entity
@Table(
        name = "order_created_log",
        indexes = {
                @Index(name = "idx_ocl_order_id", columnList = "order_id"),
                @Index(name = "idx_ocl_created_at", columnList = "created_at")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "order_id")
    private Long orderId;

    @Column(name = "order_code", length = 64)
    private String orderCode;

    @Column(name = "member_id")
    private Long memberId;

    @Column(name = "total_price")
    private Integer totalPrice;

    @Column(name = "order_status", length = 32)
    private String orderStatus;

    @Lob
    @Column(name = "payload_json", nullable = false)
    private String payloadJson;

    @Column(name = "kafka_topic", length = 256, nullable = false)
    private String kafkaTopic;

    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "kafka_key", length = 128)
    private String kafkaKey;

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
