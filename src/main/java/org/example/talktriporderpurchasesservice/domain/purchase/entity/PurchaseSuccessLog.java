package org.example.talktriporderpurchasesservice.domain.purchase.entity;

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
 * 결제 성공(payment-success) 이벤트를 감사·추적용으로 orderDB에 남기는 로그.
 */
@Entity
@Table(
        name = "purchase_success_log",
        indexes = {
                @Index(name = "idx_psl_order_id", columnList = "order_id"),
                @Index(name = "idx_psl_created_at", columnList = "created_at")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PurchaseSuccessLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "order_id")
    private Long orderId;

    @Column(name = "order_code", length = 64)
    private String orderCode;

    @Column(name = "member_email", length = 320)
    private String memberEmail;

    @Column(name = "total_amount")
    private Integer totalAmount;

    @Column(name = "payment_key", length = 200)
    private String paymentKey;

    @Column(name = "payment_method", length = 64)
    private String paymentMethod;

    @Column(name = "payment_status", length = 32)
    private String paymentStatus;

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
