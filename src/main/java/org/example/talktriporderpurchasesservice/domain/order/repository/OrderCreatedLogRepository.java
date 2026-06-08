package org.example.talktriporderpurchasesservice.domain.order.repository;

import org.example.talktriporderpurchasesservice.domain.order.entity.OrderCreatedLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderCreatedLogRepository extends JpaRepository<OrderCreatedLog, Long> {
}
