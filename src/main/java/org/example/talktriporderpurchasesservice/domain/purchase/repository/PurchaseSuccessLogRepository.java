package org.example.talktriporderpurchasesservice.domain.purchase.repository;

import org.example.talktriporderpurchasesservice.domain.purchase.entity.PurchaseSuccessLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PurchaseSuccessLogRepository extends JpaRepository<PurchaseSuccessLog, Long> {
}
