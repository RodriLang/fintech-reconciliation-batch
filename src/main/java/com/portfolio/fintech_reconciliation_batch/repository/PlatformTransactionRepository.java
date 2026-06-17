package com.portfolio.fintech_reconciliation_batch.repository;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PlatformTransactionRepository extends JpaRepository<TransactionEntity, Long> {

    Optional<TransactionEntity> findByTransactionReference(String transactionReference);

    boolean existsByTransactionReference(String transactionReference);
}