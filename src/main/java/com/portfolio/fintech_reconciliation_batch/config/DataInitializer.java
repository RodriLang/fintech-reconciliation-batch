package com.portfolio.fintech_reconciliation_batch.config;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.InternalStatus;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements ApplicationListener<ContextRefreshedEvent> {

    private final PlatformTransactionRepository platformTransactionRepository;

    @Override
    public void onApplicationEvent(@NonNull ContextRefreshedEvent event) {
        log.info("========================================================");
        log.info("Cargando datos de prueba en la base de datos SQL (H2)...");
        log.info("========================================================");

        platformTransactionRepository.save(TransactionEntity.builder()
                .transactionReference("TXN-2026-001")
                .accountId("ACC-1002")
                .amount(new BigDecimal("1500.00")) // Coincide
                .currency(CurrencyType.USD)
                .internalStatus(InternalStatus.PROCESSED)
                .createdAt(LocalDateTime.now())
                .build());

        platformTransactionRepository.save(TransactionEntity.builder()
                .transactionReference("TXN-2026-002")
                .accountId("ACC-5541")
                .amount(new BigDecimal("200.00")) // No coincide
                .currency(CurrencyType.EUR)
                .internalStatus(InternalStatus.PROCESSED)
                .createdAt(LocalDateTime.now())
                .build());

        log.info("Datos de prueba SQL inyectados con éxito.");
    }
}