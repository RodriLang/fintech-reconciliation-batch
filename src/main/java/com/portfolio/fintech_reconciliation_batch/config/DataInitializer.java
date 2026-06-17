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

    private final PlatformTransactionRepository repository;

    @Override
    public void onApplicationEvent(@NonNull ContextRefreshedEvent event) {
        log.info("Verificando datos de prueba en la base de datos...");

        seedData("TXN-2026-001", "ACC-1002", new BigDecimal("1500.00"), CurrencyType.USD);
        seedData("TXN-2026-002", "ACC-5541", new BigDecimal("200.00"), CurrencyType.EUR);

        log.info("Inicialización de datos completada.");
    }

    private void seedData(String ref, String account, BigDecimal amount, CurrencyType currency) {

        if (!repository.existsByTransactionReference(ref)) {
            repository.save(TransactionEntity.builder()
                    .transactionReference(ref)
                    .accountId(account)
                    .amount(amount)
                    .currency(currency)
                    .internalStatus(InternalStatus.PROCESSED)
                    .createdAt(LocalDateTime.now())
                    .build());
            log.info("Transacción {} creada.", ref);
        } else {
            log.debug("Transacción {} ya existe, omitiendo.", ref);
        }
    }
}