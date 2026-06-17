package com.portfolio.fintech_reconciliation_batch.step.processor;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionProcessor implements ItemProcessor<TransactionDocument, TransactionDocument> {

    private final PlatformTransactionRepository platformTransactionRepository;

    @Override
    public TransactionDocument process(TransactionDocument csvTransaction) {
        log.info("Validando transacción desde el CSV: {}", csvTransaction.getTransactionReference());

        Optional<TransactionEntity> platformTxnOptional = platformTransactionRepository
                .findByTransactionReference(csvTransaction.getTransactionReference());

        if (platformTxnOptional.isEmpty()) {
            log.warn("La transacción {} no existe en la base de datos SQL.", csvTransaction.getTransactionReference());
            csvTransaction.setStatus(TransactionStatus.ERROR);
            return csvTransaction;
        }

        TransactionEntity platformTxn = platformTxnOptional.get();

        boolean amountsMatch = csvTransaction.getAmount().compareTo(platformTxn.getAmount()) == 0;
        boolean currenciesMatch = csvTransaction.getCurrency() == platformTxn.getCurrency();

        if (!amountsMatch || !currenciesMatch) {
            log.error("INCONSISTENCIA DETECTADA. Ref: {}. CSV: {} {}. SQL: {} {}.",
                    csvTransaction.getTransactionReference(),
                    csvTransaction.getAmount(), csvTransaction.getCurrency(),
                    platformTxn.getAmount(), platformTxn.getCurrency());

            csvTransaction.setStatus(TransactionStatus.FAILED);
            return csvTransaction;
        }

        log.info("Transacción {} conciliada exitosamente.", csvTransaction.getTransactionReference());
        csvTransaction.setStatus(TransactionStatus.RECONCILED);

        return csvTransaction;
    }
}