package com.portfolio.fintech_reconciliation_batch.step.processor;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@StepScope
@RequiredArgsConstructor
public class TransactionProcessor implements ItemProcessor<TransactionDocument, TransactionDocument>, StepExecutionListener {

    private final PlatformTransactionRepository platformTransactionRepository;

    @Getter
    private final CopyOnWriteArrayList<String> failedIds = new CopyOnWriteArrayList<>();

    private final AtomicLong reconciledCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    private final Map<String, TransactionEntity> dbTransactionsCache = new ConcurrentHashMap<>();

    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {
        log.info("--> [PRE-STEP] Cargando transacciones de la base de datos en memoria para optimizar la conciliación...");

        stepExecution.getExecutionContext().put("failedIds", this.failedIds);
        stepExecution.getExecutionContext().put("reconciledCount", this.reconciledCount);
        stepExecution.getExecutionContext().put("errorCount", this.errorCount);

        List<TransactionEntity> platformTransactions = platformTransactionRepository.findAll();

        for (TransactionEntity txn : platformTransactions) {
            if (txn.getTransactionReference() != null) {
                dbTransactionsCache.put(txn.getTransactionReference(), txn);
            }
        }

        log.info("--> [PRE-STEP] ¡Caché lista! {} transacciones mapeadas en memoria.", dbTransactionsCache.size());
    }

    @Override
    public TransactionDocument process(@NonNull TransactionDocument csvTransaction) {
        String referenciaCsv = csvTransaction.getTransactionReference();

        if (!dbTransactionsCache.containsKey(referenciaCsv)) {
            log.warn("La transacción {} no existe en la base de datos SQL.", referenciaCsv);
            csvTransaction.setStatus(TransactionStatus.ERROR);
            failedIds.add(referenciaCsv);
            errorCount.incrementAndGet();
            return csvTransaction;
        }

        TransactionEntity platformTxn = dbTransactionsCache.get(referenciaCsv);

        boolean amountsMatch = csvTransaction.getAmount().compareTo(platformTxn.getAmount()) == 0;
        boolean currenciesMatch = csvTransaction.getCurrency() == platformTxn.getCurrency();

        if (!amountsMatch || !currenciesMatch) {
            log.warn("Discrepancia en transacción {}: ¿Montos iguales?: {} | ¿Monedas iguales?: {}",
                    referenciaCsv, amountsMatch, currenciesMatch);

            csvTransaction.setStatus(TransactionStatus.ERROR);
            failedIds.add(referenciaCsv);
            errorCount.incrementAndGet();
            return csvTransaction;
        }

        csvTransaction.setStatus(TransactionStatus.RECONCILED);
        reconciledCount.incrementAndGet();

        return csvTransaction;
    }
}