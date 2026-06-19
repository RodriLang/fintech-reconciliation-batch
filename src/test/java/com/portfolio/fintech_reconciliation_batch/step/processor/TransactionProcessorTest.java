package com.portfolio.fintech_reconciliation_batch.step.processor;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionProcessorTest {

    @Mock
    private PlatformTransactionRepository platformTransactionRepository;

    @InjectMocks
    private TransactionProcessor transactionProcessor;

    @Mock
    private StepExecution stepExecution;

    @Mock
    private ExecutionContext executionContext;

    @BeforeEach
    void setUp() {
        when(stepExecution.getExecutionContext()).thenReturn(executionContext);

        TransactionEntity dbTxn = new TransactionEntity();
        dbTxn.setTransactionReference("REF-123");
        dbTxn.setAmount(new BigDecimal("1500.00"));
        dbTxn.setCurrency(CurrencyType.ARS);

        when(platformTransactionRepository.findAll()).thenReturn(List.of(dbTxn));

        transactionProcessor.beforeStep(stepExecution);
    }

    @Test
    void process_WhenTransactionsMatch_ShouldReturnReconciled() {
        TransactionDocument csvDoc = new TransactionDocument();
        csvDoc.setTransactionReference("REF-123");
        csvDoc.setAmount(new BigDecimal("1500.00"));
        csvDoc.setCurrency(CurrencyType.ARS);

        TransactionDocument result = transactionProcessor.process(csvDoc);

        assertNotNull(result);
        assertThat(result)
                .isNotNull()
                .extracting(TransactionDocument::getStatus)
                .isEqualTo(TransactionStatus.RECONCILED);
        assertEquals(0, transactionProcessor.getFailedIds().size());
    }

    @Test
    void process_WhenTransactionNotFoundInDb_ShouldReturnError() {
        TransactionDocument csvDoc = new TransactionDocument();
        csvDoc.setTransactionReference("REF-UNKNOWN");
        csvDoc.setAmount(new BigDecimal("1500.00"));
        csvDoc.setCurrency(CurrencyType.ARS);

        TransactionDocument result = transactionProcessor.process(csvDoc);

        assertThat(result).isNotNull();
        assertEquals(TransactionStatus.ERROR, result.getStatus());
        assertTrue(transactionProcessor.getFailedIds().contains("REF-UNKNOWN"));
    }

    @Test
    void process_WhenAmountDiscrepancy_ShouldReturnError() {
        TransactionDocument csvDoc = new TransactionDocument();
        csvDoc.setTransactionReference("REF-123");
        csvDoc.setAmount(new BigDecimal("9999.00")); // No coincide con 1500.00
        csvDoc.setCurrency(CurrencyType.ARS);

        TransactionDocument result = transactionProcessor.process(csvDoc);

        assertThat(result).isNotNull();
        assertEquals(TransactionStatus.ERROR, result.getStatus());
        assertTrue(transactionProcessor.getFailedIds().contains("REF-123"));
    }
}