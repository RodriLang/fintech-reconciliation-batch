package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;
import com.portfolio.fintech_reconciliation_batch.exception.ReconciliationException;
import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconciliationServiceImplTest {

    @Mock
    private JobOperator jobOperator;

    @Mock
    private Job reconciliationJob;

    @Mock
    private JobExecutionRegistry jobExecutionRegistry;

    @InjectMocks
    private ReconciliationServiceImpl reconciliationService;

    @Test
    void runReconciliation_WhenLockAcquiredAndJobStarts_ShouldReturnProcessingResponse() throws Exception {
        when(jobExecutionRegistry.tryLock()).thenReturn(true);

        JobExecution mockExecution = mock(JobExecution.class);
        when(mockExecution.getId()).thenReturn(1L);
        when(mockExecution.getStatus()).thenReturn(BatchStatus.STARTING);

        when(jobOperator.start(eq(reconciliationJob), any())).thenReturn(mockExecution);

        JobResponse response = reconciliationService.runReconciliation();

        assertNotNull(response);
        assertEquals("PROCESSING", response.status());
        assertEquals(1L, response.jobExecutionId());
        verify(jobExecutionRegistry, never()).release();
    }

    @Test
    void runReconciliation_WhenAlreadyRunning_ShouldThrowReconciliationException() {
        when(jobExecutionRegistry.tryLock()).thenReturn(false);

        ReconciliationException exception = assertThrows(ReconciliationException.class, () -> {
            reconciliationService.runReconciliation();
        });

        assertEquals("El proceso de conciliación ya está en ejecución.", exception.getMessage());
        verifyNoInteractions(jobOperator);
    }

    @Test
    void runReconciliation_WhenExceptionOccurs_ShouldReleaseLockAndThrowException() throws Exception {
        when(jobExecutionRegistry.tryLock()).thenReturn(true);
        when(jobOperator.start(eq(reconciliationJob), any()))
                .thenThrow(new RuntimeException("Fallo de infraestructura de Spring Batch"));

        ReconciliationException exception = assertThrows(ReconciliationException.class, () -> {
            reconciliationService.runReconciliation();
        });

        assertTrue(exception.getMessage().contains("No se pudo iniciar el proceso de conciliación"));
        verify(jobExecutionRegistry).release();
    }
}