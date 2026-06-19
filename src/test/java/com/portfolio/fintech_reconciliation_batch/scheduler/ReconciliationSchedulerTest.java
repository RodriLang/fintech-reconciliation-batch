package com.portfolio.fintech_reconciliation_batch.scheduler;

import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReconciliationSchedulerTest {

    @Mock
    private JobOperator jobOperator;

    @Mock
    private Job reconciliationJob;

    @Mock
    private JobExecutionRegistry jobExecutionRegistry;

    @InjectMocks
    private ReconciliationScheduler reconciliationScheduler;

    @Test
    void runReconciliation_WhenLockAcquiredAndJobStarts_ShouldExecuteSuccessfully_WithoutReleasingLock() throws Exception {
        when(jobExecutionRegistry.tryLock()).thenReturn(true);
        when(jobOperator.start(eq(reconciliationJob), any())).thenReturn(mock(JobExecution.class));

        assertDoesNotThrow(() -> reconciliationScheduler.runReconciliation(),
                "El scheduler no debería lanzar excepciones en una ejecución feliz");

        verify(jobOperator, times(1)).start(eq(reconciliationJob), any());
        verify(jobExecutionRegistry, never()).release();
    }

    @Test
    void runReconciliation_WhenLockCannotBeAcquired_ShouldSkipJobExecution() {
        when(jobExecutionRegistry.tryLock()).thenReturn(false);

        reconciliationScheduler.runReconciliation();

        verifyNoInteractions(jobOperator);
        verify(jobExecutionRegistry, never()).release();
    }

    @Test
    void runReconciliation_WhenJobOperatorThrowsException_ShouldReleaseLockSafely() throws Exception {
        when(jobExecutionRegistry.tryLock()).thenReturn(true);
        when(jobOperator.start(eq(reconciliationJob), any()))
                .thenThrow(new RuntimeException("Fallo al inicializar los hilos del lote"));

        assertDoesNotThrow(() -> reconciliationScheduler.runReconciliation());

         verify(jobExecutionRegistry, times(1)).release();
    }
}