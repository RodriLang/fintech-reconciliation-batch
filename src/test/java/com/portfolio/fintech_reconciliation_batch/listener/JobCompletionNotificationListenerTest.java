package com.portfolio.fintech_reconciliation_batch.listener;

import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
import com.portfolio.fintech_reconciliation_batch.service.AuditService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobCompletionNotificationListenerTest {

    @Mock
    private AuditService auditService;

    @Mock
    private JobExecutionRegistry jobExecutionRegistry;

    @InjectMocks
    private JobCompletionNotificationListener listener;

    @Test
    void beforeJob_ShouldLogSuccessfully() {
        JobExecution jobExecution = mock(JobExecution.class);
        JobInstance jobInstance = mock(JobInstance.class);

        when(jobExecution.getJobInstance()).thenReturn(jobInstance);
        when(jobInstance.getJobName()).thenReturn("reconciliationJob");

        assertDoesNotThrow(() -> listener.beforeJob(jobExecution),
            "El método beforeJob no debería lanzar ninguna excepción al loguear");
    }

    @Test
    void afterJob_WhenStepContainsFailedIds_ShouldFlattenListAndSaveReportAndReleaseLock() {
        JobExecution jobExecution = mock(JobExecution.class);
        StepExecution stepExecution1 = mock(StepExecution.class);
        StepExecution stepExecution2 = mock(StepExecution.class);
        ExecutionContext context1 = mock(ExecutionContext.class);
        ExecutionContext context2 = mock(ExecutionContext.class);

        when(jobExecution.getStepExecutions()).thenReturn(List.of(stepExecution1, stepExecution2));

        when(stepExecution1.getExecutionContext()).thenReturn(context1);
        when(context1.get("failedIds")).thenReturn(List.of("TX-100", "TX-200"));

        when(stepExecution2.getExecutionContext()).thenReturn(context2);
        when(context2.get("failedIds")).thenReturn(Collections.emptyList());

        listener.afterJob(jobExecution);

        verify(auditService, times(1)).saveExecutionReport(jobExecution, List.of("TX-100", "TX-200"));
        verify(jobExecutionRegistry, times(1)).release();
    }

    @Test
    void afterJob_WhenContextContainsInvalidDataType_ShouldFilterItOutSafely() {
        JobExecution jobExecution = mock(JobExecution.class);
        StepExecution stepExecution = mock(StepExecution.class);
        ExecutionContext context = mock(ExecutionContext.class);

        when(jobExecution.getStepExecutions()).thenReturn(List.of(stepExecution));
        when(stepExecution.getExecutionContext()).thenReturn(context);

        when(context.get("failedIds")).thenReturn("No soy una lista, soy un String intruso");

        listener.afterJob(jobExecution);

        verify(auditService).saveExecutionReport(jobExecution, Collections.emptyList());
        verify(jobExecutionRegistry).release();
    }

    @Test
    void afterJob_WhenAuditServiceFails_ShouldStillReleaseLockInFinallyBlock() {
        JobExecution jobExecution = mock(JobExecution.class);
        when(jobExecution.getStepExecutions()).thenReturn(Collections.emptyList());

        doThrow(new RuntimeException("Database error saving audit report"))
                .when(auditService).saveExecutionReport(any(), any());

        try {
            listener.afterJob(jobExecution);
        } catch (RuntimeException _) {
            // Se esperaba la excepción
        }

        verify(jobExecutionRegistry, times(1)).release();
    }
}