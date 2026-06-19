package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.entity.ReconciliationReport;
import com.portfolio.fintech_reconciliation_batch.repository.ReportRepository;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuditServiceImplTest {

    @Mock
    private ReportRepository reportRepository;

    @InjectMocks
    private AuditServiceImpl auditService;

    @Test
    void saveExecutionReport_WhenNoFailedIds_ShouldSaveReportWithSuccessfulCounters() {
        List<String> failedIds = Collections.emptyList();

        JobExecution jobExecution = mock(JobExecution.class);
        StepExecution stepExecution = mock(StepExecution.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);

        when(jobExecution.getId()).thenReturn(100L);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.COMPLETED);
        when(jobExecution.getStepExecutions()).thenReturn(List.of(stepExecution));

        when(stepExecution.getReadCount()).thenReturn(50L);
        when(stepExecution.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.get("reconciledCount")).thenReturn(new AtomicLong(50L));
        when(executionContext.get("errorCount")).thenReturn(new AtomicLong(0L));

        ArgumentCaptor<ReconciliationReport> reportCaptor = ArgumentCaptor.forClass(ReconciliationReport.class);

        auditService.saveExecutionReport(jobExecution, failedIds);

        verify(reportRepository, times(1)).save(reportCaptor.capture());
        ReconciliationReport savedReport = reportCaptor.getValue();

        assertNotNull(savedReport);
        assertEquals(100L, savedReport.getJobExecutionId());
        assertEquals("COMPLETED", savedReport.getStatus());
        assertEquals(50L, savedReport.getTotalProcessed());
        assertEquals(50L, savedReport.getSuccessfulCount());
        assertEquals(0L, savedReport.getFailedCount());
        assertEquals("Sin errores", savedReport.getSummaryMessage());
        assertNotNull(savedReport.getExecutionDate());
    }

    @Test
    void saveExecutionReport_WithFailedIds_ShouldSaveReportWithDiscrepanciesAndSummary() {
        List<String> failedIds = List.of("TX-999", "TX-888");

        JobExecution jobExecution = mock(JobExecution.class);
        StepExecution stepExecution = mock(StepExecution.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);

        when(jobExecution.getId()).thenReturn(200L);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.FAILED);
        when(jobExecution.getStepExecutions()).thenReturn(List.of(stepExecution));

        when(stepExecution.getReadCount()).thenReturn(10L);
        when(stepExecution.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.get("reconciledCount")).thenReturn(new AtomicLong(8L));
        when(executionContext.get("errorCount")).thenReturn(new AtomicLong(2L));

        ArgumentCaptor<ReconciliationReport> reportCaptor = ArgumentCaptor.forClass(ReconciliationReport.class);

        auditService.saveExecutionReport(jobExecution, failedIds);

        verify(reportRepository, times(1)).save(reportCaptor.capture());
        ReconciliationReport savedReport = reportCaptor.getValue();

        assertNotNull(savedReport);
        assertEquals(200L, savedReport.getJobExecutionId());
        assertEquals("FAILED", savedReport.getStatus());
        assertEquals(10L, savedReport.getTotalProcessed());
        assertEquals(8L, savedReport.getSuccessfulCount());
        assertEquals(2L, savedReport.getFailedCount());
        assertEquals("Fallos en: [TX-999, TX-888]", savedReport.getSummaryMessage());
    }

    @Test
    void saveExecutionReport_WhenCountersAreMissingOrWrongType_ShouldDefaultToZero() {
        List<String> failedIds = Collections.emptyList();

        JobExecution jobExecution = mock(JobExecution.class);
        StepExecution stepExecution = mock(StepExecution.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);

        when(jobExecution.getId()).thenReturn(300L);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.COMPLETED);
        when(jobExecution.getStepExecutions()).thenReturn(List.of(stepExecution));

        when(stepExecution.getReadCount()).thenReturn(0L);
        when(stepExecution.getExecutionContext()).thenReturn(executionContext);

        when(executionContext.get("reconciledCount")).thenReturn(null);
        when(executionContext.get("errorCount")).thenReturn("No soy un AtomicLong");

        ArgumentCaptor<ReconciliationReport> reportCaptor = ArgumentCaptor.forClass(ReconciliationReport.class);

        auditService.saveExecutionReport(jobExecution, failedIds);

        verify(reportRepository).save(reportCaptor.capture());
        ReconciliationReport savedReport = reportCaptor.getValue();

        assertEquals(0L, savedReport.getSuccessfulCount());
        assertEquals(0L, savedReport.getFailedCount());
    }
}