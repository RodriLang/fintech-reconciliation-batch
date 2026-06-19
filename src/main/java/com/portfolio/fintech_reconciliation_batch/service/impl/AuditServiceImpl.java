package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.entity.ReconciliationReport;
import com.portfolio.fintech_reconciliation_batch.repository.ReportRepository;
import com.portfolio.fintech_reconciliation_batch.service.AuditService;
import jakarta.transaction.Transactional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AuditServiceImpl implements AuditService {

    private final ReportRepository reportRepository;

    @Override
    @Transactional
    public void saveExecutionReport(JobExecution jobExecution, List<String> failedIds) {

        String summary = failedIds.isEmpty() ? "Sin errores" : "Fallos en: " + failedIds;

        long read = jobExecution.getStepExecutions().stream()
                .mapToLong(StepExecution::getReadCount)
                .sum();

        long totalSuccessful = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> {
                    Object counter = step.getExecutionContext().get("reconciledCount");
                    return counter instanceof AtomicLong al ? al.get() : 0L;
                })
                .sum();

        long totalFailed = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> {
                    Object counter = step.getExecutionContext().get("errorCount");
                    return counter instanceof AtomicLong al ? al.get() : 0L;
                })
                .sum();

        ReconciliationReport report = ReconciliationReport.builder()
                .jobExecutionId(jobExecution.getId())
                .executionDate(LocalDateTime.now())
                .status(jobExecution.getStatus().toString())
                .totalProcessed(read)
                .successfulCount(totalSuccessful)
                .failedCount(totalFailed)
                .summaryMessage(summary)
                .build();

        reportRepository.save(report);
    }
}
