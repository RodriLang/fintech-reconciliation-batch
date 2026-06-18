package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.entity.ReconciliationReport;
import com.portfolio.fintech_reconciliation_batch.repository.ReportRepository;
import com.portfolio.fintech_reconciliation_batch.service.AuditService;
import jakarta.transaction.Transactional;
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

        long read = jobExecution.getStepExecutions().stream().mapToLong(StepExecution::getReadCount).sum();
        long writeTotal = jobExecution.getStepExecutions().stream().mapToLong(StepExecution::getWriteCount).sum();

        long totalFailed = failedIds.size();
        long totalSuccessful = writeTotal - totalFailed;

        ReconciliationReport report = ReconciliationReport.builder()
                .jobExecutionId(jobExecution.getId())
                .executionDate(LocalDateTime.now())
                .status(jobExecution.getStatus().toString())
                .totalProcessed(read)
                .successfulCount(writeTotal)
                .failedCount(totalFailed)
                .summaryMessage(summary)
                .build();

        reportRepository.save(report);
    }
}
