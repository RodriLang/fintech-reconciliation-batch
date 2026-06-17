package com.portfolio.fintech_reconciliation_batch.service;

import com.portfolio.fintech_reconciliation_batch.entity.ReconciliationReport;
import com.portfolio.fintech_reconciliation_batch.repository.ReportRepository;
import jakarta.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AuditServiceImpl implements AuditService {


    private final ReportRepository reportRepository;

    @Override
    @Transactional
    public void saveExecutionReport(JobExecution jobExecution, List<String> failedIds) {

    String summary = failedIds.isEmpty() ? "Sin errores" : "Fallos en: " + failedIds;

        long read = jobExecution.getStepExecutions().stream().mapToLong(StepExecution::getReadCount).sum();
        long write = jobExecution.getStepExecutions().stream().mapToLong(StepExecution::getWriteCount).sum();
        long skip = jobExecution.getStepExecutions().stream().mapToLong(StepExecution::getWriteSkipCount).sum();

        ReconciliationReport report = ReconciliationReport.builder()
                .jobExecutionId(jobExecution.getId())
                .executionDate(LocalDateTime.now())
                .status(jobExecution.getStatus().toString())
                .totalProcessed(read)
                .successfulCount(write)
                .failedCount(skip)
                .summaryMessage(summary)
                .summaryMessage("Job terminado con " + write + " éxitos y " + skip + " fallos.")
                .build();

        reportRepository.save(report);
    }
}
