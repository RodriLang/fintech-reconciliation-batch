package com.portfolio.fintech_reconciliation_batch.listener;

import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
import com.portfolio.fintech_reconciliation_batch.service.AuditService;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobCompletionNotificationListener implements JobExecutionListener {

    private final AuditService auditService;
    private final JobExecutionRegistry jobExecutionRegistry;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        LocalDateTime startTime = LocalDateTime.now();
        log.info("INICIANDO AUDITORÍA: El Job {} inició a las {}", jobExecution.getJobInstance().getJobName(), startTime);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        List<String> allFailedIds = jobExecution.getStepExecutions().stream()
                .map(step -> step.getExecutionContext().get("failedIds"))
                .filter(obj -> obj instanceof List<?>)
                .map(obj -> (List<?>) obj)
                .flatMap(Collection::stream)
                .filter(item -> item instanceof String)
                .map(item -> (String) item)
                .toList();

        auditService.saveExecutionReport(jobExecution, allFailedIds);
        jobExecutionRegistry.release();

        log.info("Reporte guardado. Total fallidos: {}", allFailedIds.size());
    }
}