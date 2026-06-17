package com.portfolio.fintech_reconciliation_batch.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReconciliationScheduler {

    private final JobOperator jobOperator;
    private final Job reconciliationJob;

    @Scheduled(cron = "${app.batch.reconciliation.cron}")
    public void runReconciliation() {

        log.info("Iniciando reconciliación programada.");

        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("triggeredBy", "CRON-AUTOMATIC-SCHEDULER")
                    .toJobParameters();

            jobOperator.start(reconciliationJob, params);

            log.info("El Job automático se ejecutó exitosamente.");

        } catch (Exception e) {
            log.error("Error crítico al ejecutar el Job programado.", e);
        }
    }
}