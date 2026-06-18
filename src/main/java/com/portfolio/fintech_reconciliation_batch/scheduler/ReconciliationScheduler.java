package com.portfolio.fintech_reconciliation_batch.scheduler;

import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
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
    private final JobExecutionRegistry jobExecutionRegistry;

    @Scheduled(cron = "${app.batch.reconciliation.cron}")
    public void runReconciliation() {

        log.info("Iniciando reconciliación programada.");

        if (!jobExecutionRegistry.tryLock()) {
            log.warn("El Job programado se canceló porque ya hay una ejecución en curso.");
            return;
        }

        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("triggeredBy", "CRON-AUTOMATIC-SCHEDULER")
                    .toJobParameters();

            jobOperator.start(reconciliationJob, params);

            log.info("El Job automático inició exitosamente.");

        } catch (Exception e) {
            jobExecutionRegistry.release();
            log.error("Error crítico al ejecutar el Job programado.", e);
        }
    }
}