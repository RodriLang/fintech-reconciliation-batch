package com.portfolio.fintech_reconciliation_batch.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobCompletionNotificationListener implements JobExecutionListener {

    private final ConfigurableApplicationContext context;
    private LocalDateTime startTime;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        this.startTime = LocalDateTime.now();
        log.info("INICIANDO AUDITORÍA: El Job {} inició a las {}", jobExecution.getJobInstance().getJobName(), startTime);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);

        log.info("RESUMEN DE EJECUCIÓN DEL JOB");

        log.info("Nombre del Job : {}", jobExecution.getJobInstance().getJobName());
        log.info("Status Final   : {}", jobExecution.getStatus());
        log.info("Tiempo Total   : {} ms", duration.toMillis());

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Resultado      : El proceso Batch corrió de principio a fin de manera limpia.");
            // TODO agregar notificación
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("Resultado      : El proceso BATCH falló técnicamente. Revisar excepciones.");
        }
        log.info("==================================================================");
    }
}