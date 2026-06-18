package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;
import com.portfolio.fintech_reconciliation_batch.exception.ReconciliationException;
import com.portfolio.fintech_reconciliation_batch.registry.JobExecutionRegistry;
import com.portfolio.fintech_reconciliation_batch.service.ReconciliationService;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class ReconciliationServiceImpl implements ReconciliationService {

    @Qualifier("asyncJobOperator")
    private final JobOperator jobOperator;
    private final Job reconciliationJob;
    private final JobExecutionRegistry jobExecutionRegistry;

    public ReconciliationServiceImpl(
            @Qualifier("asyncJobOperator") JobOperator jobOperator,
            Job reconciliationJob,
            JobExecutionRegistry jobExecutionRegistry) {
        this.jobOperator = jobOperator;
        this.reconciliationJob = reconciliationJob;
        this.jobExecutionRegistry = jobExecutionRegistry;
    }

    @Override
    public JobResponse runReconciliation() {

        if (!jobExecutionRegistry.tryLock()) {
            throw new ReconciliationException("El proceso de conciliación ya está en ejecución.");
        }

        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("triggeredBy", "REST-API-Admin")
                    .toJobParameters();

            JobExecution execution = jobOperator.start(reconciliationJob, params);

            return new JobResponse(
                    "PROCESSING",
                    "El Job de conciliación fue encolado y comenzó su ejecución en segundo plano.",
                    execution.getId(),
                    execution.getStatus().toString()
            );

        } catch (Exception e) {
            jobExecutionRegistry.release();
            throw new ReconciliationException("No se pudo iniciar el proceso de conciliación", e);
        }
    }
}
