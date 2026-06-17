package com.portfolio.fintech_reconciliation_batch.service.impl;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;
import com.portfolio.fintech_reconciliation_batch.exception.ReconciliationException;
import com.portfolio.fintech_reconciliation_batch.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ReconciliationServiceImpl implements ReconciliationService {

    private final JobOperator jobOperator;
    private final Job reconciliationJob;

    @Override
    public JobResponse runReconciliation() {

        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("triggeredBy", "REST-API-Admin")
                    .toJobParameters();

            JobExecution execution = jobOperator.start(reconciliationJob, params);

            return new JobResponse(
                    "SUCCESS",
                    "El Job de conciliación se ejecutó con éxito.",
                    execution.getId(),
                    execution.getStatus().toString()
            );

        } catch (Exception e) {
            throw new ReconciliationException("No se pudo iniciar el proceso de conciliación", e);
        }
    }
}
