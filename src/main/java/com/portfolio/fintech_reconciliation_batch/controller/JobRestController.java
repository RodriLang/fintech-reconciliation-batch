package com.portfolio.fintech_reconciliation_batch.controller;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;
import com.portfolio.fintech_reconciliation_batch.exception.JobExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobRestController {

    private final JobOperator jobOperator;
    private final Job reconciliationJob;

    @PostMapping("/run-reconciliation")
    public ResponseEntity<JobResponse> triggerReconciliationJob() {
        log.info("API REST: Solicitud manual recibida mediante JobOperator.");

        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("triggeredBy", "REST-API-Admin")
                    .toJobParameters();

            JobExecution execution = jobOperator.start(reconciliationJob, params);

            JobResponse successResponse = new JobResponse(
                    "SUCCESS",
                    "El Job de conciliación se ejecutó con éxito.",
                    execution.getId(),
                    execution.getStatus().toString()
            );

            return ResponseEntity.ok(successResponse);

        } catch (Exception e) {
            log.error("API REST: Falló el lanzamiento del Job con parámetros actuales.", e);
            throw new JobExecutionException("Error operativo al intentar lanzar el Job de conciliación: " + e.getMessage(), e);
        }
    }
}