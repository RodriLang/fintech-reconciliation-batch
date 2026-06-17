package com.portfolio.fintech_reconciliation_batch.controller;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;
import com.portfolio.fintech_reconciliation_batch.service.ReconciliationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobRestController {

    private final ReconciliationService reconciliationService;

    @PostMapping("/run-reconciliation")
    public ResponseEntity<JobResponse> trigger() {
        log.info("API REST: Solicitud manual recibida mediante JobOperator.");
        return ResponseEntity.ok(reconciliationService.runReconciliation());
    }
}
