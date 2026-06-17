package com.portfolio.fintech_reconciliation_batch.service;

import com.portfolio.fintech_reconciliation_batch.dto.response.JobResponse;

public interface ReconciliationService {

    JobResponse runReconciliation();

}
