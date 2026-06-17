package com.portfolio.fintech_reconciliation_batch.service;

import java.util.List;
import org.springframework.batch.core.job.JobExecution;

public interface AuditService {

    void saveExecutionReport(JobExecution jobExecution, List<String> failedIds);

}
