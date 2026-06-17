package com.portfolio.fintech_reconciliation_batch.dto.response;

public record JobResponse(
        String status,
        String message,
        Long jobExecutionId,
        String batchStatus
) {
}
