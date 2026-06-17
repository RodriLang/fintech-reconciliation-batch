package com.portfolio.fintech_reconciliation_batch.exception;

import java.time.LocalDateTime;

public record ErrorResponse(
        String status,
        String message,
        String details,
        LocalDateTime timestamp
) {
}
