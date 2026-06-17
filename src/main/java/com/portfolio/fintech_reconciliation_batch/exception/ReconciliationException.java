package com.portfolio.fintech_reconciliation_batch.exception;

public class ReconciliationException extends RuntimeException {

    public ReconciliationException(String message) {
        super(message);
    }

    public ReconciliationException(String message, Throwable cause) {
        super(message, cause);
    }
}