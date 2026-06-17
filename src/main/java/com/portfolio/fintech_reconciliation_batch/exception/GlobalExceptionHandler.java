package com.portfolio.fintech_reconciliation_batch.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobExecutionException.class)
    public ResponseEntity<ErrorResponse> handleJobExecutionException(JobExecutionException ex) {
        log.warn("GlobalExceptionHandler: Conflicto detectado en la ejecución del Batch -> {}", ex.getMessage());

        ErrorResponse errorDto = new ErrorResponse(
                "JOB_ERROR",
                ex.getMessage(),
                ex.getCause() != null ? ex.getCause().getClass().getSimpleName() : "No cause details",
                LocalDateTime.now()
        );

        return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(errorDto);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleAllExceptions(Exception ex) {
        log.error("GlobalExceptionHandler: Interceptando error inesperado del sistema", ex);

        ErrorResponse errorDto = new ErrorResponse(
                "INTERNAL_SERVER_ERROR",
                "Ocurrió un error inesperado en el servidor.",
                ex.getMessage(),
                LocalDateTime.now()
        );

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorDto);
    }
}