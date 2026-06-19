package com.portfolio.fintech_reconciliation_batch.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReconciliationReport {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long jobExecutionId;

    private LocalDateTime executionDate;

    private String status;

    private long totalProcessed;

    private long successfulCount;

    private long failedCount;

    @Column(name = "summary_message", columnDefinition = "TEXT")
    private String summaryMessage;
}