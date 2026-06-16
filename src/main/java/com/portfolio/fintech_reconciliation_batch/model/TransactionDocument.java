package com.portfolio.fintech_reconciliation_batch.model;

import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "transactions")
public class TransactionDocument {

    @Id
    private String id;

    private String transactionReference;
    private String accountId;
    private BigDecimal amount;
    private CurrencyType currency;
    private TransactionStatus status;
    private LocalDateTime transactionDate;

}