package com.portfolio.fintech_reconciliation_batch.step.mapper;

import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;

import org.springframework.batch.infrastructure.item.file.mapping.FieldSetMapper;
import org.springframework.batch.infrastructure.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.Objects;

@Component
public class TransactionFieldSetMapper implements FieldSetMapper<TransactionDocument> {

    @Override
    public TransactionDocument mapFieldSet(FieldSet fieldSet) {
        return TransactionDocument.builder()
                .transactionReference(fieldSet.readString("transactionReference"))
                .accountId(fieldSet.readString("accountId"))
                .amount(fieldSet.readBigDecimal("amount"))
                .currency(CurrencyType.valueOf(Objects.requireNonNull(fieldSet.readString("currency")).toUpperCase()))
                .status(TransactionStatus.PENDING)
                .transactionDate(LocalDateTime.parse(Objects.requireNonNull(fieldSet.readString("transactionDate"))))
                .build();
    }
}