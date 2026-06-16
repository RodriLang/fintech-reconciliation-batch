package com.portfolio.fintech_reconciliation_batch.repository;

import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TransactionRepository extends MongoRepository<TransactionDocument, String> {

    Optional<TransactionDocument> findByTransactionReference(String transactionReference);

}