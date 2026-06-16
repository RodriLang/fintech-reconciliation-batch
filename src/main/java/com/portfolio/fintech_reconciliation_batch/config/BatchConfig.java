package com.portfolio.fintech_reconciliation_batch.config;

import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.data.RepositoryItemWriter;
import org.springframework.batch.infrastructure.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.infrastructure.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final TransactionRepository transactionRepository;

    public static final String RECONCILIATION_JOB_NAME = "reconciliationJob";
    public static final String RECONCILIATION_STEP_NAME = "reconciliationStep";
    private static final int CHUNK_SIZE = 10;

    @Bean
    public ItemReader<TransactionDocument> reader() {
        return new ListItemReader<>(Arrays.asList(
                TransactionDocument.builder().transactionReference("TXN-001").accountId("ACC-999").amount(new BigDecimal("1500.00")).currency(CurrencyType.ARS).status(TransactionStatus.PENDING).transactionDate(LocalDateTime.now()).build(),
                TransactionDocument.builder().transactionReference("TXN-002").accountId("ACC-888").amount(new BigDecimal("200.50")).currency(CurrencyType.ARS).status(TransactionStatus.PENDING).transactionDate(LocalDateTime.now()).build(),
                TransactionDocument.builder().transactionReference("TXN-003").accountId("ACC-777").amount(new BigDecimal("99.99")).currency(CurrencyType.ARS).status(TransactionStatus.PENDING).transactionDate(LocalDateTime.now()).build()
        ));
    }

    @Bean
    public ItemProcessor<TransactionDocument, TransactionDocument> processor() {
        return transaction -> {
            log.info("Procesando transacción: {}", transaction.getTransactionReference());
            transaction.setStatus(TransactionStatus.RECONCILED);
            return transaction;
        };
    }

    @Bean
    public RepositoryItemWriter<TransactionDocument> writer() {
        return new RepositoryItemWriterBuilder<TransactionDocument>()
                .repository(transactionRepository)
                .methodName("save")
                .build();
    }t

    @Bean
    public Step reconciliationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder(RECONCILIATION_STEP_NAME, jobRepository)
                .<TransactionDocument, TransactionDocument>chunk(CHUNK_SIZE)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Job reconciliationJob(JobRepository jobRepository, Step reconciliationStep) {
        return new JobBuilder(RECONCILIATION_JOB_NAME, jobRepository)
                .start(reconciliationStep)
                .build();
    }
}