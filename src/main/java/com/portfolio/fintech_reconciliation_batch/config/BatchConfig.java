package com.portfolio.fintech_reconciliation_batch.config;

import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.listener.JobCompletionNotificationListener;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.step.processor.TransactionProcessor;
import com.portfolio.fintech_reconciliation_batch.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.data.RepositoryItemWriter;
import org.springframework.batch.infrastructure.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.Objects;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final TransactionRepository transactionRepository;
    private final TransactionProcessor transactionProcessor;
    private final JobCompletionNotificationListener jobListener;
    private final ResourceLoader resourceLoader;

    public static final String RECONCILIATION_JOB_NAME = "reconciliationJob";
    public static final String RECONCILIATION_STEP_NAME = "reconciliationStep";

    @Value("${app.batch.reconciliation.input-file-path}")
    private String inputFilePath;

    @Value("${app.batch.reconciliation.chunkSize}")
    private int chunkSize;

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("Batch-Thread-");
        executor.initialize();
        return executor;
    }

    @Bean
    public FlatFileItemReader<TransactionDocument> reader() {
        return new FlatFileItemReaderBuilder<TransactionDocument>()
                .name("transactionCsvReader")
                .resource(resourceLoader.getResource(inputFilePath))
                .linesToSkip(1)
                .delimited()
                .names("transactionReference", "accountId", "amount", "currency", "transactionDate")
                .fieldSetMapper(fieldSet -> TransactionDocument.builder()
                        .transactionReference(fieldSet.readString("transactionReference"))
                        .accountId(fieldSet.readString("accountId"))
                        .amount(fieldSet.readBigDecimal("amount"))
                        .currency(CurrencyType.valueOf(Objects.requireNonNull(fieldSet.readString("currency")).toUpperCase()))
                        .status(TransactionStatus.PENDING)
                        .transactionDate(LocalDateTime.parse(Objects.requireNonNull(fieldSet.readString("transactionDate"))))
                        .build())
                .build();
    }

    @Bean
    public ItemProcessor<TransactionDocument, TransactionDocument> processor() {
        return transactionProcessor;
    }

    @Bean
    public RepositoryItemWriter<TransactionDocument> writer() {
        return new RepositoryItemWriterBuilder<TransactionDocument>()
                .repository(transactionRepository)
                .methodName("save")
                .build();
    }

    @Bean
    public Step reconciliationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, ThreadPoolTaskExecutor taskExecutor) {
        return new StepBuilder(RECONCILIATION_STEP_NAME, jobRepository)
                .<TransactionDocument, TransactionDocument>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor)
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Job reconciliationJob(JobRepository jobRepository, Step reconciliationStep) {
        return new JobBuilder(RECONCILIATION_JOB_NAME, jobRepository)
                .listener(jobListener)
                .start(reconciliationStep)
                .build();
    }
}