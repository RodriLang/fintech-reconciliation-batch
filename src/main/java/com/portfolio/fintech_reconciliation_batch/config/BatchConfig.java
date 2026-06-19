package com.portfolio.fintech_reconciliation_batch.config;

import com.portfolio.fintech_reconciliation_batch.listener.JobCompletionNotificationListener;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import com.portfolio.fintech_reconciliation_batch.repository.TransactionRepository;
import com.portfolio.fintech_reconciliation_batch.step.mapper.TransactionFieldSetMapper;
import com.portfolio.fintech_reconciliation_batch.step.processor.TransactionProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final TransactionRepository transactionRepository;
    private final PlatformTransactionRepository platformTransactionRepository;
    private final JobCompletionNotificationListener jobListener;
    private final ResourceLoader resourceLoader;
    private final TransactionFieldSetMapper fieldSetMapper;

    public static final String RECONCILIATION_JOB_NAME = "reconciliationJob";
    public static final String RECONCILIATION_STEP_NAME = "reconciliationStep";

    @Value("${app.batch.reconciliation.input-file-path}")
    private String inputFilePath;

    @Value("${app.batch.reconciliation.chunkSize}")
    private int chunkSize;

    @Bean
    @StepScope
    public TransactionProcessor processor() {
        return new TransactionProcessor(platformTransactionRepository);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<TransactionDocument> reader() {
        return new FlatFileItemReaderBuilder<TransactionDocument>()
                .name("transactionCsvReader")
                .resource(resourceLoader.getResource(inputFilePath))
                .linesToSkip(1)
                .delimited()
                .names("transactionReference", "accountId", "amount", "currency", "transactionDate")
                .fieldSetMapper(fieldSetMapper)
                .build();
    }

    @Bean
    public ItemWriter<TransactionDocument> writer() {
        return chunk -> transactionRepository.insert(chunk.getItems());

    }

    @Bean
    public Step reconciliationStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ThreadPoolTaskExecutor taskExecutor) {
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