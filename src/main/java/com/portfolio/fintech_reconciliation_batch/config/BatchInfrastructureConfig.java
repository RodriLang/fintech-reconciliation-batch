package com.portfolio.fintech_reconciliation_batch.config;

import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.batch.core.launch.support.JobOperatorFactoryBean;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class BatchInfrastructureConfig {

    @Bean("asyncJobOperator")
    public JobOperatorFactoryBean jobOperator(JobRepository jobRepository, ThreadPoolTaskExecutor taskExecutor) {
        JobOperatorFactoryBean factory = new JobOperatorFactoryBean();
        factory.setJobRepository(jobRepository);
        factory.setTaskExecutor(taskExecutor);
        return factory;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(500);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("Batch-Thread-");
        executor.initialize();
        return executor;
    }
}