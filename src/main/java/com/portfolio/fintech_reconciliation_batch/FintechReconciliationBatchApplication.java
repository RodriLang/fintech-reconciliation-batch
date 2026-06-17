package com.portfolio.fintech_reconciliation_batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FintechReconciliationBatchApplication {

	static void main(String[] args) {
		SpringApplication.run(FintechReconciliationBatchApplication.class, args);
	}

}
