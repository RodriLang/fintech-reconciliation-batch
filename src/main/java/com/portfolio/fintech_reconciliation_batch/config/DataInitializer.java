package com.portfolio.fintech_reconciliation_batch.config;

import com.portfolio.fintech_reconciliation_batch.entity.TransactionEntity;
import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.InternalStatus;
import com.portfolio.fintech_reconciliation_batch.repository.PlatformTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements ApplicationListener<ContextRefreshedEvent> {

    private final PlatformTransactionRepository repository;

    @Value("${app.batch.reconciliation.input-file-path}")
    private String csvFilePath;

    private static final int TOTAL_RECORDS = 5000;
    private static final double DISCREPANCY_RATE = 0.05; // 5% de probabilidad de error

    @Override
    public void onApplicationEvent(@NonNull ContextRefreshedEvent event) {
        if (repository.count() > 0) {
            log.info("La base de datos ya contiene transacciones. Omitiendo inicialización.");
            return;
        }

        log.info("Iniciando generación de datos para pruebas de reconciliación ({} registros)...", TOTAL_RECORDS);

        Random random = new Random();
        CurrencyType[] currencies = CurrencyType.values();
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        LocalDateTime startDate = LocalDateTime.of(2026, 1, 1, 0, 0, 0);

        List<TransactionEntity> dbBatchList = new ArrayList<>();

        String emulatedPath = csvFilePath.replace("file:", "");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(emulatedPath))) {
            writer.write("reference,accountId,amount,currency,date");
            writer.newLine();

            for (int i = 1; i <= TOTAL_RECORDS; i++) {
                String reference = String.format("TXN-2026-%05d", i);
                String accountId = "ACC-" + (random.nextInt(9000) + 1000);

                double baseAmount = 5.0 + (10000.0 - 5.0) * random.nextDouble();
                BigDecimal amountDb = BigDecimal.valueOf(baseAmount).setScale(2, RoundingMode.HALF_UP);
                BigDecimal amountCsv = amountDb;

                CurrencyType currency = currencies[random.nextInt(currencies.length)];

                int randomSeconds = random.nextInt(15_552_000);
                LocalDateTime transactionDate = startDate.plusSeconds(randomSeconds);
                String dateStr = transactionDate.format(dateFormatter);

                double diceRoll = random.nextDouble();
                boolean saveInDb = true;

                if (diceRoll < DISCREPANCY_RATE) {
                    int errorType = random.nextInt(3);
                    switch (errorType) {
                        case 0 -> {
                            amountCsv = amountDb.add(BigDecimal.valueOf(10.00));
                            log.debug("Generando discrepancia de MONTO para {}", reference);
                        }
                        case 1 -> {
                            saveInDb = false;
                            log.debug("Generando transacción huérfana en archivo (No estará en BD): {}", reference);
                        }
                        case 2 -> {
                            dbBatchList.add(buildEntity(reference, accountId, amountDb, currency, transactionDate));
                            log.debug("Generando transacción huérfana en BD (No estará en CSV): {}", reference);
                            continue;
                        }
                        default -> {}
                    }
                }

                if (saveInDb) {
                    dbBatchList.add(buildEntity(reference, accountId, amountDb, currency, transactionDate));
                }

                String amountCsvStr = String.format(Locale.US, "%.2f", amountCsv);
                String csvLine = String.format("%s,%s,%s,%s,%s", reference, accountId, amountCsvStr, currency.name(), dateStr);
                writer.write(csvLine);
                writer.newLine();
            }

            log.info("Guardando {} registros en la base de datos...", dbBatchList.size());
            repository.saveAll(dbBatchList);

            log.info("Inicialización completada con éxito.");
            log.info("Archivo de conciliación externo generado en: {}", csvFilePath);

        } catch (IOException e) {
            log.error("Error crítico generando el archivo CSV de pruebas", e);
        }
    }

    private TransactionEntity buildEntity(String ref, String account, BigDecimal amount, CurrencyType currency, LocalDateTime date) {
        return TransactionEntity.builder()
                .transactionReference(ref)
                .accountId(account)
                .amount(amount)
                .currency(currency)
                .internalStatus(InternalStatus.PROCESSED)
                .createdAt(date)
                .build();
    }
}