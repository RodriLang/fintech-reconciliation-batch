package com.portfolio.fintech_reconciliation_batch.step.mapper;

import com.portfolio.fintech_reconciliation_batch.enums.CurrencyType;
import com.portfolio.fintech_reconciliation_batch.enums.TransactionStatus;
import com.portfolio.fintech_reconciliation_batch.model.TransactionDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.infrastructure.item.file.transform.FieldSet;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionFieldSetMapperTest {

    @Mock
    private FieldSet fieldSet;

    @InjectMocks
    private TransactionFieldSetMapper mapper;

    @Test
    void mapFieldSet_WhenDataIsValid_ShouldMapToTransactionDocument() {
        when(fieldSet.readString("transactionReference")).thenReturn("TX-12345");
        when(fieldSet.readString("accountId")).thenReturn("ACC-987");
        when(fieldSet.readBigDecimal("amount")).thenReturn(new BigDecimal("2500.50"));
        when(fieldSet.readString("currency")).thenReturn("ARS");
        when(fieldSet.readString("transactionDate")).thenReturn("2026-06-19T15:00:00");

        TransactionDocument result = mapper.mapFieldSet(fieldSet);

        assertNotNull(result);
        assertEquals("TX-12345", result.getTransactionReference());
        assertEquals("ACC-987", result.getAccountId());
        assertEquals(new BigDecimal("2500.50"), result.getAmount());
        assertEquals(CurrencyType.ARS, result.getCurrency());
        assertEquals(TransactionStatus.PENDING, result.getStatus());
        assertEquals(LocalDateTime.of(2026, 6, 19, 15, 0, 0), result.getTransactionDate());
    }

    @Test
    void mapFieldSet_WhenCurrencyIsLowercase_ShouldStillMapSuccessfully() {
        when(fieldSet.readString("transactionReference")).thenReturn("TX-12345");
        when(fieldSet.readString("accountId")).thenReturn("ACC-987");
        when(fieldSet.readBigDecimal("amount")).thenReturn(new BigDecimal("2500.50"));
        when(fieldSet.readString("currency")).thenReturn("ars");
        when(fieldSet.readString("transactionDate")).thenReturn("2026-06-19T15:00:00");

        TransactionDocument result = mapper.mapFieldSet(fieldSet);

        assertEquals(CurrencyType.ARS, result.getCurrency());
    }

    @Test
    void mapFieldSet_WhenInvalidCurrency_ShouldThrowIllegalArgumentException() {
        when(fieldSet.readString("transactionReference")).thenReturn("TX-12345");
        when(fieldSet.readString("accountId")).thenReturn("ACC-987");
        when(fieldSet.readBigDecimal("amount")).thenReturn(new BigDecimal("2500.50"));
        when(fieldSet.readString("currency")).thenReturn("BITCOIN");

        assertThrows(IllegalArgumentException.class, () -> mapper.mapFieldSet(fieldSet),
                "Debería fallar si la moneda del CSV no coincide con ninguna opción del Enum");
    }

    @Test
    void mapFieldSet_WhenDateFormatIsInvalid_ShouldThrowDateTimeParseException() {
        when(fieldSet.readString("transactionReference")).thenReturn("TX-12345");
        when(fieldSet.readString("accountId")).thenReturn("ACC-987");
        when(fieldSet.readBigDecimal("amount")).thenReturn(new BigDecimal("2500.50"));
        when(fieldSet.readString("currency")).thenReturn("USD");
        when(fieldSet.readString("transactionDate")).thenReturn("19/06/2026 15:00");

        assertThrows(DateTimeParseException.class, () -> mapper.mapFieldSet(fieldSet),
                "Debería fallar si la fecha no cumple con el formato estándar ISO-8601");
    }

    @Test
    void mapFieldSet_WhenRequiredFieldIsNull_ShouldThrowNullPointerException() {
        when(fieldSet.readString("transactionReference")).thenReturn("TX-12345");
        when(fieldSet.readString("accountId")).thenReturn("ACC-987");
        when(fieldSet.readBigDecimal("amount")).thenReturn(new BigDecimal("2500.50"));
        when(fieldSet.readString("currency")).thenReturn(null);

        assertThrows(NullPointerException.class, () -> mapper.mapFieldSet(fieldSet),
                "Objects.requireNonNull debería atajar el nulo y lanzar la excepción de inmediato");
    }
}