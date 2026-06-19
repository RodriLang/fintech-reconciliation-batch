package com.portfolio.fintech_reconciliation_batch.registry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobExecutionRegistryTest {

    private JobExecutionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new JobExecutionRegistry();
    }

    @Test
    void tryLock_WhenLockIsFree_ShouldReturnTrue() {
        boolean result = registry.tryLock();

        assertTrue(result, "El lock debería adquirirse exitosamente si está libre");
    }

    @Test
    void tryLock_WhenAlreadyAcquired_ShouldReturnFalse() {
        registry.tryLock();

        boolean result = registry.tryLock();

        // Assert
        assertFalse(result, "No debería permitir adquirir el lock si ya hay una ejecución activa");
    }

    @Test
    void release_WhenLockIsAcquired_ShouldAllowSubsequentLocking() {
        registry.tryLock();

        registry.release();
        boolean lockAgain = registry.tryLock();

        assertTrue(lockAgain, "Después de liberar el lock, se debería poder volver a adquirir");
    }

    @Test
    void tryLock_UnderConcurrentContention_ShouldAllowOnlyOneWinner() throws InterruptedException {
        int numberOfThreads = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startLatch = new CountDownLatch(1); // Para que arranquen todos juntos en simultáneo
        CountDownLatch finishLatch = new CountDownLatch(numberOfThreads); // Para esperar a que todos terminen

        AtomicInteger successfulLocks = new AtomicInteger(0);

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    if (registry.tryLock()) {
                        successfulLocks.incrementAndGet();
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await();
        executorService.shutdown();

        assertEquals(1, successfulLocks.get(),
                "Bajo presión de hilos simultáneos, estrictamente un solo hilo debió ganar el lock");
    }
}