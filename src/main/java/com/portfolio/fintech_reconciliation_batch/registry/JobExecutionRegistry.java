package com.portfolio.fintech_reconciliation_batch.registry;

import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

@Component
public class JobExecutionRegistry {

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public boolean tryLock() {
        return isRunning.compareAndSet(false, true);
    }

    public void release() {
        isRunning.set(false);
    }
}