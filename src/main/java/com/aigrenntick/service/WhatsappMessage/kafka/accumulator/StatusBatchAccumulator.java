package com.aigrenntick.service.WhatsappMessage.kafka.accumulator;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.consumer.BackpressureManager;
import com.aigrenntick.service.WhatsappMessage.kafka.processor.StatusBatchProcessor;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffers StatusEvents and flushes them as batches to StatusBatchProcessor.
 *
 * Dual-trigger flush (whichever fires first):
 *  - Count trigger : flush when buffer reaches flush-count (default 1,000)
 *  - Time trigger  : flush every flush-interval-ms (default 3,000ms) via @Scheduled
 *
 * Key implementation choices:
 *  - ConcurrentLinkedQueue  : lock-free, safe for multi-threaded add() from consumer threads
 *  - AtomicInteger counter  : ConcurrentLinkedQueue.size() is O(n) — never call it
 *  - ReentrantLock.tryLock(): prevents two threads from double-flushing the same batch
 *    (e.g. count trigger and time trigger firing simultaneously)
 *  - @PreDestroy flush      : drains remaining events before JVM shutdown
 *    (combined with uncommitted Kafka offsets → zero data loss on restart)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatusBatchAccumulator {

    private final StatusBatchProcessor batchProcessor;
    private final BackpressureManager backpressureManager;

    @Value("${webhook.accumulator.flush-count:1000}")
    private int flushCount;

    @Value("${webhook.accumulator.buffer-capacity:50000}")
    private int bufferCapacity;

    // Lock-free queue — safe for concurrent add() from multiple consumer threads
    private final ConcurrentLinkedQueue<StatusEvent> buffer = new ConcurrentLinkedQueue<>();

    // Tracks buffer size — avoids O(n) ConcurrentLinkedQueue.size() call
    private final AtomicInteger size = new AtomicInteger(0);

    // Prevents concurrent double-flush (count trigger + time trigger firing at same time)
    private final ReentrantLock flushLock = new ReentrantLock();

    // ── Add ───────────────────────────────────────────────────────────

    /**
     * Add a single event to the buffer.
     * Called by StatusEventConsumer on every consumed Kafka record.
     * Thread-safe — ConcurrentLinkedQueue.offer() is lock-free.
     */
    public void add(StatusEvent event) {
        buffer.offer(event);
        int currentSize = size.incrementAndGet();

        // Notify backpressure manager on every add
        backpressureManager.evaluate(currentSize);

        // Count trigger — flush immediately if batch is ready
        if (currentSize >= flushCount) {
            flushBatch();
        }
    }

    // ── Time trigger flush ────────────────────────────────────────────

    /**
     * Time-based flush — fires every flush-interval-ms regardless of buffer size.
     * Ensures low-volume periods don't leave events stuck in buffer indefinitely.
     * fixedDelay means next tick starts after flush completes — no overlap.
     */
    @Scheduled(fixedDelayString = "${webhook.accumulator.flush-interval-ms:3000}")
    public void scheduledFlush() {
        if (size.get() > 0) {
            log.debug("Time-triggered flush — buffer size={}", size.get());
            flushBatch();
        }
    }

    // ── Core flush ────────────────────────────────────────────────────

    /**
     * Drain up to flushCount events and hand off to StatusBatchProcessor.
     * tryLock() ensures only one thread flushes at a time.
     * If lock is held (another flush in progress), this call returns immediately —
     * the in-progress flush will drain the buffer anyway.
     */
    private void flushBatch() {
        if (!flushLock.tryLock()) {
            return; // another thread is already flushing
        }
        try {
            List<StatusEvent> batch = new ArrayList<>(flushCount);

            // Drain up to flushCount events — poll() is thread-safe and lock-free
            StatusEvent event;
            while (batch.size() < flushCount && (event = buffer.poll()) != null) {
                batch.add(event);
            }

            if (batch.isEmpty()) {
                return; // empty flush — no-op
            }

            // Decrement counter by actual drained count
            int remaining = size.addAndGet(-batch.size());

            log.debug("Flushing batch size={} remaining in buffer={}", batch.size(), remaining);

            // Notify backpressure manager after flush — may trigger resume
            backpressureManager.evaluate(remaining);

            // Hand off to processor — this is where DB write happens
            batchProcessor.process(batch);

        } finally {
            flushLock.unlock();
        }
    }

    // ── Shutdown hook ─────────────────────────────────────────────────

    /**
     * Flush all remaining events before JVM shuts down.
     * Runs before Kafka consumer closes — so offsets are not yet committed
     * for these events. If flush succeeds, great. If JVM dies mid-flush,
     * Kafka redelivers from last committed offset on restart.
     */
    @PreDestroy
    public void flushOnShutdown() {
        log.info("Shutdown detected — flushing remaining {} events from buffer", size.get());
        // Flush in batches until buffer is empty
        while (size.get() > 0) {
            flushBatch();
        }
        log.info("Shutdown flush complete.");
    }

    public int currentSize() {
        return size.get();
    }
}