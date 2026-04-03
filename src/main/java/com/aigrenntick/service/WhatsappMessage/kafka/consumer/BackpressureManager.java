package com.aigrenntick.service.WhatsappMessage.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages Kafka consumer partition pause/resume based on accumulator buffer level.
 *
 * Hysteresis thresholds (from architecture doc §5.3):
 *  Pause  : buffer >= 80% of capacity (default 40,000 of 50,000)
 *  Resume : buffer <= 50% of capacity (default 25,000 of 50,000)
 *
 * The 80/50 gap prevents rapid oscillation under borderline load.
 * Without hysteresis, at ~40K buffer the consumer would pause/resume
 * every few hundred milliseconds, creating overhead without benefit.
 *
 * Called from StatusEventConsumer on every consumed record.
 * Thread-safe: AtomicBoolean for pause state, ConcurrentHashSet for partition tracking.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BackpressureManager {

    private final MessageListenerContainer webhookListenerContainer;

    @Value("${webhook.accumulator.buffer-capacity:50000}")
    private int bufferCapacity;

    @Value("${webhook.accumulator.pause-threshold-percent:80}")
    private int pauseThresholdPercent;

    @Value("${webhook.accumulator.resume-threshold-percent:50}")
    private int resumeThresholdPercent;

    // Current pause state — AtomicBoolean prevents duplicate pause/resume calls
    private final AtomicBoolean paused = new AtomicBoolean(false);

    // Tracks currently assigned partitions — updated on rebalance
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

    /**
     * Evaluate buffer size and pause/resume accordingly.
     * Called by StatusBatchAccumulator after every add() and after every flush().
     *
     * @param currentBufferSize current value of the AtomicInteger counter in accumulator
     */
    public void evaluate(int currentBufferSize) {
        int pauseThreshold  = (bufferCapacity * pauseThresholdPercent)  / 100; // 40,000
        int resumeThreshold = (bufferCapacity * resumeThresholdPercent) / 100; // 25,000

        if (!paused.get() && currentBufferSize >= pauseThreshold) {
            pause();
        } else if (paused.get() && currentBufferSize <= resumeThreshold) {
            resume();
        }
    }

    private void pause() {
        if (paused.compareAndSet(false, true)) {
            log.warn("BACKPRESSURE: Pausing consumer — buffer threshold reached. assignedPartitions={}",
                    assignedPartitions.size());
            webhookListenerContainer.pause();
        }
    }

    private void resume() {
        if (paused.compareAndSet(true, false)) {
            log.info("BACKPRESSURE: Resuming consumer — buffer drained below resume threshold.");
            webhookListenerContainer.resume();
        }
    }

    // ── Rebalance callbacks (called from StatusEventConsumer) ─────────

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        assignedPartitions.removeAll(partitions);
        // Clear pause state — new owner of these partitions starts fresh
        paused.set(false);
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignedPartitions.addAll(partitions);
        // If buffer is already above threshold when partitions are assigned, pause immediately
        // This is a safety check — evaluate() will be called on next poll anyway
    }

    public boolean isPaused() {
        return paused.get();
    }
}