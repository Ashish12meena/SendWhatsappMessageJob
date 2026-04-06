package com.aigrenntick.service.WhatsappMessage.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
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
public class BackpressureManager {

    private final KafkaListenerEndpointRegistry registry;

    @Value("${webhook.accumulator.buffer-capacity:50000}")
    private int bufferCapacity;

    @Value("${webhook.accumulator.pause-threshold-percent:80}")
    private int pauseThresholdPercent;

    @Value("${webhook.accumulator.resume-threshold-percent:50}")
    private int resumeThresholdPercent;

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

    public BackpressureManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    // Resolve container lazily — it doesn't exist at startup time
    private MessageListenerContainer getContainer() {
        MessageListenerContainer container =
            registry.getListenerContainer("webhookInboundListener");
        if (container == null) {
            throw new IllegalStateException(
                "Listener container 'webhookInboundListener' not found in registry");
        }
        return container;
    }

    public void evaluate(int currentBufferSize) {
        int pauseThreshold  = (bufferCapacity * pauseThresholdPercent)  / 100;
        int resumeThreshold = (bufferCapacity * resumeThresholdPercent) / 100;

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
            getContainer().pause();
        }
    }

    private void resume() {
        if (paused.compareAndSet(true, false)) {
            log.info("BACKPRESSURE: Resuming consumer — buffer drained below resume threshold.");
            getContainer().resume();
        }
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        assignedPartitions.removeAll(partitions);
        paused.set(false);
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignedPartitions.addAll(partitions);
    }

    public boolean isPaused() {
        return paused.get();
    }
}