package com.aigrenntick.service.WhatsappMessage.kafka.consumer.retry;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.processor.StatusBatchProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Consumes from retry topics with non-blocking timestamp-based delay.
 *
 * One @KafkaListener per retry topic — each has its own consumer group
 * so they process independently.
 *
 * Delay mechanism (no Thread.sleep):
 *  1. Poll message from retry topic
 *  2. Check: now - message.timestamp() >= delayMs ?
 *  3. YES → attempt DB write via StatusBatchProcessor (same UPDATE logic)
 *  4. NO  → pause that partition, schedule resume after remaining delay
 *  5. On resume → re-poll and check again
 *
 * Why this works without Thread.sleep:
 *  - Pausing the partition stops Kafka from delivering more messages from it
 *  - ScheduledExecutorService resumes it after the delay without blocking any thread
 *  - The consumer thread is free to poll other partitions in the meantime
 *
 * Idempotency: The same WHERE priority guard in StatusBatchProcessor
 * makes re-processing safe if offset commit fails after a successful DB write.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryConsumer {

    private final StatusBatchProcessor batchProcessor;
    private final ObjectMapper objectMapper;

    @Value("${webhook.retry.delays.retry-1-ms:3000}")
    private long retry1DelayMs;

    @Value("${webhook.retry.delays.retry-2-ms:6000}")
    private long retry2DelayMs;

    @Value("${webhook.retry.delays.retry-3-ms:12000}")
    private long retry3DelayMs;

    // Single-threaded scheduler — only used for scheduling resume callbacks
    // Does NOT block; just schedules and returns immediately
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "retry-resume-scheduler");
                t.setDaemon(true);
                return t;
            });

    // Tracks paused partitions per container to avoid double-pause
    private final Map<TopicPartition, Boolean> pausedPartitions = new ConcurrentHashMap<>();

    // ── Retry 1 — 3 second delay ──────────────────────────────────────

    @KafkaListener(
            topics   = "${kafka.topics.status-retry-1:whatsapp.status.retry.1}",
            groupId  = "webhook-retry-1-processor",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consumeRetry1(ConsumerRecord<String, String> record,
                              Acknowledgment ack,
                              org.springframework.kafka.listener.MessageListenerContainer container) {
        handle(record, ack, container, retry1DelayMs, "retry-1");
    }

    // ── Retry 2 — 6 second delay ──────────────────────────────────────

    @KafkaListener(
            topics   = "${kafka.topics.status-retry-2:whatsapp.status.retry.2}",
            groupId  = "webhook-retry-2-processor",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consumeRetry2(ConsumerRecord<String, String> record,
                              Acknowledgment ack,
                              org.springframework.kafka.listener.MessageListenerContainer container) {
        handle(record, ack, container, retry2DelayMs, "retry-2");
    }

    // ── Retry 3 — 12 second delay ─────────────────────────────────────

    @KafkaListener(
            topics   = "${kafka.topics.status-retry-3:whatsapp.status.retry.3}",
            groupId  = "webhook-retry-3-processor",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consumeRetry3(ConsumerRecord<String, String> record,
                              Acknowledgment ack,
                              org.springframework.kafka.listener.MessageListenerContainer container) {
        handle(record, ack, container, retry3DelayMs, "retry-3");
    }

    // ── Core handler ──────────────────────────────────────────────────

    private void handle(ConsumerRecord<String, String> record,
                        Acknowledgment ack,
                        org.springframework.kafka.listener.MessageListenerContainer container,
                        long delayMs,
                        String retryLabel) {

        long messageTimestamp = record.timestamp(); // when event was published to retry topic
        long now              = System.currentTimeMillis();
        long elapsed          = now - messageTimestamp;
        long remaining        = delayMs - elapsed;

        if (remaining <= 0) {
            // ── Delay has passed — process now ────────────────────────
            processRecord(record, ack, retryLabel);
        } else {
            // ── Delay not yet elapsed — pause and schedule resume ─────
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());

            if (pausedPartitions.putIfAbsent(tp, Boolean.TRUE) == null) {
                log.debug("[{}] Delay not elapsed ({}ms remaining) — pausing partition={}",
                        retryLabel, remaining, tp);
                container.pause();
            }

            // Schedule resume after remaining delay — no thread is blocked
            scheduler.schedule(() -> {
                pausedPartitions.remove(tp);
                log.debug("[{}] Resuming partition={}", retryLabel, tp);
                container.resume();
            }, remaining, TimeUnit.MILLISECONDS);

            // Do NOT ack — Kafka will redeliver this record after resume
            // The timestamp won't change, so delay calculation stays correct on redeliver
        }
    }

    private void processRecord(ConsumerRecord<String, String> record,
                               Acknowledgment ack,
                               String retryLabel) {
        try {
            StatusEvent event = objectMapper.readValue(record.value(), StatusEvent.class);

            log.info("[{}] Processing wamid={} status={} retryCount={}",
                    retryLabel, event.getWamid(), event.getStatus(), event.getRetryCount());

            // Reuse same batch processor — single-event list
            // StatusBatchProcessor handles routing to next retry/DLQ on failure
            batchProcessor.process(List.of(event));

            ack.acknowledge();

        } catch (Exception e) {
            log.error("[{}] Failed to deserialize record partition={} offset={}: {}",
                    retryLabel, record.partition(), record.offset(), e.getMessage(), e);
            // Ack and skip poison message — DefaultErrorHandler already sent it to DLQ
            ack.acknowledge();
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}