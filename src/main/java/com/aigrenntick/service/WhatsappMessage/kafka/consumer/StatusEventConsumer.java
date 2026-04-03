package com.aigrenntick.service.WhatsappMessage.kafka.consumer;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.accumulator.StatusBatchAccumulator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

/**
 * Consumes StatusEvents from whatsapp.status.inbound.
 *
 * Responsibilities:
 *  1. Deserialize each Kafka record into StatusEvent
 *  2. Feed event into StatusBatchAccumulator
 *  3. Commit offset only after event is in the accumulator (not after DB write)
 *  4. Delegate backpressure (pause/resume) to BackpressureManager
 *
 * Offset commit strategy:
 *  - enable-auto-commit = false (manual ack)
 *  - We ack immediately after adding to accumulator — not after DB write.
 *  - If JVM crashes after ack but before DB write, the @PreDestroy flush
 *    in accumulator handles the in-memory events. If that also fails,
 *    Kafka redelivers from last committed offset on restart.
 *
 * Backpressure:
 *  - BackpressureManager checks buffer size on every poll.
 *  - If buffer >= 80% capacity → pause assigned partitions.
 *  - If buffer <= 50% capacity → resume.
 *  - max.poll.interval.ms = 300000 (5 min) gives DB time to recover.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatusEventConsumer implements ConsumerSeekAware {

    private final StatusBatchAccumulator accumulator;
    private final BackpressureManager backpressureManager;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics    = "${kafka.topics.status-inbound:whatsapp.status.inbound}",
            groupId   = "${spring.kafka.consumer.group-id:webhook-status-processor}",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            StatusEvent event = objectMapper.readValue(record.value(), StatusEvent.class);

            log.debug("Consumed wamid={} status={} partition={} offset={}",
                    event.getWamid(), event.getStatus(),
                    record.partition(), record.offset());

            // Add to accumulator buffer — flush is handled by accumulator internally
            accumulator.add(event);

            // Ack after safely in buffer
            ack.acknowledge();

        } catch (Exception e) {
            // Deserialization failure — ack and skip (poison message)
            // DefaultErrorHandler in KafkaConsumerConfig routes these to DLQ
            log.error("Failed to deserialize record at partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
            ack.acknowledge();
        }
    }

    // ── ConsumerSeekAware — rebalance callbacks ───────────────────────

    /**
     * Called when partitions are revoked (rebalance starting).
     * Clear pause state so the new owner starts fresh.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {} — clearing backpressure state", partitions);
        backpressureManager.onPartitionsRevoked(partitions);
    }

    /**
     * Called when partitions are assigned (rebalance complete).
     * Re-evaluate buffer and pause immediately if already above threshold.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("Partitions assigned: {}", assignments.keySet());
        backpressureManager.onPartitionsAssigned(assignments.keySet());
    }
}