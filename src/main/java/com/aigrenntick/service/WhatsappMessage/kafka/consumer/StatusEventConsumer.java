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
 * Listener ID is fixed as "webhookInboundListener" so BackpressureManager
 * can look up this container via KafkaListenerEndpointRegistry at runtime.
 *
 * Offset commit strategy:
 *  Ack immediately after accumulator.add() — not after DB write.
 *  If JVM crashes after ack but before DB write, @PreDestroy flush in
 *  accumulator handles in-memory events. If that also fails, uncommitted
 *  offsets cause Kafka to redeliver from last committed position on restart.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatusEventConsumer implements ConsumerSeekAware {

    private final StatusBatchAccumulator accumulator;
    private final BackpressureManager backpressureManager;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            id       = "webhookInboundListener",   // fixed ID — used by BackpressureManager
            topics   = "${kafka.topics.status-inbound:whatsapp.status.inbound}",
            groupId  = "${spring.kafka.consumer.group-id:webhook-status-processor}",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            StatusEvent event = objectMapper.readValue(record.value(), StatusEvent.class);

            log.debug("Consumed wamid={} status={} partition={} offset={}",
                    event.getWamid(), event.getStatus(),
                    record.partition(), record.offset());

            accumulator.add(event);

            // Ack after safely buffered
            ack.acknowledge();

        } catch (Exception e) {
            // Deserialization failure — DefaultErrorHandler routes to DLQ
            log.error("Failed to deserialize record partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
            ack.acknowledge();
        }
    }

    // ── Rebalance callbacks ───────────────────────────────────────────

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {} — clearing backpressure state", partitions);
        backpressureManager.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("Partitions assigned: {}", assignments.keySet());
        backpressureManager.onPartitionsAssigned(assignments.keySet());
    }
}