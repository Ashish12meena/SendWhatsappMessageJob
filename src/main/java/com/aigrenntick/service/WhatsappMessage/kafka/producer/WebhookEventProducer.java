package com.aigrenntick.service.WhatsappMessage.kafka.producer;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes StatusEvents to whatsapp.status.inbound.
 *
 * Partition key: wamid — guarantees all status transitions for the same
 * message (sent → delivered → read) land on the same partition.
 * This is critical for the WHERE priority guard to work correctly.
 *
 * Fire-and-forget: does NOT block for Kafka ack.
 * Meta has a 20-second timeout — blocking here risks 500s under Kafka pressure.
 * If Kafka is down, the failure is logged; Meta retries for up to 7 days.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WebhookEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.status-inbound:whatsapp.status.inbound}")
    private String inboundTopic;

    /**
     * Publish a single StatusEvent to whatsapp.status.inbound.
     * Called by WebhookStatusIngestController on every incoming event.
     */
    public void publishSingle(StatusEvent event) {
        publishToTopic(inboundTopic, event);
    }

    /**
     * Publish to a specific topic — used by StatusBatchProcessor for retry routing.
     * retry.1, retry.2, retry.3, dlq all go through here.
     */
    public void publishToTopic(String topic, StatusEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            String key  = event.getWamid(); // partition key = wamid

            // whenComplete callback — purely for logging, does NOT block
            kafkaTemplate.send(topic, key, json).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish wamid={} status={} to topic={}: {}",
                            event.getWamid(), event.getStatus(), topic, ex.getMessage());
                } else {
                    log.debug("Published wamid={} status={} to topic={} partition={} offset={}",
                            event.getWamid(), event.getStatus(), topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Serialization error for wamid={}: {}", event.getWamid(), e.getMessage(), e);
        }
    }
}