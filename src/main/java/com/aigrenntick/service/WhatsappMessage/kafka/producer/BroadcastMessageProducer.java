package com.aigrenntick.service.WhatsappMessage.kafka.producer;

import com.aigrenntick.service.WhatsappMessage.kafka.event.BroadcastMessageOutboundEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes broadcast batches to Kafka topic: whatsapp.messages.outbound
 *
 * Key:   wabaAccountId (= whatsappNoId) — ensures partition affinity
 * Value: BroadcastMessageOutboundEvent as JSON
 *
 * Splits large recipient lists into batches of configurable size (default 1000).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BroadcastMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.outbound-messages:whatsapp.messages.outbound}")
    private String outboundTopic;

    @Value("${broadcast.batch-size:1000}")
    private int batchSize;

    /**
     * Publishes recipient batches to Kafka.
     * Splits into chunks of batchSize and publishes each as a separate Kafka message.
     *
     * @param event the full event (may contain >1000 recipients)
     */
    public void publishBatches(BroadcastMessageOutboundEvent event) {
        List<BroadcastMessageOutboundEvent.RecipientPayloadDto> allPayloads = event.getPayloads();

        if (allPayloads == null || allPayloads.isEmpty()) {
            log.warn("No payloads to publish for campaignId={}", event.getCampaignId());
            return;
        }

        // Partition into batches of batchSize
        List<List<BroadcastMessageOutboundEvent.RecipientPayloadDto>> batches = partition(allPayloads, batchSize);

        log.info("Publishing {} Kafka batches for campaignId={} wabaAccountId={} totalRecipients={}",
                batches.size(), event.getCampaignId(), event.getWabaAccountId(), allPayloads.size());

        for (int i = 0; i < batches.size(); i++) {
            List<BroadcastMessageOutboundEvent.RecipientPayloadDto> batch = batches.get(i);

            BroadcastMessageOutboundEvent batchEvent = BroadcastMessageOutboundEvent.builder()
                    .campaignId(event.getCampaignId())
                    .wabaAccountId(event.getWabaAccountId())
                    .accessToken(event.getAccessToken())
                    .payloads(batch)
                    .build();

            publishSingleBatch(batchEvent, i + 1, batches.size());
        }
    }

    private void publishSingleBatch(BroadcastMessageOutboundEvent batchEvent, int batchNumber, int totalBatches) {
        try {
            String json = objectMapper.writeValueAsString(batchEvent);
            String key = batchEvent.getWabaAccountId(); // Partition key = phoneNumberId

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(outboundTopic, key, json);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish batch {}/{} for campaignId={}: {}",
                            batchNumber, totalBatches, batchEvent.getCampaignId(), ex.getMessage());
                } else {
                    log.info("Published batch {}/{} for campaignId={} to partition={} offset={}",
                            batchNumber, totalBatches, batchEvent.getCampaignId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Failed to serialize batch {}/{} for campaignId={}: {}",
                    batchNumber, totalBatches, batchEvent.getCampaignId(), e.getMessage(), e);
        }
    }

    private <T> List<List<T>> partition(List<T> list, int maxSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += maxSize) {
            partitions.add(list.subList(i, Math.min(i + maxSize, list.size())));
        }
        return partitions;
    }
}
