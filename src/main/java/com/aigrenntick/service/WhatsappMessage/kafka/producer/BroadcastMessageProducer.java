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

@Slf4j
@Service
@RequiredArgsConstructor
public class BroadcastMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.outbound-messages:whatsapp.broadcast.dispatch}")
    private String outboundTopic;

    @Value("${broadcast.batch-size:1000}")
    private int batchSize;

    public void publishBatches(BroadcastMessageOutboundEvent event) {
        List<BroadcastMessageOutboundEvent.RecipientPayloadDto> allPayloads = event.getPayloads();

        if (allPayloads == null || allPayloads.isEmpty()) {
            log.warn("No payloads to publish for wabaAccountId={}", event.getWabaAccountId());
            return;
        }

        List<List<BroadcastMessageOutboundEvent.RecipientPayloadDto>> batches = partition(allPayloads, batchSize);

        log.info("Publishing {} Kafka batches for wabaAccountId={} totalRecipients={} topic={}",
                batches.size(), event.getWabaAccountId(), allPayloads.size(), outboundTopic);

        for (int i = 0; i < batches.size(); i++) {
            BroadcastMessageOutboundEvent batchEvent = BroadcastMessageOutboundEvent.builder()
                    .wabaAccountId(event.getWabaAccountId())
                    .accessToken(event.getAccessToken())
                    .payloads(batches.get(i))
                    .build();

            publishSingleBatch(batchEvent, i + 1, batches.size());
        }
    }

    private void publishSingleBatch(BroadcastMessageOutboundEvent batchEvent, int batchNumber, int totalBatches) {
        try {
            String json = objectMapper.writeValueAsString(batchEvent);
            String key = batchEvent.getWabaAccountId();

            log.info("Publishing batch {}/{} to topic={} key={}", batchNumber, totalBatches, outboundTopic, key);

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(outboundTopic, key, json);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish batch {}/{} for wabaAccountId={}: {}",
                            batchNumber, totalBatches, batchEvent.getWabaAccountId(), ex.getMessage());
                } else {
                    log.info("Published batch {}/{} for wabaAccountId={} to topic={} partition={} offset={}",
                            batchNumber, totalBatches, batchEvent.getWabaAccountId(),
                            outboundTopic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Failed to serialize batch {}/{} for wabaAccountId={}: {}",
                    batchNumber, totalBatches, batchEvent.getWabaAccountId(), e.getMessage(), e);
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