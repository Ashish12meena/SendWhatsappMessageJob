package com.aigrenntick.service.WhatsappMessage.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;
import com.aigrenntick.service.WhatsappMessage.kafka.event.BroadcastMessageOutboundEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.event.BroadcastMessageOutboundEvent.RecipientPayloadDto;
import com.aigrenntick.service.WhatsappMessage.kafka.producer.BroadcastMessageProducer;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrator — builds payloads and publishes to Kafka.
 *
 * No campaignId in the event: each recipient carries its own broadcastId,
 * which is the only key needed by Broadcast Service and the callback.
 * The Kafka partition key is wabaAccountId (phoneNumberId) only.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SendWhatsappMessageJob {

    private final WhatsappPayloadBuilder payloadBuilder;
    private final BroadcastMessageProducer broadcastProducer;
    private final ObjectMapper objectMapper;

    @Async("taskExecutor")
    public void handle(List<Recipient> recipients, WabaConfig config) {
        long start = System.currentTimeMillis();
        log.info("=== SendWhatsappMessageJob STARTED at: {} | recipients={} ===",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")),
                recipients.size());

        try {
            // ── 1. Build payload for each recipient ──────────────────────────
            recipients.forEach(r -> r.setPayload(payloadBuilder.buildPayload(r, config)));

            // ── 2. Convert to Kafka event DTOs ───────────────────────────────
            List<RecipientPayloadDto> payloadDtos = recipients.stream()
                    .map(r -> RecipientPayloadDto.builder()
                            .broadcastId(r.getBroadcastId())
                            .mobile(r.getNumber())
                            .requestPayload(serializePayload(r))
                            .build())
                    .collect(Collectors.toList());

            // ── 3. Publish to Kafka ──────────────────────────────────────────
            BroadcastMessageOutboundEvent event = BroadcastMessageOutboundEvent.builder()
                    .wabaAccountId(config.getWhatsappNoId())
                    .accessToken(config.getPermanentToken())
                    .payloads(payloadDtos)
                    .build();

            broadcastProducer.publishBatches(event);

            log.info("=== SendWhatsappMessageJob published {} recipients to Kafka ===",
                    recipients.size());

        } catch (Exception e) {
            log.error("SendWhatsappMessageJob failed: {}", e.getMessage(), e);
        } finally {
            long duration = System.currentTimeMillis() - start;
            log.info("=== SendWhatsappMessageJob ENDED at: {} | Duration: {} ms ===",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")),
                    duration);
        }
    }

    private String serializePayload(Recipient recipient) {
        try {
            return objectMapper.writeValueAsString(recipient.getPayload());
        } catch (Exception e) {
            log.error("Failed to serialize payload for {}: {}", recipient.getNumber(), e.getMessage());
            return "{}";
        }
    }
}