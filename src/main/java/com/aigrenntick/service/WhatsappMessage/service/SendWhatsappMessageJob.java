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
 * MODIFIED Orchestrator — now does:
 *   1. Build payloads (same as before)
 *   2. Publish to Kafka (NEW — replaces direct Meta API calls)
 *
 * NO LONGER does:
 *   - Direct Meta API calls (handled by Broadcast Service)
 *   - Direct report DB updates (handled by callback endpoint)
 *
 * The Broadcast Service consumes from Kafka, sends to Meta in windows of 80,
 * and calls back with results. The callback endpoint updates the reports table.
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
            // ── 1. Build payload for each recipient (CPU-only, no DB, no HTTP) ──
            recipients.forEach(r -> r.setPayload(payloadBuilder.buildPayload(r, config)));

            // ── 2. Convert to Kafka event and publish ───────────────────────────
            List<RecipientPayloadDto> payloadDtos = recipients.stream()
                    .map(r -> RecipientPayloadDto.builder()
                            .broadcastId(r.getBroadcastId())
                            .mobile(r.getNumber())
                            .requestPayload(serializePayload(r))
                            .build())
                    .collect(Collectors.toList());

            BroadcastMessageOutboundEvent event = BroadcastMessageOutboundEvent.builder()
                    .campaignId(extractCampaignId(recipients))
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

    /**
     * Serializes the built payload Map to a JSON string.
     * This becomes the requestPayload that Broadcast Service sends to Meta as-is.
     */
    private String serializePayload(Recipient recipient) {
        try {
            return objectMapper.writeValueAsString(recipient.getPayload());
        } catch (Exception e) {
            log.error("Failed to serialize payload for {}: {}", recipient.getNumber(), e.getMessage());
            return "{}";
        }
    }

    /**
     * Extracts campaignId from the first recipient's broadcastId.
     * In current architecture, broadcastId serves as the campaign identifier.
     */
    private Long extractCampaignId(List<Recipient> recipients) {
        return recipients.stream()
                .map(Recipient::getBroadcastId)
                .filter(id -> id != null)
                .findFirst()
                .orElse(0L);
    }
}
