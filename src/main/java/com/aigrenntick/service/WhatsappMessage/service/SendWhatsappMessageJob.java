package com.aigrenntick.service.WhatsappMessage.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.RecipientResult;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrator — ties together payload building, HTTP dispatch, and DB update.
 * Contains NO business logic itself; delegates everything to focused services.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SendWhatsappMessageJob {

    private final WhatsappPayloadBuilder  payloadBuilder;
    private final WhatsappApiClient       apiClient;
    private final WhatsappReportUpdater   reportUpdater;

    @Async
    public void handle(List<Recipient> recipients, WabaConfig config) {
        long start = System.currentTimeMillis();
        log.info("=== SendWhatsappMessageJob STARTED at: {} ===",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

        try {
            // ── 1. Build payload for each recipient ───────────────────────────
            recipients.forEach(r -> r.setPayload(payloadBuilder.buildPayload(r, config)));

            // ── 2. Fire all HTTP requests concurrently ────────────────────────
            List<RecipientResult> results = apiClient.sendAll(recipients, config);

            // ── 3. Bulk UPDATE reports table ──────────────────────────────────
            if (results != null && !results.isEmpty()) {
                reportUpdater.bulkUpdate(results);
            }

        } catch (Exception e) {
            log.error("SendWhatsappMessageJob failed: {}", e.getMessage(), e);
        } finally {
            long duration = System.currentTimeMillis() - start;
            log.info("=== SendWhatsappMessageJob ENDED at: {} | Duration: {} ms ===",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")),
                    duration);
        }
    }
}