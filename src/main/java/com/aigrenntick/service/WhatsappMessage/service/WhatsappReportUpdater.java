package com.aigrenntick.service.WhatsappMessage.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.RecipientResult;
import com.aigrenntick.service.WhatsappMessage.repository.ReportRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Responsible for persisting WhatsApp send results back to the reports table.
 *
 * Uses JPA @Modifying query via ReportRepository.
 * All updates run inside a single @Transactional block so Hibernate batches
 * them efficiently (when hibernate.jdbc.batch_size is configured).
 *
 * Why not the raw CASE...WHEN SQL from PHP?
 *   - The PHP version used raw SQL because Laravel doesn't support bulk CASE updates natively.
 *   - In Spring/JPA we get parameterized queries for free — no SQL injection risk.
 *   - With Hibernate batching enabled, N individual UPDATEs are sent in batches
 *     (e.g., 50 at a time), which is nearly as fast as the single CASE...WHEN
 *     approach for typical broadcast sizes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WhatsappReportUpdater {

    private final ReportRepository reportRepository;
    private final ObjectMapper     objectMapper;

    // ════════════════════════════════════════════════════════════════════════
    //  PUBLIC ENTRY POINT
    // ════════════════════════════════════════════════════════════════════════

    @Transactional
    public void bulkUpdate(List<RecipientResult> results) {
        try {
            log.info("Executing bulk UPDATE for {} recipients at {}",
                    results.size(),
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

            for (RecipientResult r : results) {
                Recipient rec = r.getRecipient();

                reportRepository.updateSendResult(
                        rec.getBroadcastId(),
                        rec.getNumber(),
                        r.getMessageId(),
                        r.getMessageStatus(),
                        r.getWaId(),
                        r.getStatus(),
                        toJson(rec.getPayload())
                );
            }

            log.info("Bulk UPDATE completed at {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

        } catch (Exception e) {
            log.error("SQL Error during bulk update: {}", e.getMessage(), e);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    //  HELPER
    // ════════════════════════════════════════════════════════════════════════

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("Failed to serialize payload to JSON: {}", e.getMessage());
            return "null";
        }
    }
}