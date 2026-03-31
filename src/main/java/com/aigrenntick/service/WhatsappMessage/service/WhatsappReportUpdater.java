package com.aigrenntick.service.WhatsappMessage.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.aigrenntick.service.WhatsappMessage.dto.MessageResultCallbackRequest;
import com.aigrenntick.service.WhatsappMessage.repository.ReportRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Persists WhatsApp send results back to the reports table.
 *
 * Now triggered by HTTP callbacks from Broadcast Service (per-window, 80 results each)
 * instead of being called inline after direct Meta API calls.
 *
 * Uses JPA @Modifying query via ReportRepository.
 * All updates run inside a single @Transactional block so Hibernate batches
 * them efficiently (when hibernate.jdbc.batch_size is configured).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WhatsappReportUpdater {

    private final ReportRepository reportRepository;

    /**
     * Bulk update reports from Broadcast Service callback results.
     * Called once per window (every 80 recipients).
     */
    @Transactional
    public void bulkUpdateFromCallback(Long campaignId, List<MessageResultCallbackRequest.RecipientResultDto> results) {
        try {
            log.info("Executing bulk UPDATE for {} recipients (campaignId={}) at {}",
                    results.size(), campaignId,
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

            int updated = 0;
            int skipped = 0;

            for (MessageResultCallbackRequest.RecipientResultDto r : results) {
                if (r.getBroadcastId() == null || r.getMobile() == null) {
                    log.warn("Skipping result with null broadcastId or mobile: {}", r);
                    skipped++;
                    continue;
                }

                String status = r.isSuccess() ? "sent" : "failed";
                String messageStatus = r.getMessageStatus() != null ? r.getMessageStatus() : status;
                String messageId = r.getProviderMessageId();

                // Deduplication: skip if this wamid was already stored
                // (handles Kafka redelivery after crash)
                if (messageId != null) {
                    boolean alreadyExists = reportRepository
                            .findByBroadcastIdAndMobile(r.getBroadcastId(), r.getMobile())
                            .map(report -> messageId.equals(report.getMessageId()))
                            .orElse(false);

                    if (alreadyExists) {
                        log.debug("Skipping duplicate wamid={} for broadcastId={} mobile={}",
                                messageId, r.getBroadcastId(), r.getMobile());
                        skipped++;
                        continue;
                    }
                }

                int rows = reportRepository.updateSendResult(
                        r.getBroadcastId(),
                        r.getMobile(),
                        messageId,
                        messageStatus,
                        null, // waId — not provided in callback, can be added if needed
                        status,
                        r.getPayload()
                );

                if (rows > 0) {
                    updated++;
                } else {
                    log.warn("No report row found for broadcastId={} mobile={}",
                            r.getBroadcastId(), r.getMobile());
                }
            }

            log.info("Bulk UPDATE completed: updated={} skipped={} total={} at {}",
                    updated, skipped, results.size(),
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

        } catch (Exception e) {
            log.error("SQL Error during bulk update for campaignId={}: {}", campaignId, e.getMessage(), e);
        }
    }
}
