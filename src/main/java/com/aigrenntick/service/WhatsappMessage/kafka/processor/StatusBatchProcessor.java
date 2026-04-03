package com.aigrenntick.service.WhatsappMessage.kafka.processor;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.producer.WebhookEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;

/**
 * Processes a flushed batch of StatusEvents.
 *
 * Step 1 — In-batch deduplication (in memory, before any DB call)
 *   Group by (wamid + status). Keep earliest receivedAt per group.
 *   Handles: Meta duplicate webhooks landing in the same batch.
 *
 * Step 2 — Bulk UPDATE reports via JdbcTemplate.batchUpdate()
 *   WHERE guard: only advance status if incoming priority > current DB priority.
 *   Rows affected = 0 means duplicate or out-of-order → silently discarded.
 *   Handles: out-of-order callbacks (read before delivered), cross-batch duplicates.
 *
 * Step 3 — Retry routing on DB exception
 *   retryCount 0 → retry.1
 *   retryCount 1 → retry.2
 *   retryCount 2 → retry.3
 *   retryCount >= 3 → dlq
 *
 * Uses JdbcTemplate (not JPA) for bulk UPDATE — gives direct control over
 * the CASE expression in the WHERE clause and avoids Hibernate overhead.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatusBatchProcessor {

    private final JdbcTemplate jdbcTemplate;
    private final WebhookEventProducer webhookEventProducer;

    @Value("${kafka.topics.status-retry-1:whatsapp.status.retry.1}")
    private String retry1Topic;

    @Value("${kafka.topics.status-retry-2:whatsapp.status.retry.2}")
    private String retry2Topic;

    @Value("${kafka.topics.status-retry-3:whatsapp.status.retry.3}")
    private String retry3Topic;

    @Value("${kafka.topics.status-dlq:whatsapp.status.dlq}")
    private String dlqTopic;

    @Value("${webhook.retry.max-retries:3}")
    private int maxRetries;

    private static final String BULK_UPDATE_SQL =
            "UPDATE reports " +
            "SET message_status = ?, status = ?, updated_at = NOW() " +
            "WHERE message_id = ? " +
            "AND CASE message_status " +
            "    WHEN 'queued'    THEN 0 " +
            "    WHEN 'sent'      THEN 1 " +
            "    WHEN 'delivered' THEN 2 " +
            "    WHEN 'read'      THEN 3 " +
            "    ELSE 0 " +
            "END < ?";

    /**
     * Entry point — called by StatusBatchAccumulator after every flush.
     */
    public void process(List<StatusEvent> batch) {
        if (batch == null || batch.isEmpty()) return;

        // ── Step 1: In-batch deduplication ───────────────────────────
        List<StatusEvent> deduplicated = deduplicate(batch);

        log.debug("Processing batch: original={} after-dedup={}", batch.size(), deduplicated.size());

        // ── Step 2: Bulk UPDATE ───────────────────────────────────────
        try {
            // batchUpdate(sql, List<Object[]>) returns int[] — one entry per statement.
            // We avoid the 4-arg overload batchUpdate(sql, collection, batchSize, setter)
            // because that returns int[][] (outer = sub-batches, inner = rows per statement).
            List<Object[]> batchArgs = deduplicated.stream()
                    .map(event -> new Object[]{
                            event.getStatus(),        // SET message_status = ?
                            event.getStatus(),        // SET status = ?
                            event.getWamid(),         // WHERE message_id = ?
                            event.getStatusPriority() // AND CASE ... END < ?
                    })
                    .toList();

            int[] results = jdbcTemplate.batchUpdate(BULK_UPDATE_SQL, batchArgs);

            // ── Step 3: Handle results ────────────────────────────────
            handleResults(deduplicated, results);

        } catch (Exception e) {
            // Entire batch failed (connection down, pool exhausted, etc.)
            // Route all events in this batch to retry
            log.error("Bulk UPDATE failed for batch of {} events: {}", deduplicated.size(), e.getMessage(), e);
            routeBatchToRetry(deduplicated, e.getMessage());
        }
    }

    // ── Step 1: Deduplication ─────────────────────────────────────────

    /**
     * Group by (wamid + status). Keep the one with earliest receivedAt.
     * Pure in-memory — no DB calls.
     */
    private List<StatusEvent> deduplicate(List<StatusEvent> batch) {
        Map<String, StatusEvent> seen = new LinkedHashMap<>();

        for (StatusEvent event : batch) {
            String key = event.getWamid() + "|" + event.getStatus();
            seen.merge(key, event, (existing, incoming) -> {
                // Keep the one with earlier receivedAt
                if (existing.getReceivedAt() == null) return incoming;
                if (incoming.getReceivedAt() == null) return existing;
                return existing.getReceivedAt().isBefore(incoming.getReceivedAt()) ? existing : incoming;
            });
        }

        return new ArrayList<>(seen.values());
    }

    // ── Step 3: Result handling ───────────────────────────────────────

    /**
     * Inspect rows-affected per event.
     * 0 rows  = duplicate or out-of-order → discard (idempotent, expected).
     * -2      = Statement.SUCCESS_NO_INFO — MySQL JDBC returns this for batch
     *           updates, meaning the statement succeeded but row count is unknown.
     *           Treat as success.
     * Exception cases handled by catch block in process().
     */
    private void handleResults(List<StatusEvent> events, int[] results) {
        int updated   = 0;
        int discarded = 0;

        for (int i = 0; i < results.length; i++) {
            int rows = results[i];
            if (rows > 0 || rows == -2) {
                updated++;
            } else if (rows == 0) {
                StatusEvent event = events.get(i);
                log.debug("0 rows affected for wamid={} status={} — duplicate or out-of-order, discarding",
                        event.getWamid(), event.getStatus());
                discarded++;
            }
        }

        log.info("Batch complete: updated={} discarded={} total={}", updated, discarded, events.size());
    }

    // ── Retry routing ─────────────────────────────────────────────────

    /**
     * Route a single failed event to the correct retry topic based on retryCount.
     */
    private void routeToRetry(StatusEvent event, String failureReason) {
        Instant now = Instant.now();

        StatusEvent retryEvent = StatusEvent.builder()
                .wamid(event.getWamid())
                .status(event.getStatus())
                .phoneNumber(event.getPhoneNumber())
                .errorCode(event.getErrorCode())
                .errorMessage(event.getErrorMessage())
                .receivedAt(event.getReceivedAt())
                .retryCount(event.getRetryCount() + 1)
                .lastFailureReason(failureReason)
                .firstFailedAt(event.getFirstFailedAt() != null ? event.getFirstFailedAt() : now)
                .lastFailedAt(now)
                .build();

        String topic = resolveRetryTopic(retryEvent.getRetryCount());

        log.warn("Routing wamid={} status={} to topic={} retryCount={}",
                retryEvent.getWamid(), retryEvent.getStatus(), topic, retryEvent.getRetryCount());

        webhookEventProducer.publishToTopic(topic, retryEvent);
    }

    private void routeBatchToRetry(List<StatusEvent> events, String failureReason) {
        events.forEach(event -> routeToRetry(event, failureReason));
    }

    private String resolveRetryTopic(int retryCount) {
        return switch (retryCount) {
            case 1 -> retry1Topic;
            case 2 -> retry2Topic;
            case 3 -> retry3Topic;
            default -> dlqTopic;
        };
    }
}