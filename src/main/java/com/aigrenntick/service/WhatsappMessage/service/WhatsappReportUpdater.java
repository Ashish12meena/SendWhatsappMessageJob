package com.aigrenntick.service.WhatsappMessage.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.RecipientResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Responsible for persisting WhatsApp send results back to the reports table.
 *
 * Uses a single bulk CASE...WHEN SQL statement to update all recipients
 * in one DB round-trip (mirrors the PHP raw SQL block).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WhatsappReportUpdater {

    private final JdbcTemplate  jdbcTemplate;
    private final ObjectMapper  objectMapper;

    // ════════════════════════════════════════════════════════════════════════
    //  PUBLIC ENTRY POINT
    // ════════════════════════════════════════════════════════════════════════

    public void bulkUpdate(List<RecipientResult> results) {
        try {
            String sql = buildBulkUpdateSql(results);

            log.info("Executing bulk UPDATE for {} recipients", results.size());
            jdbcTemplate.execute(sql);
            log.info("Bulk UPDATE completed at {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

        } catch (Exception e) {
            log.error("SQL Error during bulk update: {}", e.getMessage(), e);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    //  SQL BUILDER
    // ════════════════════════════════════════════════════════════════════════

    private String buildBulkUpdateSql(List<RecipientResult> results) {
        StringBuilder messageIdCase     = new StringBuilder();
        StringBuilder messageStatusCase = new StringBuilder();
        StringBuilder waIdCase          = new StringBuilder();
        StringBuilder statusCase        = new StringBuilder();
        StringBuilder payloadCase       = new StringBuilder();

        Set<String> mobiles    = new LinkedHashSet<>();
        Set<Long>   broadcasts = new LinkedHashSet<>();

        for (RecipientResult r : results) {
            Recipient rec         = r.getRecipient();
            long      broadcastId = rec.getBroadcastId();
            String    mobile      = safe(rec.getNumber());

            String when = "WHEN broadcast_id = " + broadcastId
                    + " AND mobile = '" + mobile + "' THEN ";

            messageIdCase    .append(when).append("'").append(safe(r.getMessageId())).append("' ");
            messageStatusCase.append(when).append("'").append(safe(r.getMessageStatus())).append("' ");
            waIdCase         .append(when).append("'").append(safe(r.getWaId())).append("' ");
            statusCase       .append(when).append("'").append(safe(r.getStatus())).append("' ");
            payloadCase      .append(when).append("'").append(toJson(rec.getPayload())).append("' ");

            mobiles.add("'" + mobile + "'");
            broadcasts.add(broadcastId);
        }

        String mobilesIn    = String.join(",", mobiles);
        String broadcastsIn = broadcasts.stream().map(String::valueOf).collect(Collectors.joining(","));

        return """
                UPDATE reports
                SET
                    message_id     = CASE %s ELSE message_id END,
                    message_status = CASE %s ELSE message_status END,
                    wa_id          = CASE %s ELSE wa_id END,
                    status         = CASE %s ELSE status END,
                    payload        = CASE %s ELSE payload END
                WHERE mobile IN (%s)
                AND broadcast_id IN (%s)
                """.formatted(
                        messageIdCase, messageStatusCase, waIdCase,
                        statusCase, payloadCase, mobilesIn, broadcastsIn);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  HELPERS
    // ════════════════════════════════════════════════════════════════════════

    /** Escapes single quotes for raw SQL string literals. */
    private String safe(String s) {
        return s != null ? s.replace("'", "\\'") : "";
    }

    /** Serializes payload map to JSON string safe for SQL embedding. */
    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload).replace("'", "\\'");
        } catch (Exception e) {
            log.warn("Failed to serialize payload to JSON: {}", e.getMessage());
            return "null";
        }
    }
}