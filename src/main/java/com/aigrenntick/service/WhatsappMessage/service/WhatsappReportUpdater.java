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
 * Uses a parameterized bulk CASE...WHEN SQL statement to update all recipients
 * in one DB round-trip (mirrors the PHP raw SQL block, but with bind variables
 * instead of string concatenation to prevent SQL injection).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WhatsappReportUpdater {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    // ════════════════════════════════════════════════════════════════════════
    //  PUBLIC ENTRY POINT
    // ════════════════════════════════════════════════════════════════════════

    public void bulkUpdate(List<RecipientResult> results) {
        try {
            log.info("Executing bulk UPDATE for {} recipients at {}",
                    results.size(),
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

            BulkUpdateQuery query = buildBulkUpdateSql(results);
            jdbcTemplate.update(query.sql, query.params.toArray());

            log.info("Bulk UPDATE completed at {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS")));

        } catch (Exception e) {
            log.error("SQL Error during bulk update: {}", e.getMessage(), e);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    //  SQL BUILDER  (parameterized — no raw string injection)
    // ════════════════════════════════════════════════════════════════════════

    private BulkUpdateQuery buildBulkUpdateSql(List<RecipientResult> results) {

        StringBuilder messageIdCase     = new StringBuilder();
        StringBuilder messageStatusCase = new StringBuilder();
        StringBuilder waIdCase          = new StringBuilder();
        StringBuilder statusCase        = new StringBuilder();
        StringBuilder payloadCase       = new StringBuilder();

        List<Object> params     = new ArrayList<>();
        Set<String>  mobiles    = new LinkedHashSet<>();
        Set<Long>    broadcasts = new LinkedHashSet<>();

        for (RecipientResult r : results) {
            Recipient rec         = r.getRecipient();
            long      broadcastId = rec.getBroadcastId();
            String    mobile      = rec.getNumber();

            // Each WHEN clause uses bind params: WHEN broadcast_id = ? AND mobile = ? THEN ?
            String whenClause = "WHEN broadcast_id = ? AND mobile = ? THEN ? ";

            messageIdCase.append(whenClause);
            params.add(broadcastId);
            params.add(mobile);
            params.add(r.getMessageId());

            messageStatusCase.append(whenClause);
            params.add(broadcastId);
            params.add(mobile);
            params.add(r.getMessageStatus());

            waIdCase.append(whenClause);
            params.add(broadcastId);
            params.add(mobile);
            params.add(r.getWaId());

            statusCase.append(whenClause);
            params.add(broadcastId);
            params.add(mobile);
            params.add(r.getStatus());

            payloadCase.append(whenClause);
            params.add(broadcastId);
            params.add(mobile);
            params.add(toJson(rec.getPayload()));

            mobiles.add(mobile);
            broadcasts.add(broadcastId);
        }

        // Build the WHERE IN clause with placeholders
        String mobilePlaceholders    = mobiles.stream().map(m -> "?").collect(Collectors.joining(","));
        String broadcastPlaceholders = broadcasts.stream().map(b -> "?").collect(Collectors.joining(","));

        // Add WHERE IN params at the end
        params.addAll(mobiles);
        params.addAll(broadcasts);

        String sql = """
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
                statusCase, payloadCase,
                mobilePlaceholders, broadcastPlaceholders);

        return new BulkUpdateQuery(sql, params);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  HELPERS
    // ════════════════════════════════════════════════════════════════════════

    /** Serializes payload map to JSON string for DB storage. */
    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("Failed to serialize payload to JSON: {}", e.getMessage());
            return "null";
        }
    }

    /** Simple holder for the generated SQL and its ordered bind parameters. */
    private record BulkUpdateQuery(String sql, List<Object> params) {}
}