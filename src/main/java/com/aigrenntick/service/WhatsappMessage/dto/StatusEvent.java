package com.aigrenntick.service.WhatsappMessage.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Kafka message DTO published to whatsapp.status.inbound.
 *
 * Partition key: wamid — guarantees per-message ordering
 * (sent → delivered → read all land on the same partition).
 *
 * No campaignId — wamid is the sole lookup key against reports.message_id.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatusEvent {

    /** Meta's message ID — maps to reports.message_id */
    private String wamid;

    /** "sent" | "delivered" | "read" | "failed" */
    private String status;

    /** Recipient phone number — informational only, not used for DB lookup */
    private String phoneNumber;

    /** Populated only when status = "failed" */
    private String errorCode;
    private String errorMessage;

    /** Set by WebhookController at ingestion time — used for latency tracking */
    private Instant receivedAt;

    /**
     * 0 on first attempt. Incremented each time the event hops to the next retry topic.
     * 0 → retry.1, 1 → retry.2, 2 → retry.3, >=3 → DLQ
     */
    @Builder.Default
    private int retryCount = 0;

    /**
     * Populated when routed to DLQ — last known failure reason.
     * e.g. "connection_timeout", "row_not_found", "deadlock"
     */
    private String lastFailureReason;

    /** Timestamp of first failure — populated when retryCount transitions from 0 to 1 */
    private Instant firstFailedAt;

    /** Updated on each retry hop */
    private Instant lastFailedAt;

    /**
     * Numeric priority of this status — used in the DB WHERE guard.
     * queued=0, sent=1, delivered=2, read=3, failed=0
     *
     * Transient — derived from status. Not stored in Kafka message.
     * Callers must set this before passing to JdbcTemplate.
     */
    public int getStatusPriority() {
        if (status == null) return 0;
        return switch (status.toLowerCase()) {
            case "sent"      -> 1;
            case "delivered" -> 2;
            case "read"      -> 3;
            default          -> 0; // queued, failed, unknown
        };
    }
}