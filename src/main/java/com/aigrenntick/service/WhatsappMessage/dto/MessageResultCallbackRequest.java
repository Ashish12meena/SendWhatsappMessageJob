package com.aigrenntick.service.WhatsappMessage.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request body received from Broadcast Service's callback.
 *
 * POST /internal/broadcast/callbacks/message-results
 *
 * Sent once per window (every 80 recipients).
 * This service performs a bulk UPDATE for all results in one call.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageResultCallbackRequest {

    private Long campaignId;

    private String phoneNumberId;

    private List<RecipientResultDto> results;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RecipientResultDto {

        /** Broadcast ID — used to find the report row */
        private Long broadcastId;

        /** Phone number — used to find the report row (matches reports.mobile) */
        private String mobile;

        /** true = Meta accepted the message */
        private boolean success;

        /** wamid from Meta response. Used for deduplication. */
        private String providerMessageId;

        /** Meta's message_status: "accepted", "sent", etc. */
        private String messageStatus;

        /** JSON string of the payload sent to Meta (for auditing) */
        private String payload;

        /** JSON string of Meta's raw response (for auditing) */
        private String response;

        /** Populated when success=false */
        private String errorCode;

        private String errorMessage;
    }
}
