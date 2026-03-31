package com.aigrenntick.service.WhatsappMessage.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Kafka event published to topic: whatsapp.messages.outbound
 *
 * Key:   whatsappNoId (= phoneNumberId in broadcast service)
 * Value: this object serialized as JSON
 *
 * Contains a batch of up to 1000 recipients for one campaign,
 * one phone number, with pre-built Meta API payloads.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BroadcastMessageOutboundEvent {

    /** Campaign/broadcast identifier — used for callback correlation */
    private Long campaignId;

    /**
     * The sending phone number ID registered with Meta.
     * This is WabaConfig.whatsappNoId from the messaging service.
     * Broadcast service uses this as phoneNumberId for everything:
     *   - PhoneQueue key
     *   - Semaphore key
     *   - Meta API path param: POST /{phoneNumberId}/messages
     */
    private String wabaAccountId;

    /**
     * Bearer token for Meta API authentication.
     * This is WabaConfig.permanentToken from the messaging service.
     */
    private String accessToken;

    /**
     * Recipients in this Kafka batch (up to 1000).
     * Each contains a pre-built requestPayload ready for Meta API.
     */
    private List<RecipientPayloadDto> payloads;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RecipientPayloadDto {

        /** Broadcast ID — needed for report update callback */
        private Long broadcastId;

        /** Phone number — needed for report update callback (matches reports.mobile) */
        private String mobile;

        /**
         * Complete Meta API request body as JSON string.
         * Pre-built by WhatsappPayloadBuilder.
         * Ready to POST to /{phoneNumberId}/messages as-is.
         */
        private String requestPayload;
    }
}
