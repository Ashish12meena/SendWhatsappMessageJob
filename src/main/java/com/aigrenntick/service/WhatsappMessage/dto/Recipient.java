package com.aigrenntick.service.WhatsappMessage.dto;

import java.util.List;
import java.util.Map;

import lombok.Data;


@Data
public class Recipient {
    private String number;
    private Long broadcastId;
    private String templateName;
    private String templateLanguage;
    private String templateCategory; // e.g. "authentication"
    private List<Object> variables; // [{value: "..."}, ...] or plain strings
    private Boolean isMedia;
    private String mediaUrl;
    private String mediaType; // "image" | "video" | "document"
    private List<CarouselCard> carouselCards;
    private String sendUrl; // optional override URL
    private Boolean withCreditLine;
    private String pinnacleApiKey;

    // Set by buildPayload() — written to DB
    private Map<String, Object> payload;
}