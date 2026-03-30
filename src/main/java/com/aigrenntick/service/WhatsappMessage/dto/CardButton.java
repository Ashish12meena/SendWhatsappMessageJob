package com.aigrenntick.service.WhatsappMessage.dto;

import java.util.Map;

import lombok.Data;

@Data
public class CardButton {
    private String type; // "quick_reply" | "url" | "phone_number"
    private String text;
    private String url;
    private String phoneNumber;
    private Map<String, String> variables; // for URL dynamic suffix
}
