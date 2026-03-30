package com.aigrenntick.service.WhatsappMessage.dto;

import java.util.List;

import lombok.Data;

@Data
public class SendMessageRequest {
    private WabaConfig config;
    private List<Recipient> recipients;
}