package com.aigrenntick.service.WhatsappMessage.dto;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class CarouselCard {
    private String imageUrl;
    private Map<String, String> variables; // sorted by key in PHP: ksort($vars)
    private List<CardButton> buttons;
}
