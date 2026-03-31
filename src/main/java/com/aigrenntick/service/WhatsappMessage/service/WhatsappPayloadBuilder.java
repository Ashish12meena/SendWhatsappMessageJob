package com.aigrenntick.service.WhatsappMessage.service;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.aigrenntick.service.WhatsappMessage.dto.CardButton;
import com.aigrenntick.service.WhatsappMessage.dto.CarouselCard;
import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Responsible for building the WhatsApp template API payload per recipient.
 * UNCHANGED from original — still builds payloads before Kafka publish.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WhatsappPayloadBuilder {

    public Map<String, Object> buildPayload(Recipient recipient, WabaConfig config) {

        List<Map<String, Object>> components = new ArrayList<>();
        List<Map<String, String>> bodyParameters = buildBodyParameters(recipient.getVariables());

        Map<String, Object> template = new LinkedHashMap<>();
        template.put("name", recipient.getTemplateName());
        template.put("language", Map.of("code", recipient.getTemplateLanguage()));
        template.put("components", components);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("messaging_product", "whatsapp");
        payload.put("to", recipient.getNumber());
        payload.put("type", "template");
        payload.put("template", template);

        if (recipient.getCarouselCards() != null && !recipient.getCarouselCards().isEmpty()) {
            buildCarouselComponents(components, bodyParameters, recipient.getCarouselCards(), config);
            enrichCarouselMediaUrls(payload, recipient.getCarouselCards());
        } else {
            buildStandardComponents(components, bodyParameters, recipient, config);
        }

        return payload;
    }

    @SuppressWarnings("unchecked")
    private void enrichCarouselMediaUrls(Map<String, Object> payload, List<CarouselCard> carouselCards) {
        try {
            Map<String, Object> template = (Map<String, Object>) payload.get("template");
            List<Map<String, Object>> components = (List<Map<String, Object>>) template.get("components");

            for (Map<String, Object> component : components) {
                if (!"carousel".equals(component.get("type"))) continue;

                List<Map<String, Object>> cards = (List<Map<String, Object>>) component.get("cards");
                if (cards == null) continue;

                for (int cIndex = 0; cIndex < cards.size(); cIndex++) {
                    if (cIndex >= carouselCards.size()) break;
                    String imageUrl = carouselCards.get(cIndex).getImageUrl();
                    if (imageUrl == null || imageUrl.isBlank()) continue;

                    Map<String, Object> card = cards.get(cIndex);
                    List<Map<String, Object>> cardComponents = (List<Map<String, Object>>) card.get("components");
                    if (cardComponents == null) continue;

                    for (Map<String, Object> cComp : cardComponents) {
                        if (!"header".equals(cComp.get("type"))) continue;
                        List<Map<String, Object>> params = (List<Map<String, Object>>) cComp.get("parameters");
                        if (params == null) continue;

                        for (Map<String, Object> param : params) {
                            if (!"image".equals(param.get("type"))) continue;
                            Map<String, Object> imageMap = (Map<String, Object>) param.get("image");
                            if (imageMap != null && imageMap.get("id") != null) {
                                Map<String, Object> mutableImage = new LinkedHashMap<>(imageMap);
                                mutableImage.put("media_url", imageUrl);
                                param.put("image", mutableImage);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to enrich carousel media URLs: {}", e.getMessage());
        }
    }

    private List<Map<String, String>> buildBodyParameters(List<Object> variables) {
        List<Map<String, String>> params = new ArrayList<>();
        if (variables == null) return params;

        for (Object variable : variables) {
            if (variable instanceof Map) {
                Map<?, ?> varMap = (Map<?, ?>) variable;
                params.add(Map.of("type", "text", "text", extractString(varMap, "value", "")));
            } else {
                params.add(Map.of("type", "text", "text", String.valueOf(variable)));
            }
        }
        return params;
    }

    private void buildCarouselComponents(List<Map<String, Object>> components,
                                         List<Map<String, String>> bodyParameters,
                                         List<CarouselCard> carouselCards,
                                         WabaConfig config) {
        List<Map<String, Object>> cards = new ArrayList<>();

        for (int i = 0; i < carouselCards.size(); i++) {
            CarouselCard card = carouselCards.get(i);
            List<Map<String, Object>> cardComponents = new ArrayList<>();

            buildCarouselImageHeader(cardComponents, card, config);
            buildCarouselBody(cardComponents, card);
            buildCarouselButtons(cardComponents, card);

            Map<String, Object> cardMap = new LinkedHashMap<>();
            cardMap.put("card_index", i);
            cardMap.put("components", cardComponents);
            cards.add(cardMap);
        }

        components.add(Map.of("type", "body", "parameters", bodyParameters));

        Map<String, Object> carouselComponent = new LinkedHashMap<>();
        carouselComponent.put("type", "carousel");
        carouselComponent.put("cards", cards);
        components.add(carouselComponent);
    }

    private void buildCarouselImageHeader(List<Map<String, Object>> cardComponents,
                                          CarouselCard card, WabaConfig config) {
        if (card.getImageUrl() == null || card.getImageUrl().isBlank()) return;

        cardComponents.add(Map.of(
                "type", "header",
                "parameters", List.of(Map.of(
                        "type", "image",
                        "image", Map.of("link", card.getImageUrl())))));
    }

    private void buildCarouselBody(List<Map<String, Object>> cardComponents, CarouselCard card) {
        if (card.getVariables() == null || card.getVariables().isEmpty()) return;

        Map<String, String> sortedVars = new TreeMap<>(card.getVariables());
        List<Map<String, String>> cardParams = sortedVars.values().stream()
                .map(v -> Map.of("type", "text", "text", v))
                .collect(Collectors.toList());

        cardComponents.add(Map.of("type", "body", "parameters", cardParams));
    }

    private void buildCarouselButtons(List<Map<String, Object>> cardComponents, CarouselCard card) {
        if (card.getButtons() == null) return;

        for (int i = 0; i < card.getButtons().size(); i++) {
            CardButton btn = card.getButtons().get(i);
            Map<String, Object> button = new LinkedHashMap<>();
            button.put("type", "button");
            button.put("sub_type", btn.getType());
            button.put("index", String.valueOf(i));
            button.put("parameters", buildButtonParameters(btn));
            cardComponents.add(button);
        }
    }

    private void buildStandardComponents(List<Map<String, Object>> components,
                                         List<Map<String, String>> bodyParameters,
                                         Recipient recipient, WabaConfig config) {
        buildMediaHeader(components, recipient);
        components.add(Map.of("type", "body", "parameters", bodyParameters));
        buildAuthButton(components, bodyParameters, recipient);
    }

    private void buildMediaHeader(List<Map<String, Object>> components, Recipient recipient) {
        if (!Boolean.TRUE.equals(recipient.getIsMedia())) return;
        if (recipient.getMediaUrl() == null || recipient.getMediaUrl().isBlank()) return;

        String mediaType = recipient.getMediaType();
        components.add(Map.of(
                "type", "header",
                "parameters", List.of(Map.of(
                        "type", mediaType,
                        mediaType, Map.of("link", recipient.getMediaUrl())))));
    }

    private void buildAuthButton(List<Map<String, Object>> components,
                                 List<Map<String, String>> bodyParameters,
                                 Recipient recipient) {
        if (!"authentication".equalsIgnoreCase(
                recipient.getTemplateCategory() != null ? recipient.getTemplateCategory().trim() : "")) {
            return;
        }
        String otp = bodyParameters.isEmpty() ? "" : bodyParameters.get(0).getOrDefault("text", "");
        components.add(Map.of(
                "type", "button", "sub_type", "url", "index", "0",
                "parameters", List.of(Map.of("type", "text", "text", otp))));
    }

    private List<Map<String, String>> buildButtonParameters(CardButton btn) {
        List<Map<String, String>> params = new ArrayList<>();
        switch (btn.getType().toLowerCase()) {
            case "quick_reply" -> params.add(Map.of(
                    "type", "payload", "payload", btn.getText() != null ? btn.getText() : ""));
            case "url" -> {
                String url = btn.getUrl() != null ? btn.getUrl() : "";
                if (btn.getVariables() != null) {
                    for (Map.Entry<String, String> e : btn.getVariables().entrySet()) {
                        url = url.replace("{{" + e.getKey() + "}}", e.getValue());
                    }
                }
                params.add(Map.of("type", "text", "text", url));
            }
            case "phone_number" -> params.add(Map.of(
                    "type", "text", "text", btn.getPhoneNumber() != null ? btn.getPhoneNumber() : ""));
        }
        return params;
    }

    private String extractString(Map<?, ?> map, String key, String defaultVal) {
        if (map == null) return defaultVal;
        Object val = map.get(key);
        return val != null ? String.valueOf(val) : defaultVal;
    }
}
