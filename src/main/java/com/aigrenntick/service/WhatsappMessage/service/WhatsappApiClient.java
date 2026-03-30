package com.aigrenntick.service.WhatsappMessage.service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.RecipientResult;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Responsible for sending WhatsApp messages to Meta Graph API.
 *
 * Fires all recipient HTTP requests concurrently (mirrors PHP Http::pool).
 * Handles two auth modes:
 *   - Standard Meta Bearer token
 *   - Pinnacle credit-line apikey
 */
@Slf4j
@Component
public class WhatsappApiClient {

    private static final int    TIMEOUT_SECONDS   = 600;   // matches PHP $timeout = 600
    private static final String META_API_BASE_URL = "https://graph.facebook.com/v19.0"; // ← fixed: matches PHP v19.0

    private final WebClient webClient;

    public WhatsappApiClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    // ════════════════════════════════════════════════════════════════════════
    //  PUBLIC — fire all requests concurrently, return all results
    // ════════════════════════════════════════════════════════════════════════

    public List<RecipientResult> sendAll(List<Recipient> recipients, WabaConfig config) {
        Map<String, Long> startTimes = new HashMap<>();

        List<Mono<RecipientResult>> monos = recipients.stream()
                .map(recipient -> {
                    String url = resolveUrl(recipient, config);
                    log.info("Sending to {} via {}", recipient.getNumber(), url);
                    startTimes.put(recipient.getNumber(), System.currentTimeMillis());

                    return buildRequest(recipient, config, url)
                            .retrieve()
                            .toEntity(Map.class)
                            .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                            .map(resp -> {
                                logTiming(recipient.getNumber(), startTimes);

                                if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                                    return mapSuccessResponse(recipient, resp.getBody());
                                } else {
                                    return failedResult(recipient, "Failed");
                                }
                            })
                            .onErrorResume(ex -> {
                                log.error("Error sending to {}: {}", recipient.getNumber(), ex.getMessage());
                                return Mono.just(failedResult(recipient, ex.getMessage()));
                            });
                })
                .collect(Collectors.toList());

        return Flux.merge(monos)
                .collectList()
                .block(Duration.ofSeconds(TIMEOUT_SECONDS));
    }

    // ════════════════════════════════════════════════════════════════════════
    //  PRIVATE HELPERS
    // ════════════════════════════════════════════════════════════════════════

    private String resolveUrl(Recipient recipient, WabaConfig config) {
        if (recipient.getSendUrl() != null && !recipient.getSendUrl().isBlank()) {
            return recipient.getSendUrl();
        }
        return META_API_BASE_URL + "/" + config.getWhatsappNoId() + "/messages";
    }

    private WebClient.RequestHeadersSpec<?> buildRequest(Recipient recipient,
                                                          WabaConfig config,
                                                          String url) {
        WebClient.RequestBodySpec spec = webClient.post().uri(url);

        if (Boolean.TRUE.equals(recipient.getWithCreditLine())) {
            spec = spec.header("apikey", recipient.getPinnacleApiKey())
                       .header("Content-Type", "application/json");
        } else {
            spec = spec.header("Authorization", "Bearer " + config.getPermanentToken())
                       .header("Content-Type", "application/json");
        }

        return spec.bodyValue(recipient.getPayload());
    }

    private RecipientResult mapSuccessResponse(Recipient recipient, Map<?, ?> body) {
        List<?> messages = (List<?>) body.get("messages");
        List<?> contacts = (List<?>) body.get("contacts");

        Map<?, ?> firstMsg     = (messages != null && !messages.isEmpty()) ? (Map<?, ?>) messages.get(0) : null;
        Map<?, ?> firstContact = (contacts != null && !contacts.isEmpty()) ? (Map<?, ?>) contacts.get(0) : null;

        RecipientResult result = new RecipientResult();
        result.setRecipient(recipient);
        result.setSuccess(true);
        result.setMessageId(extractString(firstMsg, "id", null));
        result.setMessageStatus(extractString(firstMsg, "message_status", "sent"));
        result.setWaId(extractString(firstContact, "wa_id", null));
        result.setStatus("sent");
        return result;
    }

    private RecipientResult failedResult(Recipient recipient, String reason) {
        RecipientResult result = new RecipientResult();
        result.setRecipient(recipient);
        result.setSuccess(false);
        result.setStatus("failed");
        result.setMessageStatus(reason);
        return result;
    }

    private void logTiming(String number, Map<String, Long> startTimes) {
        long elapsed = System.currentTimeMillis() - startTimes.getOrDefault(number, 0L);
        log.info("[{}] WhatsApp API time: {} ms", number, elapsed);
    }

    private String extractString(Map<?, ?> map, String key, String defaultVal) {
        if (map == null) return defaultVal;
        Object val = map.get(key);
        return val != null ? String.valueOf(val) : defaultVal;
    }
}