package com.aigrenntick.service.WhatsappMessage.mock;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

/**
 * Fake Meta Graph API that runs inside your app on the "mock" profile.
 *
 * Simulates:
 * POST /v19.0/{phoneNumberId}/messages — returns fake wamid + wa_id
 * POST /v19.0/{phoneNumberId}/media — returns fake media id
 *
 * Activate with: spring.profiles.active=mock
 *
 * The WhatsappApiClient will hit this instead of real Facebook when
 * recipients have sendUrl =
 * "http://localhost:8080/v19.0/{phoneNumberId}/messages"
 */
@Slf4j
@RestController
@RequestMapping("/v19.0")
@Profile("mock")
public class MockMetaApiController {

    // ════════════════════════════════════════════════════════════════════════
    // POST /v19.0/{phoneNumberId}/messages — fake send message
    // ════════════════════════════════════════════════════════════════════════

    @PostMapping("/{phoneNumberId}/messages")
    public ResponseEntity<Map<String, Object>> sendMessage(
            @PathVariable String phoneNumberId,
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestHeader(value = "apikey", required = false) String apiKey,
            @RequestBody Map<String, Object> payload) {

        String to = String.valueOf(payload.getOrDefault("to", "unknown"));
        String fakeWamid = "wamid.mock_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);

        log.info("══ MOCK META API ══ Message to {} via phoneNumberId={}", to, phoneNumberId);
        log.info("  Auth: {}", authHeader != null ? "Bearer ***" : (apiKey != null ? "apikey ***" : "NONE"));
        log.info("  Template: {}", extractTemplateName(payload));
        log.info("  Generated wamid: {}", fakeWamid);

        // Simulate ~50-200ms latency like real Meta API
        try {
            Thread.sleep(50 + (long) (Math.random() * 150));
        } catch (InterruptedException ignored) {
        }

        // ── Success response (matches real Meta Graph API structure) ──────
        Map<String, Object> response = Map.of(
                "messaging_product", "whatsapp",
                "contacts", List.of(Map.of(
                        "input", to,
                        "wa_id", to // Meta returns the normalized phone number
                )),
                "messages", List.of(Map.of(
                        "id", fakeWamid,
                        "message_status", "accepted")));

        return ResponseEntity.ok(response);
    }

    // ════════════════════════════════════════════════════════════════════════
    // POST /v19.0/{phoneNumberId}/media — fake media upload
    // ════════════════════════════════════════════════════════════════════════

    @PostMapping("/{phoneNumberId}/media")
    public ResponseEntity<Map<String, Object>> uploadMedia(
            @PathVariable String phoneNumberId,
            @RequestHeader(value = "Authorization", required = false) String authHeader) {

        String fakeMediaId = "mock_media_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        log.info("══ MOCK META API ══ Media upload for phoneNumberId={} → id={}", phoneNumberId, fakeMediaId);

        return ResponseEntity.ok(Map.of("id", fakeMediaId));
    }

    // ════════════════════════════════════════════════════════════════════════
    // HELPER
    // ════════════════════════════════════════════════════════════════════════

    @SuppressWarnings("unchecked")
    private String extractTemplateName(Map<String, Object> payload) {
        try {
            Map<String, Object> template = (Map<String, Object>) payload.get("template");
            return template != null ? String.valueOf(template.get("name")) : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}