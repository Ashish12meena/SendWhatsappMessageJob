package com.aigrenntick.service.WhatsappMessage.controller;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.RecipientResult;
import com.aigrenntick.service.WhatsappMessage.dto.SendMessageRequest;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;
import com.aigrenntick.service.WhatsappMessage.service.SendWhatsappMessageJob;
import com.aigrenntick.service.WhatsappMessage.service.WhatsappApiClient;
import com.aigrenntick.service.WhatsappMessage.service.WhatsappPayloadBuilder;
import com.aigrenntick.service.WhatsappMessage.service.WhatsappReportUpdater;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/whatsapp")
@RequiredArgsConstructor
public class WhatsappMessageController {

    private final SendWhatsappMessageJob  messageJob;
    private final WhatsappPayloadBuilder  payloadBuilder;
    private final WhatsappApiClient       apiClient;
    private final WhatsappReportUpdater   reportUpdater;

    // ════════════════════════════════════════════════════════════════════════
    //  POST /api/whatsapp/send  — ASYNC (fire-and-forget, like PHP queue job)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Mirrors the original PHP dispatch: accepts recipients + config,
     * kicks off the job asynchronously, returns immediately.
     *
     * Use this in production.
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> send(@RequestBody SendMessageRequest request) {

        log.info("POST /api/whatsapp/send — {} recipients", request.getRecipients().size());

        // Fire async — returns immediately
        messageJob.handle(request.getRecipients(), request.getConfig());

        return ResponseEntity.accepted().body(Map.of(
                "message", "Job dispatched",
                "recipientCount", request.getRecipients().size(),
                "dispatchedAt", LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS"))
        ));
    }

    // ════════════════════════════════════════════════════════════════════════
    //  POST /api/whatsapp/send-sync  — SYNCHRONOUS (for Postman testing)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Same logic as the async job but runs synchronously so you can see
     * the full result in Postman. DO NOT use in production.
     *
     * Returns the built payloads + API results for each recipient.
     */
    @PostMapping("/send-sync")
    public ResponseEntity<Map<String, Object>> sendSync(@RequestBody SendMessageRequest request) {

        List<Recipient> recipients = request.getRecipients();
        WabaConfig config = request.getConfig();

        log.info("POST /api/whatsapp/send-sync — {} recipients", recipients.size());

        long start = System.currentTimeMillis();

        // 1. Build payloads
        recipients.forEach(r -> r.setPayload(payloadBuilder.buildPayload(r, config)));

        // 2. Send (hits mock or real depending on active profile)
        List<RecipientResult> results = apiClient.sendAll(recipients, config);

        // 3. Bulk update DB
        if (results != null && !results.isEmpty()) {
            reportUpdater.bulkUpdate(results);
        }

        long duration = System.currentTimeMillis() - start;

        // 4. Build response for Postman
        List<Map<String, Object>> resultSummaries = results.stream().map(r -> {
            Map<String, Object> summary = new java.util.LinkedHashMap<>();
            summary.put("number", r.getRecipient().getNumber());
            summary.put("status", r.getStatus());
            summary.put("messageId", r.getMessageId());
            summary.put("messageStatus", r.getMessageStatus());
            summary.put("waId", r.getWaId());
            summary.put("success", r.isSuccess());
            summary.put("payload", r.getRecipient().getPayload());
            return summary;
        }).toList();

        return ResponseEntity.ok(Map.of(
                "message", "Completed synchronously",
                "durationMs", duration,
                "results", resultSummaries
        ));
    }

    // ════════════════════════════════════════════════════════════════════════
    //  POST /api/whatsapp/build-payload  — payload preview (no send, no DB)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Builds and returns the Meta API payload for each recipient without
     * actually sending anything. Useful for debugging payload structure.
     */
    @PostMapping("/build-payload")
    public ResponseEntity<Map<String, Object>> buildPayload(@RequestBody SendMessageRequest request) {

        List<Recipient> recipients = request.getRecipients();
        WabaConfig config = request.getConfig();

        List<Map<String, Object>> payloads = recipients.stream().map(r -> {
            Map<String, Object> payload = payloadBuilder.buildPayload(r, config);
            r.setPayload(payload);

            Map<String, Object> entry = new java.util.LinkedHashMap<>();
            entry.put("number", r.getNumber());
            entry.put("payload", payload);
            return entry;
        }).toList();

        return ResponseEntity.ok(Map.of(
                "message", "Payloads built (not sent)",
                "count", payloads.size(),
                "payloads", payloads
        ));
    }
}