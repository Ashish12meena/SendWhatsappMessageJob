package com.aigrenntick.service.WhatsappMessage.controller;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.aigrenntick.service.WhatsappMessage.dto.Recipient;
import com.aigrenntick.service.WhatsappMessage.dto.SendMessageRequest;
import com.aigrenntick.service.WhatsappMessage.dto.WabaConfig;
import com.aigrenntick.service.WhatsappMessage.service.SendWhatsappMessageJob;
import com.aigrenntick.service.WhatsappMessage.service.WhatsappPayloadBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/whatsapp")
@RequiredArgsConstructor
public class WhatsappMessageController {

    private final SendWhatsappMessageJob messageJob;
    private final WhatsappPayloadBuilder payloadBuilder;

    // ════════════════════════════════════════════════════════════════════════
    //  POST /api/whatsapp/send — ASYNC (builds payloads → publishes to Kafka)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Accepts recipients + config, builds payloads, publishes to Kafka.
     * Broadcast Service consumes from Kafka, sends to Meta, and calls back
     * with results via /internal/broadcast/callbacks/message-results.
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> send(@RequestBody SendMessageRequest request) {

        log.info("POST /api/whatsapp/send — {} recipients", request.getRecipients().size());

        // Fire async — builds payloads and publishes to Kafka
        messageJob.handle(request.getRecipients(), request.getConfig());

        return ResponseEntity.accepted().body(Map.of(
                "message", "Job dispatched to Kafka",
                "recipientCount", request.getRecipients().size(),
                "dispatchedAt", LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS"))
        ));
    }

    // ════════════════════════════════════════════════════════════════════════
    //  POST /api/whatsapp/build-payload — payload preview (no send, no Kafka)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Builds and returns the Meta API payload for each recipient without
     * publishing to Kafka. Useful for debugging payload structure.
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
                "message", "Payloads built (not sent, not published)",
                "count", payloads.size(),
                "payloads", payloads
        ));
    }
}
