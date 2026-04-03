package com.aigrenntick.service.WhatsappMessage.controller;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.aigrenntick.service.WhatsappMessage.kafka.producer.WebhookEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * Internal endpoint — receives a single StatusEvent from the webhook receiver service.
 *
 * The other service handles Meta's raw POST, signature validation, and parsing.
 * This controller simply accepts the already-extracted StatusEvent and publishes
 * it to Kafka topic: whatsapp.status.inbound
 *
 * POST /internal/webhook/report/status
 *
 * This endpoint should NOT be exposed externally — internal service-to-service only.
 */
@Slf4j
@RestController
@RequestMapping("/internal/webhook/report")
@RequiredArgsConstructor
public class WebhookStatusIngestController {

    private final WebhookEventProducer webhookEventProducer;

    @PostMapping("/status")
    public ResponseEntity<Map<String, Object>> ingestStatusEvent(
            @RequestBody StatusEvent event) {

        if (event.getWamid() == null || event.getWamid().isBlank()) {
            log.warn("Received StatusEvent with null/blank wamid — rejecting");
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "wamid is required"
            ));
        }

        if (event.getStatus() == null || event.getStatus().isBlank()) {
            log.warn("Received StatusEvent with null/blank status for wamid={}", event.getWamid());
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "status is required"
            ));
        }

        // Stamp receivedAt if the other service didn't set it
        if (event.getReceivedAt() == null) {
            event.setReceivedAt(Instant.now());
        }

        log.info("Ingesting StatusEvent wamid={} status={}", event.getWamid(), event.getStatus());

        webhookEventProducer.publishSingle(event);

        return ResponseEntity.accepted().body(Map.of(
                "wamid", event.getWamid(),
                "status", event.getStatus(),
                "queued", true
        ));
    }
}