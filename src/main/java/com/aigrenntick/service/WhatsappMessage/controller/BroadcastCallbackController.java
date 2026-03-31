package com.aigrenntick.service.WhatsappMessage.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.aigrenntick.service.WhatsappMessage.dto.MessageResultCallbackRequest;
import com.aigrenntick.service.WhatsappMessage.service.WhatsappReportUpdater;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Internal callback endpoint — receives message send results from Broadcast Service.
 *
 * POST /internal/broadcast/callbacks/message-results
 *
 * Called once per window (~50 recipients) by the Broadcast Service.
 * Performs a bulk UPDATE for all results in this callback.
 *
 * This endpoint should NOT be exposed externally — internal service-to-service only.
 */
@Slf4j
@RestController
@RequestMapping("/internal/broadcast/callbacks")
@RequiredArgsConstructor
public class BroadcastCallbackController {

    private final WhatsappReportUpdater reportUpdater;

    @PostMapping("/message-results")
    public ResponseEntity<Map<String, Object>> receiveMessageResults(
            @RequestBody MessageResultCallbackRequest request) {

        log.info("Received callback: phoneNumberId={} results={}",
                request.getPhoneNumberId(),
                request.getResults() != null ? request.getResults().size() : 0);

        if (request.getResults() == null || request.getResults().isEmpty()) {
            return ResponseEntity.ok(Map.of(
                    "message", "No results to process",
                    "phoneNumberId", request.getPhoneNumberId()
            ));
        }

        reportUpdater.bulkUpdateFromCallback(request.getResults());

        return ResponseEntity.ok(Map.of(
                "message", "Results processed",
                "phoneNumberId", request.getPhoneNumberId(),
                "resultsProcessed", request.getResults().size()
        ));
    }
}