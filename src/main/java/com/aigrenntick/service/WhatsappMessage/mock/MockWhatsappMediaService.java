package com.aigrenntick.service.WhatsappMessage.mock;

import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.aigrenntick.service.WhatsappMessage.service.WhatsappMediaService;

import lombok.extern.slf4j.Slf4j;

/**
 * Mock implementation of WhatsappMediaService.
 * Returns fake media IDs without calling Meta Graph API.
 *
 * Active only when profile = "mock".
 * The real WhatsappMediaService has @Profile("!mock"), so only this bean exists.
 */
@Slf4j
@Component
@Profile("mock")
public class MockWhatsappMediaService extends WhatsappMediaService {

    public MockWhatsappMediaService(WebClient.Builder webClientBuilder) {
        super(webClientBuilder); // safe — parent stores it but mock never uses it
    }

    @Override
    public String getMediaId(String whatsappNoId, String permanentToken, String imageUrl) {
        String fakeId = "mock_media_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        log.info("══ MOCK MEDIA ══ getMediaId({}) → {}", imageUrl, fakeId);
        return fakeId;
    }
}