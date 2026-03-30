package com.aigrenntick.service.WhatsappMessage.service;

import java.util.Map;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Mirrors PHP  Whatsapp::getMediaId($whatsappNoId, $permanentToken, $imageUrl)
 *
 * Flow:
 *   1. Download the image from the given URL
 *   2. Upload it to Meta Graph API  POST /{phoneNumberId}/media
 *   3. Return the media "id" from Meta's response
 */
@Slf4j
@Component
public class WhatsappMediaService {

    private static final String META_API_BASE = "https://graph.facebook.com/v23.0";

    private final WebClient webClient;

    public WhatsappMediaService(WebClient.Builder builder) {
        this.webClient = builder.build();
    }

    /**
     * Downloads the image from {@code imageUrl} and uploads it to Meta,
     * returning the media ID that can be used in template payloads.
     *
     * @return media ID string, or {@code null} on failure
     */
    public String getMediaId(String whatsappNoId, String permanentToken, String imageUrl) {
        try {
            // ── 1. Download image bytes ───────────────────────────────────────
            byte[] imageBytes = webClient.get()
                    .uri(imageUrl)
                    .retrieve()
                    .bodyToMono(byte[].class)
                    .block();

            if (imageBytes == null || imageBytes.length == 0) {
                log.warn("Downloaded empty image from {}", imageUrl);
                return null;
            }

            // ── 2. Determine content type from URL extension ──────────────────
            String contentType = guessContentType(imageUrl);

            // ── 3. Upload to Meta Graph API ───────────────────────────────────
            String uploadUrl = META_API_BASE + "/" + whatsappNoId + "/media";

            MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
            bodyBuilder.part("messaging_product", "whatsapp");
            bodyBuilder.part("type", "image");
            bodyBuilder.part("file", new ByteArrayResource(imageBytes) {
                @Override
                public String getFilename() {
                    return extractFilename(imageUrl);
                }
            }).contentType(MediaType.parseMediaType(contentType));

            Map<?, ?> response = webClient.post()
                    .uri(uploadUrl)
                    .header("Authorization", "Bearer " + permanentToken)
                    .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            if (response != null && response.get("id") != null) {
                String mediaId = String.valueOf(response.get("id"));
                log.info("Uploaded media for {} → id={}", imageUrl, mediaId);
                return mediaId;
            }

            log.warn("Meta media upload returned no id for {}", imageUrl);
            return null;

        } catch (Exception e) {
            log.error("Failed to get media ID for {}: {}", imageUrl, e.getMessage());
            return null;
        }
    }

    // ════════════════════════════════════════════════════════════════════════

    private String guessContentType(String url) {
        String lower = url.toLowerCase();
        if (lower.contains(".png"))  return "image/png";
        if (lower.contains(".gif"))  return "image/gif";
        if (lower.contains(".webp")) return "image/webp";
        return "image/jpeg"; // default
    }

    private String extractFilename(String url) {
        String path = url.contains("?") ? url.substring(0, url.indexOf('?')) : url;
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : "image.jpg";
    }
}