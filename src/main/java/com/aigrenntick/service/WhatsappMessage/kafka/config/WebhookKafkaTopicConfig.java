package com.aigrenntick.service.WhatsappMessage.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka topic declarations for the webhook status processing pipeline.
 *
 * Topic inventory (from architecture doc §4.1):
 *  - whatsapp.status.inbound   12 partitions  7d retention  — main ingestion
 *  - whatsapp.status.retry.1    6 partitions  3d retention  — 1st retry (3s delay)
 *  - whatsapp.status.retry.2    6 partitions  3d retention  — 2nd retry (6s delay)
 *  - whatsapp.status.retry.3    3 partitions  3d retention  — 3rd retry (12s delay)
 *  - whatsapp.status.dlq        3 partitions 30d retention  — dead letter queue
 *
 * Retention is configured via broker/topic config, not in TopicBuilder.
 * Set via AdminClient or broker config: retention.ms = <millis>.
 */
@Configuration
public class WebhookKafkaTopicConfig {

    @Value("${kafka.topics.status-inbound:whatsapp.status.inbound}")
    private String statusInbound;

    @Value("${kafka.topics.status-retry-1:whatsapp.status.retry.1}")
    private String statusRetry1;

    @Value("${kafka.topics.status-retry-2:whatsapp.status.retry.2}")
    private String statusRetry2;

    @Value("${kafka.topics.status-retry-3:whatsapp.status.retry.3}")
    private String statusRetry3;

    @Value("${kafka.topics.status-dlq:whatsapp.status.dlq}")
    private String statusDlq;

    // ── Main ingestion ────────────────────────────────────────────────
    // 12 partitions: supports up to 12 consumer instances for horizontal scaling.
    // Partition key = wamid → per-message ordering guaranteed.
    @Bean
    public NewTopic statusInboundTopic() {
        return TopicBuilder.name(statusInbound)
                .partitions(12)
                .replicas(1)
                .config("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L))
                .build();
    }

    // ── Retry tier 1 — 3s delay ───────────────────────────────────────
    @Bean
    public NewTopic statusRetry1Topic() {
        return TopicBuilder.name(statusRetry1)
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(3 * 24 * 60 * 60 * 1000L))
                .build();
    }

    // ── Retry tier 2 — 6s delay ───────────────────────────────────────
    @Bean
    public NewTopic statusRetry2Topic() {
        return TopicBuilder.name(statusRetry2)
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(3 * 24 * 60 * 60 * 1000L))
                .build();
    }

    // ── Retry tier 3 — 12s delay ──────────────────────────────────────
    @Bean
    public NewTopic statusRetry3Topic() {
        return TopicBuilder.name(statusRetry3)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", String.valueOf(3 * 24 * 60 * 60 * 1000L))
                .build();
    }

    // ── Dead Letter Queue — 30 day retention ─────────────────────────
    @Bean
    public NewTopic statusDlqTopic() {
        return TopicBuilder.name(statusDlq)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000L))
                .build();
    }
}