package com.aigrenntick.service.WhatsappMessage.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the webhook status pipeline.
 *
 * Separate from any existing Kafka config — named bean
 * "webhookKafkaListenerContainerFactory" to avoid conflicts.
 *
 * Key settings:
 *  - MANUAL_IMMEDIATE ack mode: offset committed only after accumulator.add()
 *  - concurrency = 3: 3 threads each owning 4 of the 12 partitions
 *  - DefaultErrorHandler with FixedBackOff(0, 0): no local retry —
 *    deserialization failures go directly to DLQ via DeadLetterPublishingRecoverer
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:webhook-status-processor}")
    private String groupId;

    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${kafka.topics.status-dlq:whatsapp.status.dlq}")
    private String dlqTopic;

    @Bean
    public ConsumerFactory<String, String> webhookConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 min
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> webhookKafkaListenerContainerFactory(
            KafkaTemplate<String, String> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(webhookConsumerFactory());

        // Manual ack — offset committed explicitly in StatusEventConsumer after accumulator.add()
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 3 threads × 4 partitions each = 12 total (matches topic partition count)
        // Increase concurrency only when partition count also increases
        factory.setConcurrency(3);

        // Poison message handling:
        // FixedBackOff(0, 0) = 0 retries, 0 delay → fail immediately
        // DeadLetterPublishingRecoverer sends to <topic>.DLT or our dlqTopic
        factory.setCommonErrorHandler(buildErrorHandler(kafkaTemplate));

        return factory;
    }

    /**
     * Error handler for unrecoverable errors (e.g. deserialization failure).
     * No local retry — send directly to DLQ.
     * DB failures are handled in StatusBatchProcessor, not here.
     */
    private DefaultErrorHandler buildErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // Always route to our DLQ regardless of source topic name
                (record, ex) -> {
                    log.error("Poison message — routing to DLQ. topic={} partition={} offset={} error={}",
                            record.topic(), record.partition(), record.offset(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition(dlqTopic, 0);
                }
        );

        // 0 retries, 0 backoff — fail fast, let DLQ handle it
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L));
    }
}