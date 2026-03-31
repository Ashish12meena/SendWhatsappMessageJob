package com.aigrenntick.service.WhatsappMessage.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopicProperties {

    private Map<String, TopicConfig> topics;

    @Data
    public static class TopicConfig {
        private String name;
        private int partitions;
        private short replicas;
    }
}