package com.aigrenntick.service.WhatsappMessage.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private final KafkaTopicProperties topicProperties;

    @Bean
    public List<NewTopic> kafkaTopics() {
        List<NewTopic> topics = new ArrayList<>();

        topicProperties.getTopics().forEach((key, config) -> {
            topics.add(new NewTopic(
                    config.getName(),
                    config.getPartitions(),
                    config.getReplicas()
            ));
        });

        return topics;
    }
}