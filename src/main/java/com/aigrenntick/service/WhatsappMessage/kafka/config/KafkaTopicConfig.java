package com.aigrenntick.service.WhatsappMessage.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topics.outbound-messages:whatsapp.broadcast.dispatch}")
    private String outboundMessagesTopic;

    @Bean
    public NewTopic outboundMessagesTopic() {
        return TopicBuilder
                .name(outboundMessagesTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}