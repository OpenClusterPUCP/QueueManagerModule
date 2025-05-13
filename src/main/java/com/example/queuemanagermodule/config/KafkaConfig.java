package com.example.queuemanagermodule.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.LINUX_ZONE1_HIGH}")
    private String linuxZone1HighTopic;

    @Value("${kafka.topics.LINUX_ZONE1_MEDIUM}")
    private String linuxZone1MediumTopic;

    @Value("${kafka.topics.LINUX_ZONE1_LOW}")
    private String linuxZone1LowTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic linuxZone1HighTopic() {
        return new NewTopic(linuxZone1HighTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic linuxZone1MediumTopic() {
        return new NewTopic(linuxZone1MediumTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic linuxZone1LowTopic() {
        return new NewTopic(linuxZone1LowTopic, 1, (short) 1);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}