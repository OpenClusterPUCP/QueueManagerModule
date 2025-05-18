package com.example.queuemanagermodule.config;

import com.example.queuemanagermodule.model.QueueItem;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
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

    @Value("${kafka.partitions.high:8}")
    private int highPriorityPartitions;

    @Value("${kafka.partitions.medium:4}")
    private int mediumPriorityPartitions;

    @Value("${kafka.partitions.low:2}")
    private int lowPriorityPartitions;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic linuxZone1HighTopic() {
        // Aumentar particiones para tópico de alta prioridad
        return new NewTopic(linuxZone1HighTopic, highPriorityPartitions, (short) 1);
    }

    @Bean
    public NewTopic linuxZone1MediumTopic() {
        // Particiones para prioridad media
        return new NewTopic(linuxZone1MediumTopic, mediumPriorityPartitions, (short) 1);
    }

    @Bean
    public NewTopic linuxZone1LowTopic() {
        // Particiones para prioridad baja
        return new NewTopic(linuxZone1LowTopic, lowPriorityPartitions, (short) 1);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);

        // Configurar particionador personalizado
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                SliceIdPartitioner.class.getName());

        // Configuraciones para mejorar confiabilidad
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 10);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        // Para mensajes grandes de DEPLOY_SLICE
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 5242880); // 5MB
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean("generalKafkaTemplate")  // Agregar nombre específico
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, QueueItem> queueItemProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class);

        // Reutilizar las mismas configuraciones que tu producer existente
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                SliceIdPartitioner.class.getName());
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 10);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 5242880); // 5MB
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    @Primary  // Marcar como primary para que sea la preferida cuando se inyecte KafkaTemplate<String, QueueItem>
    public KafkaTemplate<String, QueueItem> queueItemKafkaTemplate() {
        return new KafkaTemplate<>(queueItemProducerFactory());
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}