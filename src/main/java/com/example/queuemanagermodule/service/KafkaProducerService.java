package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.QueueItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;


import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, QueueItem> kafkaTemplate;

    /**
     * Envía un item de cola al tópico Kafka correspondiente.
     */
    public void sendQueueItem(String topicName, QueueItem queueItem) {
        log.info("Enviando QueueItem al tópico {}: {}", topicName, queueItem);

        CompletableFuture<SendResult<String, QueueItem>> future = kafkaTemplate.send(topicName, queueItem);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Error enviando QueueItem a {}: {}", topicName, ex.getMessage(), ex);
            } else {
                log.info("QueueItem enviado correctamente a {}, offset: {}",
                        topicName, result.getRecordMetadata().offset());
            }
        });
    }

}
