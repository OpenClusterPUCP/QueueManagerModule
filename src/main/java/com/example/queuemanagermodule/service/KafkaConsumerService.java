package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.QueueItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final OperationProcessorService operationProcessorService;

    @KafkaListener(topics = "${kafka.topics.LINUX_ZONE1_HIGH}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeLinuxZone1High(QueueItem queueItem) {
        log.info("Consumiendo item de alta prioridad para Linux Zona 1: {}", queueItem.getId());
        processQueueItem(queueItem);
    }

    @KafkaListener(topics = "${kafka.topics.LINUX_ZONE1_MEDIUM}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeLinuxZone1Medium(QueueItem queueItem) {
        log.info("Consumiendo item de prioridad media para Linux Zona 1: {}", queueItem.getId());
        processQueueItem(queueItem);
    }

    @KafkaListener(topics = "${kafka.topics.LINUX_ZONE1_LOW}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeLinuxZone1Low(QueueItem queueItem) {
        log.info("Consumiendo item de baja prioridad para Linux Zona 1: {}", queueItem.getId());
        processQueueItem(queueItem);
    }

    private void processQueueItem(QueueItem queueItem) {
        try {
            log.debug("Iniciando procesamiento de ítem: {}, tipo: {}",
                    queueItem.getId(), queueItem.getOperationType());

            operationProcessorService.processOperation(
                    queueItem.getId(),
                    queueItem.getOperationType(),
                    queueItem.getClusterType(),
                    queueItem.getZoneId(),
                    queueItem.getUserId(),
                    queueItem.getPayload()
            );

            log.info("Procesamiento exitoso de ítem: {}", queueItem.getId());
        } catch (Exception e) {
            log.error("Error procesando ítem de cola: {}", queueItem.getId(), e);
        }
    }
}