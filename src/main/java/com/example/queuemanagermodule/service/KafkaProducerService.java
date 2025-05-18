package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.OperationType;
import com.example.queuemanagermodule.model.QueueItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;


import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, QueueItem> kafkaTemplate;

    /**
     * Envía un item de cola al tópico Kafka correspondiente.
     * @return true si el envío fue exitoso, false si falló
     */
    public boolean sendQueueItem(String topicName, QueueItem queueItem) {
        log.info("Enviando QueueItem al tópico {}: ID={}, Tipo={}",
                topicName, queueItem.getId(), queueItem.getOperationType());

        try {
            // Usar sliceId como clave para asegurar que mensajes del mismo slice
            // vayan a la misma partición
            String messageKey = extractMessageKey(queueItem);

            CompletableFuture<SendResult<String, QueueItem>> future =
                    kafkaTemplate.send(topicName, messageKey, queueItem);

            // Esperar confirmación con timeout
            SendResult<String, QueueItem> result = future.get(10, TimeUnit.SECONDS);

            log.info("QueueItem enviado correctamente a {}, tópico={}, partición={}, offset={}",
                    topicName,
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Envío interrumpido para QueueItem ID={}: {}", queueItem.getId(), e.getMessage(), e);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.error("Error enviando QueueItem ID={} a {}: {}",
                    queueItem.getId(), topicName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Extrae la clave para el mensaje basada en el tipo de operación.
     */
    private String extractMessageKey(QueueItem queueItem) {
        try {
            // Para DEPLOY_SLICE, extraer sliceId del payload
            if (queueItem.getOperationType() == OperationType.DEPLOY_SLICE) {
                Map<String, Object> payload = queueItem.getPayload();
                if (payload != null && payload.containsKey("slice_info")) {
                    Map<String, Object> sliceInfo = (Map<String, Object>) payload.get("slice_info");
                    if (sliceInfo != null && sliceInfo.containsKey("id")) {
                        return "slice-" + sliceInfo.get("id").toString();
                    }
                }
            }
            // Para otras operaciones como STOP_SLICE, RESTART_SLICE
            else if (queueItem.getOperationType() == OperationType.STOP_SLICE ||
                    queueItem.getOperationType() == OperationType.RESTART_SLICE) {

                Map<String, Object> payload = queueItem.getPayload();
                if (payload != null && payload.containsKey("slice_id")) {
                    return "slice-" + payload.get("slice_id").toString();
                }
            }
        } catch (Exception e) {
            log.warn("Error al extraer clave del mensaje: {}", e.getMessage());
        }

        // Si no se encuentra sliceId específico, usar userId + operationId como clave
        return "user-" + queueItem.getUserId() + "-op-" + queueItem.getId();
    }

}