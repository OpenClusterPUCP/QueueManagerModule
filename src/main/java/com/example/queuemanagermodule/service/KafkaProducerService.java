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
     * DEBE ser exactamente igual a la lógica del SliceIdPartitioner.
     */
    private String extractMessageKey(QueueItem queueItem) {
        try {
            Map<String, Object> payload = queueItem.getPayload();
            if (payload == null) {
                log.debug("Payload es null para QueueItem ID={}", queueItem.getId());
                return "user-" + queueItem.getUserId();
            }

            String sliceId = null;

            // Para DEPLOY_SLICE - buscar en slice_info.id
            if (queueItem.getOperationType() == OperationType.DEPLOY_SLICE) {
                log.debug("Extrayendo clave para DEPLOY_SLICE (ID: {}), payload keys: {}",
                        queueItem.getId(), payload.keySet());

                // Buscar en slice_info.id (fuente principal)
                if (payload.containsKey("slice_info")) {
                    Object sliceInfoObj = payload.get("slice_info");
                    if (sliceInfoObj instanceof Map) {
                        Map<String, Object> sliceInfo = (Map<String, Object>) sliceInfoObj;
                        if (sliceInfo.containsKey("id")) {
                            sliceId = String.valueOf(sliceInfo.get("id"));
                            log.debug("DEPLOY_SLICE: SliceId extraído de slice_info.id: {}", sliceId);
                        }
                    }
                }

                // Buscar en network_config.slice_id como respaldo
                if (sliceId == null && payload.containsKey("network_config")) {
                    Object networkConfigObj = payload.get("network_config");
                    if (networkConfigObj instanceof Map) {
                        Map<String, Object> networkConfig = (Map<String, Object>) networkConfigObj;
                        if (networkConfig.containsKey("slice_id")) {
                            sliceId = String.valueOf(networkConfig.get("slice_id"));
                            log.debug("DEPLOY_SLICE: SliceId extraído de network_config.slice_id: {}", sliceId);
                        }
                    }
                }
            }
            // Para operaciones de control
            else if (queueItem.getOperationType() == OperationType.STOP_SLICE ||
                    queueItem.getOperationType() == OperationType.RESTART_SLICE) {

                log.debug("Extrayendo clave para {} (ID: {}), payload keys: {}",
                        queueItem.getOperationType(), queueItem.getId(), payload.keySet());

                if (payload.containsKey("slice_id")) {
                    sliceId = String.valueOf(payload.get("slice_id"));
                    log.debug("{}: SliceId extraído de slice_id: {}", queueItem.getOperationType(), sliceId);
                }
            }
            // Para operaciones de VM
            else if (queueItem.getOperationType() == OperationType.PAUSE_VM ||
                    queueItem.getOperationType() == OperationType.RESUME_VM ||
                    queueItem.getOperationType() == OperationType.RESTART_VM) {

                log.debug("Extrayendo clave para {} (ID: {}), payload keys: {}",
                        queueItem.getOperationType(), queueItem.getId(), payload.keySet());

                if (payload.containsKey("slice_id")) {
                    sliceId = String.valueOf(payload.get("slice_id"));
                    log.debug("{}: SliceId extraído de slice_id: {}", queueItem.getOperationType(), sliceId);
                }

                if (sliceId == null && payload.containsKey("vm_info")) {
                    Object vmInfoObj = payload.get("vm_info");
                    if (vmInfoObj instanceof Map) {
                        Map<String, Object> vmInfo = (Map<String, Object>) vmInfoObj;
                        if (vmInfo.containsKey("slice_id")) {
                            sliceId = String.valueOf(vmInfo.get("slice_id"));
                            log.debug("{}: SliceId extraído de vm_info.slice_id: {}", queueItem.getOperationType(), sliceId);
                        }
                    }
                }
            }

            // Si encontramos sliceId, usarlo como clave
            if (sliceId != null && !sliceId.isEmpty()) {
                String key = "slice-" + sliceId;
                log.info("Clave de particionamiento generada con sliceId {}: '{}'", sliceId, key);
                return key;
            }

            // Fallback: usar solo userId
            String fallbackKey = "user-" + queueItem.getUserId();
            log.warn("No se encontró sliceId para operación {} (ID: {}), usando clave fallback: '{}'",
                    queueItem.getOperationType(), queueItem.getId(), fallbackKey);
            return fallbackKey;

        } catch (Exception e) {
            log.error("Error al extraer clave del mensaje para QueueItem ID={}: {}",
                    queueItem.getId(), e.getMessage(), e);

            String lastResortKey = "user-" + queueItem.getUserId();
            log.warn("Usando clave de último recurso: '{}'", lastResortKey);
            return lastResortKey;
        }
    }

}