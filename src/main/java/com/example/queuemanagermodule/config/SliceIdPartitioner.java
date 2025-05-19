package com.example.queuemanagermodule.config;

import com.example.queuemanagermodule.model.OperationRequest;
import com.example.queuemanagermodule.model.OperationType;
import com.example.queuemanagermodule.model.QueueItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
@Slf4j
public class SliceIdPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();

        try {
            // Si ya tenemos una clave definida, usarla directamente
            if (key != null && !((String) key).isEmpty()) {
                String keyStr = (String) key;
                int partition = Utils.toPositive(Utils.murmur2(keyStr.getBytes(StandardCharsets.UTF_8))) % numPartitions;
                log.debug("Usando clave explícita '{}' -> partición {} de {} (tópico: {})",
                        keyStr, partition, numPartitions, topic);
                return partition;
            }

            // Extraer sliceId de la estructura de QueueItem
            QueueItem queueItem = (QueueItem) value;
            String sliceId = extractSliceId(queueItem);

            if (sliceId != null && !sliceId.isEmpty()) {
                String partitionKey = "slice-" + sliceId;
                int partition = Utils.toPositive(Utils.murmur2(partitionKey.getBytes(StandardCharsets.UTF_8))) % numPartitions;
                log.info("SliceId {} -> clave '{}' -> partición {} de {} (tópico: {})",
                        sliceId, partitionKey, partition, numPartitions, topic);
                return partition;
            }

            // Fallback: usar userId
            String fallbackKey = "user-" + queueItem.getUserId();
            int partition = Utils.toPositive(Utils.murmur2(fallbackKey.getBytes(StandardCharsets.UTF_8))) % numPartitions;
            log.warn("No se encontró sliceId para operación {} (ID: {}), usando fallback '{}' -> partición {} de {} (tópico: {})",
                    queueItem.getOperationType(), queueItem.getId(), fallbackKey, partition, numPartitions, topic);
            return partition;

        } catch (Exception e) {
            log.error("Error en particionador para tópico {}, usando distribución por defecto: {}",
                    topic, e.getMessage(), e);
            return Utils.toPositive(Utils.murmur2(valueBytes)) % numPartitions;
        }
    }

    private String extractSliceId(QueueItem queueItem) {
        try {
            Map<String, Object> payload = queueItem.getPayload();
            if (payload == null) {
                log.debug("Payload es null para operación {} (ID: {})",
                        queueItem.getOperationType(), queueItem.getId());
                return null;
            }

            String sliceId = null;

            // Para DEPLOY_SLICE - buscar en slice_info.id
            if (queueItem.getOperationType() == OperationType.DEPLOY_SLICE) {
                log.debug("Procesando DEPLOY_SLICE (ID: {}), payload keys: {}",
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

                log.debug("Procesando {} (ID: {}), payload keys: {}",
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

                log.debug("Procesando {} (ID: {}), payload keys: {}",
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

            // Log final
            if (sliceId != null) {
                log.info("SliceId extraído exitosamente: {} para operación {} (ID: {})",
                        sliceId, queueItem.getOperationType(), queueItem.getId());
            } else {
                log.warn("NO se pudo extraer sliceId para operación {} (ID: {}) con payload keys: {}",
                        queueItem.getOperationType(), queueItem.getId(), payload.keySet());
            }

            return sliceId;
        } catch (Exception e) {
            log.error("Error extrayendo sliceId de QueueItem {} (tipo: {}): {}",
                    queueItem.getId(), queueItem.getOperationType(), e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}