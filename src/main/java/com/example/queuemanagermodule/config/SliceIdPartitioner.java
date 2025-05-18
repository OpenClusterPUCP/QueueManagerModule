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
public class SliceIdPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        try {
            // Si ya tenemos una clave definida, usarla directamente
            if (key != null) {
                return Utils.toPositive(Utils.murmur2(((String) key).getBytes(StandardCharsets.UTF_8))) %
                        cluster.partitionsForTopic(topic).size();
            }

            // Extraer sliceId de la estructura de QueueItem
            QueueItem queueItem = (QueueItem) value;
            String sliceId = extractSliceId(queueItem);

            if (sliceId != null && !sliceId.isEmpty()) {
                return Utils.toPositive(Utils.murmur2(sliceId.getBytes(StandardCharsets.UTF_8))) %
                        cluster.partitionsForTopic(topic).size();
            }

            // Si no se puede determinar sliceId, usar un algoritmo alternativo
            // (por ejemplo, basado en userId y operationId)
            String fallbackKey = queueItem.getUserId() + "-" + queueItem.getId();
            return Utils.toPositive(Utils.murmur2(fallbackKey.getBytes(StandardCharsets.UTF_8))) %
                    cluster.partitionsForTopic(topic).size();

        } catch (Exception e) {
            // En caso de cualquier error, usar la lógica por defecto (basada en bytes del valor)
            return Utils.toPositive(Utils.murmur2(valueBytes)) % cluster.partitionsForTopic(topic).size();
        }
    }

    private String extractSliceId(QueueItem queueItem) {
        try {
            if (queueItem.getOperationType() == OperationType.DEPLOY_SLICE) {
                Map<String, Object> payload = queueItem.getPayload();

                // Extraer según la estructura real del payload
                Map<String, Object> sliceInfo = (Map<String, Object>) payload.get("slice_info");
                if (sliceInfo != null && sliceInfo.containsKey("id")) {
                    // Convertir a String para usar como clave de partición
                    return String.valueOf(sliceInfo.get("id"));
                }
            } else if (queueItem.getOperationType() == OperationType.STOP_SLICE ||
                    queueItem.getOperationType() == OperationType.RESTART_SLICE) {
                // Para estas operaciones también necesitamos extraer el sliceId
                Map<String, Object> payload = queueItem.getPayload();
                if (payload.containsKey("slice_id")) {
                    return String.valueOf(payload.get("slice_id"));
                }
            }

            // Para otras operaciones o si no se encuentra, retornar null
            return null;
        } catch (Exception e) {
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