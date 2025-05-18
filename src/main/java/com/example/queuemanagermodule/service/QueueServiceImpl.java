package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.*;
import com.example.queuemanagermodule.repository.OperationRequestRepository;
import com.example.queuemanagermodule.repository.QueueMetricsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class QueueServiceImpl implements QueueService {

    private final OperationRequestRepository operationRequestRepository;
    private final QueueMetricsRepository queueMetricsRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    @Value("${queue.retry.max-attempts}")
    private Integer maxRetryAttempts;

    @Value("${kafka.topics.LINUX_ZONE1_HIGH}")
    private String linuxZone1HighTopic;

    @Value("${kafka.topics.LINUX_ZONE1_MEDIUM}")
    private String linuxZone1MediumTopic;

    @Value("${kafka.topics.LINUX_ZONE1_LOW}")
    private String linuxZone1LowTopic;

    /**
     * Determina la prioridad adecuada para una operación basada en varios factores
     */
    private Priority determinePriority(OperationType operationType, ClusterType clusterType,
                                       Long userId, Map<String, Object> payload) {

        // Prioridad base - por defecto es MEDIUM
        Priority calculatedPriority = Priority.MEDIUM;

        // Optimizado para DEPLOY_SLICE
        if (operationType == OperationType.DEPLOY_SLICE && payload != null) {
            try {
                // Analizar la estructura del payload actual
                Map<String, Object> topologyInfo = (Map<String, Object>) payload.get("topology_info");
                if (topologyInfo != null) {
                    // 1. Cantidad de VMs
                    List<Map<String, Object>> vms = (List<Map<String, Object>>) topologyInfo.get("vms");
                    if (vms != null) {
                        int vmCount = vms.size();

                        // Slices con muchas VMs tienen alta prioridad
                        if (vmCount > 5) {
                            calculatedPriority = Priority.HIGH;
                        } else if (vmCount <= 2) {
                            calculatedPriority = Priority.LOW;
                        }

                        // 2. Complejidad de la topología - basada en cantidad de enlaces
                        List<Map<String, Object>> links = (List<Map<String, Object>>) topologyInfo.get("links");
                        if (links != null && links.size() > 3) {
                            // Topología compleja
                            if (calculatedPriority != Priority.HIGH) {
                                calculatedPriority = Priority.MEDIUM;
                            }
                        }

                        // 3. Complejidad de la red - basada en cantidad de interfaces
                        List<Map<String, Object>> interfaces = (List<Map<String, Object>>) topologyInfo.get("interfaces");
                        if (interfaces != null && interfaces.size() > 6) {
                            // Muchas interfaces indican una topología compleja
                            if (calculatedPriority != Priority.HIGH) {
                                calculatedPriority = Priority.MEDIUM;
                            }
                        }
                    }
                }

                // 4. Flag de urgencia o importancia (si existe)
                if (payload.containsKey("urgent") && Boolean.TRUE.equals(payload.get("urgent"))) {
                    calculatedPriority = Priority.HIGH;
                }

            } catch (Exception e) {
                log.warn("Error analizando payload para determinar prioridad: {}", e.getMessage());
                // Mantener la prioridad calculada hasta ahora
            }
        } else {
            // Para otras operaciones
            switch (operationType) {
                case STOP_SLICE:
                    // Detenciones son importantes
                    calculatedPriority = Priority.HIGH;
                    break;
                case RESTART_SLICE:
                    calculatedPriority = Priority.MEDIUM;
                    break;
                case PAUSE_VM:
                case RESUME_VM:
                case RESTART_VM:
                    calculatedPriority = Priority.MEDIUM;
                    break;
                case SYNC_IMAGES:
                case GENERATE_VNC_TOKEN:
                    calculatedPriority = Priority.LOW;
                    break;
                default:
                    calculatedPriority = Priority.MEDIUM;
                    break;
            }
        }

        // Si la prioridad viene explícitamente indicada, respetarla
        if (payload != null && payload.containsKey("priority")) {
            try {
                String priorityStr = payload.get("priority").toString();
                Priority explicitPriority = Priority.valueOf(priorityStr);
                log.info("Usando prioridad explícita: {}", explicitPriority);
                return explicitPriority;
            } catch (Exception e) {
                log.warn("Prioridad especificada inválida: {}", e.getMessage());
            }
        }

        log.info("Prioridad calculada para operación {}: {}", operationType, calculatedPriority);
        return calculatedPriority;
    }

    @Override
    @Transactional
    public Long enqueueOperation(OperationType type, ClusterType clusterType, Integer zoneId,
                                 Long userId, Map<String, Object> payload, Priority requestedPriority) {
        log.info("Encolando operación: {}, cluster: {}, zona: {}, usuario: {}, prioridad solicitada: {}",
                type, clusterType, zoneId, userId, requestedPriority);

        // Determinar prioridad adecuada
        Priority finalPriority = requestedPriority;

        // Si la prioridad es nula, calcularla automáticamente
        if (finalPriority == null) {
            finalPriority = determinePriority(type, clusterType, userId, payload);
            log.info("Prioridad calculada automáticamente: {}", finalPriority);
        } else {
            log.info("Usando prioridad solicitada: {}", finalPriority);
        }

        // Determinar nombre de cola basado en cluster, zona y prioridad
        String queueName = buildQueueName(clusterType, zoneId, finalPriority);

        try {
            // Convertir payload a JSON string
            String payloadJson = objectMapper.writeValueAsString(payload);

            // Crear y guardar la entidad de solicitud
            OperationRequest operationRequest = OperationRequest.builder()
                    .operationType(type)
                    .clusterType(clusterType)
                    .zoneId(zoneId)
                    .userId(userId)
                    .priority(finalPriority)
                    .payloadJson(payloadJson)
                    .submittedAt(LocalDateTime.now())
                    .status(OperationStatus.PENDING)
                    .retryCount(0)
                    .maxRetries(maxRetryAttempts)
                    .queueName(queueName)
                    .build();

            operationRequestRepository.save(operationRequest);

            // Crear y enviar el item a Kafka
            QueueItem queueItem = QueueItem.builder()
                    .id(operationRequest.getId())
                    .queueName(queueName)
                    .operationType(type)
                    .clusterType(clusterType)
                    .zoneId(zoneId)
                    .userId(userId)
                    .payload(payload)
                    .priority(finalPriority)
                    .enqueuedAt(operationRequest.getSubmittedAt())
                    .status(OperationStatus.PENDING)
                    .retryCount(0)
                    .maxRetries(maxRetryAttempts)
                    .build();

            // Determinar el tópico de Kafka basado en la cola
            String topicName = getKafkaTopicForQueue(queueName);
            boolean sent = kafkaProducerService.sendQueueItem(topicName, queueItem);

            if (!sent) {
                // Si hubo error al enviar a Kafka, marcar la operación como fallida
                operationRequest.setStatus(OperationStatus.FAILED);
                operationRequest.setErrorMessage("Error al publicar en Kafka");
                operationRequest.setCompletedAt(LocalDateTime.now());
                operationRequestRepository.save(operationRequest);

                throw new RuntimeException("Error al publicar en Kafka");
            }

            log.info("Operación encolada exitosamente. ID: {}, Cola: {}, Tópico: {}",
                    operationRequest.getId(), queueName, topicName);

            return operationRequest.getId();
        } catch (JsonProcessingException e) {
            log.error("Error al convertir payload a JSON", e);
            throw new RuntimeException("Error al procesar el payload", e);
        }
    }

    @Override
    public OperationStatus getOperationStatus(Long operationId) {
        log.debug("Consultando estado de operación ID: {}", operationId);
        return operationRequestRepository.findById(operationId)
                .map(OperationRequest::getStatus)
                .orElseThrow(() -> new RuntimeException("Operación no encontrada: " + operationId));
    }

    @Override
    public List<QueueStats> getAllQueueStats() {
        log.debug("Obteniendo estadísticas de todas las colas");
        List<String> queueNames = operationRequestRepository.findAll().stream()
                .map(OperationRequest::getQueueName)
                .distinct()
                .collect(Collectors.toList());

        return queueNames.stream()
                .map(this::getQueueStats)
                .filter(stats -> stats != null)
                .collect(Collectors.toList());
    }

    @Override
    public QueueStats getQueueStats(String queueName) {
        log.debug("Obteniendo estadísticas de la cola: {}", queueName);

        Long pendingCount = operationRequestRepository.countByQueueNameAndStatus(queueName, OperationStatus.PENDING);
        Long inProgressCount = operationRequestRepository.countByQueueNameAndStatus(queueName, OperationStatus.IN_PROGRESS);
        Long completedCount = operationRequestRepository.countByQueueNameAndStatus(queueName, OperationStatus.COMPLETED);
        Long failedCount = operationRequestRepository.countByQueueNameAndStatus(queueName, OperationStatus.FAILED);

        Double avgWaitTime = operationRequestRepository.calculateAverageWaitTime(queueName);
        Double avgProcessingTime = operationRequestRepository.calculateAverageProcessingTime(queueName);

        return QueueStats.builder()
                .queueName(queueName)
                .pendingOperations(pendingCount)
                .inProgressOperations(inProgressCount)
                .completedOperations(completedCount)
                .failedOperations(failedCount)
                .averageWaitTimeSeconds(avgWaitTime)
                .averageProcessingTimeSeconds(avgProcessingTime)
                .lastUpdated(LocalDateTime.now())
                .build();
    }

    @Override
    @Transactional
    public boolean cancelOperation(Long operationId) {
        log.info("Intentando cancelar operación ID: {}", operationId);

        Optional<OperationRequest> operationOpt = operationRequestRepository.findById(operationId);
        if (operationOpt.isPresent()) {
            OperationRequest operation = operationOpt.get();
            if (operation.getStatus() == OperationStatus.PENDING) {
                operation.setStatus(OperationStatus.CANCELLED);
                operation.setCompletedAt(LocalDateTime.now());
                operationRequestRepository.save(operation);
                log.info("Operación cancelada exitosamente: {}", operationId);
                return true;
            } else {
                log.warn("No se puede cancelar la operación {} porque su estado es: {}",
                        operationId, operation.getStatus());
                return false;
            }
        } else {
            log.warn("No se puede cancelar la operación, no encontrada: {}", operationId);
            return false;
        }
    }

    @Override
    public List<QueueItem> getUserOperations(Long userId, List<OperationStatus> statuses) {
        log.debug("Consultando operaciones del usuario ID: {} con estados: {}", userId, statuses);

        List<OperationStatus> statusesToQuery = statuses;
        if (statusesToQuery == null || statusesToQuery.isEmpty()) {
            statusesToQuery = List.of(OperationStatus.PENDING, OperationStatus.IN_PROGRESS,
                    OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED);
        }

        List<OperationRequest> operations = operationRequestRepository.findByUserIdAndStatusIn(userId, statusesToQuery);
        List<QueueItem> results = new ArrayList<>();

        for (OperationRequest op : operations) {
            try {
                Map<String, Object> payload = op.getPayloadJson() != null ?
                        objectMapper.readValue(op.getPayloadJson(), Map.class) : null;
                Map<String, Object> result = op.getResultJson() != null ?
                        objectMapper.readValue(op.getResultJson(), Map.class) : null;

                QueueItem item = QueueItem.builder()
                        .id(op.getId())
                        .queueName(op.getQueueName())
                        .operationType(op.getOperationType())
                        .clusterType(op.getClusterType())
                        .zoneId(op.getZoneId())
                        .userId(op.getUserId())
                        .payload(payload)
                        .priority(op.getPriority())
                        .enqueuedAt(op.getSubmittedAt())
                        .processedAt(op.getStartedAt())
                        .status(op.getStatus())
                        .errorMessage(op.getErrorMessage())
                        .retryCount(op.getRetryCount())
                        .maxRetries(op.getMaxRetries())
                        .build();

                results.add(item);
            } catch (Exception e) {
                log.error("Error procesando operación ID: {}", op.getId(), e);
            }
        }

        return results;
    }

    /**
     * Construye el nombre de la cola basado en el cluster, zona y prioridad
     */
    private String buildQueueName(ClusterType clusterType, Integer zoneId, Priority priority) {
        return clusterType.name() + "_ZONE" + zoneId + "_" + priority.name();
    }

    /**
     * Retorna el nombre del tópico Kafka para la cola especificada
     */
    private String getKafkaTopicForQueue(String queueName) {
        // Por ahora solo tenemos una zona en LINUX, así que es sencillo
        if (queueName.equals("LINUX_ZONE1_HIGH")) {
            return linuxZone1HighTopic;
        } else if (queueName.equals("LINUX_ZONE1_MEDIUM")) {
            return linuxZone1MediumTopic;
        } else if (queueName.equals("LINUX_ZONE1_LOW")) {
            return linuxZone1LowTopic;
        } else {
            // Default para cualquier otra cola no configurada
            return linuxZone1LowTopic;
        }
    }
}