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
    private final OperationProcessorService operationProcessorService;
    private final ObjectMapper objectMapper;

    @Value("${queue.retry.max-attempts}")
    private Integer maxRetryAttempts;

    @Value("${kafka.topics.LINUX_ZONE1_HIGH}")
    private String linuxZone1HighTopic;

    @Value("${kafka.topics.LINUX_ZONE1_MEDIUM}")
    private String linuxZone1MediumTopic;

    @Value("${kafka.topics.LINUX_ZONE1_LOW}")
    private String linuxZone1LowTopic;

    @Override
    @Transactional
    public Long enqueueOperation(OperationType type, ClusterType clusterType, Integer zoneId,
                                 Long userId, Map<String, Object> payload, Priority priority) {
        log.info("Encolando operación: {}, cluster: {}, zona: {}, usuario: {}, prioridad: {}",
                type, clusterType, zoneId, userId, priority);

        // Determinar el nombre de la cola basado en el cluster, zona y prioridad
        String queueName = buildQueueName(clusterType, zoneId, priority);

        try {
            // Convertir payload a JSON string
            String payloadJson = objectMapper.writeValueAsString(payload);

            // Crear y guardar la entidad de solicitud
            OperationRequest operationRequest = OperationRequest.builder()
                    .operationType(type)
                    .clusterType(clusterType)
                    .zoneId(zoneId)
                    .userId(userId)
                    .priority(priority)
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
                    .priority(priority)
                    .enqueuedAt(operationRequest.getSubmittedAt())
                    .status(OperationStatus.PENDING)
                    .retryCount(0)
                    .maxRetries(maxRetryAttempts)
                    .build();

            // Determinar el tópico de Kafka basado en la cola
            String topicName = getKafkaTopicForQueue(queueName);
            kafkaProducerService.sendQueueItem(topicName, queueItem);

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

    @Override
    @Transactional
    public void processNextQueueItem(String queueName) {
        log.info("Procesando próximo ítem de la cola: {}", queueName);

        List<OperationRequest> pendingOperations = operationRequestRepository
                .findByQueueNameAndStatus(queueName, OperationStatus.PENDING);

        if (pendingOperations.isEmpty()) {
            log.info("No hay operaciones pendientes en la cola: {}", queueName);
            return;
        }

        // Ordenar por fecha de envío (FIFO)
        pendingOperations.sort((a, b) -> a.getSubmittedAt().compareTo(b.getSubmittedAt()));
        OperationRequest nextOperation = pendingOperations.get(0);

        // Actualizar estado a IN_PROGRESS
        nextOperation.setStatus(OperationStatus.IN_PROGRESS);
        nextOperation.setStartedAt(LocalDateTime.now());
        operationRequestRepository.save(nextOperation);

        try {
            // Convertir payload de JSON a Map
            Map<String, Object> payload = objectMapper.readValue(nextOperation.getPayloadJson(), Map.class);

            // Procesar la operación
            operationProcessorService.processOperation(
                    nextOperation.getId(),
                    nextOperation.getOperationType(),
                    nextOperation.getClusterType(),
                    nextOperation.getZoneId(),
                    nextOperation.getUserId(),
                    payload
            );
        } catch (Exception e) {
            log.error("Error procesando operación ID: {}", nextOperation.getId(), e);

            // Actualizar estado a FAILED
            nextOperation.setStatus(OperationStatus.FAILED);
            nextOperation.setErrorMessage(e.getMessage());
            nextOperation.setCompletedAt(LocalDateTime.now());
            operationRequestRepository.save(nextOperation);
        }
    }

    /**
     * Tarea programada para verificar y procesar timeouts en operaciones
     */
    @Scheduled(fixedRate = 60000) // cada minuto
    @Transactional
    public void checkForTimeouts() {
        log.debug("Verificando timeouts en operaciones");

        // Considerar timeout si una operación ha estado en IN_PROGRESS por más de 5 minutos
        LocalDateTime timeoutThreshold = LocalDateTime.now().minusMinutes(5);

        List<OperationRequest> potentialTimeouts = operationRequestRepository
                .findByStatusAndStartedAtBefore(OperationStatus.IN_PROGRESS, timeoutThreshold);

        for (OperationRequest op : potentialTimeouts) {
            log.warn("Operación potencialmente en timeout: ID: {}, Tipo: {}, Inicio: {}",
                    op.getId(), op.getOperationType(), op.getStartedAt());

            // Marcar como TIMEOUT
            op.setStatus(OperationStatus.TIMEOUT);
            op.setCompletedAt(LocalDateTime.now());
            op.setErrorMessage("Operación marcada como timeout después de 5 minutos");
            operationRequestRepository.save(op);
        }
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
