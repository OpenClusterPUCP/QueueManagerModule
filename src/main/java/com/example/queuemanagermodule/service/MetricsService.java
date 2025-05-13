package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.OperationStatus;
import com.example.queuemanagermodule.model.QueueMetrics;
import com.example.queuemanagermodule.repository.OperationRequestRepository;
import com.example.queuemanagermodule.repository.QueueMetricsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetricsService {

    private final OperationRequestRepository operationRequestRepository;
    private final QueueMetricsRepository queueMetricsRepository;

    /**
     * Tarea programada para actualizar las métricas de las colas cada 5 minutos
     */
    @Scheduled(fixedRate = 300000) // 5 minutos
    @Transactional
    public void updateQueueMetrics() {
        log.info("Actualizando métricas de todas las colas");

        // Obtener nombres de colas únicos
        Set<String> queueNames = new HashSet<>();
        operationRequestRepository.findAll().forEach(op -> queueNames.add(op.getQueueName()));

        for (String queueName : queueNames) {
            updateMetricsForQueue(queueName);
        }
    }

    /**
     * Actualiza métricas para una cola específica
     */
    private void updateMetricsForQueue(String queueName) {
        log.debug("Actualizando métricas para cola: {}", queueName);

        try {
            // Contar operaciones por estado
            Long pendingCount = operationRequestRepository
                    .countByQueueNameAndStatus(queueName, OperationStatus.PENDING);

            Long inProgressCount = operationRequestRepository
                    .countByQueueNameAndStatus(queueName, OperationStatus.IN_PROGRESS);

            Long completedCount = operationRequestRepository
                    .countByQueueNameAndStatus(queueName, OperationStatus.COMPLETED);

            Long failedCount = operationRequestRepository
                    .countByQueueNameAndStatus(queueName, OperationStatus.FAILED);

            // Calcular tiempos promedio
            Double avgWaitTime = operationRequestRepository.calculateAverageWaitTime(queueName);
            Double avgProcessingTime = operationRequestRepository.calculateAverageProcessingTime(queueName);

            // Crear registro de métricas
            QueueMetrics metrics = QueueMetrics.builder()
                    .queueName(queueName)
                    .pendingCount(pendingCount)
                    .inProgressCount(inProgressCount)
                    .completedCount(completedCount)
                    .failedCount(failedCount)
                    .averageWaitTimeSeconds(avgWaitTime)
                    .averageProcessingTimeSeconds(avgProcessingTime)
                    .lastUpdated(LocalDateTime.now())
                    .recordDate(LocalDateTime.now())
                    .build();

            queueMetricsRepository.save(metrics);
            log.debug("Métricas actualizadas para cola: {}", queueName);
        } catch (Exception e) {
            log.error("Error actualizando métricas para cola: {}", queueName, e);
        }
    }
}
