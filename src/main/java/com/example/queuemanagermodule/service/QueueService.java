package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.*;

import java.util.List;
import java.util.Map;

public interface QueueService {

    /**
     * Encola una nueva operación con la prioridad especificada
     */
    Long enqueueOperation(OperationType type, ClusterType clusterType, Integer zoneId,
                          Long userId, Map<String, Object> payload, Priority priority);

    /**
     * Obtiene el estado actual de una operación
     */
    OperationStatus getOperationStatus(Long operationId);

    /**
     * Obtiene las estadísticas de todas las colas
     */
    List<QueueStats> getAllQueueStats();

    /**
     * Obtiene estadísticas de una cola específica
     */
    QueueStats getQueueStats(String queueName);

    /**
     * Cancela una operación pendiente
     */
    boolean cancelOperation(Long operationId);

    /**
     * Obtiene las operaciones de un usuario
     */
    List<QueueItem> getUserOperations(Long userId, List<OperationStatus> statuses);

}