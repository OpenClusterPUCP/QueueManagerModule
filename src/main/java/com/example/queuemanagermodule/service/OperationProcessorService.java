package com.example.queuemanagermodule.service;

import com.example.queuemanagermodule.model.ClusterType;
import com.example.queuemanagermodule.model.OperationRequest;
import com.example.queuemanagermodule.model.OperationStatus;
import com.example.queuemanagermodule.model.OperationType;
import com.example.queuemanagermodule.repository.OperationRequestRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class OperationProcessorService {

    private final OperationRequestRepository operationRequestRepository;
    private final SliceControllerService sliceControllerService;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processOperation(Long operationId, OperationType operationType,
                                 ClusterType clusterType, Integer zoneId,
                                 Long userId, Map<String, Object> payload) {
        log.info("Procesando operación: ID={}, Tipo={}", operationId, operationType);

        Optional<OperationRequest> operationOpt = operationRequestRepository.findById(operationId);
        if (operationOpt.isEmpty()) {
            log.error("No se encontró la operación con ID: {}", operationId);
            return;
        }

        OperationRequest operation = operationOpt.get();

        // Actualizar estado si aún no está en progreso
        if (operation.getStatus() != OperationStatus.IN_PROGRESS) {
            operation.setStatus(OperationStatus.IN_PROGRESS);
            operation.setStartedAt(LocalDateTime.now());
            operationRequestRepository.save(operation);
        }

        try {
            // Procesar operación según su tipo
            Map<String, Object> result = processOperationByType(operationType, payload);

            // Actualizar operación con resultado exitoso
            operation.setStatus(OperationStatus.COMPLETED);
            operation.setCompletedAt(LocalDateTime.now());
            operation.setResultJson(objectMapper.writeValueAsString(result));
            operationRequestRepository.save(operation);

            log.info("Operación completada exitosamente: ID={}", operationId);
        } catch (Exception e) {
            log.error("Error procesando operación ID={}: {}", operationId, e.getMessage(), e);

            // Incrementar contador de reintentos
            operation.setRetryCount(operation.getRetryCount() + 1);

            // Verificar si se pueden hacer más reintentos
            if (operation.getRetryCount() < operation.getMaxRetries()) {
                // Marcar para reintentar
                operation.setStatus(OperationStatus.PENDING);
                log.info("Operación marcada para reintento: ID={}, Intento: {}/{}",
                        operationId, operation.getRetryCount(), operation.getMaxRetries());
            } else {
                // Marcar como fallida
                operation.setStatus(OperationStatus.FAILED);
                operation.setCompletedAt(LocalDateTime.now());
                operation.setErrorMessage(e.getMessage());
                log.warn("Operación marcada como fallida después de {} intentos: ID={}",
                        operation.getRetryCount(), operationId);
            }

            operationRequestRepository.save(operation);
        }
    }

    private Map<String, Object> processOperationByType(OperationType operationType, Map<String, Object> payload) {
        switch (operationType) {
            case DEPLOY_SLICE:
                return sliceControllerService.deploySlice(payload);

            case STOP_SLICE:
                String sliceId = payload.get("sliceId").toString();
                return sliceControllerService.stopSlice(sliceId, payload);

            case RESTART_SLICE:
                sliceId = payload.get("sliceId").toString();
                // Usando el mismo endpoint pero con diferente URL
                return sliceControllerService.restartSlice(sliceId, payload);

            case PAUSE_VM:
                String vmId = payload.get("vmId").toString();
                return sliceControllerService.pauseVm(vmId, payload);

            case RESUME_VM:
                vmId = payload.get("vmId").toString();
                return sliceControllerService.resumeVm(vmId, payload);

            case RESTART_VM:
                vmId = payload.get("vmId").toString();
                return sliceControllerService.restartVm(vmId, payload);

            case GENERATE_VNC_TOKEN:
                vmId = payload.get("vmId").toString();
                return sliceControllerService.generateVncToken(vmId, payload);

            case SYNC_IMAGES:
                return sliceControllerService.syncImages(payload);

            default:
                throw new IllegalArgumentException("Tipo de operación no soportado: " + operationType);
        }
    }
}