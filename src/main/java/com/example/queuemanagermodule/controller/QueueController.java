package com.example.queuemanagermodule.controller;

import com.example.queuemanagermodule.model.*;
import com.example.queuemanagermodule.service.QueueService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/queue")
@RequiredArgsConstructor
@Slf4j
public class QueueController {

    private final QueueService queueService;

    @PostMapping("/operations")
    public ResponseEntity<Map<String, Object>> enqueueOperation(
            @RequestBody @Valid Map<String, Object> request) {

        log.info("Recibida solicitud para encolar operación: {}", request);

        try {
            OperationType operationType = OperationType.valueOf((String) request.get("operationType"));
            ClusterType clusterType = ClusterType.valueOf((String) request.get("clusterType"));
            Integer zoneId = (Integer) request.get("zoneId");
            Long userId = Long.valueOf(request.get("userId").toString());
            Map<String, Object> payload = (Map<String, Object>) request.get("payload");
            Priority priority = Priority.valueOf((String) request.getOrDefault("priority", "MEDIUM"));

            Long operationId = queueService.enqueueOperation(
                    operationType, clusterType, zoneId, userId, payload, priority);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Operación encolada exitosamente",
                    "operationId", operationId
            ));
        } catch (Exception e) {
            log.error("Error al encolar la operación", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al encolar la operación: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/operations/{operationId}")
    public ResponseEntity<Map<String, Object>> getOperationStatus(@PathVariable Long operationId) {
        log.info("Consultando estado de la operación ID: {}", operationId);

        try {
            OperationStatus status = queueService.getOperationStatus(operationId);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "operationId", operationId,
                    "status", status
            ));
        } catch (Exception e) {
            log.error("Error al consultar el estado de la operación", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al consultar el estado: " + e.getMessage()
            ));
        }
    }

    @DeleteMapping("/operations/{operationId}")
    public ResponseEntity<Map<String, Object>> cancelOperation(@PathVariable Long operationId) {
        log.info("Solicitud de cancelación de operación ID: {}", operationId);

        try {
            boolean cancelled = queueService.cancelOperation(operationId);
            if (cancelled) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "message", "Operación cancelada exitosamente"
                ));
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                        "success", false,
                        "message", "No se pudo cancelar la operación, puede estar en progreso o ya completada"
                ));
            }
        } catch (Exception e) {
            log.error("Error al cancelar la operación", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al cancelar la operación: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/user/{userId}/operations")
    public ResponseEntity<Map<String, Object>> getUserOperations(
            @PathVariable Long userId,
            @RequestParam(required = false) List<String> statuses) {

        log.info("Consultando operaciones del usuario ID: {}", userId);

        try {
            List<OperationStatus> statusList = Collections.emptyList();
            if (statuses != null && !statuses.isEmpty()) {
                statusList = statuses.stream()
                        .map(OperationStatus::valueOf)
                        .toList();
            }

            List<QueueItem> operations = queueService.getUserOperations(userId, statusList);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "userId", userId,
                    "operations", operations
            ));
        } catch (Exception e) {
            log.error("Error al consultar operaciones del usuario", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al consultar operaciones: " + e.getMessage()
            ));
        }
    }
}