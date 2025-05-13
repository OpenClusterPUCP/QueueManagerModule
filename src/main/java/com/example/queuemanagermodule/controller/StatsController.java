package com.example.queuemanagermodule.controller;

import com.example.queuemanagermodule.model.QueueStats;
import com.example.queuemanagermodule.service.QueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/stats")
@RequiredArgsConstructor
@Slf4j
public class StatsController {

    private final QueueService queueService;

    @GetMapping("/queues")
    public ResponseEntity<Map<String, Object>> getAllQueueStats() {
        log.info("Consultando estadísticas de todas las colas");

        try {
            List<QueueStats> stats = queueService.getAllQueueStats();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "queues", stats
            ));
        } catch (Exception e) {
            log.error("Error al consultar estadísticas de colas", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al consultar estadísticas: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/queues/{queueName}")
    public ResponseEntity<Map<String, Object>> getQueueStats(@PathVariable String queueName) {
        log.info("Consultando estadísticas de la cola: {}", queueName);

        try {
            QueueStats stats = queueService.getQueueStats(queueName);
            if (stats != null) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "stats", stats
                ));
            } else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                        "success", false,
                        "message", "No se encontró la cola especificada"
                ));
            }
        } catch (Exception e) {
            log.error("Error al consultar estadísticas de la cola", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "success", false,
                    "message", "Error al consultar estadísticas: " + e.getMessage()
            ));
        }
    }
}
