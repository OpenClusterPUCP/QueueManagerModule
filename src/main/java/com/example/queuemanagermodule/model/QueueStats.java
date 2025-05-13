package com.example.queuemanagermodule.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueStats {
    private String queueName;
    private Long pendingOperations;
    private Long inProgressOperations;
    private Long completedOperations;
    private Long failedOperations;
    private Double averageWaitTimeSeconds;
    private Double averageProcessingTimeSeconds;
    private LocalDateTime lastUpdated;
}
