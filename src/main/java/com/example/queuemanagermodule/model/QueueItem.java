package com.example.queuemanagermodule.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueItem {
    private Long id;
    private String queueName;
    private OperationType operationType;
    private ClusterType clusterType;
    private Integer zoneId;
    private Long userId;
    private Map<String, Object> payload;
    private Priority priority;
    private LocalDateTime enqueuedAt;
    private LocalDateTime processedAt;
    private OperationStatus status;
    private String errorMessage;
    private Integer retryCount;
    private Integer maxRetries;
}