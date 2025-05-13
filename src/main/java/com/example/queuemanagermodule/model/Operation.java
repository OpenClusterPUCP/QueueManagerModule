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
public class Operation {
    private Long id;
    private OperationType type;
    private ClusterType clusterType;
    private Integer zoneId;
    private Long userId;
    private Map<String, Object> payload;
    private Priority priority;
    private LocalDateTime submittedAt;
    private Integer maxRetries;
}
