package com.example.queuemanagermodule.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import java.time.LocalDateTime;

@Entity
@Table(name = "operation_requests")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OperationType operationType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ClusterType clusterType;

    @Column(nullable = false)
    private Integer zoneId;

    @Column(nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Priority priority;

    @Column(nullable = false)
    private LocalDateTime submittedAt;

    @Column
    private LocalDateTime startedAt;

    @Column
    private LocalDateTime completedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OperationStatus status;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Column
    private Integer retryCount;

    @Column
    private Integer maxRetries;

    @Column(nullable = false)
    private String queueName;

    // Almacenamos el payload como JSON
    @Column(columnDefinition = "JSON")
    private String payloadJson;

    // Almacenamos también el resultado como JSON
    @Column(columnDefinition = "JSON")
    private String resultJson;
}