package com.example.queuemanagermodule.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "queue_metrics")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueMetrics {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String queueName;

    @Column(nullable = false)
    private Long pendingCount;

    @Column(nullable = false)
    private Long inProgressCount;

    @Column(nullable = false)
    private Long completedCount;

    @Column(nullable = false)
    private Long failedCount;

    @Column
    private Double averageWaitTimeSeconds;

    @Column
    private Double averageProcessingTimeSeconds;

    @Column
    private Long maxWaitTimeSeconds;

    @Column
    private Long maxProcessingTimeSeconds;

    @Column(nullable = false)
    private LocalDateTime lastUpdated;

    @Column(nullable = false)
    private LocalDateTime recordDate;
}
