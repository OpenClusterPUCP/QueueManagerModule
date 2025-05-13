package com.example.queuemanagermodule.repository;

import com.example.queuemanagermodule.model.QueueMetrics;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface QueueMetricsRepository extends JpaRepository<QueueMetrics, Long> {

    List<QueueMetrics> findByQueueNameOrderByRecordDateDesc(String queueName);

    QueueMetrics findTopByQueueNameOrderByRecordDateDesc(String queueName);

    @Query("SELECT q FROM QueueMetrics q WHERE q.queueName = ?1 AND q.recordDate BETWEEN ?2 AND ?3 ORDER BY q.recordDate")
    List<QueueMetrics> findByQueueNameAndTimeRange(String queueName, LocalDateTime startTime, LocalDateTime endTime);
}
