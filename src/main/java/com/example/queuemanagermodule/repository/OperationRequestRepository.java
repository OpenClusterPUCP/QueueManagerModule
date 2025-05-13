package com.example.queuemanagermodule.repository;

import com.example.queuemanagermodule.model.OperationRequest;
import com.example.queuemanagermodule.model.OperationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OperationRequestRepository extends JpaRepository<OperationRequest, Long> {

    List<OperationRequest> findByQueueNameAndStatus(String queueName, OperationStatus status);

    List<OperationRequest> findByStatusAndStartedAtBefore(OperationStatus status, LocalDateTime time);

    List<OperationRequest> findByUserIdAndStatusIn(Long userId, List<OperationStatus> statuses);

    @Query("SELECT COUNT(o) FROM OperationRequest o WHERE o.queueName = ?1 AND o.status = ?2")
    Long countByQueueNameAndStatus(String queueName, OperationStatus status);

    @Query("SELECT AVG(TIMESTAMPDIFF(SECOND, o.submittedAt, o.startedAt)) FROM OperationRequest o WHERE o.queueName = ?1 AND o.status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')")
    Double calculateAverageWaitTime(String queueName);

    @Query("SELECT AVG(TIMESTAMPDIFF(SECOND, o.startedAt, o.completedAt)) FROM OperationRequest o WHERE o.queueName = ?1 AND o.status IN ('COMPLETED', 'FAILED') AND o.startedAt IS NOT NULL AND o.completedAt IS NOT NULL")
    Double calculateAverageProcessingTime(String queueName);
}