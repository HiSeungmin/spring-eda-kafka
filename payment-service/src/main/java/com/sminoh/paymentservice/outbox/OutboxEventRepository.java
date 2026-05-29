package com.sminoh.paymentservice.outbox;

import com.sminoh.paymentservice.outbox.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    Optional<OutboxEvent> findByEventId(String eventId);

    @Query("SELECT o FROM OutboxEvent o " +
            "WHERE o.status IN (com.sminoh.paymentservice.outbox.OutboxStatus.INIT, " +
            "                   com.sminoh.paymentservice.outbox.OutboxStatus.SEND_FAIL) " +
            "AND o.createdAt < :threshold " +
            "ORDER BY o.createdAt")
    List<OutboxEvent> findRetryableBefore(@Param("threshold") LocalDateTime threshold);
}
