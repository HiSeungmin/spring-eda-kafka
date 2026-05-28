package com.sminoh.orderservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    Optional<OutboxEvent> findByEventId(String eventId);
}
