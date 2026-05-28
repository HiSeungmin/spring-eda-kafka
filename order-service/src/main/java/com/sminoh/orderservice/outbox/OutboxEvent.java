package com.sminoh.orderservice.outbox;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "outbox")
@Getter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String topic;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "sent_at")
    private LocalDateTime sentAt;

    private static final int MAX_RETRY = 5;

    public static OutboxEvent create(String eventId, String aggregateType, String aggregateId,
                                     String type, String topic, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.eventId = eventId;
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.type = type;
        event.topic = topic;
        event.payload = payload;
        event.status = OutboxStatus.INIT;
        event.retryCount = 0;
        event.createdAt = LocalDateTime.now();
        return event;
    }

    public void markAsSuccess() {
        this.status = OutboxStatus.SEND_SUCCESS;
        this.sentAt = LocalDateTime.now();
    }

    public void markAsFail(String error) {
        this.retryCount++;
        this.lastError = error;
        // 최대 재시도 초과 시 DEAD 처리
        this.status = (this.retryCount >= MAX_RETRY)
                ? OutboxStatus.DEAD
                : OutboxStatus.SEND_FAIL;
    }
}
