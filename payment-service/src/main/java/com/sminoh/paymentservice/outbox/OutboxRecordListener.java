package com.sminoh.paymentservice.outbox;


import com.sminoh.paymentservice.event.internal.PaymentCompletedAppEvent;
import com.sminoh.paymentservice.event.internal.PaymentFailedAppEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.databind.ObjectMapper;


@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRecordListener {
    private static final String AGGREGATE_TYPE = "Payment";
    private static final String TOPIC_COMPLETED = "payment.completed";
    private static final String TOPIC_FAILED = "payment.failed";

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void recordCompleted(PaymentCompletedAppEvent appEvent) {
        PaymentCompletedEvent payload = appEvent.getPayload();
        saveOutbox(payload.getEventId(), payload.getOrderId(),
                "PaymentCompleted", TOPIC_COMPLETED, payload);
        log.info("action=OUTBOX_RECORDED type=PaymentCompleted orderId={} eventId={}",
                payload.getOrderId(), payload.getEventId());
    }

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void recordFailed(PaymentFailedAppEvent appEvent) {
        PaymentFailedEvent payload = appEvent.getPayload();
        saveOutbox(payload.getEventId(), payload.getOrderId(),
                "PaymentFailed", TOPIC_FAILED, payload);
        log.info("action=OUTBOX_RECORDED type=PaymentFailed orderId={} eventId={}",
                payload.getOrderId(), payload.getEventId());
    }

    private void saveOutbox(String eventId, String aggregateId, String type, String topic, Object payload) {
        OutboxEvent outboxEvent = OutboxEvent.create(
                eventId, AGGREGATE_TYPE, aggregateId, type, topic, toJson(payload));
        outboxEventRepository.save(outboxEvent);
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("이벤트 직렬화 실패", e);
        }
    }

}
