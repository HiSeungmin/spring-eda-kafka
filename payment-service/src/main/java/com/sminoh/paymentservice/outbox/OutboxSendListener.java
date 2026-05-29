package com.sminoh.paymentservice.outbox;

import com.sminoh.paymentservice.event.internal.PaymentCompletedAppEvent;
import com.sminoh.paymentservice.event.internal.PaymentFailedAppEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxSendListener {

    private static final String TOPIC_COMPLETED = "payment.completed";
    private static final String TOPIC_FAILED = "payment.failed";

    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Async("outboxTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void sendCompleted(PaymentCompletedAppEvent appEvent) {
        PaymentCompletedEvent payload = appEvent.getPayload();
        publish(TOPIC_COMPLETED, payload.getOrderId(), payload.getEventId(), payload);
    }

    @Async("outboxTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void sendFailed(PaymentFailedAppEvent appEvent) {
        PaymentFailedEvent payload = appEvent.getPayload();
        publish(TOPIC_FAILED, payload.getOrderId(), payload.getEventId(), payload);
    }

    private void publish(String topic, String key, String eventId, Object payload) {
        try {
            String json = toJson(payload);
            stringKafkaTemplate.send(topic, key, json);
            log.info("event=PUBLISH topic={} eventId={} orderId={}", topic, eventId, key);

            outboxEventRepository.findByEventId(eventId)
                    .ifPresent(OutboxEvent::markAsSuccess);

        } catch (Exception e) {
            log.error("event=PUBLISH_FAIL topic={} eventId={} reason={}",
                    topic, eventId, e.getMessage());
            // 상태 변경 없음 → 배치가 재발행
        }
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("이벤트 직렬화 실패", e);
        }
    }


}
