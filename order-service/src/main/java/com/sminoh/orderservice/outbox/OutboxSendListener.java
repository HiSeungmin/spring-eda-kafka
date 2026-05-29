package com.sminoh.orderservice.outbox;

import com.sminoh.orderservice.event.internal.OrderCreatedAppEvent;
import com.sminoh.orderservice.event.published.OrderCreatedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxSendListener {

    private static final String TOPIC = "order.created";
    private final KafkaTemplate<String, Object> stringKafkaTemplate;
    private final OutboxEventRepository outboxEventRepository;

    @Async("outboxTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void send(OrderCreatedAppEvent appEvent){
        OrderCreatedEvent payload = appEvent.getPayload();

        try {
            stringKafkaTemplate.send(TOPIC, payload.getOrderId(), payload);

            log.info("event=PUBLISH topic={} eventId={} orderId={}",
                    TOPIC, payload.getEventId(), payload.getOrderId());

            outboxEventRepository.findByEventId(payload.getEventId())
                    .ifPresent(OutboxEvent::markAsSuccess);
        } catch(Exception e){
            log.error("event=PUBLISH_FAIL topic={} eventId={} reason={}",
                    TOPIC, payload.getEventId(), e.getMessage());
        }
    }

}
