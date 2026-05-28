package com.sminoh.orderservice.outbox;

import com.sminoh.orderservice.event.internal.OrderCreatedAppEvent;
import com.sminoh.orderservice.event.published.OrderCreatedEvent;
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

    private static final String TOPIC = "order.created";

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void record(OrderCreatedAppEvent appEvent){
        OrderCreatedEvent payload = appEvent.getPayload();

        OutboxEvent outboxEvent = OutboxEvent.create(
                payload.getEventId(),
                "Order",
                payload.getOrderId(),
                "OrderCreated",
                TOPIC,
                toJson(payload)
        );
        outboxEventRepository.save(outboxEvent);

        log.info("action=OUTBOX_RECORDED type=OrderCreated eventId={} orderId={}",
                payload.getEventId(), payload.getOrderId());
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("이벤트 직렬화 실패", e);
        }
    }
}
