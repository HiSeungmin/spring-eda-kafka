package com.sminoh.orderservice.event.published;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {
    private static final String TOPIC = "order.created";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(OrderCreatedEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
        log.info("event=PUBLISH topic={} orderId={}", TOPIC, event.getOrderId());
    }
}
