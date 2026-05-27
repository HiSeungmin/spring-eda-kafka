package com.sminoh.paymentservice.event.consumed;

import com.sminoh.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedConsumer {
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "order.created",
            groupId = "payment-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(OrderCreatedEvent event) {
        log.info("event=CONSUME topic=order.created orderId={}", event.getOrderId());
        paymentService.processPayment(event);
    }
}
