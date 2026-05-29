package com.sminoh.orderservice.event.consumed;

import com.sminoh.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final OrderService orderService;

    @KafkaListener(
            topics = "payment.completed",
            groupId = "order-service",
            containerFactory = "paymentCompletedListenerContainerFactory"
    )
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("event=CONSUME topic=payment.completed orderId={}", event.getOrderId());
        orderService.completeOrder(event.getOrderId());
    }

    @KafkaListener(
            topics = "payment.failed",
            groupId = "order-service",
            containerFactory = "paymentCompletedListenerContainerFactory"
    )
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("event=CONSUME topic=payment.failed orderId={} reason={}", event.getOrderId(), event.getReason());
        orderService.failOrder(event.getOrderId(), event.getReason());
    }
}
