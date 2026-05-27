package com.sminoh.paymentservice.event.published;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventPublisher {
    private static final String PAYMENT_COMPLETED = "payment.completed";
    private static final String PAYMENT_FAILED = "payment.failed";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishCompleted(PaymentCompletedEvent event) {
        kafkaTemplate.send(PAYMENT_COMPLETED, event.getOrderId(), event);
        log.info("[Kafka] publish → {} | orderId={}", PAYMENT_COMPLETED, event.getOrderId());
    }

    public void publishFailed(PaymentFailedEvent event) {
        kafkaTemplate.send(PAYMENT_FAILED, event.getOrderId(), event);
        log.info("[Kafka] publish → {} | orderId={}", PAYMENT_FAILED, event.getOrderId());
    }
}
