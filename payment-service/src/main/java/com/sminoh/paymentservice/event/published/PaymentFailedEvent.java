package com.sminoh.paymentservice.event.published;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Getter
@NoArgsConstructor
public class PaymentFailedEvent {
    private String eventId;
    private String orderId;
    private String userId;
    private String reason;

    public PaymentFailedEvent(String orderId, String userId, String reason) {
        this.eventId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.userId = userId;
        this.reason = reason;
    }
}
