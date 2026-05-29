package com.sminoh.paymentservice.event.published;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@NoArgsConstructor
public class PaymentCompletedEvent {
    private String eventId;
    private String paymentId;
    private String orderId;
    private String userId;
    private BigDecimal amount;
    private LocalDateTime paidAt;

    public PaymentCompletedEvent(String paymentId, String orderId, String userId,
                                 BigDecimal amount, LocalDateTime paidAt) {
        this.eventId = UUID.randomUUID().toString();
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.paidAt = paidAt;
    }
}
