package com.sminoh.orderservice.event.consumed;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class PaymentFailedEvent {
    private String orderId;
    private String userId;
    private String reason;
}
