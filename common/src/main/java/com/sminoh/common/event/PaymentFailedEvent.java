package com.sminoh.common.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentFailedEvent {
    private String orderId;
    private String userId;
    private String reason;
}
