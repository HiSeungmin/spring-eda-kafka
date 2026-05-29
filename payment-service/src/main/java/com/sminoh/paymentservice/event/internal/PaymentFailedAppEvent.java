package com.sminoh.paymentservice.event.internal;

import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class PaymentFailedAppEvent {
    private final PaymentFailedEvent payload;
}
