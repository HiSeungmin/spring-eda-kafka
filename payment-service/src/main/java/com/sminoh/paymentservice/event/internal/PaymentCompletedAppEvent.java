package com.sminoh.paymentservice.event.internal;

import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class PaymentCompletedAppEvent {
    private final PaymentCompletedEvent payload;
}
