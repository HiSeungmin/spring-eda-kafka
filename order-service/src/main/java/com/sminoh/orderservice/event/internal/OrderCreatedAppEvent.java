package com.sminoh.orderservice.event.internal;

import com.sminoh.orderservice.event.published.OrderCreatedEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class OrderCreatedAppEvent {
    private final OrderCreatedEvent payload;
}
