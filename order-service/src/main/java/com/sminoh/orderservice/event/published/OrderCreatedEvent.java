package com.sminoh.orderservice.event.published;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Getter
@NoArgsConstructor
public class OrderCreatedEvent {
    private String eventId;
    private String orderId;
    private String userId;
    private BigDecimal totalAmount;
    private List<OrderItem> items;
    private LocalDateTime createdAt;

    public OrderCreatedEvent(String orderId, String userId, BigDecimal totalAmount, List<OrderItem> items) {
        this.eventId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.userId = userId;
        this.totalAmount = totalAmount;
        this.items = items;
        this.createdAt = LocalDateTime.now();
    }

    @Getter
    @NoArgsConstructor
    public static class OrderItem {
        private String productId;
        private int quantity;
        private BigDecimal price;

        public OrderItem(String productId, int quantity, BigDecimal price) {
            this.productId = productId;
            this.quantity = quantity;
            this.price = price;
        }
    }
}
