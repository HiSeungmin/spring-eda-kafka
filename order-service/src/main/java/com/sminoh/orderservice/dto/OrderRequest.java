package com.sminoh.orderservice.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Getter
public class OrderRequest {
    private String userId;
    private List<OrderItemRequest> items;

    public OrderRequest() {}

    public OrderRequest(String userId, List<OrderItemRequest> items) {
        this.userId = userId;
        this.items = items;
    }

    @Getter
    public static class OrderItemRequest {
        private String productId;
        private int quantity;
        private BigDecimal price;

        public OrderItemRequest() {}

        public OrderItemRequest(String productId, int quantity, BigDecimal price) {
            this.productId = productId;
            this.quantity = quantity;
            this.price = price;
        }
    }
}