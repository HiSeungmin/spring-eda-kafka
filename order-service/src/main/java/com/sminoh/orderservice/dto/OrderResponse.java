package com.sminoh.orderservice.dto;

import com.sminoh.orderservice.domain.Order;
import com.sminoh.orderservice.domain.OrderStatus;

import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
public class OrderResponse {
    private String orderId;
    private String userId;
    private OrderStatus status;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;

    public static OrderResponse from(Order order) {
        OrderResponse response = new OrderResponse();
        response.orderId = order.getId();
        response.userId = order.getUserId();
        response.status = order.getStatus();
        response.totalAmount = order.getTotalAmount();
        response.createdAt = order.getCreatedAt();
        return response;
    }

}
