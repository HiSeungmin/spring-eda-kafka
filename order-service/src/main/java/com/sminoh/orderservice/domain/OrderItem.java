package com.sminoh.orderservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "order_items")
@Getter
@NoArgsConstructor
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private Order order;

    private String productId;
    private int quantity;
    private BigDecimal price;

    public static OrderItem create(Order order, String productId, int quantity, BigDecimal price) {
        OrderItem item = new OrderItem();
        item.order = order;
        item.productId = productId;
        item.quantity = quantity;
        item.price = price;
        return item;
    }
}
