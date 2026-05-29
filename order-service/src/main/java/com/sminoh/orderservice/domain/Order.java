package com.sminoh.orderservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name="orders")
@Getter
@NoArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    private String userId;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private BigDecimal totalAmount;

    private LocalDateTime createdAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    public static Order create(String userId, BigDecimal totalAmount){
        Order order = new Order();
        order.userId = userId;
        order.totalAmount = totalAmount;
        order.status = OrderStatus.PENDING;
        order.createdAt = LocalDateTime.now();
        return order;
    }

    public void confirm(){
        if (this.status == OrderStatus.CONFIRMED) {
            return;
        }
        if (this.status != OrderStatus.PENDING) {
            throw new IllegalStateException("PENDING 상태만 CONFIRMED 가능. 현재: " + this.status);
        }
        this.status = OrderStatus.CONFIRMED;
    }

    public void cancel(){
        if (this.status == OrderStatus.CANCELLED) {
            return;
        }
        this.status = OrderStatus.CANCELLED;
    }
}
