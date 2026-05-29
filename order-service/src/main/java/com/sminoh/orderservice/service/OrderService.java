package com.sminoh.orderservice.service;

import com.sminoh.orderservice.event.internal.OrderCreatedAppEvent;
import com.sminoh.orderservice.event.published.OrderCreatedEvent;
import com.sminoh.orderservice.domain.Order;
import com.sminoh.orderservice.domain.OrderItem;
import com.sminoh.orderservice.dto.OrderRequest;
import com.sminoh.orderservice.dto.OrderResponse;
import com.sminoh.orderservice.repository.OrderRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public OrderResponse placeOrder(OrderRequest request) {
        // 1. 총 금액 계산
        BigDecimal totalAmount = request.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // 2. Order 생성
        Order order = Order.create(request.getUserId(), totalAmount);
        request.getItems().forEach(req -> {
            OrderItem item = OrderItem.create(order, req.getProductId(), req.getQuantity(), req.getPrice());
            order.getItems().add(item);
        });

        orderRepository.save(order);
        log.info("action=ORDER_CREATED orderId={} userId={} totalAmount={}",
                order.getId(), order.getUserId(), order.getTotalAmount());

        // 3. 도메인 이벤트 발행 (Spring 내부 이벤트)
        OrderCreatedEvent payload = new OrderCreatedEvent(
                order.getId(),
                order.getUserId(),
                order.getTotalAmount(),
                request.getItems().stream()
                        .map(i -> new OrderCreatedEvent.OrderItem(i.getProductId(), i.getQuantity(), i.getPrice()))
                        .toList()
        );
        eventPublisher.publishEvent(new OrderCreatedAppEvent(payload));

        return OrderResponse.from(order);
    }

    @Transactional
    public void completeOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("주문을 찾을 수 없습니다. orderId=" + orderId));
        order.confirm();
        log.info("action=ORDER_COMPLETED orderId={}", orderId);
    }

    @Transactional
    public void failOrder(String orderId, String reason) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("주문을 찾을 수 없습니다. orderId=" + orderId));


        order.cancel();
        log.info("action=ORDER_FAILED orderId={} reason={}", orderId, reason);
    }
}
