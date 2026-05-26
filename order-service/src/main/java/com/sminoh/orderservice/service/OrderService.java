package com.sminoh.orderservice.service;

import com.sminoh.common.event.OrderCreatedEvent;
import com.sminoh.orderservice.domain.Order;
import com.sminoh.orderservice.domain.OrderItem;
import com.sminoh.orderservice.dto.OrderRequest;
import com.sminoh.orderservice.dto.OrderResponse;
import com.sminoh.orderservice.event.OrderEventPublisher;
import com.sminoh.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;

    public OrderResponse placeOrder(OrderRequest request){
        // 1. 총 금액 계산
        BigDecimal totalAmount = request.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // 2. Order 생성
        Order order = Order.create(request.getUserId(), totalAmount);

        request.getItems().forEach(orderItemRequest -> {
            OrderItem item = OrderItem.create(
                   order,
                   orderItemRequest.getProductId(),
                    orderItemRequest.getQuantity(),
                    orderItemRequest.getPrice()
            );
            order.getItems().add(item);
        });

        orderRepository.save(order);
        log.info("주문 저장 완료 | orderId={}", order.getId());

        // 3. Kafka 이벤트 발행
        List<OrderCreatedEvent.OrderItem> eventItems = request.getItems().stream()
                .map(i -> new OrderCreatedEvent.OrderItem(
                        i.getProductId(), i.getQuantity(), i.getPrice()
                ))
                .toList();

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(),
                order.getUserId(),
                order.getTotalAmount(),
                eventItems,
                LocalDateTime.now()
        );

        orderEventPublisher.publish(event);

        return OrderResponse.from(order);
    }
}
