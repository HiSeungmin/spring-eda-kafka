package com.sminoh.orderservice;

import com.sminoh.common.event.OrderCreatedEvent;
import com.sminoh.orderservice.domain.Order;
import com.sminoh.orderservice.domain.OrderStatus;
import com.sminoh.orderservice.dto.OrderRequest;
import com.sminoh.orderservice.dto.OrderResponse;
import com.sminoh.orderservice.event.OrderEventPublisher;
import com.sminoh.orderservice.repository.OrderRepository;
import com.sminoh.orderservice.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {
    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventPublisher orderEventPublisher;

    @InjectMocks
    private OrderService orderService;

    private OrderRequest createOrderRequest(){
        OrderRequest.OrderItemRequest item = new OrderRequest.OrderItemRequest("product-001",2, BigDecimal.valueOf(15000));
        return new OrderRequest("user-123", List.of(item));
    }

    @Test
    @DisplayName("주문 생성 시 총 금액이 올바르게 계산된다")
    void placeOrder_totalAmountCalculated(){
        // given
        OrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        // when
        OrderResponse response = orderService.placeOrder(request);

        // then
        assertThat(response.getTotalAmount()).isEqualByComparingTo(BigDecimal.valueOf(30000));
    }


    @Test
    @DisplayName("주문 생성 시 상태가 PENDING이다")
    void placeOrder_statusIsPending() {
        // given
        OrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        // when
        OrderResponse response = orderService.placeOrder(request);

        // then
        assertThat(response.getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    @DisplayName("주문 생성 시 Kafka 이벤트가 발행된다")
    void placeOrder_kafkaEventPublished(){
        // given
        OrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(i->i.getArgument(0));

        // when
        orderService.placeOrder(request);

        // then
        verify(orderEventPublisher, times(1)).publish(any(OrderCreatedEvent.class));
    }

    @Test
    @DisplayName("주문 생성 시 발행된 이벤트에 올바른 정보가 담긴다")
    void placeOrder_eventContainsCorrectData(){
        // given
        OrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(i->i.getArgument(0));

        ArgumentCaptor<OrderCreatedEvent> captor = ArgumentCaptor.forClass(OrderCreatedEvent.class);

        // when
        orderService.placeOrder(request);

        // then
        verify(orderEventPublisher).publish(captor.capture());
        OrderCreatedEvent event = captor.getValue();

        assertThat(event.getUserId()).isEqualTo("user-123");
        assertThat(event.getTotalAmount()).isEqualByComparingTo(BigDecimal.valueOf(30000));
        assertThat(event.getItems().size()).isEqualTo(1);
        assertThat(event.getItems().get(0).getProductId()).isEqualTo("product-001");
    }

    @Test
    @DisplayName("주문 시 DB에 저장된다")
    void placeOrder_savedToRepository() {
        // given
        OrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        // when
        orderService.placeOrder(request);

        //then
        verify(orderRepository, times(1)).save(any(Order.class));
    }
}