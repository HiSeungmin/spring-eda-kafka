package com.sminoh.paymentservice;


import com.sminoh.paymentservice.domain.Payment;
import com.sminoh.paymentservice.event.consumed.OrderCreatedEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentEventPublisher;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import com.sminoh.paymentservice.repository.PaymentRepository;
import com.sminoh.paymentservice.service.PaymentService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentEventPublisher paymentEventPublisher;

    @InjectMocks
    private PaymentService paymentService;

    @Test
    @DisplayName("결제 성공 시 Payment가 COMPLETED 상태로 저장되고 PaymentCompletedEvent가 발행된다")
    void processPayment_success(){
        // given
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", "user-1", new BigDecimal("15000"), LocalDateTime.now());

        when(paymentRepository.existsByOrderId("order-1")).thenReturn(false);
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // when
        paymentService.processPayment(event);

        // then
        verify(paymentRepository, times(2)).save(any(Payment.class));
        verify(paymentEventPublisher, times(1)).publishCompleted(any(PaymentCompletedEvent.class));
        verify(paymentEventPublisher, never()).publishFailed(any(PaymentFailedEvent.class));
    }

    @Test
    @DisplayName("처리된 주문 데이터 멱등성 검증")
    void processPayment_duplicate(){
        // given
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", "user-1", new BigDecimal("15000"), LocalDateTime.now());

        when(paymentRepository.existsByOrderId("order-1")).thenReturn(true);

        // when
        paymentService.processPayment(event);

        // then
        verify(paymentRepository, never()).save(any(Payment.class));
        verify(paymentEventPublisher, never()).publishCompleted(any());
        verify(paymentEventPublisher, never()).publishFailed(any());
    }
}
