package com.sminoh.paymentservice.service;

import com.sminoh.paymentservice.event.consumed.OrderCreatedEvent;
import com.sminoh.paymentservice.domain.Payment;
import com.sminoh.paymentservice.infra.PgClient;
import com.sminoh.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final PaymentProcessor paymentProcessor;
    private final PgClient pgClient;


    public void processPayment(OrderCreatedEvent event){
        if(paymentRepository.existsByOrderId(event.getOrderId())){
            log.warn("event=DUPLICATE orderId={} skip", event.getOrderId());
            return;
        }

        // 1. 결제 PENDING 상태 저장
        Payment payment = paymentProcessor.createPendingPayment(event);

        // 2. 외부 PG사 API호출
        boolean success = pgClient.pay(event.getOrderId(), event.getTotalAmount());

        // 3. 결과에 따른 상태 업데이트 후 이벤트 발행
        if(success){
            paymentProcessor.completePayment(payment);
        }else{
            paymentProcessor.failPayment(payment);
        }
    }
}
